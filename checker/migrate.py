"""Offline object-store path migration for Iceberg v2 positional deletes.

The operational scenario is a change of object-store layout: the lakehouse
on-call needs to keep verifying revoked tickets against the new paths, but
the data files themselves are not copied here.  This module therefore only
reads the positional-delete Parquet replicas, rewrites their ``file_path``
values and the manifest's data-file paths, and emits a ZIP that can be fed
straight back into the original checker together with the data replicas.

Rules implemented here:

* The manifest is the existing Iceberg v2 CSV manifest.  Its columns, column
  order, values other than paths, and row order are preserved.
* The mapping CSV has ``old_path`` / ``new_path`` columns.  Every old path
  must be unique and must name a *data* file in the manifest.
* Every replacement is done against the pre-migration value exactly once.
  Old and new paths may cross; a value written by one mapping is never fed
  into another mapping.
* The migrated manifest may not contain duplicate paths.  A delete record
  that did not point at a manifest data file before the migration but would
  point at a migrated data file afterwards is a conflict as well.
* Equality deletes, missing delete replicas, and read/write failures abort
  the whole migration with a reason.  No ZIP is produced in that case.
* Each positional-delete replica keeps its filename, column structure and
  Parquet field IDs, is sorted by ``file_path`` then ``pos``, and only its
  ``file_path`` values change.  All other values stay untouched.
"""

from __future__ import annotations

import csv
import io
import os
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .engine import DELETE_PATH_COLUMN, DELETE_POS_COLUMN
from .errors import CheckError, PathConflict
from .manifest import (
    _ALIASES,
    FileKind,
    ManifestEntry,
    _clean_text,
    _normalise_key,
    parse_manifest,
)

_MAPPING_ALIASES = {
    "old_path": {
        "old_path",
        "old",
        "from_path",
        "origin_path",
    },
    "new_path": {
        "new_path",
        "new",
        "to_path",
        "target_path",
        "destination_path",
    },
}


@dataclass(frozen=True)
class PathMappingRow:
    old_path: str
    new_path: str
    row_label: str


@dataclass
class RawManifest:
    headers: list[str]
    rows: list[tuple[int, list[str]]]
    path_index: int
    entries: list[ManifestEntry]


@dataclass
class MigratedDeleteFile:
    name: str
    data: bytes
    row_count: int


@dataclass
class MigrationPackage:
    manifest_name: str
    manifest_bytes: bytes
    delete_files: list[MigratedDeleteFile]
    mapping: dict[str, str]
    data_paths_before: list[str] = field(default_factory=list)
    data_paths_after: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def delete_names(self) -> list[str]:
        return [item.name for item in self.delete_files]

    @property
    def total_delete_rows(self) -> int:
        return sum(item.row_count for item in self.delete_files)

    def to_zip(self) -> bytes:
        buffer = io.BytesIO()
        try:
            with zipfile.ZipFile(
                buffer, "w", compression=zipfile.ZIP_DEFLATED
            ) as archive:
                archive.writestr(self.manifest_name, self.manifest_bytes)
                for replica in self.delete_files:
                    archive.writestr(replica.name, replica.data)
        except Exception as exc:
            raise CheckError(f"无法写出迁移结果 ZIP：{exc}") from None
        return buffer.getvalue()


def _decode(data: bytes | str, label: str) -> str:
    if isinstance(data, str):
        return data
    try:
        return data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CheckError(f"{label}不是有效的 UTF-8 文本：{exc}") from None


def _looks_like_json(text: str) -> bool:
    return text.lstrip("﻿ \t\r\n").startswith(("[", "{"))


def _csv_rows(text: str, label: str) -> list[list[str]]:
    try:
        return list(csv.reader(io.StringIO(text)))
    except csv.Error as exc:
        raise CheckError(f"{label}解析失败：{exc}") from None


def _column_indexes(
    headers: list[str], label: str, aliases: Mapping[str, set[str]]
) -> dict[str, int]:
    normalised = [_normalise_key(header) for header in headers]
    if len(set(normalised)) != len(normalised):
        raise CheckError(f"{label}表头在规范化后出现重复列名。")

    indexes: dict[str, int] = {}
    for name, names in aliases.items():
        hits = [i for i, header in enumerate(normalised) if header in names]
        if not hits:
            wanted = " / ".join(sorted(names))
            raise CheckError(
                f"{label}缺少必需列 “{name}”。可用列名：{wanted}。"
            )
        if len(hits) > 1:
            raise CheckError(
                f"{label}包含多个表示 “{name}” 的列，无法确定使用哪一列。"
            )
        indexes[name] = hits[0]
    return indexes


def _is_blank_row(row: list[str]) -> bool:
    return not any(_clean_text(cell) for cell in row)


def parse_path_mapping(data: bytes | str) -> list[PathMappingRow]:
    """Parse and validate the ``old_path`` / ``new_path`` mapping CSV."""

    text = _decode(data, "路径对照表")
    if not text.strip():
        raise CheckError("路径对照表为空。")
    if _looks_like_json(text):
        raise CheckError(
            "路径对照表必须是 CSV，包含 old_path 和 new_path 两列。"
        )

    rows = _csv_rows(text, "路径对照表")
    if not rows or not any(_clean_text(cell) for cell in rows[0]):
        raise CheckError("路径对照表为空或缺少表头。")

    headers = rows[0]
    indexes = _column_indexes(headers, "路径对照表", _MAPPING_ALIASES)
    old_index = indexes["old_path"]
    new_index = indexes["new_path"]

    parsed: list[PathMappingRow] = []
    seen: dict[str, str] = {}
    duplicates: dict[str, list[str]] = {}
    for line_number, row in enumerate(rows[1:], start=2):
        if _is_blank_row(row):
            continue
        if len(row) != len(headers):
            raise CheckError(
                f"路径对照表第 {line_number} 行有 {len(row)} 列，"
                f"表头有 {len(headers)} 列。"
            )
        old_path = _clean_text(row[old_index])
        new_path = _clean_text(row[new_index])
        label = f"路径对照表第 {line_number} 行"
        if not old_path:
            raise CheckError(f"{label}的 old_path 为空。")
        if not new_path:
            raise CheckError(f"{label}的 new_path 为空。")
        if old_path in seen:
            duplicates.setdefault(old_path, [seen[old_path]]).append(label)
            continue
        seen[old_path] = label
        parsed.append(
            PathMappingRow(
                old_path=old_path, new_path=new_path, row_label=label
            )
        )

    if duplicates:
        conflicts = [
            PathConflict(
                path=path,
                detail="旧路径在路径对照表中重复出现：" + "、".join(labels),
            )
            for path, labels in sorted(duplicates.items())
        ]
        raise CheckError(
            "迁移未完成：路径对照表中的旧路径重复，未生成 ZIP。",
            conflicts=conflicts,
        )

    if not parsed:
        raise CheckError("路径对照表中没有条目，未生成 ZIP。")
    return parsed


def _parse_raw_manifest(data: bytes | str) -> RawManifest:
    text = _decode(data, "清单")
    if not text.strip():
        raise CheckError("清单为空。")
    if _looks_like_json(text):
        raise CheckError(
            "离线迁移入口只接受现有格式的 Iceberg v2 CSV 清单，"
            "请上传 CSV 文件。"
        )

    rows = _csv_rows(text, "清单")
    if not rows or not any(_clean_text(cell) for cell in rows[0]):
        raise CheckError("清单为空或缺少表头。")

    headers = rows[0]
    wanted_aliases = {
        name: _ALIASES[name]
        for name in ("original_path", "uploaded_filename", "file_type")
    }
    indexes = _column_indexes(headers, "清单", wanted_aliases)

    data_rows: list[tuple[int, list[str]]] = []
    for line_number, row in enumerate(rows[1:], start=2):
        if _is_blank_row(row):
            continue
        if len(row) != len(headers):
            raise CheckError(
                f"清单第 {line_number} 行有 {len(row)} 列，"
                f"表头有 {len(headers)} 列。"
            )
        data_rows.append((line_number, row))

    if not data_rows:
        raise CheckError("清单中没有文件条目。")

    # Reuse the checker's parser for value validation, alias handling,
    # duplicate detection and file-type recognition.
    manifest = parse_manifest(text)
    if len(manifest.entries) != len(data_rows):
        raise CheckError("清单条目数与 CSV 行数不一致，无法安全迁移。")

    return RawManifest(
        headers=headers,
        rows=data_rows,
        path_index=indexes["original_path"],
        entries=manifest.entries,
    )


def _validate_mapping_against_manifest(
    mapping_rows: list[PathMappingRow],
    entries: list[ManifestEntry],
) -> tuple[dict[str, str], list[PathConflict]]:
    data_entries = [entry for entry in entries if entry.is_data]
    if not data_entries:
        raise CheckError(
            "清单中没有 data 类型的数据文件，无法按数据路径迁移。"
        )
    data_paths = {entry.original_path for entry in data_entries}
    all_paths = {entry.original_path for entry in entries}

    mapping: dict[str, str] = {}
    conflicts: list[PathConflict] = []
    for row in mapping_rows:
        if row.old_path not in data_paths:
            if row.old_path in all_paths:
                detail = (
                    f"{row.row_label} 的旧路径指向清单中的非数据文件；"
                    "旧路径必须指向 data 类型的数据文件"
                )
            else:
                detail = (
                    f"{row.row_label} 的旧路径不在清单的数据文件路径中"
                )
            conflicts.append(PathConflict(path=row.old_path, detail=detail))
            continue
        mapping[row.old_path] = row.new_path

    return mapping, conflicts


def _migrated_manifest_paths(
    entries: list[ManifestEntry], mapping: Mapping[str, str]
) -> list[tuple[ManifestEntry, str]]:
    result: list[tuple[ManifestEntry, str]] = []
    for entry in entries:
        if entry.is_data:
            new_path = mapping.get(entry.original_path, entry.original_path)
        else:
            new_path = entry.original_path
        result.append((entry, new_path))
    return result


def _find_duplicate_manifest_paths(
    pairs: list[tuple[ManifestEntry, str]],
) -> list[PathConflict]:
    by_path: dict[str, ManifestEntry] = {}
    conflicts: list[PathConflict] = []
    for entry, new_path in pairs:
        previous = by_path.get(new_path)
        if previous is not None:
            conflicts.append(
                PathConflict(
                    path=new_path,
                    detail=(
                        f"迁移后清单路径重复：{previous.row_label} 与 "
                        f"{entry.row_label} 都指向该路径"
                    ),
                )
            )
        else:
            by_path[new_path] = entry
    return conflicts


def _require_delete_columns(table: pa.Table, label: str) -> None:
    schema = table.schema
    if DELETE_PATH_COLUMN not in schema.names:
        raise CheckError(
            f"位置删除文件 “{label}” 缺少列 “{DELETE_PATH_COLUMN}”。"
            "位置删除文件必须包含 file_path 和 pos。",
            files=[label],
        )
    if DELETE_POS_COLUMN not in schema.names:
        raise CheckError(
            f"位置删除文件 “{label}” 缺少列 “{DELETE_POS_COLUMN}”。"
            "位置删除文件必须包含 file_path 和 pos。",
            files=[label],
        )
    path_field = schema.field(DELETE_PATH_COLUMN)
    pos_field = schema.field(DELETE_POS_COLUMN)
    if not (
        pa.types.is_string(path_field.type)
        or pa.types.is_large_string(path_field.type)
    ):
        raise CheckError(
            f"位置删除文件 “{label}” 的列 “{DELETE_PATH_COLUMN}” 类型为 "
            f"{path_field.type}，不是字符串类型。",
            files=[label],
        )
    if not pa.types.is_integer(pos_field.type):
        raise CheckError(
            f"位置删除文件 “{label}” 的列 “{DELETE_POS_COLUMN}” 类型为 "
            f"{pos_field.type}，不是整数类型。",
            files=[label],
        )


def _validate_delete_rows(
    table: pa.Table, label: str
) -> tuple[list[str], list[int]]:
    paths = table.column(DELETE_PATH_COLUMN).to_pylist()
    positions = table.column(DELETE_POS_COLUMN).to_pylist()
    for row_index, record_path in enumerate(paths):
        if not isinstance(record_path, str) or record_path == "":
            raise CheckError(
                f"位置删除文件 “{label}” 第 {row_index} 行（从 0 起算）"
                "的 file_path 为空或不是字符串。",
                files=[label],
            )
    for row_index, record_pos in enumerate(positions):
        if (
            record_pos is None
            or isinstance(record_pos, bool)
            or not isinstance(record_pos, int)
        ):
            raise CheckError(
                f"位置删除文件 “{label}” 第 {row_index} 行（从 0 起算）"
                "的 pos 不是整数。",
                files=[label],
            )
        if record_pos < 0:
            raise CheckError(
                f"位置删除文件 “{label}” 第 {row_index} 行（从 0 起算）"
                f"的 pos={record_pos} 为负数。",
                files=[label],
            )
    return list(paths), list(positions)


def _check_required_delete_replicas(
    entries: list[ManifestEntry], uploaded_paths: Mapping[str, Path]
) -> None:
    missing: list[str] = []
    for entry in entries:
        if not entry.is_position_delete:
            continue
        path = uploaded_paths.get(entry.uploaded_filename)
        if path is None or not Path(path).is_file():
            missing.append(entry.uploaded_filename)
    if missing:
        raise CheckError(
            "清单中以下位置删除文件缺少上传副本，无法迁移，未生成 ZIP。",
            files=missing,
        )


def _load_delete_tables(
    entries: list[ManifestEntry], uploaded_paths: Mapping[str, Path]
) -> list[tuple[ManifestEntry, pa.Table]]:
    loaded: list[tuple[ManifestEntry, pa.Table]] = []
    for entry in entries:
        if not entry.is_position_delete:
            continue
        label = entry.uploaded_filename
        source = uploaded_paths[label]
        try:
            table = pq.read_table(str(source))
        except Exception as exc:
            raise CheckError(
                f"无法读取位置删除文件 “{label}”（原始路径 "
                f"{entry.original_path}）：{exc}",
                files=[label],
            ) from None
        _require_delete_columns(table, label)
        _validate_delete_rows(table, label)
        loaded.append((entry, table))
    return loaded


def _find_orphan_delete_paths(
    tables: list[tuple[ManifestEntry, pa.Table]],
    data_paths_before: set[str],
    data_paths_after: set[str],
) -> list[PathConflict]:
    """Delete rows that newly point at a data file without having done so.

    A delete record normally has to name a data file in the manifest.  If a
    record named something else *before* the migration and its value is a
    post-migration data path, the rewrite would silently turn an unrelated
    record into an effective delete.  That is unsafe.
    """

    by_path: dict[str, list[str]] = {}
    for entry, table in tables:
        paths = table.column(DELETE_PATH_COLUMN).to_pylist()
        for row_index, record_path in enumerate(paths):
            if (
                record_path not in data_paths_before
                and record_path in data_paths_after
            ):
                by_path.setdefault(record_path, []).append(
                    f"{entry.uploaded_filename} 第 {row_index} 行"
                )

    return [
        PathConflict(
            path=path,
            detail=(
                "删除记录的 file_path 原先不指向清单内的数据文件，迁移后"
                f"会指向数据文件：{'、'.join(row_labels)}"
            ),
        )
        for path, row_labels in sorted(by_path.items())
    ]


def _rewrite_manifest(
    raw: RawManifest,
    mapping: Mapping[str, str],
) -> bytes:
    path_index = raw.path_index
    output_rows: list[list[str]] = [list(raw.headers)]
    for (_line_number, row), entry in zip(raw.rows, raw.entries):
        cells = list(row)
        if entry.is_data:
            # Look up the *pre-migration* value exactly once.  Never rewrite
            # a cell a second time from an intermediate value.
            cells[path_index] = mapping.get(
                entry.original_path, cells[path_index]
            )
        output_rows.append(cells)

    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    try:
        writer.writerows(output_rows)
    except csv.Error as exc:
        raise CheckError(f"无法写出迁移后的清单：{exc}") from None
    return buffer.getvalue().encode("utf-8")


def _rewrite_delete_table(
    table: pa.Table, mapping: Mapping[str, str], label: str
) -> tuple[pa.Table, int]:
    paths = table.column(DELETE_PATH_COLUMN).to_pylist()
    # One lookup per original value; the result is never rewritten again.
    migrated_paths = [mapping.get(path, path) for path in paths]

    path_index = table.schema.get_field_index(DELETE_PATH_COLUMN)
    path_field = table.schema.field(path_index)
    try:
        table = table.set_column(
            path_index,
            path_field,
            pa.array(migrated_paths, type=path_field.type),
        )
        indices = pc.sort_indices(
            table,
            sort_keys=[
                (DELETE_PATH_COLUMN, "ascending"),
                (DELETE_POS_COLUMN, "ascending"),
            ],
        )
        table = table.take(indices)
    except Exception as exc:
        raise CheckError(
            f"无法更新位置删除文件 “{label}”：{exc}",
            files=[label],
        ) from None
    return table, table.num_rows


def _encode_delete_table(table: pa.Table, label: str) -> bytes:
    buffer = io.BytesIO()
    try:
        pq.write_table(table, buffer, compression="snappy")
    except Exception as exc:
        raise CheckError(
            f"无法写出迁移后的位置删除文件 “{label}”：{exc}",
            files=[label],
        ) from None
    return buffer.getvalue()


def _manifest_entry_name(
    original_name: str, taken: set[str]
) -> str:
    """Pick a non-colliding ZIP name for the migrated manifest."""

    base = os.path.basename((original_name or "").replace("\\", "/"))
    stem = Path(base).stem or "manifest"
    candidates = ["manifest.csv"]
    if stem and stem != "manifest":
        candidates.append(f"{stem}.csv")
    for number in range(2, 1000):
        candidates.append(f"manifest-{number}.csv")
    for candidate in candidates:
        if candidate not in taken:
            return candidate
    raise CheckError("无法为迁移后的清单选择不重复的 ZIP 文件名。")


def _validate_replica_names(entries: list[ManifestEntry]) -> None:
    for entry in entries:
        name = entry.uploaded_filename
        if name in {".", ".."} or "/" in name or "\\" in name:
            raise CheckError(
                f"{entry.row_label} 的上传文件名 “{name}” 不是安全的纯"
                "文件名，无法写入 ZIP。",
                files=[name],
            )


def build_migration(
    manifest_data: bytes | str,
    mapping_data: bytes | str,
    uploaded_paths: Mapping[str, Path],
    manifest_filename: str = "manifest.csv",
) -> MigrationPackage:
    """Build the migrated manifest and delete replicas, or raise CheckError.

    ``uploaded_paths`` maps the manifest's ``uploaded_filename`` values to
    local Parquet replicas.  Only positional-delete replicas are needed; data
    replicas are deliberately not required.
    """

    raw = _parse_raw_manifest(manifest_data)

    equality = [
        entry
        for entry in raw.entries
        if entry.kind is FileKind.EQUALITY_DELETE
    ]
    if equality:
        raise CheckError(
            "清单含等值删除（equality_delete），离线路径迁移只处理位置"
            "删除，未生成 ZIP。",
            files=[entry.uploaded_filename for entry in equality],
        )

    mapping_rows = parse_path_mapping(mapping_data)
    mapping, mapping_conflicts = _validate_mapping_against_manifest(
        mapping_rows, raw.entries
    )
    if mapping_conflicts:
        raise CheckError(
            "迁移未完成：路径对照表中有旧路径无法使用，未生成 ZIP。",
            conflicts=mapping_conflicts,
        )

    pairs = _migrated_manifest_paths(raw.entries, mapping)
    duplicate_conflicts = _find_duplicate_manifest_paths(pairs)
    if duplicate_conflicts:
        raise CheckError(
            "迁移未完成：迁移后的清单存在重复路径，未生成 ZIP。",
            conflicts=duplicate_conflicts,
        )

    _validate_replica_names(raw.entries)
    _check_required_delete_replicas(raw.entries, uploaded_paths)
    tables = _load_delete_tables(raw.entries, uploaded_paths)

    data_paths_before = {
        entry.original_path for entry in raw.entries if entry.is_data
    }
    data_paths_after = {
        new_path for entry, new_path in pairs if entry.is_data
    }
    orphan_conflicts = _find_orphan_delete_paths(
        tables, data_paths_before, data_paths_after
    )
    if orphan_conflicts:
        raise CheckError(
            "迁移未完成：删除记录在迁移后会指向数据文件，但原先并不指向"
            "清单内的数据文件，未生成 ZIP。",
            conflicts=orphan_conflicts,
        )

    manifest_bytes = _rewrite_manifest(raw, mapping)

    delete_files: list[MigratedDeleteFile] = []
    for entry, table in tables:
        rewritten, row_count = _rewrite_delete_table(
            table, mapping, entry.uploaded_filename
        )
        delete_files.append(
            MigratedDeleteFile(
                name=entry.uploaded_filename,
                data=_encode_delete_table(
                    rewritten, entry.uploaded_filename
                ),
                row_count=row_count,
            )
        )

    taken = {replica.name for replica in delete_files}
    manifest_name = _manifest_entry_name(manifest_filename, taken)

    packaged = set(taken) | {manifest_name}
    warnings = [
        f"上传文件 “{name}” 不是位置删除副本，未写入 ZIP。"
        for name in sorted(uploaded_paths)
        if name not in packaged
    ]

    return MigrationPackage(
        manifest_name=manifest_name,
        manifest_bytes=manifest_bytes,
        delete_files=delete_files,
        mapping=mapping,
        data_paths_before=sorted(data_paths_before),
        data_paths_after=sorted(data_paths_after),
        warnings=warnings,
    )
