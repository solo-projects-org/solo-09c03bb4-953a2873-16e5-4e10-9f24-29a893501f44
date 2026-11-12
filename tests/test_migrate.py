from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from checker.errors import CheckError
from checker.manifest import parse_manifest
from checker.migrate import (
    build_migration,
    parse_path_mapping,
)

SAMPLE = Path(__file__).resolve().parents[1] / "sample"
MIGRATE = SAMPLE / "migrate"
BASE = "s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05"
DATA1 = f"{BASE}/data/data-0001.parquet"
DATA2 = f"{BASE}/data/data-0002.parquet"
DELETE1 = f"{BASE}/delete-pos/delete-pos-0001.parquet"


def migrate_inputs() -> tuple[bytes, bytes, dict[str, Path]]:
    manifest = (MIGRATE / "manifest.csv").read_bytes()
    mapping = (MIGRATE / "path-mapping.csv").read_bytes()
    uploads = {
        name: MIGRATE / name
        for name in (
            "delete-pos-0001.parquet",
            "delete-pos-0002.parquet",
            "delete-pos-stale-0001.parquet",
        )
    }
    return manifest, mapping, uploads


def manifest_rows(data: bytes) -> list[list[str]]:
    return list(csv.reader(io.StringIO(data.decode("utf-8"))))[1:]


def test_parse_mapping_rejects_duplicate_old_paths():
    text = (
        "old_path,new_path\n"
        f"{DATA1},s3://new/a.parquet\n"
        f"{DATA1},s3://new/b.parquet\n"
    )
    with pytest.raises(CheckError) as excinfo:
        parse_path_mapping(text)
    assert excinfo.value.conflicts
    assert excinfo.value.conflicts[0].path == DATA1


def test_parse_mapping_requires_both_columns():
    with pytest.raises(CheckError, match="缺少必需列"):
        parse_path_mapping("old_path,other\ns3://a,b\n")


def test_parse_mapping_rejects_json():
    with pytest.raises(CheckError, match="CSV"):
        parse_path_mapping('[{"old_path": "a", "new_path": "b"}]')


def test_crossing_paths_are_rewritten_exactly_once():
    manifest, mapping, uploads = migrate_inputs()
    package = build_migration(manifest, mapping, uploads)

    rows = manifest_rows(package.manifest_bytes)
    # The two data rows must have swapped paths, not collapsed to one.
    assert rows[0][0] == DATA2
    assert rows[1][0] == DATA1
    # Header and row order are preserved; equality/non-path values untouched.
    assert rows[0][1:] == ["data-0001.parquet", "data", "7"]
    assert rows[1][1:] == ["data-0002.parquet", "data", "7"]


def test_manifest_paths_target_original_values_only():
    manifest, mapping, uploads = migrate_inputs()
    package = build_migration(manifest, mapping, uploads)
    rows = manifest_rows(package.manifest_bytes)
    # If values were rewritten in sequence, DATA1 would bounce back to DATA1.
    assert rows[0][0] == DATA2 and rows[1][0] == DATA1


def test_delete_files_keep_name_columns_field_ids_and_sort_order():
    manifest, mapping, uploads = migrate_inputs()
    package = build_migration(manifest, mapping, uploads)
    assert sorted(package.delete_names) == [
        "delete-pos-0001.parquet",
        "delete-pos-0002.parquet",
        "delete-pos-stale-0001.parquet",
    ]

    for replica in package.delete_files:
        original = pq.read_table(str(uploads[replica.name]))
        migrated = pq.read_table(io.BytesIO(replica.data))
        assert migrated.schema.names == original.schema.names
        assert migrated.schema.metadata == original.schema.metadata
        for name in original.schema.names:
            assert migrated.schema.field(name).metadata == (
                original.schema.field(name).metadata
            )
        assert migrated.num_rows == original.num_rows

        # Sorted by (file_path, pos).
        rows = migrated.to_pylist()
        assert sorted(
            range(len(rows)), key=lambda i: (
                rows[i]["file_path"], rows[i]["pos"]
            )
        ) == list(range(len(rows)))

        # Only file_path changed; ``row`` and ``pos`` are untouched.
        original_rows = original.to_pylist()
        assert sorted(
            (r["pos"], r["row"]) for r in original_rows
        ) == sorted((r["pos"], r["row"]) for r in rows)
        for row in rows:
            assert row["row"] == row["pos"]


def test_delete_paths_follow_the_mapping():
    manifest, mapping, uploads = migrate_inputs()
    package = build_migration(manifest, mapping, uploads)
    delete1 = pq.read_table(
        io.BytesIO(
            next(
                item.data
                for item in package.delete_files
                if item.name == "delete-pos-0001.parquet"
            )
        )
    ).to_pylist()
    # Original (DATA1,3), (DATA1,99), (DATA2,99); after the swap the sort
    # order by migrated path is DATA1/99, DATA2/3, DATA2/99.
    assert [(row["file_path"], row["pos"]) for row in delete1] == [
        (DATA1, 99),
        (DATA2, 3),
        (DATA2, 99),
    ]


def test_zip_contains_manifest_and_every_delete_replica():
    manifest, mapping, uploads = migrate_inputs()
    package = build_migration(manifest, mapping, uploads, "manifest.csv")
    payload = package.to_zip()
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = set(archive.namelist())
        assert names == {
            "manifest.csv",
            "delete-pos-0001.parquet",
            "delete-pos-0002.parquet",
            "delete-pos-stale-0001.parquet",
        }
        assert archive.read("manifest.csv") == package.manifest_bytes


def test_migrated_zip_feeds_back_into_the_checker(tmp_path: Path):
    manifest, mapping, uploads = migrate_inputs()
    package = build_migration(manifest, mapping, uploads)
    payload = package.to_zip()
    (tmp_path / "out.zip").write_bytes(payload)
    with zipfile.ZipFile(tmp_path / "out.zip") as archive:
        archive.extractall(tmp_path / "out")

    # The migrated manifest plus local data replicas must still find 4242
    # at row 99 and treat it as deleted.
    from checker.engine import run_check

    migrated_manifest = parse_manifest(
        (tmp_path / "out" / "manifest.csv").read_bytes()
    )
    replicas = {
        "data-0001.parquet": SAMPLE / "data-0001.parquet",
        "data-0002.parquet": SAMPLE / "data-0002.parquet",
    }
    for name in package.delete_names:
        replicas[name] = tmp_path / "out" / name
    result = run_check(migrated_manifest, replicas, [4242])
    assert result.tickets[0].deleted


def test_old_path_must_name_a_data_file_in_manifest():
    mapping = (
        "old_path,new_path\n"
        f"{DELETE1},s3://new/delete.parquet\n"
    )
    manifest, _, uploads = migrate_inputs()
    with pytest.raises(CheckError) as excinfo:
        build_migration(manifest, mapping, uploads)
    assert excinfo.value.conflicts
    assert excinfo.value.conflicts[0].path == DELETE1
    assert "data" in excinfo.value.conflicts[0].detail


def test_old_path_outside_manifest_is_a_conflict():
    mapping = "old_path,new_path\ns3://elsewhere/x.parquet,s3://new/x.parquet\n"
    manifest, _, uploads = migrate_inputs()
    with pytest.raises(CheckError) as excinfo:
        build_migration(manifest, mapping, uploads)
    assert excinfo.value.conflicts[0].path == "s3://elsewhere/x.parquet"


def test_new_manifest_duplicate_paths_are_conflicts():
    mapping = (
        "old_path,new_path\n"
        f"{DATA1},s3://new/shared.parquet\n"
        f"{DATA2},s3://new/shared.parquet\n"
    )
    manifest, _, uploads = migrate_inputs()
    with pytest.raises(CheckError) as excinfo:
        build_migration(manifest, mapping, uploads)
    assert excinfo.value.conflicts
    assert excinfo.value.conflicts[0].path == "s3://new/shared.parquet"


def test_delete_that_would_newly_target_a_data_file_is_a_conflict(tmp_path: Path):
    manifest = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://old/data/a.parquet,a.parquet,data,7\n"
        "s3://old/other/b.parquet,b.parquet,position_delete,9\n"
    )
    mapping = (
        "old_path,new_path\n"
        "s3://old/data/a.parquet,s3://old/other/b.parquet\n"
    )
    delete = tmp_path / "b.parquet"
    pq.write_table(
        pa.table({"file_path": ["s3://old/other/b.parquet"], "pos": [0]}),
        delete,
    )
    with pytest.raises(CheckError) as excinfo:
        build_migration(manifest, mapping, {"b.parquet": delete})
    assert excinfo.value.conflicts
    assert excinfo.value.conflicts[0].path == "s3://old/other/b.parquet"


def test_equality_delete_blocks_the_migration():
    manifest = (SAMPLE / "manifest.csv").read_bytes()
    mapping = "old_path,new_path\ns3://irrelevant,a\n"
    uploads = {"delete-eq-0001.parquet": SAMPLE / "delete-pos-0001.parquet"}
    with pytest.raises(CheckError) as excinfo:
        build_migration(manifest, mapping, uploads)
    assert "等值删除" in excinfo.value.message
    assert "delete-eq-0001.parquet" in excinfo.value.files


def test_missing_delete_replica_names_the_file():
    manifest = (MIGRATE / "manifest.csv").read_bytes()
    mapping = (
        "old_path,new_path\n"
        f"{DATA1},s3://new/a.parquet\n"
        f"{DATA2},s3://new/b.parquet\n"
    )
    with pytest.raises(CheckError) as excinfo:
        build_migration(manifest, mapping, {})
    assert "delete-pos-0001.parquet" in excinfo.value.files
    assert "缺少上传副本" in excinfo.value.message


def test_bad_delete_parquet_names_the_file(tmp_path: Path):
    manifest = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://old/data/a.parquet,a.parquet,data,7\n"
        "s3://old/del/d.parquet,d.parquet,position_delete,9\n"
    )
    mapping = "old_path,new_path\ns3://old/data/a.parquet,s3://new/a.parquet\n"
    bad = tmp_path / "d.parquet"
    bad.write_bytes(b"not parquet")
    with pytest.raises(CheckError) as excinfo:
        build_migration(manifest, mapping, {"d.parquet": bad})
    assert "d.parquet" in excinfo.value.files
    assert "无法读取位置删除文件" in excinfo.value.message


def test_delete_missing_pos_is_rejected(tmp_path: Path):
    manifest = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://old/data/a.parquet,a.parquet,data,7\n"
        "s3://old/del/d.parquet,d.parquet,position_delete,9\n"
    )
    mapping = "old_path,new_path\ns3://old/data/a.parquet,s3://new/a.parquet\n"
    delete = tmp_path / "d.parquet"
    pq.write_table(pa.table({"file_path": ["s3://old/data/a.parquet"]}), delete)
    with pytest.raises(CheckError) as excinfo:
        build_migration(manifest, mapping, {"d.parquet": delete})
    assert "pos" in excinfo.value.message


def test_negative_pos_is_rejected(tmp_path: Path):
    manifest = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://old/data/a.parquet,a.parquet,data,7\n"
        "s3://old/del/d.parquet,d.parquet,position_delete,9\n"
    )
    mapping = "old_path,new_path\ns3://old/data/a.parquet,s3://new/a.parquet\n"
    delete = tmp_path / "d.parquet"
    pq.write_table(
        pa.table({"file_path": ["s3://old/data/a.parquet"], "pos": [-2]}),
        delete,
    )
    with pytest.raises(CheckError, match="负数"):
        build_migration(manifest, mapping, {"d.parquet": delete})


def test_manifest_json_is_rejected_for_migration():
    payload = (
        '[{"original_path": "s3://a", "uploaded_filename": "a.parquet",'
        ' "file_type": "data", "data_sequence_number": 1}]'
    )
    mapping = "old_path,new_path\ns3://a,s3://b\n"
    with pytest.raises(CheckError, match="CSV"):
        build_migration(payload, mapping, {})


def test_extra_manifest_columns_are_preserved():
    manifest = (
        "original_path,uploaded_filename,file_type,data_sequence_number,note\n"
        "s3://old/data/a.parquet,a.parquet,data,7,keep me\n"
        "s3://old/del/d.parquet,d.parquet,position_delete,9,keep too\n"
    )
    mapping = "old_path,new_path\ns3://old/data/a.parquet,s3://new/a.parquet\n"
    delete = SAMPLE / "delete-pos-0001.parquet"
    package = build_migration(manifest, mapping, {"d.parquet": delete})
    rows = manifest_rows(package.manifest_bytes)
    assert rows[0] == [
        "s3://new/a.parquet", "a.parquet", "data", "7", "keep me"
    ]
    assert rows[1] == [
        "s3://old/del/d.parquet", "d.parquet", "position_delete", "9",
        "keep too"
    ]


def test_unmapped_data_file_keeps_its_path():
    manifest, _, uploads = migrate_inputs()
    mapping = "old_path,new_path\n" f"{DATA1},s3://new/a.parquet\n"
    package = build_migration(manifest, mapping, uploads)
    rows = manifest_rows(package.manifest_bytes)
    assert rows[0][0] == "s3://new/a.parquet"
    assert rows[1][0] == DATA2
    # Delete rows still pointing at DATA2 are unchanged.
    delete2 = pq.read_table(
        io.BytesIO(
            next(
                item.data
                for item in package.delete_files
                if item.name == "delete-pos-0002.parquet"
            )
        )
    ).to_pylist()
    assert delete2[0]["file_path"] == DATA2
