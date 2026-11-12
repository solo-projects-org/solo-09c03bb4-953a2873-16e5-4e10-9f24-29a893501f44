"""Generate the bundled sample used by the "载入样例" button.

The sample deliberately contains:

* data-0001.parquet: five row groups.  Ticket 4242 lives in row group 4
  (zero-based), so row groups 0-3 are skipped by min/max statistics before
  the ticket is found.
* data-0002.parquet: written without column statistics.  Ticket 8888 is in
  its second row group and the checker must read both row groups because the
  statistics are missing.
* delete-pos-0001.parquet: positional delete for 4242 at row 99.
* delete-pos-0002.parquet: positional delete for 8888 at row 19.
* delete-pos-stale-0001.parquet: a tempting delete for 4242/row 99, but with
  sequence number 6 < data file sequence number 7; it must be ignored.
* An equality-delete manifest entry with no uploaded replica; it must be
  ignored because only positional deletes are processed.

The offline-migration sample in ``sample/migrate/`` deliberately uses
*crossing* old and new paths: the mapping swaps the two data-file paths.
A naive implementation that keeps replacing values in sequence would turn
the swap into a no-op (or bounce forever); the migration must look every
replacement up against the pre-migration value exactly once.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "sample"
MIGRATE_SAMPLE = SAMPLE / "migrate"

BASE = "s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05"
DATA1_PATH = f"{BASE}/data/data-0001.parquet"
DATA2_PATH = f"{BASE}/data/data-0002.parquet"
DELETE1_PATH = f"{BASE}/delete-pos/delete-pos-0001.parquet"
DELETE2_PATH = f"{BASE}/delete-pos/delete-pos-0002.parquet"
STALE_DELETE_PATH = f"{BASE}/delete-pos/delete-pos-stale-0001.parquet"
EQ_DELETE_PATH = f"{BASE}/delete-eq/delete-eq-0001.parquet"

DATA_SCHEMA = pa.schema(
    [
        ("ticket_no", pa.int64()),
        ("passenger_name", pa.string()),
        ("fare_cents", pa.int64()),
        ("booking_status", pa.string()),
        ("voyage_date", pa.string()),
    ]
)

# Positional deletes keep Iceberg's reserved field IDs.  The optional ``row``
# column is present on purpose: the migration must preserve every column,
# including ones it never reads.
DELETE_SCHEMA = pa.schema(
    [
        pa.field(
            "file_path",
            pa.string(),
            metadata={b"PARQUET:field_id": b"2147483546"},
        ),
        pa.field(
            "pos",
            pa.int64(),
            metadata={b"PARQUET:field_id": b"2147483545"},
        ),
        pa.field(
            "row",
            pa.int64(),
            metadata={b"PARQUET:field_id": b"2147483544"},
        ),
    ]
)


def data_rows(tickets: list[int]) -> list[dict]:
    return [
        {
            "ticket_no": ticket,
            "passenger_name": f"P-{ticket}",
            "fare_cents": 1500 + (ticket % 700),
            "booking_status": "checked_in",
            "voyage_date": "2026-11-05",
        }
        for ticket in tickets
    ]


def write_data1() -> None:
    tickets = (
        list(range(1000, 1020))
        + list(range(2000, 2020))
        + list(range(3000, 3020))
        + list(range(4000, 4020))
        + list(range(4200, 4219))
        + [4242]
    )
    table = pa.Table.from_pylist(data_rows(tickets), schema=DATA_SCHEMA)
    # 20 rows per row group -> exactly five row groups.
    pq.write_table(
        table,
        SAMPLE / "data-0001.parquet",
        row_group_size=20,
        compression="snappy",
    )


def write_data2() -> None:
    tickets = list(range(7000, 7010)) + list(range(8000, 8009)) + [8888]
    table = pa.Table.from_pylist(data_rows(tickets), schema=DATA_SCHEMA)
    # No statistics at all: the checker must fall back to reading row groups.
    pq.write_table(
        table,
        SAMPLE / "data-0002.parquet",
        row_group_size=10,
        write_statistics=False,
        compression="snappy",
    )


def write_delete(path: Path, records: list[tuple[str, int]]) -> None:
    table = pa.Table.from_pylist(
        [
            {"file_path": file_path, "pos": pos, "row": pos}
            for file_path, pos in records
        ],
        schema=DELETE_SCHEMA,
    )
    pq.write_table(table, path, row_group_size=10, compression="snappy")


def write_manifest() -> None:
    lines = [
        "original_path,uploaded_filename,file_type,data_sequence_number",
        f"{DATA1_PATH},data-0001.parquet,data,7",
        f"{DATA2_PATH},data-0002.parquet,data,7",
        f"{DELETE1_PATH},delete-pos-0001.parquet,position_delete,12",
        f"{DELETE2_PATH},delete-pos-0002.parquet,position_delete,8",
        f"{STALE_DELETE_PATH},delete-pos-stale-0001.parquet,position_delete,6",
        f"{EQ_DELETE_PATH},delete-eq-0001.parquet,equality_delete,12",
    ]
    (SAMPLE / "manifest.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_migration_sample() -> None:
    """Copy the base sample and add crossing old/new paths.

    ``manifest.csv`` keeps the original paths (the "old" layout).  The
    mapping swaps the two data paths, so:

    * data-0001.parquet moves to the *old* location of data-0002.parquet;
    * data-0002.parquet moves to the *old* location of data-0001.parquet.

    The delete replicas contain records pointing at both old data paths;
    the migrate page must rewrite both exactly once.  Only the positional
    delete replicas are uploaded; the data files are not copied because the
    migration never needs them.
    """

    MIGRATE_SAMPLE.mkdir(parents=True, exist_ok=True)
    for name in (
        "delete-pos-0001.parquet",
        "delete-pos-0002.parquet",
        "delete-pos-stale-0001.parquet",
    ):
        shutil.copyfile(SAMPLE / name, MIGRATE_SAMPLE / name)

    # The migration manifest holds data and positional deletes only.  The
    # equality-delete row of the checker sample would make the migration
    # refuse to package anything, which is the correct behaviour but not a
    # useful demonstration.
    manifest_lines = [
        "original_path,uploaded_filename,file_type,data_sequence_number",
        f"{DATA1_PATH},data-0001.parquet,data,7",
        f"{DATA2_PATH},data-0002.parquet,data,7",
        f"{DELETE1_PATH},delete-pos-0001.parquet,position_delete,12",
        f"{DELETE2_PATH},delete-pos-0002.parquet,position_delete,8",
        f"{STALE_DELETE_PATH},delete-pos-stale-0001.parquet,position_delete,6",
    ]
    (MIGRATE_SAMPLE / "manifest.csv").write_text(
        "\n".join(manifest_lines) + "\n", encoding="utf-8"
    )

    mapping_lines = [
        "old_path,new_path",
        f"{DATA1_PATH},{DATA2_PATH}",
        f"{DATA2_PATH},{DATA1_PATH}",
    ]
    (MIGRATE_SAMPLE / "path-mapping.csv").write_text(
        "\n".join(mapping_lines) + "\n", encoding="utf-8"
    )


def main() -> None:
    SAMPLE.mkdir(parents=True, exist_ok=True)
    write_data1()
    write_data2()
    write_delete(
        SAMPLE / "delete-pos-0001.parquet",
        [
            (DATA1_PATH, 3),   # ticket 1003
            (DATA1_PATH, 99),  # ticket 4242, last row of row group 4
            (DATA2_PATH, 99),  # wrong path / out-of-range; must not match
        ],
    )
    write_delete(
        SAMPLE / "delete-pos-0002.parquet",
        [(DATA2_PATH, 19)],  # ticket 8888
    )
    write_delete(
        SAMPLE / "delete-pos-stale-0001.parquet",
        [
            (DATA1_PATH, 99),  # matches 4242 by path/row but sequence is 6 < 7
        ],
    )
    write_manifest()
    write_migration_sample()

    for name in (
        "data-0001.parquet",
        "data-0002.parquet",
        "delete-pos-0001.parquet",
        "delete-pos-0002.parquet",
        "delete-pos-stale-0001.parquet",
        "manifest.csv",
        "migrate/path-mapping.csv",
        "migrate/manifest.csv",
    ):
        print(f"wrote {SAMPLE / name}")


if __name__ == "__main__":
    main()
