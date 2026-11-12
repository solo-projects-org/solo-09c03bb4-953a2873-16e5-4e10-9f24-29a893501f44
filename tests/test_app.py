from __future__ import annotations

import io
from pathlib import Path

import pytest

from app import app

SAMPLE = Path(__file__).resolve().parents[1] / "sample"


@pytest.fixture()
def client():
    app.config.update(TESTING=True)
    with app.test_client() as client:
        yield client


def sample_uploads():
    files = []
    for name in (
        "data-0001.parquet",
        "data-0002.parquet",
        "delete-pos-0001.parquet",
        "delete-pos-0002.parquet",
        "delete-pos-stale-0001.parquet",
    ):
        files.append(
            (
                io.BytesIO((SAMPLE / name).read_bytes()),
                name,
                "application/octet-stream",
            )
        )
    manifest = (
        io.BytesIO((SAMPLE / "manifest.csv").read_bytes()),
        "manifest.csv",
    )
    return manifest, files


def test_index(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "Iceberg" in response.get_data(as_text=True)


def test_health(client):
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.get_json() == {"status": "ok"}


def test_sample_shows_skipped_row_groups_and_deleted_ticket(client):
    response = client.get("/sample")
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "内置样例" in html
    assert "4242" in html
    assert "跳过 4 / 5 个行组" in html
    assert "已删除" in html
    assert "delete-pos-0001.parquet" in html
    # The stale, lower-sequence delete must be called out as ignored.
    assert "delete-pos-stale-0001.parquet" in html


def test_upload_round_trip(client):
    manifest, files = sample_uploads()
    response = client.post(
        "/check",
        data={
            "manifest": manifest,
            "files": files,
            "tickets": "4242, 1003, 4218, 8888, 9999",
            "ticket_column": "ticket_no",
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "核对结果" in html
    assert "文件内行号" in html
    assert "delete-pos-0001.parquet" in html
    assert "未找到" in html


def test_missing_file_returns_error_and_no_ticket_results(client):
    manifest = (
        io.BytesIO((SAMPLE / "manifest.csv").read_bytes()),
        "manifest.csv",
    )
    response = client.post(
        "/check",
        data={
            "manifest": manifest,
            "tickets": "4242",
            "ticket_column": "ticket_no",
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "核对未完成" in html
    assert "data-0001.parquet" in html
    assert "没有返回任何票号的核对结果" in html
    assert "<h2>核对结果</h2>" not in html
    assert 'class="ticket-card' not in html


def test_invalid_ticket_is_rejected(client):
    manifest, files = sample_uploads()
    response = client.post(
        "/check",
        data={
            "manifest": manifest,
            "files": files,
            "tickets": "4242, nope",
            "ticket_column": "ticket_no",
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "不是整数" in html


def test_missing_manifest_is_rejected(client):
    response = client.post(
        "/check",
        data={"tickets": "4242", "ticket_column": "ticket_no"},
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "请上传" in html


def test_bad_manifest_is_rejected(client):
    response = client.post(
        "/check",
        data={
            "manifest": (io.BytesIO(b"not,a,manifest\n1,2,3\n"), "manifest.csv"),
            "tickets": "4242",
            "ticket_column": "ticket_no",
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "缺少必需列" in html


def test_duplicate_uploaded_name_is_rejected(client):
    manifest, files = sample_uploads()
    duplicate = list(files) + [files[0]]
    response = client.post(
        "/check",
        data={
            "manifest": manifest,
            "files": duplicate,
            "tickets": "4242",
            "ticket_column": "ticket_no",
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "上传文件名重复" in html


MIGRATE = SAMPLE / "migrate"


def migrate_uploads():
    manifest = (io.BytesIO((MIGRATE / "manifest.csv").read_bytes()), "manifest.csv")
    mapping = (
        io.BytesIO((MIGRATE / "path-mapping.csv").read_bytes()),
        "path-mapping.csv",
    )
    files = [
        (
            io.BytesIO((MIGRATE / name).read_bytes()),
            name,
            "application/octet-stream",
        )
        for name in (
            "delete-pos-0001.parquet",
            "delete-pos-0002.parquet",
            "delete-pos-stale-0001.parquet",
        )
    ]
    return manifest, mapping, files


def test_migrate_page_is_reachable(client):
    response = client.get("/migrate")
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "路径离线迁移" in html
    assert 'action="/migrate"' in html


def test_migrate_upload_returns_zip(client):
    manifest, mapping, files = migrate_uploads()
    response = client.post(
        "/migrate",
        data={"manifest": manifest, "mapping": mapping, "files": files},
        content_type="multipart/form-data",
    )
    assert response.status_code == 200
    assert response.mimetype == "application/zip"
    assert "migrated-files.zip" in response.headers["Content-Disposition"]

    import zipfile

    with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
        assert set(archive.namelist()) == {
            "manifest.csv",
            "delete-pos-0001.parquet",
            "delete-pos-0002.parquet",
            "delete-pos-stale-0001.parquet",
        }


def test_migrate_sample_page_and_zip(client):
    response = client.get("/migrate/sample")
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "迁移结果" in html
    assert "data-0001.parquet" in html
    assert "data-0002.parquet" in html

    response = client.get("/migrate/sample.zip")
    assert response.status_code == 200
    assert response.mimetype == "application/zip"


def test_migrate_conflict_shows_paths_and_no_zip(client):
    manifest = (
        io.BytesIO(
            (
                "original_path,uploaded_filename,file_type,data_sequence_number\n"
                "s3://old/a.parquet,a.parquet,data,1\n"
                "s3://old/b.parquet,b.parquet,data,1\n"
            ).encode()
        ),
        "manifest.csv",
    )
    mapping = (
        io.BytesIO(
            b"old_path,new_path\n"
            b"s3://old/a.parquet,s3://new/shared.parquet\n"
            b"s3://old/b.parquet,s3://new/shared.parquet\n"
        ),
        "mapping.csv",
    )
    response = client.post(
        "/migrate",
        data={"manifest": manifest, "mapping": mapping},
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "迁移未完成" in html
    assert "s3://new/shared.parquet" in html
    assert "没有输出 ZIP 包" in html
    assert response.mimetype != "application/zip"


def test_migrate_missing_mapping_is_rejected(client):
    manifest, _, files = migrate_uploads()
    response = client.post(
        "/migrate",
        data={"manifest": manifest, "files": files},
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "路径对照表" in html
