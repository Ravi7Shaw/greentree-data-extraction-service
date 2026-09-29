"""Opt in: BE2_INTEGRATION=1 pytest -m integration (real MinIO/ClickHouse)."""

import json
import multiprocessing
import os
import signal
import threading
import time
import uuid
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from cryptography.fernet import Fernet

from config import Config
from services.be2_scan_service import BE2ScanService
from services.pipeline.be2_controller import BE2PipelineController
from services.pipeline.hubspot_pipeline import (
    HUBSPOT_RESOURCES,
    HubSpotBE2Pipeline,
    HubSpotResourceClient,
)
from services.storage_service import StorageService

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("BE2_INTEGRATION") != "1",
        reason="Set BE2_INTEGRATION=1 with local MinIO and ClickHouse",
    ),
]


class FixtureAPI:
    """Synthetic HubSpot response fixtures; the storage boundary is real."""

    def __init__(self):
        from requests import Session

        self.session = Session()

    def page(self, token, resource, cursor, limit):
        record = {"id": "1", "updatedAt": "2026-01-01T00:00:00Z"}
        if resource == "deals":
            record["properties"] = {
                "dealname": "Synthetic deal",
                "amount": "125.50",
                "pipeline": "1",
            }
            record["associations"] = {"companies": {"results": [{"id": "1"}]}}
        elif resource == "companies":
            record["properties"] = {
                "name": "Synthetic company",
                "domain": "example.test",
            }
        elif resource == "pipelines":
            record.update(
                label="Sales pipeline", stages=[{"id": "won", "label": "Won"}]
            )
        elif resource == "owners":
            record["email"] = "owner@example.test"
        elif resource == "engagements":
            record = {
                "engagement": {"id": 1, "lastUpdated": 1767225600000},
                "metadata": {"body": "Synthetic note"},
            }
        if cursor:
            if resource == "engagements":
                record["engagement"]["id"] = 2
            else:
                record["id"] = "2"
            if resource == "deals":
                record.pop(
                    "associations"
                )  # A deal without a company must survive the LEFT JOIN.
        return [record], "page-2" if cursor is None else None


def process_worker(state_dir, key, database, bucket, marker, base_url, crash_point):
    Config.HUBSPOT_API_BASE_URL = base_url
    Config.CLICKHOUSE_DATABASE = database
    Config.MINIO_BUCKET = bucket
    Config.CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "be2")
    Config.CLICKHOUSE_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]
    controller = BE2PipelineController(state_dir, key)
    storage = StorageService(state_dir)

    def crash(resource, pending):
        if resource == "deals":
            Path(marker).write_text(
                "durable load acknowledged; checkpoint not committed"
            )
            while True:
                time.sleep(0.1)

    pipeline = HubSpotBE2Pipeline(
        controller, storage, after_load=crash if crash_point else None
    )
    service = BE2ScanService(controller, storage, pipeline)
    service.start()  # Automatically discovers interrupted scans.
    try:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            state = controller.get("crash-scan")
            if state["status"] == "completed":
                return
            if state["status"] == "failed":
                raise RuntimeError(state["error"])
            time.sleep(0.05)
        raise RuntimeError("Worker timeout")
    finally:
        service.close()


@pytest.fixture
def hubspot_server():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            resource = next(
                (
                    name
                    for name, (endpoint, _) in HUBSPOT_RESOURCES.items()
                    if endpoint.rstrip("/") == parsed.path.rstrip("/")
                ),
                None,
            )
            if (
                not resource
                or self.headers.get("Authorization") != "Bearer synthetic-token"
            ):
                self.send_error(404)
                return
            params = parse_qs(parsed.query)
            cursor = params.get(
                "offset" if resource == "engagements" else "after", [None]
            )[0]
            records, next_cursor = FixtureAPI().page(
                "synthetic-token", resource, cursor, 100
            )
            data = {"results": records}
            if resource == "engagements":
                data.update(hasMore=next_cursor is not None, offset=next_cursor)
            elif next_cursor:
                data["paging"] = {"next": {"after": next_cursor}}
            body = json.dumps(data).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()
    worker.join()


@pytest.fixture
def real_storage(tmp_path, monkeypatch):
    identity = uuid.uuid4().hex
    monkeypatch.setattr(Config, "CLICKHOUSE_DATABASE", f"test_be2_{identity}")
    monkeypatch.setattr(Config, "MINIO_BUCKET", f"test-be2-{identity}")
    monkeypatch.setattr(Config, "CLICKHOUSE_USER", os.getenv("CLICKHOUSE_USER", "be2"))
    monkeypatch.setattr(
        Config, "CLICKHOUSE_PASSWORD", os.environ["CLICKHOUSE_PASSWORD"]
    )
    storage = StorageService(tmp_path)
    storage.initialize()
    yield storage
    client = storage.client("default")
    try:
        client.command(f"DROP DATABASE `{storage.database}`")
    finally:
        client.close()
    for obj in storage.minio.list_objects(Config.MINIO_BUCKET, recursive=True):
        storage.minio.remove_object(Config.MINIO_BUCKET, obj.object_name)
    storage.minio.remove_bucket(Config.MINIO_BUCKET)


def test_all_resources_parquet_views_and_crash(tmp_path, real_storage, hubspot_server):
    key = Fernet.generate_key()
    controller = BE2PipelineController(tmp_path, key)
    controller.create(
        "crash-scan", "tenant-a", "synthetic-token", list(HUBSPOT_RESOURCES), 100
    )
    context = multiprocessing.get_context("spawn")
    marker = tmp_path / "loaded"
    args = (
        str(tmp_path),
        key,
        Config.CLICKHOUSE_DATABASE,
        Config.MINIO_BUCKET,
        str(marker),
        hubspot_server,
    )
    worker = context.Process(target=process_worker, args=(*args, True))
    worker.start()
    try:
        deadline = time.monotonic() + 100
        while not marker.exists() and worker.is_alive() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert marker.exists(), (
            f"Worker did not reach crash boundary (exit {worker.exitcode})"
        )
        cp = controller.get("crash-scan")["resources"]["deals"]
        assert cp["page"] == 0 and cp["pending"]
        client = real_storage.client()
        assert (
            client.query(
                "SELECT count() FROM hubspot_batches WHERE resource='deals'"
            ).result_rows[0][0]
            == 1
        )
        os.kill(worker.pid, signal.SIGKILL)
        worker.join(10)
        assert worker.exitcode == -signal.SIGKILL
        # No API resume call: a fresh supervisor recovers the durable running scan.
        recovered = context.Process(target=process_worker, args=(*args, False))
        recovered.start()
        recovered.join(120)
        if recovered.is_alive():
            recovered.kill()
            recovered.join()
        assert recovered.exitcode == 0
        assert controller.get("crash-scan")["status"] == "completed"
        assert client.query("SELECT count() FROM hubspot_batches").result_rows == [
            (16,)
        ]
        assert client.query("SELECT count() FROM hubspot_records").result_rows == [
            (16,)
        ]
        assert client.query(
            "SELECT deal_id, amount, company_name, pipeline_name FROM v_hubspot_deal_pipeline ORDER BY deal_id"
        ).result_rows == [
            ("1", 125.5, "Synthetic company", "Sales pipeline"),
            ("2", 125.5, "", "Sales pipeline"),
        ]
        files = [
            obj.object_name
            for obj in real_storage.minio.list_objects(
                Config.MINIO_BUCKET, prefix="hubspot/", recursive=True
            )
            if obj.object_name.endswith(".parquet") and "/year=" in obj.object_name
        ]
        assert len(files) == 16
        assert {path.split("/")[1] for path in files} == set(HUBSPOT_RESOURCES)
        response = real_storage.minio.get_object(Config.MINIO_BUCKET, files[0])
        try:
            parquet = pq.ParquetFile(pa.BufferReader(response.read()))
            assert parquet.metadata.row_group(0).column(0).compression != "UNCOMPRESSED"
        finally:
            response.close()
            response.release_conn()
        client.close()
    finally:
        if worker.is_alive():
            worker.kill()
            worker.join()


def test_api_pause_resume_real_storage(tmp_path, real_storage, hubspot_server):
    from fastapi.testclient import TestClient
    from app import create_app
    from test_pipeline import signed, SECRET, wait_for

    controller = BE2PipelineController(tmp_path, Fernet.generate_key())
    entered = threading.Barrier(3)
    release = threading.Event()
    initial = set()

    def after_load(resource, pending):
        if resource not in initial:
            initial.add(resource)
            entered.wait(timeout=30)
            assert release.wait(30)

    pipeline = HubSpotBE2Pipeline(
        controller,
        real_storage,
        lambda: HubSpotResourceClient(hubspot_server),
        after_load,
    )
    service = BE2ScanService(controller, real_storage, pipeline)
    try:
        with TestClient(create_app(service, SECRET)) as client:
            path = "/api/v1/scan/start"
            body = json.dumps(
                {
                    "scanId": "pause-scan",
                    "organizationId": "tenant-a",
                    "auth": {"accessToken": "synthetic-token"},
                    "resources": ["deals", "companies"],
                }
            ).encode()
            assert (
                client.post(
                    path, content=body, headers=signed("POST", path, body)
                ).status_code
                == 202
            )
            entered.wait(
                timeout=30
            )  # Two real Parquet/MinIO/ClickHouse workers overlapped.
            path = "/api/v1/scan/pause-scan/pause"
            assert (
                client.post(path, headers=signed("POST", path)).json()["status"]
                == "pausing"
            )
            release.set()
            wait_for(lambda: controller.get("pause-scan")["status"] == "paused")
            wait_for(lambda: service.futures["pause-scan"].done())
            assert all(
                cp["page"] == 1 and cp["cursor"] == "page-2"
                for cp in controller.get("pause-scan")["resources"].values()
            )
        # Restart retains the pause instead of accidentally scheduling work.
        restarted = BE2ScanService(
            controller,
            real_storage,
            HubSpotBE2Pipeline(
                controller, real_storage, lambda: HubSpotResourceClient(hubspot_server)
            ),
        )
        with TestClient(create_app(restarted, SECRET)) as client:
            assert not restarted.futures
            path = "/api/v1/scan/pause-scan/resume"
            assert client.post(path, headers=signed("POST", path)).status_code == 202
            wait_for(lambda: controller.get("pause-scan")["status"] == "completed")
            db = real_storage.client()
            try:
                assert db.query("SELECT count() FROM hubspot_batches").result_rows == [
                    (4,)
                ]
                assert db.query("SELECT count() FROM hubspot_records").result_rows == [
                    (4,)
                ]
            finally:
                db.close()
    finally:
        release.set()
