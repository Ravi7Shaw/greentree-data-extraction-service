import json
import threading
import time
from unittest.mock import Mock

import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from api.auth import signature
from app import create_app
from services.be2_scan_service import BE2ScanService
from services.pipeline.be2_controller import BE2PipelineController, ScanConflict
from services.pipeline.hubspot_pipeline import (
    HUBSPOT_RESOURCES,
    HubSpotBE2Pipeline,
    HubSpotResourceClient,
    transform_record,
)

SECRET = "synthetic-coordinator-secret-for-tests"


def signed(method, target, body=b"", tenant="tenant-a", nonce=None, stamp=None):
    import uuid

    stamp = stamp or str(int(time.time()))
    nonce = nonce or uuid.uuid4().hex
    return {
        "X-Organization-Id": tenant,
        "X-Coordinator-Timestamp": stamp,
        "X-Coordinator-Nonce": nonce,
        "X-Coordinator-Signature": signature(
            SECRET, method, target, tenant, stamp, nonce, body
        ),
    }


def wait_for(predicate):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.01)
    raise AssertionError("Timed out waiting for worker")


class FakeAPI:
    def __init__(self):
        self.session = Mock()
        self.calls = []

    def page(self, token, resource, cursor, limit):
        self.calls.append((resource, cursor))
        identity = "1" if cursor is None else "2"
        return [
            {
                "id": identity,
                "updatedAt": "2026-01-01T00:00:00Z",
                "properties": {"name": resource},
            }
        ], "next" if cursor is None else None


class MemoryStorage:
    def __init__(self):
        self.batches = {}
        self.lock = threading.Lock()

    def load(self, page):
        with self.lock:
            self.batches[page["batch_id"]] = page["records"]


@pytest.fixture
def controller(tmp_path):
    return BE2PipelineController(tmp_path, Fernet.generate_key())


def test_pause_resume_parallel_and_durable_replay(controller):
    entered = threading.Barrier(3)
    release = threading.Event()
    storage = MemoryStorage()
    api = FakeAPI()
    calls = []

    def after_load(resource, pending):
        if resource not in calls:
            calls.append(resource)
            entered.wait(timeout=5)
            assert release.wait(5)

    pipeline = HubSpotBE2Pipeline(controller, storage, lambda: api, after_load)
    service = BE2ScanService(controller, storage, pipeline)
    service.start()
    try:
        service.create(
            "scan", "tenant-a", "upstream-token", ["deals", "companies"], 100
        )
        entered.wait(
            timeout=5
        )  # Both independent workers reached storage concurrently.
        service.control("scan", "pause")
        release.set()
        wait_for(lambda: controller.get("scan")["status"] == "paused")
        wait_for(lambda: service.futures["scan"].done())
        cp = controller.get("scan")["resources"]
        assert all(
            item["page"] == 1 and item["cursor"] == "next" for item in cp.values()
        )
        assert len(api.calls) == 2
        service.control("scan", "resume")
        wait_for(lambda: controller.get("scan")["status"] == "completed")
        assert all(
            item["page"] == 2 and item["records"] == 2
            for item in controller.get("scan")["resources"].values()
        )
        assert len(storage.batches) == 4
    finally:
        release.set()
        service.close()


def test_storage_failure_does_not_advance_checkpoint(controller):
    api = FakeAPI()
    storage = MemoryStorage()
    storage.load = Mock(side_effect=RuntimeError("storage unavailable"))
    controller.create("scan", "tenant-a", "upstream-token", ["deals"], 100)
    controller.transition("scan", {"pending"}, "running")
    pipeline = HubSpotBE2Pipeline(controller, storage, lambda: api)
    with pytest.raises(RuntimeError):
        pipeline.run("scan", threading.Event())
    cp = controller.get("scan")["resources"]["deals"]
    assert cp["page"] == 0 and cp["cursor"] is None and cp["pending"]
    storage.load = Mock()
    pipeline.run("scan", threading.Event())
    assert api.calls == [("deals", None), ("deals", "next")]
    assert controller.get("scan")["status"] == "completed"


def test_encrypted_token_tenant_collision_and_persisted_pause(controller):
    controller.create("scan", "tenant-a", "sensitive-hubspot-token", ["deals"], 100)
    assert b"sensitive-hubspot-token" not in controller.path.read_bytes()
    assert controller.token("scan") == "sensitive-hubspot-token"
    with pytest.raises(ScanConflict):
        controller.create("scan", "tenant-b", "token", ["deals"], 100)
    with pytest.raises(ScanConflict):
        controller.create("other", "tenant-a", "token", ["deals"], 100)
    controller.transition("scan", {"pending"}, "pausing")
    service = BE2ScanService(controller, MemoryStorage())
    service.start()
    assert controller.get("scan")["status"] == "paused"
    assert not service.futures
    service.close()


def test_signed_api_auth_tenant_isolation_and_validation(controller):
    storage = MemoryStorage()
    service = BE2ScanService(
        controller, storage, HubSpotBE2Pipeline(controller, storage, FakeAPI)
    )
    with TestClient(create_app(service, SECRET)) as client:
        path = "/api/v1/scan/start"
        body = json.dumps(
            {
                "scanId": "scan",
                "organizationId": "tenant-a",
                "auth": {"accessToken": "secret-token"},
            }
        ).encode()
        assert client.post(path, content=body).status_code == 401
        headers = signed("POST", path, body)
        assert (
            client.post(path, content=body + b" ", headers=headers).status_code == 401
        )
        response = client.post(path, content=body, headers=headers)
        assert response.status_code == 202
        assert set(response.json()["resources"]) == set(HUBSPOT_RESOURCES)
        assert "secret-token" not in response.text
        assert client.post(path, content=body, headers=headers).status_code == 401
        wait_for(lambda: controller.get("scan")["status"] == "completed")
        target = "/api/v1/scan/scan/status"
        assert (
            client.get(
                target, headers=signed("GET", target, tenant="tenant-b")
            ).status_code
            == 404
        )
        assert (
            client.get(target, headers=signed("GET", target, stamp="1")).status_code
            == 401
        )
        assert client.get(target, headers=signed("GET", target)).status_code == 200
        assert client.post(
            path, content=body, headers=signed("POST", path, body)
        ).json()["duplicate"]
        invalid = json.dumps(
            {
                "scanId": "../bad",
                "organizationId": "tenant-a",
                "auth": {"accessToken": "secret-token"},
            }
        ).encode()
        response = client.post(
            path, content=invalid, headers=signed("POST", path, invalid)
        )
        assert response.status_code == 422 and "secret-token" not in response.text
        mismatch = client.post(
            path, content=body, headers=signed("POST", path, body, tenant="tenant-b")
        )
        assert mismatch.status_code == 403
        for action in ("pause", "resume", "cancel"):
            target = f"/api/v1/scan/scan/{action}"
            assert (
                client.post(target, headers=signed("POST", target)).status_code == 409
            )
        target = "/api/v1/scan/list?limit=1&offset=0"
        assert (
            client.get(target, headers=signed("GET", target)).json()["data"][0][
                "scanId"
            ]
            == "scan"
        )
        assert (
            client.get(target + "0", headers=signed("GET", target)).status_code == 401
        )


@pytest.mark.parametrize("resource", list(HUBSPOT_RESOURCES))
def test_resource_adapters(resource):
    api = HubSpotResourceClient()
    data = {"results": [{"id": "1"}], "paging": {"next": {"after": "next"}}}
    if resource == "engagements":
        data = {"results": [{"engagement": {"id": 1}}], "hasMore": True, "offset": 100}
    api._request = Mock(return_value=data)
    records, cursor = api.page("token", resource, "previous", 50)
    args = api._request.call_args.args
    assert args[1].endswith(HUBSPOT_RESOURCES[resource][0])
    assert args[2].get("offset" if resource == "engagements" else "after") == "previous"
    assert cursor == ("100" if resource == "engagements" else "next")
    row = transform_record(
        resource, records[0], "scan", "tenant", "2026-01-01T00:00:00.000+00:00"
    )
    assert row["id"] == "1"
    assert json.loads(row["payload"]) == records[0]


def test_watermark_is_inclusive_and_only_committed_on_completion(controller):
    storage = MemoryStorage()
    pipeline = HubSpotBE2Pipeline(controller, storage, FakeAPI)
    controller.create("first", "tenant-a", "token", ["deals"], 100)
    controller.transition("first", {"pending"}, "running")
    pipeline.run("first", threading.Event())
    state, _ = controller.create("second", "tenant-a", "token", ["deals"], 100)
    assert state["resources"]["deals"]["watermark"] == "2026-01-01T00:00:00.000+00:00"
    controller.transition("second", {"pending"}, "running")
    pipeline.run("second", threading.Event())
    assert controller.get("second")["resources"]["deals"]["records"] == 2


def test_process_lock(controller):
    first = BE2ScanService(controller, MemoryStorage())
    second = BE2ScanService(controller, MemoryStorage())
    first.start()
    try:
        with pytest.raises(RuntimeError, match="one API worker"):
            second.start()
    finally:
        first.close()
        second.close()


def test_removed_scan_id_cannot_replace_existing_storage_batches(controller):
    controller.create("scan", "tenant-a", "token", ["deals"], 100)
    controller.transition("scan", {"pending"}, "cancelled")
    controller.remove("scan", "tenant-a")
    with pytest.raises(KeyError):
        controller.get("scan", "tenant-a")
    assert controller.list("tenant-a") == []
    with controller.connect() as db:
        raw, token = db.execute(
            "SELECT state, token FROM scans WHERE id='scan'"
        ).fetchone()
    assert json.loads(raw) == {
        "scanId": "scan",
        "organizationId": "tenant-a",
        "status": "removed",
    }
    assert token == b""
    with pytest.raises(ScanConflict, match="new scanId"):
        controller.create("scan", "tenant-a", "token", ["deals"], 100)


def test_failed_scan_resume_cannot_overlap_another_scan(controller):
    controller.create("first", "tenant-a", "token", ["deals"], 100)
    controller.transition("first", {"pending"}, "failed")
    controller.create("second", "tenant-a", "token", ["deals"], 100)
    with pytest.raises(ScanConflict):
        controller.transition("first", {"failed"}, "pending")
    assert controller.get("first")["status"] == "failed"


def test_watermark_does_not_pass_scan_start(controller):
    controller.create("scan", "tenant-a", "token", ["deals"], 100)
    controller.mutate(
        "scan", lambda state: state.update(createdAt=1767225600, status="running")
    )
    controller.mutate(
        "scan",
        lambda state: state["resources"]["deals"].update(
            done=True, updated_at="2026-01-02T00:00:00.000+00:00"
        ),
    )
    controller.finish("scan")
    next_scan, _ = controller.create("next", "tenant-a", "token", ["deals"], 100)
    assert (
        next_scan["resources"]["deals"]["watermark"] == "2026-01-01T00:00:00.000+00:00"
    )
