from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from api.auth import coordinator_tenant
from api.schemas import StartScan

router = APIRouter(prefix="/api/v1/scan")
Tenant = Annotated[str, Depends(coordinator_tenant)]


def public(state):
    return {
        "success": True,
        **{k: v for k, v in state.items() if k != "resources"},
        "recordsExtracted": sum(cp["records"] for cp in state["resources"].values()),
        "resources": {
            name: {k: v for k, v in cp.items() if k != "pending"}
            for name, cp in state["resources"].items()
        },
    }


@router.post("/start", status_code=202)
def start_scan(data: StartScan, request: Request, tenant: Tenant):
    if data.organizationId != tenant:
        raise HTTPException(403, "Signed tenant does not match organizationId")
    state, duplicate = request.app.state.scans.create(
        data.scanId,
        tenant,
        data.auth.accessToken.get_secret_value(),
        data.resources,
        data.filters.limit,
    )
    return {**public(state), "duplicate": duplicate}


@router.get("/list")
def list_scans(
    request: Request,
    tenant: Tenant,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    states = request.app.state.scans.controller.list(tenant)
    return {
        "success": True,
        "data": [public(s) for s in states[offset : offset + limit]],
        "limit": limit,
        "offset": offset,
    }


@router.get("/statistics")
def statistics(request: Request, tenant: Tenant):
    states = request.app.state.scans.controller.list(tenant)
    counts = {}
    for state in states:
        counts[state["status"]] = counts.get(state["status"], 0) + 1
    return {
        "success": True,
        "totalScans": len(states),
        "byStatus": counts,
        "totalRecordsExtracted": sum(public(s)["recordsExtracted"] for s in states),
    }


@router.get("/{scan_id}/status")
def status(scan_id: str, request: Request, tenant: Tenant):
    return public(request.app.state.scans.controller.get(scan_id, tenant))


@router.get("/{scan_id}/result")
def result(scan_id: str, request: Request, tenant: Tenant):
    state = request.app.state.scans.controller.get(scan_id, tenant)
    if state["status"] != "completed":
        raise HTTPException(409, "Scan is not completed")
    return {
        **public(state),
        "table": "hubspot_records",
        "view": "v_hubspot_deal_pipeline",
        "message": "Query ClickHouse with a tenant_id predicate; Parquet is stored in MinIO.",
    }


@router.post("/{scan_id}/pause", status_code=202)
def pause(scan_id: str, request: Request, tenant: Tenant):
    request.app.state.scans.controller.get(scan_id, tenant)
    return public(request.app.state.scans.control(scan_id, "pause"))


@router.post("/{scan_id}/resume", status_code=202)
def resume(scan_id: str, request: Request, tenant: Tenant):
    request.app.state.scans.controller.get(scan_id, tenant)
    return public(request.app.state.scans.control(scan_id, "resume"))


@router.post("/{scan_id}/cancel", status_code=202)
def cancel(scan_id: str, request: Request, tenant: Tenant):
    request.app.state.scans.controller.get(scan_id, tenant)
    return public(request.app.state.scans.control(scan_id, "cancel"))


@router.delete("/{scan_id}/remove")
def remove(scan_id: str, request: Request, tenant: Tenant):
    request.app.state.scans.controller.remove(scan_id, tenant)
    return {"success": True, "scanId": scan_id}
