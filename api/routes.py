import logging
import threading

from flask import Blueprint, request, jsonify
from marshmallow import ValidationError

from models.models import SessionLocal, Scan
from api.schemas import StartScanSchema, PaginationSchema
from services.extraction_service import ExtractionService

bp = Blueprint("api", __name__)
log = logging.getLogger(__name__)


def _json_error(message, code=400):
    return jsonify({"success": False, "message": message}), code


def _run_scan(scan_id, token, tenant_id):
    session = SessionLocal()
    scan = session.get(Scan, scan_id)

    if not scan:
        session.close()
        return

    filters = scan.metadata_json.get("filters", {}) if scan.metadata_json else {}
    limit = filters.get("limit", 100)

    scan.status = "running"
    session.commit()
    session.close()

    def checkpoint(page, cursor, count):
        session = SessionLocal()

        try:
            current_scan = session.get(Scan, scan_id)

            if current_scan:
                current_scan.cursor = cursor
                current_scan.records_extracted = (
                    current_scan.records_extracted or 0
                ) + count
                session.commit()
        finally:
            session.close()

    try:
        result = ExtractionService().run(
            token,
            scan_id,
            tenant_id,
            limit=limit,
            checkpoint_callback=checkpoint,
        )

        session = SessionLocal()

        try:
            current_scan = session.get(Scan, scan_id)

            if current_scan and current_scan.status != "cancelled":
                current_scan.status = "completed"
                current_scan.metadata_json = {
                    **(current_scan.metadata_json or {}),
                    **result,
                }
                session.commit()
        finally:
            session.close()

    except Exception as exc:
        log.exception("scan failed")

        session = SessionLocal()

        try:
            current_scan = session.get(Scan, scan_id)

            if current_scan:
                current_scan.status = "failed"
                current_scan.error = str(exc)
                session.commit()
        finally:
            session.close()


@bp.post("/v1/scan/start")
def start_scan():
    try:
        data = StartScanSchema().load(request.get_json(force=True) or {})
    except (ValidationError, ValueError) as exc:
        return _json_error(str(exc), 400)

    auth = data.get("auth") or {}
    token = auth.get("accessToken")

    if not token:
        return _json_error("auth.accessToken is required", 400)

    filters = data.get("filters") or {}
    limit = filters.get("limit", 100)

    try:
        limit = min(max(int(limit), 1), 100)
    except (TypeError, ValueError):
        return _json_error("filters.limit must be an integer between 1 and 100", 400)

    filters["limit"] = limit
    data["filters"] = filters

    session = SessionLocal()

    try:
        existing = session.get(Scan, data["scanId"])

        if (
            existing
            and existing.tenant_id == data["organizationId"]
            and existing.status in ("pending", "running")
        ):
            return jsonify(
                {
                    "success": True,
                    "scanId": existing.id,
                    "status": existing.status,
                    "duplicate": True,
                }
            ), 202

        if existing and existing.tenant_id != data["organizationId"]:
            return _json_error("scanId already belongs to another tenant", 409)

        scan = Scan(
            id=data["scanId"],
            tenant_id=data["organizationId"],
            status="pending",
            metadata_json={
                "type": data["type"],
                "filters": filters,
            },
        )

        session.add(scan)
        session.commit()

        scan_id = scan.id
        tenant_id = scan.tenant_id

        threading.Thread(
            target=_run_scan,
            args=(scan_id, token, tenant_id),
            daemon=True,
        ).start()

        return jsonify(
            {
                "success": True,
                "scanId": scan_id,
                "status": "pending",
            }
        ), 202

    finally:
        session.close()


@bp.get("/v1/scan/<scan_id>/status")
def status(scan_id):
    session = SessionLocal()
    scan = session.get(Scan, scan_id)
    session.close()

    if not scan:
        return _json_error("Scan not found", 404)

    return jsonify(
        {
            "success": True,
            "scanId": scan.id,
            "organizationId": scan.tenant_id,
            "status": scan.status,
            "recordsExtracted": scan.records_extracted,
            "cursor": scan.cursor,
            "error": scan.error,
        }
    )


@bp.post("/v1/scan/<scan_id>/cancel")
def cancel(scan_id):
    session = SessionLocal()
    scan = session.get(Scan, scan_id)

    if not scan:
        session.close()
        return _json_error("Scan not found", 404)

    if scan.status not in ("pending", "running"):
        session.close()
        return _json_error(f"Cannot cancel scan in {scan.status} state", 409)

    scan.status = "cancelled"
    session.commit()
    session.close()

    return jsonify(
        {
            "success": True,
            "scanId": scan_id,
            "status": "cancelled",
        }
    )


@bp.get("/v1/scan/list")
def list_scans():
    try:
        data = PaginationSchema().load(request.args.to_dict())
    except ValidationError as exc:
        return _json_error(str(exc), 400)

    session = SessionLocal()

    try:
        query = session.query(Scan).order_by(Scan.created_at.desc())

        if data.get("organizationId"):
            query = query.filter_by(tenant_id=data["organizationId"])

        rows = query.offset(data["offset"]).limit(data["limit"]).all()

        output = [
            {
                "scanId": scan.id,
                "organizationId": scan.tenant_id,
                "status": scan.status,
                "recordsExtracted": scan.records_extracted,
                "createdAt": (scan.created_at.isoformat() if scan.created_at else None),
            }
            for scan in rows
        ]

        return jsonify(
            {
                "success": True,
                "data": output,
                "limit": data["limit"],
                "offset": data["offset"],
            }
        )

    finally:
        session.close()


@bp.get("/v1/scan/statistics")
def statistics():
    session = SessionLocal()

    try:
        rows = session.query(Scan).all()
    finally:
        session.close()

    counts = {}

    for scan in rows:
        counts[scan.status] = counts.get(scan.status, 0) + 1

    return jsonify(
        {
            "success": True,
            "totalScans": len(rows),
            "byStatus": counts,
            "totalRecordsExtracted": sum(scan.records_extracted or 0 for scan in rows),
        }
    )


@bp.get("/v1/scan/<scan_id>/result")
def result(scan_id):
    session = SessionLocal()
    scan = session.get(Scan, scan_id)
    session.close()

    if not scan:
        return _json_error("Scan not found", 404)

    if scan.status != "completed":
        return _json_error(f"Scan not completed. Current status: {scan.status}", 409)

    return jsonify(
        {
            "success": True,
            "scanId": scan_id,
            "dataset": (
                scan.metadata_json.get("dataset") if scan.metadata_json else None
            ),
            "recordsExtracted": scan.records_extracted,
            "message": (
                "Results are stored by DLT in PostgreSQL; "
                "query the deals table for records."
            ),
        }
    )


@bp.delete("/v1/scan/<scan_id>/remove")
def remove(scan_id):
    session = SessionLocal()
    scan = session.get(Scan, scan_id)

    if not scan:
        session.close()
        return _json_error("Scan not found", 404)

    if scan.status in ("pending", "running"):
        session.close()
        return _json_error("Cancel the active scan before removal", 409)

    session.delete(scan)
    session.commit()
    session.close()

    return jsonify(
        {
            "success": True,
            "scanId": scan_id,
        }
    )


@bp.get("/health")
def health():
    try:
        from sqlalchemy import text

        session = SessionLocal()
        session.execute(text("SELECT 1"))
        session.close()

        return jsonify(
            {
                "status": "healthy",
                "database": "healthy",
            }
        )

    except Exception:
        log.exception("Health check failed")
        return jsonify(
            {
                "status": "unhealthy",
                "database": "unhealthy",
                "error": "Internal server error",
            }
        ), 503


@bp.get("/stats")
def stats():
    return statistics()
