# HubSpot BE-2 ingestion service

FastAPI → authenticated Coordinator request → persistent scan supervisor → parallel HubSpot resources → dlt filesystem/S3 destination → compressed Parquet in MinIO → ClickHouse tables and analytical views.

Supported resources: **Contacts, Companies, Deals, Tickets, LineItems (`line_items`), Engagements, deal Pipelines, Owners**. The live API uses `BE2ScanService`; there is one ingestion implementation.

## Run

Python **3.12.11**, Docker with Compose, and a HubSpot private app token with read access to the requested resources are required. For local Python commands:

```bash
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
```

Generate local secrets and put them in `.env` (never commit the values):

```bash
.venv/bin/python -c 'import secrets; print(secrets.token_hex(32))'  # COORDINATOR_HMAC_SECRET
.venv/bin/python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'  # TOKEN_ENCRYPTION_KEY
.venv/bin/python -c 'import secrets; print(secrets.token_urlsafe(32))'  # CLICKHOUSE_PASSWORD
./run.sh
```

`run.sh` starts MinIO, ClickHouse and the API, with readiness checks. Open `http://localhost:5200/docs`; the generated contract is `/openapi.json`. Health is `/api/health` (API/state readiness, not a provider connectivity check). `./stop.sh` stops the stack without deleting volumes.

For host development, start only dependencies with `docker compose up -d --wait minio clickhouse`, then `.venv/bin/python app.py`. Use **one ASGI worker**, no reload process while ingesting. A filesystem lock enforces one owner per `STATE_DIR`; parallelism happens across scans and resources inside that owner. Compose persists `/state` and restarts the service after a crash.

## Signed API

All scan endpoints require Coordinator HMAC authentication. The HubSpot access token remains an upstream credential; it does not authorize access to this service. It is encrypted in the state journal with `TOKEN_ENCRYPTION_KEY`. Keep that key unchanged across restarts and back it up separately from state.

The HMAC v1 contract is documented in [API documentation](docs/api-documentation.md). It is a defined integration contract; an existing Coordinator must implement the same canonical string and headers.

Create a private JSON file outside Git with:

```json
{
  "scanId": "hubspot-001",
  "organizationId": "tenant-001",
  "auth": {"accessToken": "YOUR_HUBSPOT_PRIVATE_APP_TOKEN"},
  "resources": ["contacts", "companies", "deals", "tickets", "line_items", "engagements", "pipelines", "owners"],
  "filters": {"limit": 100}
}
```

Omitting `resources` selects all eight. Legacy `type: "deals"` selects one resource; do not combine `type` and `resources`.

```bash
.venv/bin/python scripts/coordinator_request.py POST /api/v1/scan/start --tenant tenant-001 --body /private/path/scan.json
.venv/bin/python scripts/coordinator_request.py POST /api/v1/scan/hubspot-001/pause --tenant tenant-001
.venv/bin/python scripts/coordinator_request.py GET /api/v1/scan/hubspot-001/status --tenant tenant-001
.venv/bin/python scripts/coordinator_request.py POST /api/v1/scan/hubspot-001/resume --tenant tenant-001
```

Pause returns `pausing` while in-flight pages finish, then `paused`. Resume continues each resource at its next committed cursor. Cancellation also drains in-flight pages; committed output is retained. A removed scan ID stays reserved to protect stored batch identity. A repeated scan ID returns the existing scan; another tenant cannot reuse it. Overlapping active scans of the same tenant/resource are rejected.

## Data and recovery

- dlt writes Parquet directly to MinIO using signed S3 requests at `s3://<bucket>/hubspot/<resource>/year=YYYY/month=MM/tenant=<hash>/<batch-id>/*.parquet`.
- The SQLite WAL journal stores per-resource next cursor, page/record offsets, updated-at watermark, and a pending page payload. It uses full synchronous transactions. dlt's working directories are isolated per batch; the journal is the custom incremental/recovery state authority.
- A page becomes committed only after Parquet landing **and** ClickHouse loading succeed. A killed worker replays the same persisted payload and batch ID. ClickHouse atomically replaces that batch's partition from a staging table, so ambiguous acknowledgement/replay cannot append duplicate rows.
- `hubspot_records` chooses the latest record per `(tenant_id, resource, id)` across scans. New or changed properties remain available in JSON envelopes without lossy flattening. `v_hubspot_deal_pipeline` joins deals, companies and deal pipelines within the same tenant; deals without companies remain visible. Amounts are parsed from HubSpot strings.
- Incremental runs traverse HubSpot list pages and filter by updated-at timestamps locally, inclusively. Committed watermarks are capped at scan creation time so a later update to an already-read page is eligible on the next scan. They do not rely on the Search API's result limit. Owners, pipelines or other records without timestamps are refreshed as snapshots. Scans are not point-in-time HubSpot snapshots; deleted/archived object reconciliation is not implemented.
- Startup automatically recovers `pending`/`running` scans and preserves durable pauses/cancellations. Resume retries a `failed` scan's pending page after dependencies recover.

Query with your ClickHouse client:

```sql
SELECT tenant_id, deal_id, deal_name, amount, company_name, pipeline_name
FROM analytics.v_hubspot_deal_pipeline
WHERE tenant_id = 'tenant-001';
```

ClickHouse access is for trusted analytical clients. Configure tenant-specific database roles/views before granting direct access to individual tenants.

## Demonstrate acceptance criteria

```bash
.venv/bin/python -m pytest -q -m 'not integration'
# Start MinIO and ClickHouse first; tests load their settings from .env.
BE2_INTEGRATION=1 .venv/bin/python -m pytest -q -m integration
```

Integration tests use synthetic HubSpot HTTP responses and **real dlt, MinIO and ClickHouse**. They verify every resource, compressed Parquet, view joins, concurrent workers, API pause/resume across restart, and an actual `SIGKILL` between ClickHouse acknowledgement and checkpoint commit. A fresh supervisor recovers without an API resume request; physical and logical row counts are checked. Each test uses and cleans its own bucket/database.

For a manual worker crash during a real scan, run `docker compose kill -s SIGKILL service`, then `docker compose up -d service`. Use signed status requests and query the view after recovery. Explicit Compose kill requires the explicit start command; unplanned exits are handled by `restart: unless-stopped`.

See [requirement audit](docs/be2-audit.md), [storage schema](docs/database-schema.md), and [verification evidence](test-results/README.md).

## Operational scope

This implementation uses one durable service instance and bounded thread pools. It is not a distributed scheduler. One ClickHouse partition per page provides deterministic replay but requires a retention/compaction policy before high-volume deployment. Keep state on a persistent local filesystem; losing both the state volume and its encryption key prevents recovery. Source scopes, actual HubSpot account behavior, and compatibility with an external Coordinator must be verified with those systems before deployment.
