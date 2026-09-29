# BE-2 requirement audit

Baseline reviewed: `50619e55f6e1de928e841c523e748fd4cbac1cb3`.

| Requirement | Baseline | Updated implementation |
|---|---|---|
| FastAPI | Flask app | FastAPI with generated OpenAPI and lifespan supervisor |
| Coordinator/HMAC | No service authentication | Timestamped, nonce-protected, tenant-bound HMAC; protocol documented |
| All eight resources | Live path only Deals; unwired partial pipeline | All eight wired through live API; corrected engagement endpoint and metadata preservation |
| dlt + Parquet + MinIO | PostgreSQL live destination; incomplete alternatives | dlt filesystem S3 destination, compressed Parquet in structured MinIO paths |
| ClickHouse loading/views | Unwired/incomplete; company join and tenant issues | Landed Parquet loader, atomic page replacement, tenant-safe current-record and deal/company/pipeline views |
| Pause/resume | No live routes; service imports missing class names | Authenticated routes, per-resource page checkpoint, durable pause across restart |
| Persistent incremental state | Narrow cursor counters; unsafe pre-load checkpoint | SQLite WAL journal, encrypted upstream credential, pending page and updated-at watermarks |
| Worker crash recovery | Not demonstrated; checkpoint could precede storage | Startup recovery; real SIGKILL test after storage acknowledgement, no duplicate replay insertion |
| Parallel execution | Not wired | Independent resource thread pool; overlapping workers verified |
| Tenant scan-ID safety/retries | Useful existing behavior | Retained and expanded; fixed simultaneous retry-header precedence |

## Verification boundaries

The automated tests use real dlt, MinIO and ClickHouse with synthetic HTTP HubSpot responses. They verify storage and lifecycle behavior, including process death. A real HubSpot account and external Coordinator were not supplied; their scopes and signature compatibility require a connected-system smoke test. The implementation defines an explicit HMAC contract rather than assuming an undocumented Coordinator format.

The deployment model is one service process with a durable state volume, bounded threads, and restart supervision. It does not provide distributed scheduling, deleted-object reconciliation, or a high-volume partition compactor.
