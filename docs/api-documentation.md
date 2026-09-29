# Coordinator API contract (HMAC v1)

FastAPI generates the authoritative schema at `/openapi.json` and Swagger UI at `/docs`.

Every `/api/v1/scan/*` request requires:

| Header | Value |
|---|---|
| `X-Organization-Id` | Authorized tenant, also matching the start body's `organizationId` |
| `X-Coordinator-Timestamp` | Unix epoch seconds, within 300 seconds of server time |
| `X-Coordinator-Nonce` | Unique 16–128 character value using letters, digits, `_`, `-` |
| `X-Coordinator-Signature` | Lowercase HMAC-SHA256 hex using `COORDINATOR_HMAC_SECRET` |

Canonical UTF-8 string, with newline separators and **no trailing newline**:

```text
<timestamp>
<nonce>
<UPPERCASE_METHOD>
<exact raw path, including ?query-string if present>
<organization-id>
<lowercase SHA256 hex of exact request body bytes>
```

Nonces are stored durably and rejected on replay, including after restart. Requests with bodies must sign the same bytes sent over HTTP. For GET and empty POST, hash the empty byte string. The nonce can be retried only by generating a new signed request; use the same `scanId` for idempotent creation. HTTPS is required outside a trusted local network.

`COORDINATOR_HMAC_SECRET` is a trust credential for the Coordinator, which may sign for all tenants. Individual tenants must not receive it. Read/control endpoints look up scans within the signed tenant. List/statistics only include that tenant.

| Method | Path after `/api/v1/scan` | Behavior |
|---|---|---|
| POST | `/start` | 202; enqueue selected resources or return existing scan |
| GET | `/{scan_id}/status` | Per-resource checkpoint and lifecycle state |
| POST | `/{scan_id}/pause` | 202; running/pending → pausing → paused |
| POST | `/{scan_id}/resume` | 202; paused/failed → pending → running |
| POST | `/{scan_id}/cancel` | 202; drain in-flight work, then cancelled |
| GET | `/{scan_id}/result` | Completed result location; otherwise 409 |
| GET | `/list?limit=20&offset=0` | Paginated tenant scan metadata |
| GET | `/statistics` | Tenant counts and committed record totals |
| DELETE | `/{scan_id}/remove` | Remove terminal scan metadata; retain output |

401: bad/stale/replayed signature; 403: tenant/body mismatch; 404: missing scan or different tenant; 409: conflicting lifecycle/ownership; 422: invalid body/query. Validation responses exclude submitted values so upstream tokens are not echoed. Pending page payloads and encrypted credentials are never included in scan responses.

Pause/cancel are asynchronous. Poll status until `paused`/`cancelled` before treating them as complete. Resume returns 409 while the previous worker is still exiting; retry with a new nonce. Failed scans preserve their pending page. Use a fresh scan ID for the next incremental run.

The service's built-in protocol needs to be matched to the external Coordinator; no Coordinator implementation or pre-existing signature specification was supplied with this repository.
