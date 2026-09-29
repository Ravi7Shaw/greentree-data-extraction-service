# HubSpot adapters

The shared requests client retains cursor pagination and bounded retries for 429/5xx and transport failures. `Retry-After` (seconds or HTTP date) takes precedence over `X-HubSpot-RateLimit-Interval-Milliseconds`. Each resource worker owns its own session.

| Resource | Endpoint | Pagination |
|---|---|---|
| contacts | `/crm/v3/objects/contacts` | `paging.next.after` |
| companies | `/crm/v3/objects/companies` | `paging.next.after` |
| deals | `/crm/v3/objects/deals` | `paging.next.after`; company associations requested and their continuation pages read |
| tickets | `/crm/v3/objects/tickets` | `paging.next.after` |
| line_items | `/crm/v3/objects/line_items` | `paging.next.after` |
| engagements | `/engagements/v1/engagements/paged` | `hasMore` / `offset`; legacy engagement payload retained |
| pipelines | `/crm/v3/pipelines/deals` | Deal pipeline definitions, normally one response |
| owners | `/crm/v3/owners/` | `paging.next.after` |

The nonexistent generic CRM v3 `/objects/engagements` endpoint is not used. Supply a private app token with account access and read scopes for every selected resource; availability varies with the HubSpot account. Secrets are passed as bearer credentials to HubSpot and stored encrypted for restart recovery.

`HUBSPOT_API_BASE_URL` permits a synthetic local HTTP fixture in tests. The application never substitutes an emulator for a real account automatically. No actual HubSpot credentials are needed for the integration suite; that suite does not certify a real account's scopes or live endpoint behavior.

Updated-at values are normalized to UTC milliseconds. Missing timestamps result in snapshot refresh. Incremental filtering traverses list pages rather than calling Search; it reduces output volume, not API request count. Archived/deleted source reconciliation is outside this implementation.
