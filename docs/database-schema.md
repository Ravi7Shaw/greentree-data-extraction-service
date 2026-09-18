# Database Schema

The DLT destination is PostgreSQL. Deal rows use HubSpot `id` as the primary key and are stored in the configured `hubspot_deals` dataset.

| Field | PostgreSQL type | Purpose |
|---|---|---|
| id | TEXT PRIMARY KEY | HubSpot deal ID |
| dealname | TEXT | Deal name |
| amount | NUMERIC | Monetary amount |
| dealstage | TEXT | Deal stage |
| pipeline | TEXT | Pipeline |
| closedate | TIMESTAMPTZ/TEXT | Source close date |
| createdate | TIMESTAMPTZ/TEXT | Source creation date |
| lastmodifieddate | TIMESTAMPTZ/TEXT | Source modification date |
| dealtype | TEXT | Deal type |
| description | TEXT | Description |
| archived | BOOLEAN | HubSpot archived flag |
| _extracted_at | TIMESTAMPTZ | ETL extraction timestamp |
| _scan_id | TEXT | Extraction scan identifier |
| _tenant_id | TEXT | HubSpot portal/tenant identifier |

Recommended indexes: `(_tenant_id)`, `(_tenant_id, dealstage)`, `(_tenant_id, closedate)`, and `(_tenant_id, _extracted_at)`.

Multi-tenant isolation is enforced by carrying `_tenant_id` on every extracted record and filtering API results by the tenant associated with each scan.

Conceptual DDL:
```sql
CREATE TABLE deals (
  id TEXT PRIMARY KEY,
  dealname TEXT,
  amount NUMERIC,
  dealstage TEXT,
  pipeline TEXT,
  closedate TIMESTAMPTZ,
  createdate TIMESTAMPTZ,
  lastmodifieddate TIMESTAMPTZ,
  dealtype TEXT,
  description TEXT,
  archived BOOLEAN NOT NULL DEFAULT FALSE,
  _extracted_at TIMESTAMPTZ NOT NULL,
  _scan_id TEXT NOT NULL,
  _tenant_id TEXT NOT NULL
);
CREATE INDEX ix_deals_tenant_stage ON deals (_tenant_id, dealstage);
CREATE INDEX ix_deals_tenant_close ON deals (_tenant_id, closedate);
```
