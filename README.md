# HubSpot Deals ETL

A Flask-based asynchronous ETL service that extracts HubSpot CRM Deals using the HubSpot API, transforms the data, and loads it into PostgreSQL using DLT.

## Features

- HubSpot Private App authentication
- HubSpot CRM Deals API v3
- Pagination using HubSpot `after` cursors
- Retry and exponential backoff for 429 and 5xx responses
- HubSpot credential validation
- DLT PostgreSQL destination
- Deal data transformation
- Tenant metadata with `_tenant_id`
- Scan lifecycle management
- Background asynchronous extraction
- Scan checkpoint/cursor tracking
- PostgreSQL persistence
- Swagger/OpenAPI documentation
- Docker Compose support
- Automated tests

## Architecture

```text
Client
  |
  | POST /api/v1/scan/start
  v
Flask API
  |
  | Background worker
  v
ExtractionService
  |
  v
HubSpot CRM API
  |
  | Paginated Deals
  v
Data Transformation
  |
  v
DLT
  |
  v
PostgreSQL
