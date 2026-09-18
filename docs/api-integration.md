# HubSpot Deals API Integration

## API
HubSpot CRM API v3 deals endpoint: `GET https://api.hubapi.com/crm/v3/objects/deals`.

Authentication uses a HubSpot private-app access token in `Authorization: Bearer <token>`.

Supported query parameters: `limit` (max 100), `after`, `properties`, and `archived`.

The service handles cursor pagination, 429 rate limits, transient 5xx errors, timeouts, malformed responses, and 401/403/404 errors. The assessment specifies a HubSpot rate limit of 150 requests/10 seconds; the client retries with `Retry-After` when available and exponential backoff otherwise.

## Example
```bash
curl 'https://api.hubapi.com/crm/v3/objects/deals?limit=100&properties=dealname,amount,dealstage,pipeline,closedate' \
  -H 'Authorization: Bearer $HUBSPOT_ACCESS_TOKEN'
```

## Required scope
`crm.objects.deals.read`

## Response shape
Each record contains an `id`, `properties`, `createdAt`, `updatedAt`, and `archived`; pagination is exposed through `paging.next.after`.

## Properties
The extractor requests common deal fields including `dealname`, `amount`, `dealstage`, `pipeline`, `closedate`, `createdate`, `hs_lastmodifieddate`, `dealtype`, and `description`. Additional properties can be passed through the data-source configuration.
