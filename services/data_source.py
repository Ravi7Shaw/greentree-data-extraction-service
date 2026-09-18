from datetime import datetime, timezone
from typing import Any, Dict, Iterator

import dlt

from .hubspot_api_service import HubSpotDealsAPIService


def transform_deal(
    deal: Dict[str, Any],
    scan_id: str,
    tenant_id: str,
) -> Dict[str, Any]:
    properties = deal.get("properties") or {}

    def num(value):
        try:
            return float(value) if value not in (None, "") else None
        except (TypeError, ValueError):
            return None

    return {
        "id": str(deal.get("id")),
        "dealname": properties.get("dealname"),
        "amount": num(properties.get("amount")),
        "dealstage": properties.get("dealstage"),
        "pipeline": properties.get("pipeline"),
        "closedate": properties.get("closedate"),
        "createdate": properties.get("createdate"),
        "lastmodifieddate": properties.get("hs_lastmodifieddate"),
        "dealtype": properties.get("dealtype"),
        "description": properties.get("description"),
        "archived": bool(deal.get("archived", False)),
        "_extracted_at": datetime.now(timezone.utc).isoformat(),
        "_scan_id": scan_id,
        "_tenant_id": tenant_id,
    }


def create_deals_resource(
    api: HubSpotDealsAPIService,
    token: str,
    scan_id: str,
    tenant_id: str,
    limit: int = 100,
    properties=None,
    checkpoint_callback=None,
    resume_after=None,
):
    @dlt.resource(
        name="deals",
        write_disposition="merge",
        primary_key="id",
        columns={
            "id": {"data_type": "text"},
            "dealname": {"data_type": "text"},
            "amount": {"data_type": "double"},
            "dealstage": {"data_type": "text"},
            "pipeline": {"data_type": "text"},
            "closedate": {"data_type": "timestamp"},
            "createdate": {"data_type": "timestamp"},
            "lastmodifieddate": {"data_type": "timestamp"},
            "dealtype": {"data_type": "text"},
            "description": {"data_type": "text"},
            "archived": {"data_type": "bool"},
            "_extracted_at": {"data_type": "timestamp"},
            "_scan_id": {"data_type": "text"},
            "_tenant_id": {"data_type": "text"},
        },
    )
    def deals() -> Iterator[Dict[str, Any]]:
        for page, data in api.iter_deals(
            token,
            limit=limit,
            properties=properties,
            after=resume_after,
        ):
            results = data.get("results", [])

            for raw in results:
                yield transform_deal(
                    raw,
                    scan_id,
                    tenant_id,
                )

            if checkpoint_callback:
                next_cursor = ((data.get("paging") or {}).get("next") or {}).get(
                    "after"
                )

                checkpoint_callback(
                    page,
                    next_cursor,
                    len(results),
                )

    return deals
