"""Resource adapters and page transactions shared by live workers and recovery."""

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from config import Config
from services.hubspot_api_service import HubSpotDealsAPIService

HUBSPOT_RESOURCES = {
    "contacts": (
        "/crm/v3/objects/contacts",
        "firstname,lastname,email,phone,company,lastmodifieddate",
    ),
    "companies": (
        "/crm/v3/objects/companies",
        "name,domain,industry,hs_lastmodifieddate",
    ),
    "deals": (
        "/crm/v3/objects/deals",
        "dealname,amount,dealstage,pipeline,hs_lastmodifieddate",
    ),
    "tickets": (
        "/crm/v3/objects/tickets",
        "subject,content,hs_pipeline,hs_pipeline_stage,hs_ticket_priority,hs_lastmodifieddate",
    ),
    "line_items": (
        "/crm/v3/objects/line_items",
        "name,quantity,price,amount,hs_product_id,hs_lastmodifieddate",
    ),
    "engagements": ("/engagements/v1/engagements/paged", ""),
    "pipelines": ("/crm/v3/pipelines/deals", ""),
    "owners": ("/crm/v3/owners/", ""),
}


def timestamp(value):
    if value is None:
        return None
    if isinstance(value, (int, float)) or str(value).isdigit():
        date = datetime.fromtimestamp(float(value) / 1000, timezone.utc)
    else:
        date = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return date.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def updated_at(record):
    properties = record.get("properties") or {}
    engagement = record.get("engagement") or {}
    return timestamp(
        record.get("updatedAt")
        or properties.get("hs_lastmodifieddate")
        or properties.get("lastmodifieddate")
        or engagement.get("lastUpdated")
    )


def transform_record(resource, record, scan_id, tenant_id, extracted_at):
    engagement = record.get("engagement") or {}
    identity = record.get("id", engagement.get("id"))
    if identity is None:
        raise ValueError(f"Missing {resource} record id")
    associations = record.get("associations") or {}
    companies = associations.get("companies") or {}
    company_ids = [str(item["id"]) for item in companies.get("results", [])]
    return dict(
        id=str(identity),
        resource=resource,
        properties=json.dumps(record.get("properties", record), sort_keys=True),
        payload=json.dumps(record, sort_keys=True),
        company_ids=json.dumps(company_ids),
        archived=bool(record.get("archived", False)),
        scan_id=scan_id,
        tenant_id=tenant_id,
        updated_at=updated_at(record) or extracted_at,
        extracted_at=extracted_at,
    )


class HubSpotResourceClient(HubSpotDealsAPIService):
    def page(self, token, resource, cursor, limit):
        endpoint, properties = HUBSPOT_RESOURCES[resource]
        params = {} if resource == "pipelines" else {"limit": limit}
        if properties:
            params["properties"] = properties
        if resource == "deals":
            params["associations"] = "companies"
        if cursor is not None:
            params["offset" if resource == "engagements" else "after"] = cursor
        data = self._request(token, self.base_url + endpoint, params)
        if resource == "engagements":
            next_cursor = str(data["offset"]) if data.get("hasMore") else None
        else:
            next_cursor = ((data.get("paging") or {}).get("next") or {}).get("after")
            next_cursor = str(next_cursor) if next_cursor is not None else None
        if next_cursor is not None and next_cursor == cursor:
            raise ValueError("HubSpot returned a repeated pagination cursor")
        records = data["results"]
        # Associations embedded in object lists may themselves be paginated.
        if resource == "deals":
            for record in records:
                association = (record.get("associations") or {}).get("companies") or {}
                after = ((association.get("paging") or {}).get("next") or {}).get(
                    "after"
                )
                seen = set()
                while after is not None:
                    if str(after) in seen:
                        raise ValueError("Repeated association cursor")
                    seen.add(str(after))
                    page = self._request(
                        token,
                        self.base_url
                        + f"/crm/v3/objects/deals/{record['id']}/associations/companies",
                        {"after": after, "limit": 100},
                    )
                    association.setdefault("results", []).extend(page["results"])
                    after = ((page.get("paging") or {}).get("next") or {}).get("after")
        return records, next_cursor


class HubSpotBE2Pipeline:
    def __init__(self, controller, storage, api_factory=None, after_load=None):
        self.controller = controller
        self.storage = storage
        self.api_factory = api_factory or (
            lambda: HubSpotResourceClient(
                Config.HUBSPOT_API_BASE_URL,
                Config.HUBSPOT_API_TIMEOUT,
                Config.HUBSPOT_RETRY_ATTEMPTS,
                Config.HUBSPOT_RETRY_DELAY,
            )
        )
        self.after_load = after_load  # Fault injection seam for the process-kill test.

    def run_resource(self, scan_id, resource, stopping):
        api = self.api_factory()
        try:
            token = self.controller.token(scan_id)
            while not stopping.is_set():
                state = self.controller.get(scan_id)
                cp = state["resources"][resource]
                if state["status"] != "running" or cp["done"]:
                    return
                pending = cp["pending"]
                if pending is None:
                    records, cursor = api.page(
                        token, resource, cp["cursor"], state["limit"]
                    )
                    extracted = datetime.now(timezone.utc).isoformat(
                        timespec="milliseconds"
                    )
                    newest = cp["updated_at"]
                    selected = []
                    for record in records:
                        updated = updated_at(record)
                        newest = max(filter(None, (newest, updated)), default=None)
                        # List endpoints avoid HubSpot Search's 10k-result ceiling.
                        # Inclusive replay handles equal timestamps; snapshot resources
                        # without timestamps are always loaded.
                        if (
                            not updated
                            or not cp["watermark"]
                            or updated >= cp["watermark"]
                        ):
                            selected.append(
                                transform_record(
                                    resource,
                                    record,
                                    scan_id,
                                    state["organizationId"],
                                    extracted,
                                )
                            )
                    batch = hashlib.sha256(
                        f"{state['organizationId']}\0{scan_id}\0{resource}\0{cp['page']}".encode()
                    ).hexdigest()
                    pending = dict(
                        batch_id=batch,
                        records=selected,
                        next_cursor=cursor,
                        updated_at=newest,
                        extracted_at=extracted,
                        resource=resource,
                    )
                    self.controller.mutate(
                        scan_id,
                        lambda s: s["resources"][resource].update(pending=pending),
                    )
                # A pending page is a write-ahead record. Its stable batch ID and
                # payload are replayed if killed after either sink acknowledges.
                self.storage.load(pending)
                if self.after_load:
                    self.after_load(resource, pending)
                self.controller.checkpoint(scan_id, resource, pending)
        finally:
            api.session.close()

    def run(self, scan_id, stopping):
        resources = self.controller.get(scan_id)["resources"]
        with ThreadPoolExecutor(
            max_workers=min(Config.RESOURCE_WORKERS, len(resources))
        ) as executor:
            futures = [
                executor.submit(self.run_resource, scan_id, resource, stopping)
                for resource in resources
            ]
            for future in futures:
                future.result()
        return self.controller.finish(scan_id)
