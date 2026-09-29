from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from services.pipeline.hubspot_pipeline import HUBSPOT_RESOURCES

Resource = Literal[
    "contacts",
    "companies",
    "deals",
    "tickets",
    "line_items",
    "engagements",
    "pipelines",
    "owners",
]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class HubSpotAuth(StrictModel):
    accessToken: SecretStr = Field(min_length=1)


class ScanFilters(StrictModel):
    limit: int = Field(default=100, ge=1, le=100, strict=True)


class StartScan(StrictModel):
    scanId: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")
    organizationId: str = Field(min_length=1, max_length=128)
    auth: HubSpotAuth
    resources: list[Resource] = Field(
        default_factory=lambda: list(HUBSPOT_RESOURCES), min_length=1, max_length=8
    )
    type: Resource | None = None  # Compatibility for existing single-resource clients.
    filters: ScanFilters = Field(default_factory=ScanFilters)

    @model_validator(mode="after")
    def select_resources(self):
        if self.type:
            if "resources" in self.model_fields_set:
                raise ValueError("Specify type or resources, not both")
            self.resources = [self.type]
        if len(set(self.resources)) != len(self.resources):
            raise ValueError("resources must be unique")
        return self
