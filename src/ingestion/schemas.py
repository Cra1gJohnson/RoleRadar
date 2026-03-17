from pydantic import BaseModel, Field


class NormalizedJob(BaseModel):
    source: str
    company: str
    external_id: str
    title: str
    location: str | None = None
    team: str | None = None
    employment_type: str | None = None
    workplace_type: str | None = None
    url: str
    description: str | None = None
    metadata: dict = Field(default_factory=dict)

