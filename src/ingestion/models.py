from datetime import datetime, timezone

from sqlalchemy import JSON, Boolean, DateTime, String, Text, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from ingestion.schemas import NormalizedJob


class Base(DeclarativeBase):
    pass


class JobPosting(Base):
    __tablename__ = "job_postings"
    __table_args__ = (UniqueConstraint("source", "external_id", name="uq_job_posting_source_external"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    company: Mapped[str] = mapped_column(String(255), nullable=False)
    external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    location: Mapped[str | None] = mapped_column(String(500))
    team: Mapped[str | None] = mapped_column(String(255))
    employment_type: Mapped[str | None] = mapped_column(String(255))
    workplace_type: Mapped[str | None] = mapped_column(String(255))
    url: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    metadata_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")

    @classmethod
    def from_normalized(cls, job: NormalizedJob) -> "JobPosting":
        return cls(
            source=job.source,
            company=job.company,
            external_id=job.external_id,
            title=job.title,
            location=job.location,
            team=job.team,
            employment_type=job.employment_type,
            workplace_type=job.workplace_type,
            url=job.url,
            description=job.description,
            metadata_json=job.metadata,
            is_active=True,
        )

    def update_from_normalized(self, job: NormalizedJob) -> None:
        self.company = job.company
        self.title = job.title
        self.location = job.location
        self.team = job.team
        self.employment_type = job.employment_type
        self.workplace_type = job.workplace_type
        self.url = job.url
        self.description = job.description
        self.metadata_json = job.metadata
        self.last_seen_at = datetime.now(timezone.utc)
        self.is_active = True
