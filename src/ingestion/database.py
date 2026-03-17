from collections.abc import Iterable

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from ingestion.models import Base, JobPosting
from ingestion.schemas import NormalizedJob


def create_session_factory(database_url: str) -> sessionmaker[Session]:
    engine = create_engine(database_url, future=True)
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)


def create_schema(database_url: str) -> None:
    engine = create_engine(database_url, future=True)
    Base.metadata.create_all(engine)


def upsert_jobs(session: Session, jobs: Iterable[NormalizedJob]) -> int:
    count = 0
    seen_pairs: set[tuple[str, str]] = set()

    for job in jobs:
        seen_pairs.add((job.source, job.external_id))
        existing = session.execute(
            select(JobPosting).where(
                JobPosting.source == job.source,
                JobPosting.external_id == job.external_id,
            )
        ).scalar_one_or_none()

        if existing is None:
            session.add(JobPosting.from_normalized(job))
        else:
            existing.update_from_normalized(job)

        count += 1

    if seen_pairs:
        sources = {source for source, _ in seen_pairs}
        existing_jobs = session.execute(
            select(JobPosting).where(JobPosting.source.in_(sources))
        ).scalars()
        for record in existing_jobs:
            if (record.source, record.external_id) not in seen_pairs:
                record.is_active = False

    session.commit()
    return count

