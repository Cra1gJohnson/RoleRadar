from collections.abc import Iterable

from sqlalchemy.orm import sessionmaker

from ingestion.clients.greenhouse import GreenhouseClient
from ingestion.clients.lever import LeverClient
from ingestion.config import Settings
from ingestion.database import upsert_jobs
from ingestion.schemas import NormalizedJob


def collect_jobs(settings: Settings) -> list[NormalizedJob]:
    jobs: list[NormalizedJob] = []
    clients = [
        *[GreenhouseClient(board_token=board, user_agent=settings.user_agent) for board in settings.greenhouse_boards],
        *[LeverClient(company_slug=slug, user_agent=settings.user_agent) for slug in settings.lever_company_slugs],
    ]

    try:
        for client in clients:
            jobs.extend(client.fetch_jobs())
    finally:
        for client in clients:
            client.close()

    return jobs


def run_ingestion(settings: Settings, session_factory: sessionmaker) -> int:
    jobs = collect_jobs(settings)
    with session_factory() as session:
        return upsert_jobs(session, jobs)

