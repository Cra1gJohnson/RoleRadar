import os
from datetime import datetime
from dataclasses import dataclass
from typing import Mapping, Optional

import psycopg

from env_loader import load_shared_env
from collection.archive.normalization import NormalizedJobRow

load_shared_env()


@dataclass(frozen=True)
class ExistingJobRow:
    """Existing database row fields used for comparison and updates."""

    job_id: int
    greenhouse_job_id: int
    updated_at: Optional[datetime]
    united_states: Optional[bool]


@dataclass
class UpsertSummary:
    """Summary of job writes performed for one board payload."""

    inserted_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    failed_count: int = 0

    @property
    def success(self) -> bool:
        """Treat any per-row failure as a non-successful run."""
        return self.failed_count == 0


async def db_connect() -> psycopg.AsyncConnection:
    """Create an async PostgreSQL connection using the shared env-based settings."""
    return await psycopg.AsyncConnection.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        autocommit=False,
    )


async def fetch_existing_jobs(
    conn: psycopg.AsyncConnection,
    token: str,
    greenhouse_job_ids: list[int],
) -> dict[int, ExistingJobRow]:
    """Load existing rows for a token keyed by indexed greenhouse_job_id."""
    if not greenhouse_job_ids:
        return {}

    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT job_id, greenhouse_job_id, updated_at, united_states
            FROM green_job
            WHERE token = %s
              AND greenhouse_job_id = ANY(%s)
            """,
            (token, greenhouse_job_ids),
        )
        rows = await cur.fetchall()

    return {
        row[1]: ExistingJobRow(
            job_id=row[0],
            greenhouse_job_id=row[1],
            updated_at=row[2],
            united_states=row[3],
        )
        for row in rows
    }


async def insert_job(
    conn: psycopg.AsyncConnection,
    token: str,
    snapshot_id: int,
    job: NormalizedJobRow,
) -> None:
    """Insert a fully normalized Greenhouse job row."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO green_job (
                snapshot_id,
                token,
                greenhouse_job_id,
                company_name,
                title,
                location,
                united_states,
                url,
                first_fetched_at,
                updated_at,
                candidate,
                enriched
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s)
            """,
            (
                snapshot_id,
                token,
                job.greenhouse_job_id,
                job.company_name,
                job.title,
                job.location,
                job.united_states,
                job.url,
                job.updated_at,
                None,
                False,
            ),
        )


async def update_job(
    conn: psycopg.AsyncConnection,
    existing_job_id: int,
    token: str,
    snapshot_id: int,
    job: NormalizedJobRow,
) -> None:
    """Update all mutable fields for an existing Greenhouse job row."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE green_job
            SET snapshot_id = %s,
                token = %s,
                greenhouse_job_id = %s,
                company_name = %s,
                title = %s,
                location = %s,
                united_states = %s,
                url = %s,
                updated_at = %s
            WHERE job_id = %s
            """,
            (
                snapshot_id,
                token,
                job.greenhouse_job_id,
                job.company_name,
                job.title,
                job.location,
                job.united_states,
                job.url,
                job.updated_at,
                existing_job_id,
            ),
        )


async def upsert_jobs(
    conn: psycopg.AsyncConnection,
    token: str,
    snapshot_id: int,
    jobs: list[NormalizedJobRow],
    existing_jobs: Mapping[int, ExistingJobRow] | None = None,
) -> UpsertSummary:
    """
    Normalize a board payload and write the response jobs into green_job.

    Rows present in both the database and the response are updated only when the
    upstream timestamp changed.
    """
    summary = UpsertSummary()
    if not jobs:
        return summary

    existing_rows = dict(existing_jobs or {})
    if not existing_rows:
        existing_rows = await fetch_existing_jobs(
            conn,
            token,
            [job.greenhouse_job_id for job in jobs],
        )

    for job in jobs:
        existing_job = existing_rows.get(job.greenhouse_job_id)
        if (
            existing_job is not None
            and existing_job.updated_at == job.updated_at
            and existing_job.united_states == job.united_states
        ):
            summary.skipped_count += 1
            continue

        if existing_job is None:
            await insert_job(conn, token, snapshot_id, job)
            summary.inserted_count += 1
        else:
            await update_job(conn, existing_job.job_id, token, snapshot_id, job)
            summary.updated_count += 1

    return summary
