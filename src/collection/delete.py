import os
from dataclasses import dataclass
from typing import Mapping, Optional

import psycopg

from env_loader import load_shared_env
from upsert import ExistingJobRow

load_shared_env()

CASCADE_CHILD_TABLES = ("green_enrich", "green_score", "green_apply")


@dataclass
class DeleteSummary:
    """Track the outcome of one stale-job cleanup run."""

    scanned_count: int = 0
    deleted_count: int = 0
    missing_count: int = 0
    failed_count: int = 0

    @property
    def success(self) -> bool:
        """Treat any delete failure as a non-successful run."""
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


async def verify_cascade_contract(conn: psycopg.AsyncConnection) -> None:
    """
    Verify that the known job tables point at green_job with ON DELETE CASCADE.

    The collection refresh deletes from green_job so child rows only disappear if the
    foreign keys are configured correctly.
    """
    async with conn.cursor() as cur:
        for child_table in CASCADE_CHILD_TABLES:
            await cur.execute(
                """
                SELECT pg_get_constraintdef(c.oid)
                FROM pg_constraint AS c
                JOIN pg_class AS child
                  ON child.oid = c.conrelid
                JOIN pg_class AS parent
                  ON parent.oid = c.confrelid
                WHERE c.contype = 'f'
                  AND child.relname = %s
                  AND parent.relname = 'green_job'
                """,
                (child_table,),
            )
            row = await cur.fetchone()
            if row is None:
                raise ValueError(
                    f"Missing foreign key from {child_table} to green_job(job_id)"
                )

            constraint_def = row[0] or ""
            if "ON DELETE CASCADE" not in constraint_def.upper():
                raise ValueError(
                    f"Foreign key for {child_table} does not use ON DELETE CASCADE: "
                    f"{constraint_def}"
                )


async def fetch_existing_job_rows(
    conn: psycopg.AsyncConnection,
    token: str,
) -> dict[int, ExistingJobRow]:
    """Load existing rows for a token keyed by indexed greenhouse_job_id."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT job_id, greenhouse_job_id, updated_at
            FROM green_job
            WHERE token = %s
            """,
            (token,),
        )
        rows = await cur.fetchall()

    return {
        row[1]: ExistingJobRow(
            job_id=row[0],
            greenhouse_job_id=row[1],
            updated_at=row[2],
        )
        for row in rows
    }


async def delete_jobs(
    conn: psycopg.AsyncConnection,
    job_ids: list[int],
) -> int:
    """Delete parent job rows so the downstream tables cascade automatically."""
    if not job_ids:
        return 0

    async with conn.cursor() as cur:
        await cur.execute(
            """
            DELETE FROM green_job
            WHERE job_id = ANY(%s)
            RETURNING job_id
            """,
            (job_ids,),
        )
        deleted_rows = await cur.fetchall()

    return len(deleted_rows)


async def delete_missing_jobs(
    conn: psycopg.AsyncConnection,
    token: str,
    response_job_ids: list[int],
    existing_jobs: Mapping[int, ExistingJobRow] | None = None,
) -> DeleteSummary:
    """Delete every db row for a token that is not present in the live response."""
    summary = DeleteSummary()

    existing_rows = dict(existing_jobs or {})
    if not existing_rows:
        existing_rows = await fetch_existing_job_rows(conn, token)

    response_id_set = set(response_job_ids)
    missing_job_ids = [
        row.job_id
        for greenhouse_job_id, row in existing_rows.items()
        if greenhouse_job_id not in response_id_set
    ]

    summary.scanned_count = len(existing_rows)
    summary.missing_count = len(missing_job_ids)
    summary.deleted_count = await delete_jobs(conn, missing_job_ids)
    return summary
