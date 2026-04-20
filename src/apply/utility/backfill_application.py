import argparse
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

import psycopg

SRC_ROOT = Path(__file__).resolve().parents[2]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env

load_shared_env()


@dataclass(frozen=True)
class SubmittedJobRow:
    """A submitted green_apply row joined to its source Greenhouse job id."""

    job_id: int
    source_job_id: int


@dataclass
class BackfillSummary:
    """Track the outcome of one application backfill run."""

    discovered: int = 0
    inserted: int = 0
    skipped: int = 0
    failed: int = 0
    missing_job_ids: list[int] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """Return True when no row insertions failed."""
        return self.failed == 0


def db_connect(autocommit: bool = False) -> psycopg.Connection:
    """Create a PostgreSQL connection using the shared env-based settings."""
    return psycopg.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        autocommit=autocommit,
    )


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the application backfill utility."""
    parser = argparse.ArgumentParser(
        description=(
            "Backfill submitted Greenhouse applications into the persistent application table."
        )
    )
    return parser.parse_args()


def fetch_submitted_job_rows(conn: psycopg.Connection) -> list[SubmittedJobRow]:
    """Load submitted jobs and their Greenhouse source ids from the queue."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                ga.job_id,
                gj.greenhouse_job_id
            FROM green_apply AS ga
            JOIN green_job AS gj
              ON gj.job_id = ga.job_id
            WHERE ga.submitted_at IS NOT NULL
            ORDER BY ga.job_id
            """
        )
        rows = cur.fetchall()

    submitted_rows: list[SubmittedJobRow] = []
    for row in rows:
        job_id, source_job_id = row
        if not isinstance(job_id, int) or job_id <= 0:
            raise ValueError(f"Invalid green_apply.job_id returned from database: {job_id!r}")
        if not isinstance(source_job_id, int) or source_job_id <= 0:
            raise ValueError(
                "Invalid green_job.greenhouse_job_id returned from database: "
                f"{source_job_id!r}"
            )
        submitted_rows.append(SubmittedJobRow(job_id=job_id, source_job_id=source_job_id))

    return submitted_rows


def fetch_existing_application_source_job_ids(conn: psycopg.Connection) -> set[int]:
    """Load source ids already present in the persistent application table."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT source_job_id
            FROM application
            WHERE source_job_id IS NOT NULL
            """
        )
        rows = cur.fetchall()

    existing_ids: set[int] = set()
    for row in rows:
        source_job_id = row[0]
        if isinstance(source_job_id, int) and source_job_id > 0:
            existing_ids.add(source_job_id)
    return existing_ids


def fetch_source_job_id(conn: psycopg.Connection, job_id: int) -> int:
    """Resolve the Greenhouse source job id for one queued job."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT gj.greenhouse_job_id
            FROM green_job AS gj
            WHERE gj.job_id = %s
            """,
            (job_id,),
        )
        row = cur.fetchone()

    if row is None:
        raise RuntimeError(f"job_id={job_id} was not found in green_job")

    source_job_id = row[0]
    if not isinstance(source_job_id, int) or source_job_id <= 0:
        raise RuntimeError(
            f"job_id={job_id} has an invalid greenhouse_job_id: {source_job_id!r}"
        )
    return source_job_id


def application_source_job_exists(conn: psycopg.Connection, source_job_id: int) -> bool:
    """Return True when the persistent application row already exists."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM application
            WHERE source_job_id = %s
            LIMIT 1
            """,
            (source_job_id,),
        )
        row = cur.fetchone()

    return row is not None


def find_submitted_job_ids_missing_application(conn: psycopg.Connection) -> list[int]:
    """Return submitted green_apply job ids that do not yet exist in application."""
    submitted_rows = fetch_submitted_job_rows(conn)
    existing_source_ids = fetch_existing_application_source_job_ids(conn)

    missing_job_ids = [
        row.job_id
        for row in submitted_rows
        if row.source_job_id not in existing_source_ids
    ]
    return missing_job_ids


def insert_application_row(conn: psycopg.Connection, job_id: int) -> bool:
    """Insert one submitted job into the persistent application table."""
    source_job_id = fetch_source_job_id(conn, job_id)
    if application_source_job_exists(conn, source_job_id):
        return False

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO application (
                source,
                source_job_id,
                internal_job_id,
                company_name,
                title,
                location,
                url,
                first_fetched_at,
                description,
                min,
                max,
                currency,
                application_questions,
                enriched_at,
                overall,
                score_prompt,
                score_model,
                scored_at,
                apply_prompt,
                apply_model,
                apply_response,
                resume,
                cover_letter,
                packaged_at,
                submitted_at,
                time_to_submit
            )
            SELECT
                'greenhouse',
                gj.greenhouse_job_id,
                ge.internal_job_id,
                gj.company_name,
                gj.title,
                gj.location,
                gj.url,
                gj.first_fetched_at,
                ge.description,
                ge.min_salary,
                ge.max_salary,
                ge.currency,
                ge.application_questions::text,
                ge.enriched_at,
                gs.overall,
                gs.prompt,
                gs.model,
                gs.scored_at,
                ga.prompt,
                ga.model,
                ga.response,
                ga.resume,
                ga.cover_letter,
                ga.packaged_at,
                ga.submitted_at,
                ga.time_to_submit
            FROM green_apply AS ga
            JOIN green_job AS gj
              ON gj.job_id = ga.job_id
            JOIN green_enrich AS ge
              ON ge.job_id = gj.job_id
            JOIN green_score AS gs
              ON gs.job_id = gj.job_id
            WHERE ga.job_id = %s
              AND ga.submitted_at IS NOT NULL
            RETURNING app_id
            """,
            (job_id,),
        )
        inserted_row = cur.fetchone()

    if inserted_row is None and not application_source_job_exists(conn, source_job_id):
        raise RuntimeError(
            f"job_id={job_id} could not be inserted into application because the join "
            "did not produce a complete row"
        )

    return inserted_row is not None


def backfill_application_rows(conn: psycopg.Connection) -> BackfillSummary:
    """Backfill every submitted job that is not yet present in application."""
    summary = BackfillSummary()
    missing_job_ids = find_submitted_job_ids_missing_application(conn)
    summary.discovered = len(missing_job_ids)
    summary.missing_job_ids = list(missing_job_ids)

    for job_id in missing_job_ids:
        try:
            inserted = insert_application_row(conn, job_id)
            conn.commit()
        except Exception as e:
            conn.rollback()
            summary.failed += 1
            print(f"{e}")
            print(f"job_id={job_id} failed")
            continue

        if inserted:
            summary.inserted += 1
            print(f"job_id={job_id} inserted")
        else:
            summary.skipped += 1
            print(f"job_id={job_id} skipped")

    return summary


def main() -> None:
    """CLI entrypoint for backfilling submitted applications."""
    parse_args()

    try:
        with db_connect() as conn:
            summary = backfill_application_rows(conn)
    except (OSError, ValueError, psycopg.Error) as exc:
        print(str(exc))
        raise SystemExit(1) from exc

    print(
        f"Final summary: discovered={summary.discovered} inserted={summary.inserted} "
        f"skipped={summary.skipped} failed={summary.failed}"
    )
    if not summary.success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
