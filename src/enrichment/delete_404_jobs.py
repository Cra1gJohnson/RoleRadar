import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import psycopg

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env

load_shared_env()

DEFAULT_BATCH_SIZE = 500
CASCADE_CHILD_TABLES = ("green_enrich", "green_score", "green_apply")


@dataclass
class DeleteSummary:
    """Track the outcome of one stale-job cleanup run."""

    scanned_count: int = 0
    deleted_count: int = 0
    skipped_count: int = 0
    failed_count: int = 0

    @property
    def success(self) -> bool:
        """Treat any delete failure as a non-successful run."""
        return self.failed_count == 0


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the stale-job deleter."""
    parser = argparse.ArgumentParser(
        description="Delete Greenhouse jobs whose enrichment request_status is 404."
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional limit on how many stale jobs to delete",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Maximum number of jobs to delete per transaction batch",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the rows that would be deleted without writing to the database",
    )
    return parser.parse_args()


def db_connect(autocommit: bool = True) -> psycopg.Connection:
    """Create a PostgreSQL connection using the shared env-based settings."""
    return psycopg.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        autocommit=autocommit,
    )


def fetch_stale_job_ids(
    conn: psycopg.Connection,
    limit: Optional[int],
) -> list[int]:
    """Load job ids whose enrichment row is marked as a terminal 404."""
    query = """
        SELECT ge.job_id
        FROM green_enrich AS ge
        WHERE ge.request_status = 404
        ORDER BY ge.job_id
    """
    params: tuple[object, ...] = ()
    if limit is not None:
        query += " LIMIT %s"
        params = (limit,)

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    return [row[0] for row in rows]


def verify_cascade_contract(conn: psycopg.Connection) -> None:
    """
    Verify that the known job tables point at green_job with ON DELETE CASCADE.

    The cleanup script deletes from green_job so child rows only disappear if the
    foreign keys are configured correctly.
    """
    with conn.cursor() as cur:
        for child_table in CASCADE_CHILD_TABLES:
            cur.execute(
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
            row = cur.fetchone()
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


def delete_jobs(conn: psycopg.Connection, job_ids: list[int]) -> int:
    """Delete parent job rows so the downstream tables cascade automatically."""
    if not job_ids:
        return 0

    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM green_job
            WHERE job_id = ANY(%s)
            RETURNING job_id
            """,
            (job_ids,),
        )
        deleted_rows = cur.fetchall()

    return len(deleted_rows)


def process_cleanup(limit: Optional[int], batch_size: int, dry_run: bool) -> DeleteSummary:
    """Delete stale jobs in batches and rely on foreign-key cascades for cleanup."""
    summary = DeleteSummary()

    with db_connect(autocommit=False) as conn:
        verify_cascade_contract(conn)
        stale_job_ids = fetch_stale_job_ids(conn, limit)
        summary.scanned_count = len(stale_job_ids)

        if not stale_job_ids:
            conn.commit()
            return summary

        if dry_run:
            print(f"dry-run job_ids={stale_job_ids}")
            summary.skipped_count = len(stale_job_ids)
            conn.rollback()
            return summary

        try:
            for index in range(0, len(stale_job_ids), batch_size):
                batch = stale_job_ids[index : index + batch_size]
                deleted_count = delete_jobs(conn, batch)
                summary.deleted_count += deleted_count
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return summary


def main() -> None:
    """CLI entrypoint for stale-job cleanup."""
    args = parse_args()
    if args.limit is not None and args.limit <= 0:
        print("limit must be greater than 0")
        raise SystemExit(1)
    if args.batch_size <= 0:
        print("batch-size must be greater than 0")
        raise SystemExit(1)

    try:
        summary = process_cleanup(
            limit=args.limit,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
        )
    except psycopg.Error as exc:
        print(f"Database error in delete_404_jobs: {exc}")
        raise SystemExit(1) from exc
    except ValueError as exc:
        print(str(exc))
        raise SystemExit(1) from exc

    print(
        f"scanned={summary.scanned_count} deleted={summary.deleted_count} "
        f"skipped={summary.skipped_count} failed={summary.failed_count}"
    )
    if not summary.success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
