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


@dataclass
class RefreshSummary:
    """Track the outcome of one viewed-flag refresh run."""

    requested: int = 0
    reset: int = 0
    skipped_applied: int = 0
    missing_job_ids: list[int] = field(default_factory=list)


def db_connect() -> psycopg.Connection:
    """Create a PostgreSQL connection using the shared env-based settings."""
    return psycopg.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        autocommit=True,
    )


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the green_score viewed refresh utility."""
    parser = argparse.ArgumentParser(
        description="Reset green_score.viewed for one or more un-applied job ids."
    )
    parser.add_argument(
        "job_ids",
        nargs="+",
        type=int,
        help="One or more green_score.job_id values to mark unseen again",
    )
    return parser.parse_args()


def refresh_green_score_rows(
    conn: psycopg.Connection,
    job_ids: list[int],
) -> RefreshSummary:
    """Reset viewed to FALSE for jobs that were marked no and not applied."""
    summary = RefreshSummary(requested=len(job_ids))

    with conn.cursor() as cur:
        for job_id in job_ids:
            cur.execute(
                """
                SELECT applied
                FROM green_score
                WHERE job_id = %s
                """,
                (job_id,),
            )
            row = cur.fetchone()
            if row is None:
                summary.missing_job_ids.append(job_id)
                print(f"job_id={job_id} not found")
                continue

            applied = row[0]
            if applied is True:
                summary.skipped_applied += 1
                print(f"job_id={job_id} already applied; viewed flag left unchanged")
                continue

            cur.execute(
                """
                UPDATE green_score
                SET viewed = FALSE
                WHERE job_id = %s
                """,
                (job_id,),
            )
            summary.reset += 1
            print(f"job_id={job_id} refreshed")

    return summary


def main() -> None:
    """CLI entrypoint for clearing the viewed flag on scored jobs."""
    args = parse_args()
    job_ids = list(dict.fromkeys(args.job_ids))

    invalid_job_ids = [job_id for job_id in job_ids if job_id <= 0]
    if invalid_job_ids:
        print(
            "job_ids must be positive integers: "
            + ", ".join(str(job_id) for job_id in invalid_job_ids)
        )
        raise SystemExit(1)

    try:
        with db_connect() as conn:
            summary = refresh_green_score_rows(conn, job_ids)
    except (OSError, ValueError, psycopg.Error) as exc:
        print(str(exc))
        raise SystemExit(1) from exc

    missing_count = len(summary.missing_job_ids)
    missing_text = (
        "none"
        if not summary.missing_job_ids
        else ", ".join(str(job_id) for job_id in summary.missing_job_ids)
    )
    print(
        f"Final summary: requested={summary.requested} reset={summary.reset} "
        f"skipped_applied={summary.skipped_applied} missing={missing_count} "
        f"missing_job_ids={missing_text}"
    )


if __name__ == "__main__":
    main()
