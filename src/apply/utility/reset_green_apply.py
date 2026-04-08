import argparse
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
from psycopg import sql

SRC_ROOT = Path(__file__).resolve().parents[2]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env

load_shared_env()


@dataclass(frozen=True)
class GreenApplyColumn:
    """A column discovered from the live green_apply table."""

    name: str
    not_null: bool


@dataclass
class ResetSummary:
    """Track the result of one reset run."""

    requested: int = 0
    reset: int = 0
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
    """Parse CLI arguments for the green_apply reset utility."""
    parser = argparse.ArgumentParser(
        description="Reset one or more green_apply rows back to a blank application state."
    )
    parser.add_argument(
        "job_ids",
        nargs="+",
        type=int,
        help="One or more green_apply.job_id values to clear",
    )
    return parser.parse_args()


def fetch_green_apply_columns(conn: psycopg.Connection) -> list[GreenApplyColumn]:
    """Inspect the live green_apply table and return its columns in ordinal order."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                a.attname,
                a.attnotnull
            FROM pg_attribute AS a
            WHERE a.attrelid = to_regclass('green_apply')
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            """
        )
        rows = cur.fetchall()

    if not rows:
        raise RuntimeError("green_apply table was not found on the current search path")

    return [
        GreenApplyColumn(
            name=row[0],
            not_null=bool(row[1]),
        )
        for row in rows
    ]


def build_reset_update(columns: list[GreenApplyColumn]) -> sql.SQL:
    """Build the UPDATE statement that clears all mutable green_apply columns."""
    non_resettable = [
        column.name
        for column in columns
        if column.name not in {"job_id", "questions"} and column.not_null
    ]
    if non_resettable:
        joined = ", ".join(non_resettable)
        raise RuntimeError(
            "green_apply has non-nullable columns that cannot be cleared safely: "
            f"{joined}"
        )

    assignments: list[sql.SQL] = []
    for column in columns:
        if column.name == "job_id":
            continue
        if column.name == "questions":
            assignments.append(sql.SQL("{} = FALSE").format(sql.Identifier(column.name)))
            continue
        assignments.append(sql.SQL("{} = NULL").format(sql.Identifier(column.name)))

    if not assignments:
        raise RuntimeError("green_apply did not expose any mutable columns to reset")

    return sql.SQL("UPDATE green_apply SET {assignments} WHERE job_id = %s").format(
        assignments=sql.SQL(", ").join(assignments)
    )


def reset_green_apply_rows(
    conn: psycopg.Connection,
    job_ids: list[int],
) -> ResetSummary:
    """Reset one or more queued apply rows in place."""
    columns = fetch_green_apply_columns(conn)
    update_query = build_reset_update(columns)
    summary = ResetSummary(requested=len(job_ids))

    with conn.cursor() as cur:
        for job_id in job_ids:
            cur.execute(update_query, (job_id,))
            if cur.rowcount == 0:
                summary.missing_job_ids.append(job_id)
                print(f"job_id={job_id} not found")
                continue

            summary.reset += 1
            print(f"job_id={job_id} reset")

    return summary


def main() -> None:
    """CLI entrypoint for clearing queued apply rows."""
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
            summary = reset_green_apply_rows(conn, job_ids)
    except (OSError, ValueError, psycopg.Error, RuntimeError) as exc:
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
        f"missing={missing_count} missing_job_ids={missing_text}"
    )


if __name__ == "__main__":
    main()
