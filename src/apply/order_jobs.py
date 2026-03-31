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
from apply.green_apply_schema import ensure_green_apply_schema

load_shared_env()

THRESHOLD_OPTIONS = (
    (1, 60),
    (2, 70),
    (3, 80),
    (4, 90),
)


@dataclass
class ApplyJob:
    """Joined job row ready for keyboard review."""

    job_id: int
    company_name: Optional[str]
    title: Optional[str]
    location: Optional[str]
    min_salary: Optional[int]
    url: Optional[str]
    overall: int


@dataclass
class ApplySummary:
    """Track one apply session."""

    selected_threshold: int = 0
    available: int = 0
    reviewed: int = 0
    approved: int = 0
    skipped: int = 0
    failures: int = 0


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


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the apply queue tool."""
    parser = argparse.ArgumentParser(
        description="Review ranked Greenhouse jobs and queue approved jobs for application."
    )
    return parser.parse_args()


def prompt_for_threshold() -> int:
    """Ask the user which quality tier they want to review."""
    print("Choose the minimum overall score to review today:")
    for option_number, threshold in THRESHOLD_OPTIONS:
        label = f"{threshold}+"
        suffix = " (recommended)" if threshold == 70 else ""
        print(f"  {option_number}) {label}{suffix}")

    option_map = {str(option_number): threshold for option_number, threshold in THRESHOLD_OPTIONS}

    while True:
        choice = input("Select 1-4: ").strip()
        threshold = option_map.get(choice)
        if threshold is not None:
            return threshold
        print("Enter 1, 2, 3, or 4.")


def count_jobs(conn: psycopg.Connection, threshold: int) -> int:
    """Count un-applied ranked jobs at or above the selected score threshold."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM green_job AS gj
            JOIN green_enrich AS ge
              ON ge.job_id = gj.job_id
            JOIN green_score AS gs
              ON gs.job_id = gj.job_id
            WHERE gs.applied IS FALSE
              AND gs.overall >= %s
            """,
            (threshold,),
        )
        row = cur.fetchone()
    return int(row[0]) if row else 0


def fetch_jobs(conn: psycopg.Connection, threshold: int) -> list[ApplyJob]:
    """Load ranked jobs ready for review."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                gj.job_id,
                gj.company_name,
                gj.title,
                gj.location,
                ge.min_salary,
                gj.url,
                gs.overall
            FROM green_job AS gj
            JOIN green_enrich AS ge
              ON ge.job_id = gj.job_id
            JOIN green_score AS gs
              ON gs.job_id = gj.job_id
            WHERE gs.applied IS FALSE
              AND gs.overall >= %s
            ORDER BY gs.overall DESC, gj.job_id ASC
            """,
            (threshold,),
        )
        rows = cur.fetchall()

    return [
        ApplyJob(
            job_id=row[0],
            company_name=row[1],
            title=row[2],
            location=row[3],
            min_salary=row[4],
            url=row[5],
            overall=row[6],
        )
        for row in rows
    ]


def format_salary(value: Optional[int]) -> str:
    """Render a salary value for terminal display."""
    if value is None:
        return "N/A"
    return f"${value:,.0f}"


def format_url(url: Optional[str]) -> str:
    """Render a URL as a terminal hyperlink when possible."""
    if not url:
        return "N/A"
    if sys.stdout.isatty():
        return f"\033]8;;{url}\033\\{url}\033]8;;\033\\"
    return url


def display_job(job: ApplyJob, index: int, total: int) -> None:
    """Print one job in a keyboard-friendly review format."""
    print()
    print(f"[{index}/{total}] overall={job.overall}")
    print(f"Company: {job.company_name or 'N/A'}")
    print(f"Title: {job.title or 'N/A'}")
    print(f"Location: {job.location or 'N/A'}")
    print(f"Min salary: {format_salary(job.min_salary)}")
    print(f"URL: {format_url(job.url)}")


def prompt_approval() -> bool:
    """Ask the user to approve or skip the current job."""
    while True:
        choice = input("Apply this job? [y/n]: ").strip().lower()
        if choice == "y":
            return True
        if choice == "n":
            return False
        print("Enter y or n.")


def approve_job(job_id: int) -> None:
    """Insert the job into the apply queue and mark it as applied."""
    with db_connect(autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO green_apply (job_id)
                    VALUES (%s)
                    ON CONFLICT (job_id) DO NOTHING
                    """,
                    (job_id,),
                )
                cur.execute(
                    """
                    UPDATE green_score
                    SET applied = TRUE
                    WHERE job_id = %s
                    """,
                    (job_id,),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def run_apply_queue() -> ApplySummary:
    """Run the interactive apply queue."""
    summary = ApplySummary()

    with db_connect() as conn:
        ensure_green_apply_schema(conn)
        threshold = prompt_for_threshold()
        summary.selected_threshold = threshold
        summary.available = count_jobs(conn, threshold)
        jobs = fetch_jobs(conn, threshold)

    print(
        f"Found {summary.available} jobs with overall >= {summary.selected_threshold} "
        f"and applied = FALSE."
    )

    if not jobs:
        print("No jobs matched your selection.")
        return summary

    for index, job in enumerate(jobs, start=1):
        summary.reviewed += 1
        display_job(job, index, len(jobs))
        try:
            approved = prompt_approval()
        except KeyboardInterrupt:
            print("\nStopped by user.")
            break

        if not approved:
            summary.skipped += 1
            continue

        try:
            approve_job(job.job_id)
        except psycopg.Error as exc:
            summary.failures += 1
            print(f"job_id={job.job_id} failed to queue: {exc}")
            continue

        summary.approved += 1
        print(f"job_id={job.job_id} added to green_apply")

    return summary


def main() -> None:
    """CLI entrypoint for the apply queue tool."""
    parse_args()

    try:
        summary = run_apply_queue()
    except (OSError, ValueError, psycopg.Error) as exc:
        print(str(exc))
        raise SystemExit(1) from exc

    print(
        f"Final summary: threshold={summary.selected_threshold} "
        f"available={summary.available} reviewed={summary.reviewed} "
        f"approved={summary.approved} skipped={summary.skipped} "
        f"failures={summary.failures}"
    )


if __name__ == "__main__":
    main()
