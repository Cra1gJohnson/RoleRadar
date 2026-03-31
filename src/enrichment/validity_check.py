import argparse
import os
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import psycopg
import requests

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env

load_shared_env()

GREENHOUSE_JOB_API = (
    "https://boards-api.greenhouse.io/v1/boards/{token}/jobs/{job_id}"
    "?pay_transparency=true&questions=true"
)
GREENHOUSE_API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}
DEFAULT_RATE_PER_MINUTE = 100
DEFAULT_MAX_WORKERS = 8
PROGRESS_PRINT_INTERVAL = 25

_THREAD_LOCAL = threading.local()


@dataclass
class ValidityWorkItem:
    """Identifiers needed to check one tracked Greenhouse job."""

    job_id: int
    token: str
    greenhouse_job_id: int


@dataclass
class ValidityResult:
    """Result of checking one tracked Greenhouse job."""

    job_id: int
    request_status: Optional[int] = None
    request_failed: bool = False
    database_failed: bool = False
    message: str = ""


@dataclass
class RunSummary:
    """Track the aggregate outcome of a validity check run."""

    scheduled: int = 0
    completed: int = 0
    updated: int = 0
    status_200: int = 0
    status_404: int = 0
    status_other: int = 0
    request_failures: int = 0
    database_failures: int = 0

    @property
    def success(self) -> bool:
        """Treat any request or database failure as a non-successful run."""
        return (self.request_failures + self.database_failures) == 0


class EvenRateLimiter:
    """Spread request starts evenly across time instead of bursting within a minute."""

    def __init__(self, rate_per_minute: int) -> None:
        """Initialize the limiter with a fixed interval between dispatches."""
        self.dispatch_interval = 60.0 / rate_per_minute
        self.next_dispatch_at = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        """Wait until the next evenly spaced dispatch slot is available."""
        with self.lock:
            now = time.monotonic()
            sleep_seconds = self.next_dispatch_at - now
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
                now = time.monotonic()

            self.next_dispatch_at = max(self.next_dispatch_at + self.dispatch_interval, now)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the validity checker."""
    parser = argparse.ArgumentParser(
        description="Refresh Greenhouse job request_status values from live GET requests."
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional limit on how many tracked jobs to process",
    )
    parser.add_argument(
        "--rate-per-minute",
        type=int,
        default=DEFAULT_RATE_PER_MINUTE,
        help="Maximum number of job-detail GETs to start per 60 seconds",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help="Maximum number of concurrent worker threads",
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


def fetch_tracked_jobs(
    conn: psycopg.Connection,
    limit: Optional[int],
) -> list[ValidityWorkItem]:
    """Load the Greenhouse jobs currently tracked for validity checks."""
    query = """
        SELECT ge.job_id, gj.token, gj.greenhouse_job_id
        FROM green_enrich AS ge
        JOIN green_job AS gj
          ON gj.job_id = ge.job_id
        WHERE gj.token IS NOT NULL
          AND gj.greenhouse_job_id IS NOT NULL
        ORDER BY ge.job_id
    """
    params: tuple[object, ...] = ()
    if limit is not None:
        query += " LIMIT %s"
        params = (limit,)

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    return [
        ValidityWorkItem(job_id=row[0], token=row[1], greenhouse_job_id=row[2])
        for row in rows
    ]


def get_thread_session() -> requests.Session:
    """Reuse one requests session per worker thread."""
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        _THREAD_LOCAL.session = session
    return session


def build_job_api_url(token: str, greenhouse_job_id: int) -> str:
    """Construct the Greenhouse individual-job API URL."""
    return GREENHOUSE_JOB_API.format(
        token=quote(token, safe=""),
        job_id=greenhouse_job_id,
    )


def fetch_job_status(token: str, greenhouse_job_id: int) -> int:
    """Fetch one Greenhouse job payload and return the HTTP status code."""
    session = get_thread_session()
    response = session.get(
        build_job_api_url(token, greenhouse_job_id),
        headers=GREENHOUSE_API_HEADERS,
        timeout=30,
    )
    return response.status_code


def update_request_status(
    conn: psycopg.Connection,
    job_id: int,
    request_status: int,
) -> None:
    """Persist the latest live request_status for one tracked job."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE green_enrich
            SET request_status = %s
            WHERE job_id = %s
            """,
            (request_status, job_id),
        )


def process_work_item(work_item: ValidityWorkItem) -> ValidityResult:
    """Check one tracked job and return the observed HTTP status."""
    try:
        status_code = fetch_job_status(work_item.token, work_item.greenhouse_job_id)
    except requests.RequestException as exc:
        message = f"job_id={work_item.job_id} request failed: {exc}"
        print(message)
        return ValidityResult(job_id=work_item.job_id, request_failed=True, message=message)

    return ValidityResult(
        job_id=work_item.job_id,
        request_status=status_code,
        message=f"job_id={work_item.job_id} request_status={status_code}",
    )


def consume_finished_futures(
    conn: psycopg.Connection,
    futures: dict[Future[ValidityResult], int],
    summary: RunSummary,
    total_jobs: int,
    force_progress: bool = False,
) -> None:
    """Collect completed worker results and update run counters."""
    done_futures = [future for future in futures if future.done()]

    for future in done_futures:
        futures.pop(future)
        try:
            result = future.result()
        except Exception as exc:
            print(f"Unexpected worker error: {exc}")
            summary.database_failures += 1
            summary.completed += 1
            continue

        summary.completed += 1
        if result.request_failed:
            summary.request_failures += 1
            continue

        try:
            update_request_status(conn, result.job_id, int(result.request_status or 0))
        except psycopg.Error as exc:
            print(f"job_id={result.job_id} database failed: {exc}")
            summary.database_failures += 1
            continue

        summary.updated += 1
        if result.request_status == 200:
            summary.status_200 += 1
        elif result.request_status == 404:
            summary.status_404 += 1
        else:
            summary.status_other += 1

    if summary.completed and (
        force_progress or summary.completed % PROGRESS_PRINT_INTERVAL == 0
    ):
        print_progress(summary, total_jobs)


def print_progress(summary: RunSummary, total_jobs: int) -> None:
    """Print a concise progress snapshot during a validity run."""
    print(
        f"Progress: completed={summary.completed}/{total_jobs} "
        f"scheduled={summary.scheduled} updated={summary.updated} "
        f"status_200={summary.status_200} status_404={summary.status_404} "
        f"status_other={summary.status_other} "
        f"request_failures={summary.request_failures} "
        f"database_failures={summary.database_failures}"
    )


def run_validity_check(limit: Optional[int], rate_per_minute: int, max_workers: int) -> int:
    """Refresh live request_status values for tracked Greenhouse jobs."""
    with db_connect() as conn:
        work_items = fetch_tracked_jobs(conn, limit)

    if not work_items:
        print("No green_enrich rows found")
        return 0

    print(
        f"jobs={len(work_items)} rate_per_minute={rate_per_minute} "
        f"max_workers={max_workers}"
    )

    rate_limiter = EvenRateLimiter(rate_per_minute=rate_per_minute)
    summary = RunSummary()
    next_index = 0
    futures: dict[Future[ValidityResult], int] = {}

    with db_connect() as conn:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while next_index < len(work_items) or futures:
                while next_index < len(work_items) and len(futures) < max_workers:
                    rate_limiter.acquire()
                    work_item = work_items[next_index]
                    future = executor.submit(process_work_item, work_item)
                    futures[future] = work_item.job_id
                    summary.scheduled += 1
                    next_index += 1

                if not futures:
                    break

                done, _ = wait(set(futures.keys()), timeout=0.5, return_when=FIRST_COMPLETED)
                if done:
                    consume_finished_futures(
                        conn,
                        futures,
                        summary,
                        total_jobs=len(work_items),
                    )

            consume_finished_futures(
                conn,
                futures,
                summary,
                total_jobs=len(work_items),
                force_progress=True,
            )

    print(
        f"Final summary: scheduled={summary.scheduled} completed={summary.completed} "
        f"updated={summary.updated} status_200={summary.status_200} "
        f"status_404={summary.status_404} status_other={summary.status_other} "
        f"request_failures={summary.request_failures} "
        f"database_failures={summary.database_failures}"
    )
    return 0 if summary.success else 1


def main() -> None:
    """CLI entrypoint for the Greenhouse validity checker."""
    args = parse_args()
    if args.limit is not None and args.limit <= 0:
        print("limit must be greater than 0")
        raise SystemExit(1)
    if args.rate_per_minute <= 0:
        print("rate-per-minute must be greater than 0")
        raise SystemExit(1)
    if args.max_workers <= 0:
        print("max-workers must be greater than 0")
        raise SystemExit(1)

    try:
        exit_code = run_validity_check(
            limit=args.limit,
            rate_per_minute=args.rate_per_minute,
            max_workers=args.max_workers,
        )
    except psycopg.Error as exc:
        print(f"Database error in validity_check: {exc}")
        raise SystemExit(1) from exc

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
