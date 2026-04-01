import argparse
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import urlopen

from playwright.sync_api import sync_playwright
from handle_jobs import handle_standard_greenhouse_job
from handle_jobs import handle_nonstandard_job
from order_jobs import ReadyApplyJob
import psycopg

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env

load_shared_env()

DEFAULT_CDP_ENDPOINT = "http://127.0.0.1:9222"
DEFAULT_LIMIT = 1
DEFAULT_WAIT_SECONDS = 30
STANDARD_GREENHOUSE_DOMAINS = {"job-boards.greenhouse.io", "boards.greenhouse.io"}


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
    """Parse CLI arguments for the browser router."""
    parser = argparse.ArgumentParser(
        description="Open queued Greenhouse application URLs in Chrome and route them by site type."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="Maximum number of queued application URLs to open",
    )
    parser.add_argument(
        "--cdp-endpoint",
        default=DEFAULT_CDP_ENDPOINT,
        help="Chrome remote debugging endpoint started by src/execute.sh",
    )
    parser.add_argument(
        "--wait-seconds",
        type=int,
        default=DEFAULT_WAIT_SECONDS,
        help="Maximum number of seconds to wait for the Chrome CDP endpoint",
    )
    return parser.parse_args()


def is_standard_greenhouse_url(url: str) -> bool:
    """Return True when the URL points at a standard Greenhouse job board."""
    parsed = urlparse(url)
    return parsed.netloc.lower() in STANDARD_GREENHOUSE_DOMAINS


def route_name_for_url(url: str) -> str:
    """Classify the URL into the standard or nonstandard Playwright route."""
    return "standard_greenhouse" if is_standard_greenhouse_url(url) else "nonstandard"


def fetch_ready_jobs(conn: psycopg.Connection, limit: int) -> list[ReadyApplyJob]:
    """Load queued jobs that still need browser automation."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                gj.job_id,
                gj.url,
                gj.company_name,
                gj.title
            FROM green_apply AS ga
            JOIN green_job AS gj
              ON gj.job_id = ga.job_id
            JOIN green_score AS gs
              ON gs.job_id = ga.job_id
            WHERE ga.questions IS TRUE
              AND gj.url IS NOT NULL
            ORDER BY gs.overall DESC, ga.job_id ASC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()

    return [
        ReadyApplyJob(
            job_id=row[0],
            url=row[1],
            company_name=row[2],
            title=row[3],
        )
        for row in rows
    ]


def wait_for_cdp_endpoint(endpoint: str, wait_seconds: int) -> None:
    """Wait for the Chrome remote debugging endpoint to become available."""
    deadline = time.monotonic() + wait_seconds
    version_url = f"{endpoint.rstrip('/')}/json/version"

    while True:
        try:
            with urlopen(version_url, timeout=1) as response:
                if response.status == 200:
                    return
        except (URLError, TimeoutError, OSError):
            pass

        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Chrome CDP endpoint was not reachable at {version_url} within {wait_seconds} seconds"
            )
        time.sleep(1)

def route_job(page: object, job: ReadyApplyJob) -> None:
    """Dispatch one opened page to the appropriate Playwright route."""
    if route_name_for_url(job.url) == "standard_greenhouse":
        handle_standard_greenhouse_job(page, job)
    else:
        handle_nonstandard_job(page, job)


def open_job_urls(endpoint: str, jobs: list[ReadyApplyJob]) -> None:
    """Attach to Chrome over CDP, open each queued job URL, and classify it."""

    with sync_playwright() as playwright:
        browser = playwright.chromium.connect_over_cdp(endpoint)
        if not browser.contexts:
            raise RuntimeError(
                "Chrome exposed no browser contexts over CDP. Start src/execute.sh first."
            )

        context = browser.contexts[0]
        for job in jobs:
            page = context.new_page()
            page.goto(job.url, wait_until="domcontentloaded")
            page.bring_to_front()

            route = route_name_for_url(job.url)
            print(
                f"opened job_id={job.job_id} route={route} url={job.url}"
            )
            route_job(page, job)


def main() -> None:
    """CLI entrypoint for opening queued apply URLs and routing them by site type."""
    args = parse_args()
    if args.limit <= 0:
        print("limit must be greater than 0")
        raise SystemExit(1)
    if args.wait_seconds <= 0:
        print("wait-seconds must be greater than 0")
        raise SystemExit(1)

    try:
        wait_for_cdp_endpoint(args.cdp_endpoint, args.wait_seconds)
        with db_connect() as conn:
            jobs = fetch_ready_jobs(conn, args.limit)

        if not jobs:
            print("No queued jobs with questions = TRUE were found")
            return

        open_job_urls(args.cdp_endpoint, jobs)
    except (OSError, TimeoutError, ValueError, psycopg.Error, RuntimeError) as exc:
        print(str(exc))
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
