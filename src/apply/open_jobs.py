import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, parse_qs

import requests

from handle_jobs import reciever
import psycopg

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env

load_shared_env()

DEFAULT_LIMIT = 1

# Browser/CDP behavior now lives in handle_jobs.
# DEFAULT_CDP_ENDPOINT = "http://127.0.0.1:9222"
# DEFAULT_WAIT_SECONDS = 30
# STANDARD_GREENHOUSE_DOMAINS = {"job-boards.greenhouse.io", "boards.greenhouse.io"}

STANDARD_GREENHOUSE_DOMAINS = {"job-boards.greenhouse.io", "boards.greenhouse.io"}
GREENHOUSE_URL_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass(frozen=True)
class JobsPackageItem:
    """A single job payload prepared for downstream browser handling."""

    job_id: int
    title: str | None
    url: str
    standard_job: bool
    response: Any


@dataclass(frozen=True)
class JobsPackage:
    """The full payload passed to handle_jobs."""

    jobs: list[JobsPackageItem]


def db_connect() -> psycopg.Connection:
    """Create a PostgreSQL connection using the shared env-based settings."""
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")

    missing = [
        name
        for name, value in (
            ("DB_NAME", db_name),
            ("DB_USER", db_user),
            ("DB_PASSWORD", db_password),
            ("DB_HOST", db_host),
            ("DB_PORT", db_port),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(
            "Cannot connect to PostgreSQL because these environment variables are missing: "
            + ", ".join(missing)
        )

    return psycopg.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        autocommit=True,
    )


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the package builder."""
    parser = argparse.ArgumentParser(
        description="Build a jobs_package for queued Greenhouse apply rows."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="Maximum number of queued jobs to package",
    )
    return parser.parse_args()


def normalize_prompt_text(value: Any) -> str:
    """Return a compact prompt-safe string."""
    if not isinstance(value, str):
        return ""
    return " ".join(value.split()).strip()


def normalize_hostname(url: str) -> str:
    """Return a normalized hostname for standard-board checks."""
    parsed = urlparse(url)
    hostname = (parsed.hostname or "").lower().strip()
    if hostname.startswith("www."):
        hostname = hostname[4:]
    return hostname


def is_standard_greenhouse_url(url: str) -> bool:
    """Return True when the URL points at a standard Greenhouse job board."""
    return normalize_hostname(url) in STANDARD_GREENHOUSE_DOMAINS


def validate_url(job_id: int, url: Any) -> str:
    """Validate the job URL and return a normalized string."""
    if not isinstance(url, str) or not url.strip():
        raise ValueError(f"job_id={job_id} has an empty or missing url")

    parsed = urlparse(url.strip())
    if parsed.scheme not in {"http", "https"}:
        raise ValueError(
            f"job_id={job_id} has an invalid url scheme: {url!r}. Expected http or https."
        )
    if not parsed.hostname:
        raise ValueError(
            f"job_id={job_id} has an invalid url without a hostname: {url!r}"
        )

    return url.strip()


def validate_job_row(row: Any, row_index: int) -> JobsPackageItem:
    """Convert one database row into a validated package item."""
    if not isinstance(row, tuple) or len(row) < 4:
        raise ValueError(
            f"Row {row_index} from the package query did not return the expected shape "
            f"(job_id, title, url, response). Got: {row!r}"
        )

    job_id, title, url, response = row[:4]

    if not isinstance(job_id, int) or job_id <= 0:
        raise ValueError(
            f"Row {row_index} returned an invalid job_id: {job_id!r}"
        )

    normalized_title = normalize_prompt_text(title)
    validated_url = validate_url(job_id, url)
    standard_job = is_standard_greenhouse_url(validated_url)

    return JobsPackageItem(
        job_id=job_id,
        title=normalized_title or None,
        url=validated_url,
        standard_job=standard_job,
        response=response,
    )


def fetch_jobs_package(conn: psycopg.Connection, limit: int) -> JobsPackage:
    """Load queued jobs and package only the fields needed by handle_jobs."""
    if limit <= 0:
        raise ValueError("limit must be greater than 0")

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                ga.job_id,
                gj.title,
                gj.url,
                ga.response
            FROM green_apply AS ga
            JOIN green_job AS gj
              ON gj.job_id = ga.job_id
            WHERE ga.packaged_at IS NOT NULL
            AND ga.submitted_at IS NULL
            ORDER BY ga.job_id ASC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()

    jobs: list[JobsPackageItem] = []
    for row_index, row in enumerate(rows, start=1):
        try:
            jobs.append(validate_job_row(row, row_index))
        except Exception as exc:
            raise RuntimeError(
                f"Failed to build jobs_package from row {row_index}: {exc}"
            ) from exc

    return JobsPackage(jobs=jobs)


def probe_url_status(url: str) -> int:
    """Check whether a job URL still resolves, returning the HTTP status code."""
    response = requests.get(
        url,
        headers=GREENHOUSE_URL_HEADERS,
        timeout=30,
        allow_redirects=True,
    )
    final_url = response.url
    final_query = parse_qs(urlparse(final_url).query)

    if response.status_code == 200 and final_query.get("error") == ["true"]:
        return 404


    return int(response.status_code)


def mark_job_request_status(conn: psycopg.Connection, job_id: int, request_status: int) -> None:
    """Persist a non-200 result in green_enrich so the job is skipped next time."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE green_enrich
            SET request_status = %s
            WHERE job_id = %s
            """,
            (request_status, job_id),
        )


def main() -> None:
    """CLI entrypoint for building the jobs_package."""
    args = parse_args()

    try:
        with db_connect() as conn:
            jobs_package = fetch_jobs_package(conn, args.limit)
            if not jobs_package.jobs:
                print("No queued jobs with packaged_at IS NOT NULL were found")
                return

            for job in jobs_package.jobs:
                try:
                    request_status = probe_url_status(job.url)
                except requests.RequestException as exc:
                    print(f"job_id={job.job_id} url check failed: {exc}")
                    continue

                if request_status != 200:
                    mark_job_request_status(conn, job.job_id, request_status)
                    print(
                        f"job_id={job.job_id} url returned {request_status}; "
                        "marked green_enrich.request_status"
                    )
                    continue

                reciever(JobsPackage(jobs=[job]))
                print()
                print(f"job_id={job.job_id}")
                print(f"title={job.title or 'N/A'}")
                print(f"url={job.url}")

                while True:
                    submitted = input("Was the job submitted? [y/n]: ").strip().lower()
                    if submitted in {"y", "n"}:
                        break
                    print("Enter y or n.")

                if submitted == "y":
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE green_apply
                            SET submitted_at = NOW()
                            WHERE job_id = %s
                            """,
                            (job.job_id,),
                        )
                    print(f"job_id={job.job_id} marked submitted")
    except (OSError, ValueError, psycopg.Error, RuntimeError) as exc:
        print(str(exc))
        raise SystemExit(1) from exc

    print("playwright success")


if __name__ == "__main__":
    main()
