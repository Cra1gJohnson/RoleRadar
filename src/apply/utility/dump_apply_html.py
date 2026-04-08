import argparse
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import psycopg
import requests

SRC_ROOT = Path(__file__).resolve().parents[2]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env

load_shared_env()

DEFAULT_LIMIT = 1
DEFAULT_TIMEOUT_SECONDS = 30
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "green_questions"
HTML_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}


@dataclass
class ApplyHtmlJob:
    """Queued apply row with the URL we want to snapshot."""

    job_id: int
    company_name: Optional[str]
    title: Optional[str]
    url: str


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the HTML snapshot utility."""
    parser = argparse.ArgumentParser(
        description="Fetch a queued apply URL and write the raw HTML response to src/apply/green_questions/."
    )
    parser.add_argument(
        "--job-id",
        type=int,
        help="Optional green_apply job_id to snapshot",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="Maximum number of queued jobs to snapshot",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout for the page fetch",
    )
    return parser.parse_args()


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


def normalize_filename_part(value: str) -> str:
    """Convert a title or company string into a safe filename fragment."""
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "unknown"


def fetch_apply_jobs(
    conn: psycopg.Connection,
    limit: int,
    job_id: Optional[int],
) -> list[ApplyHtmlJob]:
    """Load queued apply rows with URLs to snapshot."""
    query = """
        SELECT
            ga.job_id,
            gj.company_name,
            gj.title,
            gj.url
        FROM green_apply AS ga
        JOIN green_job AS gj
          ON gj.job_id = ga.job_id
        WHERE ga.packaged_at IS NOT NULL
          AND gj.url IS NOT NULL
    """
    params: list[object] = []
    if job_id is not None:
        query += " AND ga.job_id = %s"
        params.append(job_id)

    query += " ORDER BY ga.job_id ASC"

    if job_id is None:
        query += " LIMIT %s"
        params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()

    return [
        ApplyHtmlJob(
            job_id=row[0],
            company_name=row[1],
            title=row[2],
            url=row[3],
        )
        for row in rows
    ]


def build_output_path(job: ApplyHtmlJob) -> Path:
    """Build a readable, collision-resistant output filename."""
    company_part = normalize_filename_part(job.company_name or "company")
    title_part = normalize_filename_part(job.title or "title")
    return OUTPUT_DIR / f"{job.job_id}_{company_part}_{title_part}.txt"


def fetch_html(session: requests.Session, url: str, timeout_seconds: int) -> requests.Response:
    """Fetch the page HTML for a queued apply URL."""
    response = session.get(url, headers=HTML_HEADERS, timeout=timeout_seconds)
    response.raise_for_status()
    return response


def write_html_snapshot(job: ApplyHtmlJob, response: requests.Response) -> Path:
    """Write the HTML response to a text file inside src/apply/green_questions/."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = build_output_path(job)
    parsed_url = urlparse(job.url)
    output_path.write_text(
        "\n".join(
            [
                f"job_id: {job.job_id}",
                f"company_name: {job.company_name or ''}",
                f"title: {job.title or ''}",
                f"url: {job.url}",
                f"status_code: {response.status_code}",
                f"host: {parsed_url.netloc}",
                "",
                response.text,
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return output_path


def main() -> None:
    """CLI entrypoint for fetching apply HTML snapshots."""
    args = parse_args()
    if args.limit <= 0:
        print("limit must be greater than 0")
        raise SystemExit(1)
    if args.timeout_seconds <= 0:
        print("timeout-seconds must be greater than 0")
        raise SystemExit(1)

    try:
        with db_connect() as conn, requests.Session() as session:
            jobs = fetch_apply_jobs(conn, limit=args.limit, job_id=args.job_id)
            if not jobs:
                print("No queued apply jobs with packaged_at IS NOT NULL were found")
                return

            for job in jobs:
                response = fetch_html(session, job.url, args.timeout_seconds)
                output_path = write_html_snapshot(job, response)
                print(f"Wrote {output_path.name}")
    except (psycopg.Error, requests.RequestException, OSError, ValueError) as exc:
        print(str(exc))
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
