import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

import psycopg
import requests

SRC_ROOT = Path(__file__).resolve().parents[2]
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
DEFAULT_LIMIT = 10
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "greenhouse_job_response"


@dataclass
class CandidateJobSample:
    """Fields needed to fetch one Greenhouse individual job response."""

    token: str
    greenhouse_job_id: int


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the local sampling utility."""
    parser = argparse.ArgumentParser(
        description="Pull sample individual Greenhouse job responses for enrichment context."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="Number of random candidate jobs to pull (default: 10)",
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


def fetch_random_candidate_jobs(
    conn: psycopg.Connection,
    limit: int,
) -> list[CandidateJobSample]:
    """Load random candidate jobs that have the identifiers needed for detail pulls."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT token, greenhouse_job_id
            FROM green_job
            WHERE candidate = TRUE
              AND token IS NOT NULL
              AND greenhouse_job_id IS NOT NULL
            ORDER BY RANDOM()
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()

    jobs = [CandidateJobSample(token=row[0], greenhouse_job_id=row[1]) for row in rows]
    if len(jobs) != limit:
        raise RuntimeError(f"Expected {limit} candidate jobs, found {len(jobs)}")
    return jobs


def fetch_job_response(
    session: requests.Session,
    token: str,
    greenhouse_job_id: int,
) -> tuple[str, dict]:
    """Fetch one individual-job Greenhouse payload and validate its top-level shape."""
    api_url = GREENHOUSE_JOB_API.format(
        token=quote(token, safe=""),
        job_id=greenhouse_job_id,
    )
    response = session.get(api_url, headers=GREENHOUSE_API_HEADERS, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Greenhouse job API did not return a JSON object")
    return api_url, payload


def write_job_response(token: str, greenhouse_job_id: int, api_url: str, payload: dict) -> Path:
    """Write one markdown file using the established response-example format."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"{token}_{greenhouse_job_id}.md"
    output_path.write_text(
        f"## GET {api_url}\n{json.dumps(payload, ensure_ascii=False, indent=4)}\n",
        encoding="utf-8",
    )
    return output_path


def main() -> None:
    """CLI entrypoint for Greenhouse job-response sampling."""
    args = parse_args()
    if args.limit <= 0:
        print("limit must be greater than 0")
        raise SystemExit(1)

    try:
        with db_connect() as conn, requests.Session() as session:
            for job in fetch_random_candidate_jobs(conn, args.limit):
                api_url, payload = fetch_job_response(session, job.token, job.greenhouse_job_id)
                output_path = write_job_response(
                    job.token,
                    job.greenhouse_job_id,
                    api_url,
                    payload,
                )
                print(f"Wrote {output_path.name}")
    except (psycopg.Error, requests.RequestException, ValueError, RuntimeError) as exc:
        print(str(exc))
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
