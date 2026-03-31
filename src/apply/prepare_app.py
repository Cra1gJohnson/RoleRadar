import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import psycopg
from google import genai
from google.genai import types

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env
from apply.green_apply_schema import ensure_green_apply_schema
from scoring.utility.green_as_json import parse_application_questions

load_shared_env()

DEFAULT_RATE_PER_MINUTE = 12
MODEL_NAME = "gemini-2.5-flash-lite"
PROMPT_FILE_NAME = "prompt1.md"
PROMPT_PATH = Path(__file__).resolve().parent / PROMPT_FILE_NAME


@dataclass
class ApplicationPrepJob:
    """Joined job row ready for application-question prep."""

    job_id: int
    company_name: Optional[str]
    title: Optional[str]
    location: Optional[str]
    url: Optional[str]
    description: Optional[str]
    min_salary: Optional[int]
    max_salary: Optional[int]
    overall: Optional[int]
    application_questions: Any


@dataclass
class PrepSummary:
    """Track the aggregate outcome of one application-prep run."""

    selected: int = 0
    prepared: int = 0
    api_failures: int = 0
    parse_failures: int = 0
    database_failures: int = 0

    @property
    def failed(self) -> int:
        """Return the total number of failed jobs."""
        return self.api_failures + self.parse_failures + self.database_failures

    @property
    def success(self) -> bool:
        """Treat any failed job as a non-successful run."""
        return self.failed == 0


class EvenRateLimiter:
    """Spread request starts evenly across time instead of bursting within a minute."""

    def __init__(self, rate_per_minute: int) -> None:
        self.dispatch_interval = 60.0 / rate_per_minute
        self.next_dispatch_at = time.monotonic()

    def acquire(self) -> None:
        now = time.monotonic()
        sleep_seconds = self.next_dispatch_at - now
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
            now = time.monotonic()

        self.next_dispatch_at = max(self.next_dispatch_at + self.dispatch_interval, now)


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
    """Parse CLI arguments for application preparation."""
    parser = argparse.ArgumentParser(
        description="Prepare AI answers for queued Greenhouse applications."
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum number of queued jobs to prepare",
    )
    parser.add_argument(
        "--rate-per-minute",
        type=int,
        default=DEFAULT_RATE_PER_MINUTE,
        help="Maximum number of AI requests to start per 60 seconds",
    )
    return parser.parse_args()


def resolve_limit(explicit_limit: Optional[int]) -> Optional[int]:
    """Return the user-provided limit or None for all pending jobs."""
    return explicit_limit


def fetch_jobs_to_prepare(
    conn: psycopg.Connection,
    limit: Optional[int],
) -> list[ApplicationPrepJob]:
    """Load queued jobs that still need application-question preparation."""
    query = """
        SELECT
            gj.job_id,
            gj.company_name,
            gj.title,
            gj.location,
            gj.url,
            ge.description,
            ge.min_salary,
            ge.max_salary,
            gs.overall,
            ge.application_questions
        FROM green_apply AS ga
        JOIN green_job AS gj
          ON gj.job_id = ga.job_id
        JOIN green_enrich AS ge
          ON ge.job_id = ga.job_id
        JOIN green_score AS gs
          ON gs.job_id = ga.job_id
        WHERE ga.questions IS FALSE
        ORDER BY gs.overall DESC, ga.job_id ASC
    """
    params: list[Any] = []
    if limit is not None:
        query += " LIMIT %s"
        params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()

    return [
        ApplicationPrepJob(
            job_id=row[0],
            company_name=row[1],
            title=row[2],
            location=row[3],
            url=row[4],
            description=row[5],
            min_salary=row[6],
            max_salary=row[7],
            overall=row[8],
            application_questions=parse_application_questions(row[9]),
        )
        for row in rows
    ]


def build_job_payload(job: ApplicationPrepJob) -> dict[str, Any]:
    """Build the prompt payload for a single job."""
    return {
        "job_id": job.job_id,
        "company_name": job.company_name,
        "title": job.title,
        "location": job.location,
        "url": job.url,
        "description": job.description,
        "min_salary": job.min_salary,
        "max_salary": job.max_salary,
        "overall": job.overall,
        "application_questions": job.application_questions,
    }


def load_prompt_template() -> str:
    """Load the application-prep prompt template."""
    return PROMPT_PATH.read_text(encoding="utf-8")


def render_prompt(prompt_template: str, job_payload: dict[str, Any]) -> str:
    """Render the final prompt by injecting the job JSON."""
    rendered_job = json.dumps(job_payload, ensure_ascii=False, indent=2)
    return prompt_template.replace("{JOB JSON HERE}", rendered_job)


def build_client() -> genai.Client:
    """Create the Gemini client from the configured API key."""
    api_key = os.getenv("GEMINI_API")
    if not api_key:
        raise ValueError("Missing GEMINI_API environment variable")
    return genai.Client(api_key=api_key)


def request_ai_response(client: genai.Client, prompt: str) -> str:
    """Send the prep prompt to Gemini and return the raw response text."""
    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt,
        config=types.GenerateContentConfig(
            responseMimeType="application/json",
        ),
    )
    response_text = getattr(response, "text", None)
    if not isinstance(response_text, str) or not response_text.strip():
        raise ValueError("Gemini returned an empty response")
    return response_text


def parse_ai_response(response_text: str, expected_job_id: int) -> dict[str, Any]:
    """Validate that the model returned JSON for the expected job."""
    payload = json.loads(response_text)
    if not isinstance(payload, dict):
        raise ValueError("Gemini JSON response must be an object")
    raw_job_id = payload.get("job_id")
    if isinstance(raw_job_id, str) and raw_job_id.strip().isdigit():
        raw_job_id = int(raw_job_id.strip())
    if raw_job_id != expected_job_id:
        raise ValueError("Gemini response job_id does not match the requested job")
    answers = payload.get("answers")
    if not isinstance(answers, list):
        raise ValueError("Gemini response must include an answers array")
    return payload


def persist_response(job_id: int, response_text: str, questions_done: bool) -> None:
    """Store the raw response and optionally mark the job as prepared."""
    with db_connect(autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE green_apply
                    SET response = %s,
                        questions = %s
                    WHERE job_id = %s
                    """,
                    (response_text, questions_done, job_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def prepare_applications(limit: Optional[int], rate_per_minute: int) -> PrepSummary:
    """Prepare queued applications with AI-generated answers."""
    prompt_template = load_prompt_template()
    client = build_client()
    rate_limiter = EvenRateLimiter(rate_per_minute=rate_per_minute)
    summary = PrepSummary()

    with db_connect() as conn:
        ensure_green_apply_schema(conn)
        jobs = fetch_jobs_to_prepare(conn, limit)

    summary.selected = len(jobs)
    if not jobs:
        print("No queued jobs need application prep")
        return summary

    print(
        f"selected={len(jobs)} limit={'all' if limit is None else limit} "
        f"rate_per_minute={rate_per_minute} model={MODEL_NAME}"
    )

    for job in jobs:
        prompt = render_prompt(prompt_template, build_job_payload(job))

        try:
            rate_limiter.acquire()
            response_text = request_ai_response(client, prompt)
        except Exception as exc:
            print(f"job_id={job.job_id} api failed: {exc}")
            summary.api_failures += 1
            continue

        try:
            parse_ai_response(response_text, job.job_id)
        except Exception as exc:
            print(f"job_id={job.job_id} parse failed: {exc}")
            try:
                persist_response(job.job_id, response_text, False)
            except psycopg.Error as db_exc:
                print(f"job_id={job.job_id} database failed: {db_exc}")
                summary.database_failures += 1
                continue
            summary.parse_failures += 1
            continue

        try:
            persist_response(job.job_id, response_text, True)
        except psycopg.Error as exc:
            print(f"job_id={job.job_id} database failed: {exc}")
            summary.database_failures += 1
            continue

        summary.prepared += 1
        print(f"job_id={job.job_id} prepared")

    return summary


def main() -> None:
    """CLI entrypoint for application preparation."""
    args = parse_args()
    if args.limit is not None and args.limit <= 0:
        print("limit must be greater than 0")
        raise SystemExit(1)
    if args.rate_per_minute <= 0:
        print("rate-per-minute must be greater than 0")
        raise SystemExit(1)

    try:
        summary = prepare_applications(
            limit=resolve_limit(args.limit),
            rate_per_minute=args.rate_per_minute,
        )
    except (OSError, ValueError, psycopg.Error) as exc:
        print(str(exc))
        raise SystemExit(1) from exc

    print(
        f"Final summary: selected={summary.selected} prepared={summary.prepared} "
        f"api_failures={summary.api_failures} parse_failures={summary.parse_failures} "
        f"database_failures={summary.database_failures}"
    )
    if not summary.success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
