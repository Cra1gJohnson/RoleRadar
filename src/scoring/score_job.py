import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import psycopg
from google import genai
from google.genai import types

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env

load_shared_env()

DEFAULT_FULL_LIMIT = 200
DEFAULT_TEST_LIMIT = 1
DEFAULT_RATE_PER_MINUTE = 12
MODEL_NAME = "gemini-2.5-flash-lite"
INPUT_COST_PER_MILLION_TOKENS = 0.10
OUTPUT_COST_PER_MILLION_TOKENS = 0.40
INPUT_COST_PER_TOKEN = INPUT_COST_PER_MILLION_TOKENS / 1_000_000
OUTPUT_COST_PER_TOKEN = OUTPUT_COST_PER_MILLION_TOKENS / 1_000_000
PROMPT_FILE_NAME = "prompt2.txt"
PROMPT_PATH = Path(__file__).resolve().parent / PROMPT_FILE_NAME
BATCH_SIZE = 10


@dataclass
class JobScoreInput:
    """Joined job row ready for prompt rendering and scoring."""

    job_id: int
    company_name: Optional[str]
    title: Optional[str]
    location: Optional[str]
    description: Optional[str]
    min_salary: int
    max_salary: int


@dataclass
class ScoredJob:
    """Validated scoring response for one job in a batch."""

    job_id: int
    scores: "ScoreBreakdown"


@dataclass
class ScoreBreakdown:
    """Validated sub-scores returned by the model."""

    job_fit: int
    interview_chances: int
    compensation: int
    location: int

    @property
    def overall(self) -> int:
        """Return the equal-weight rounded arithmetic mean."""
        return round(
            (
                self.job_fit * 0.2
                + self.interview_chances * 0.5
                + self.compensation * 0.1
                + self.location * 0.2
            )
        )


@dataclass
class RunSummary:
    """Track the aggregate outcome of one scoring run."""

    selected: int = 0
    scored: int = 0
    api_failures: int = 0
    parse_failures: int = 0
    database_failures: int = 0
    prompt_tokens: int = 0
    response_tokens: int = 0
    total_tokens: int = 0
    prompt_cost: float = 0.0
    response_cost: float = 0.0
    total_cost: float = 0.0

    @property
    def failed(self) -> int:
        """Return the total number of failed jobs."""
        return self.api_failures + self.parse_failures + self.database_failures

    @property
    def success(self) -> bool:
        """Treat any failed job as a non-successful run."""
        return self.failed == 0


@dataclass
class GeminiBatchResponse:
    """Raw Gemini response text and token accounting."""

    response_text: str
    prompt_tokens: int = 0
    response_tokens: int = 0
    total_tokens: int = 0


class EvenRateLimiter:
    """Spread request starts evenly across time instead of bursting within a minute."""

    def __init__(self, rate_per_minute: int) -> None:
        """Initialize the limiter with a fixed interval between request starts."""
        self.dispatch_interval = 60.0 / rate_per_minute
        self.next_dispatch_at = time.monotonic()

    def acquire(self) -> None:
        """Wait until the next evenly spaced request slot is available."""
        now = time.monotonic()
        sleep_seconds = self.next_dispatch_at - now
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
            now = time.monotonic()

        self.next_dispatch_at = max(self.next_dispatch_at + self.dispatch_interval, now)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for job scoring."""
    parser = argparse.ArgumentParser(
        description="Score enriched Greenhouse jobs with Gemini and persist the results."
    )
    parser.add_argument(
        "mode",
        choices=("test", "full"),
        help="Score one job in test mode or a batch in full mode",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum number of jobs to score",
    )
    parser.add_argument(
        "--rate-per-minute",
        type=int,
        default=DEFAULT_RATE_PER_MINUTE,
        help="Maximum number of Gemini requests to start per 60 seconds",
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


def resolve_limit(mode: str, explicit_limit: Optional[int]) -> int:
    """Resolve the run limit from mode defaults and optional override."""
    if explicit_limit is not None:
        return explicit_limit
    if mode == "test":
        return DEFAULT_TEST_LIMIT
    return DEFAULT_FULL_LIMIT


def chunk_jobs(jobs: list[JobScoreInput], batch_size: int) -> list[list[JobScoreInput]]:
    """Split the job list into fixed-size batches."""
    return [jobs[index : index + batch_size] for index in range(0, len(jobs), batch_size)]


def fetch_jobs_to_score(conn: psycopg.Connection, limit: int) -> list[JobScoreInput]:
    """Load enriched but unscored jobs in the same JSON shape used by the export utility."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                gj.job_id,
                gj.company_name,
                gj.title,
                gj.location,
                ge.description,
                COALESCE(ge.min_salary, 0) AS min_salary,
                COALESCE(ge.max_salary, 0) AS max_salary
            FROM green_job AS gj
            JOIN green_enrich AS ge
              ON ge.job_id = gj.job_id
            WHERE gj.enriched = TRUE
              AND ge.scored IS NULL
            ORDER BY gj.job_id
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()

    return [
        JobScoreInput(
            job_id=row[0],
            company_name=row[1],
            title=row[2],
            location=row[3],
            description=row[4],
            min_salary=row[5],
            max_salary=row[6],
        )
        for row in rows
    ]


def build_job_payload(job: JobScoreInput) -> dict[str, Any]:
    """Build the job JSON payload in the same shape as the export utility."""
    return {
        "job_id": job.job_id,
        "company_name": job.company_name,
        "title": job.title,
        "location": job.location,
        "description": job.description,
        "min_salary": job.min_salary,
        "max_salary": job.max_salary,
    }


def load_prompt_template() -> str:
    """Load the base scoring prompt template."""
    return PROMPT_PATH.read_text(encoding="utf-8")


def render_prompt(prompt_template: str, jobs_payload: list[dict[str, Any]]) -> str:
    """Render the final prompt by injecting the batch job JSON where the template expects it."""
    rendered_jobs = f'"jobs": {json.dumps(jobs_payload, ensure_ascii=False, indent=2)}'
    return prompt_template.replace("{JOB JSON HERE}", rendered_jobs)


def build_client() -> genai.Client:
    """Create the Gemini client from the configured API key."""
    api_key = os.getenv("GEMINI_API")
    if not api_key:
        raise ValueError("Missing GEMINI_API environment variable")
    return genai.Client(api_key=api_key)


def extract_usage_metadata(response: Any) -> tuple[int, int]:
    """Extract token counts from a Gemini response when available."""
    metadata = getattr(response, "usage_metadata", None)
    if metadata is None:
        metadata = getattr(response, "usageMetadata", None)
    if metadata is None:
        return 0, 0
    return (
        getattr(metadata, "prompt_token_count", 0) or 0,
        getattr(metadata, "candidates_token_count", 0) or 0,
    )


def token_cost(tokens: int, per_token_rate: float) -> float:
    """Calculate the dollar cost for a token count."""
    return tokens * per_token_rate


def request_score_text(client: genai.Client, prompt: str) -> GeminiBatchResponse:
    """Send the scoring prompt to Gemini and return the raw text response."""
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
    prompt_tokens, response_tokens = extract_usage_metadata(response)
    total_tokens = getattr(getattr(response, "usage_metadata", None), "total_token_count", 0) or getattr(
        getattr(response, "usageMetadata", None), "total_token_count", 0
    ) or prompt_tokens + response_tokens
    return GeminiBatchResponse(
        response_text=response_text,
        prompt_tokens=prompt_tokens,
        response_tokens=response_tokens,
        total_tokens=total_tokens,
    )


def print_token_usage(response: GeminiBatchResponse) -> None:
    """Print the token usage block for one Gemini response."""
    print(
        f"Prompt tokens:\t  {response.prompt_tokens} "
        f"(cost ${token_cost(response.prompt_tokens, INPUT_COST_PER_TOKEN):.6f})"
    )
    print(
        f"Output tokens:\t  {response.response_tokens} "
        f"(cost ${token_cost(response.response_tokens, OUTPUT_COST_PER_TOKEN):.6f})"
    )
    print("--------------")
    total_cost = (
        token_cost(response.prompt_tokens, INPUT_COST_PER_TOKEN)
        + token_cost(response.response_tokens, OUTPUT_COST_PER_TOKEN)
    )
    print(f"Total tokens:\t {response.total_tokens} (cost ${total_cost:.6f})")


def parse_score_response_json(response_text: str) -> dict[str, Any]:
    """Parse the raw Gemini response text into a JSON object."""
    payload = json.loads(response_text)
    if isinstance(payload, list):
        payload = {"jobs": payload}
    if not isinstance(payload, dict):
        raise ValueError("Gemini JSON response must be an object")
    return payload


def parse_job_id(raw_job_id: Any) -> int:
    """Normalize the returned job_id value."""
    if isinstance(raw_job_id, bool):
        raise ValueError("Response job_id must be an integer")
    if isinstance(raw_job_id, int):
        return raw_job_id
    if isinstance(raw_job_id, str):
        stripped = raw_job_id.strip()
        if stripped.isdigit():
            return int(stripped)
    raise ValueError("Response job_id must be an integer")


def extract_score_field(payload: dict[str, Any], field_name: str) -> int:
    """Extract and validate one integer score field from the model response."""
    raw_section = payload.get(field_name)
    if not isinstance(raw_section, dict):
        raise ValueError(f"Response missing object field: {field_name}")

    raw_score = raw_section.get("score")
    if isinstance(raw_score, bool):
        raise ValueError(f"Response field {field_name}.score must be an integer")
    if isinstance(raw_score, str) and raw_score.strip().isdigit():
        raw_score = int(raw_score.strip())
    if not isinstance(raw_score, int):
        raise ValueError(f"Response field {field_name}.score must be an integer")
    if raw_score < 0 or raw_score > 100:
        raise ValueError(f"Response field {field_name}.score must be between 0 and 100")
    return raw_score


def parse_score_breakdown(payload: dict[str, Any]) -> ScoreBreakdown:
    """Validate the response schema and convert it into DB-ready scores."""
    return ScoreBreakdown(
        job_fit=extract_score_field(payload, "job_fit"),
        interview_chances=extract_score_field(payload, "interview_chances"),
        compensation=extract_score_field(payload, "compensation"),
        location=extract_score_field(payload, "location"),
    )


def parse_scored_jobs(
    payload: dict[str, Any],
    expected_job_ids: list[int],
) -> list[ScoredJob]:
    """Validate a batch response and convert it into job-level score rows."""
    raw_jobs = payload.get("jobs")
    if not isinstance(raw_jobs, list):
        raise ValueError("Gemini JSON response must include a jobs array")
    if len(raw_jobs) != len(expected_job_ids):
        raise ValueError(
            f"Gemini JSON response returned {len(raw_jobs)} jobs, expected {len(expected_job_ids)}"
        )

    expected_job_id_set = set(expected_job_ids)
    seen_job_ids: set[int] = set()
    scored_jobs: list[ScoredJob] = []

    for raw_job in raw_jobs:
        if not isinstance(raw_job, dict):
            raise ValueError("Each jobs[] entry must be an object")
        job_id = parse_job_id(raw_job.get("job_id"))
        if job_id not in expected_job_id_set:
            raise ValueError(f"Gemini returned unexpected job_id {job_id}")
        if job_id in seen_job_ids:
            raise ValueError(f"Gemini returned duplicate job_id {job_id}")
        scored_jobs.append(ScoredJob(job_id=job_id, scores=parse_score_breakdown(raw_job)))
        seen_job_ids.add(job_id)

    missing_job_ids = sorted(expected_job_id_set - seen_job_ids)
    if missing_job_ids:
        raise ValueError(f"Gemini response missing job_ids: {missing_job_ids}")
    return scored_jobs


def persist_score(job_id: int, scores: ScoreBreakdown, response_text: str) -> None:
    """Upsert the score row and mark the enrichment row as scored atomically."""
    with db_connect(autocommit=False) as conn:
        try:
            scored_at = datetime.now(timezone.utc)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO green_score (
                        job_id,
                        job_fit,
                        interview_chances,
                        compensation,
                        location,
                        overall,
                        scored_at,
                        prompt,
                        model,
                        response
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_id) DO UPDATE
                    SET job_fit = EXCLUDED.job_fit,
                        interview_chances = EXCLUDED.interview_chances,
                        compensation = EXCLUDED.compensation,
                        location = EXCLUDED.location,
                        overall = EXCLUDED.overall,
                        prompt = EXCLUDED.prompt,
                        model = EXCLUDED.model,
                        response = EXCLUDED.response
                    """,
                    (
                        job_id,
                        scores.job_fit,
                        scores.interview_chances,
                        scores.compensation,
                        scores.location,
                        scores.overall,
                        scored_at,
                        PROMPT_FILE_NAME,
                        MODEL_NAME,
                        response_text,
                    ),
                )
                cur.execute(
                    """
                    UPDATE green_enrich
                    SET scored = TRUE,
                    WHERE job_id = %s
                    """,
                    (job_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def score_jobs(mode: str, limit: int, rate_per_minute: int) -> RunSummary:
    """Score the selected jobs in batches and persist successful results."""
    prompt_template = load_prompt_template()
    client = build_client()
    rate_limiter = EvenRateLimiter(rate_per_minute=rate_per_minute)
    summary = RunSummary()

    with db_connect() as conn:
        jobs = fetch_jobs_to_score(conn, limit)

    summary.selected = len(jobs)
    if not jobs:
        print("No enriched unscored jobs found")
        return summary

    print(
        f"mode={mode} selected={len(jobs)} limit={limit} "
        f"rate_per_minute={rate_per_minute} batch_size={BATCH_SIZE} model={MODEL_NAME}"
    )

    for batch_index, batch in enumerate(chunk_jobs(jobs, BATCH_SIZE), start=1):
        batch_job_ids = [job.job_id for job in batch]
        batch_payload = [build_job_payload(job) for job in batch]
        prompt = render_prompt(prompt_template, batch_payload)

        try:
            rate_limiter.acquire()
            response = request_score_text(client, prompt)
        except Exception as exc:
            print(f"batch={batch_index} job_ids={batch_job_ids} api failed: {exc}")
            summary.api_failures += len(batch)
            continue

        summary.prompt_tokens += response.prompt_tokens
        summary.response_tokens += response.response_tokens
        summary.total_tokens += response.total_tokens
        summary.prompt_cost += token_cost(response.prompt_tokens, INPUT_COST_PER_TOKEN)
        summary.response_cost += token_cost(response.response_tokens, OUTPUT_COST_PER_TOKEN)
        summary.total_cost += (
            token_cost(response.prompt_tokens, INPUT_COST_PER_TOKEN)
            + token_cost(response.response_tokens, OUTPUT_COST_PER_TOKEN)
        )

        try:
            raw_response = parse_score_response_json(response.response_text)
            scored_jobs = parse_scored_jobs(raw_response, batch_job_ids)
        except Exception as exc:
            print(f"batch={batch_index} job_ids={batch_job_ids} parse failed: {exc}")
            summary.parse_failures += len(batch)
            continue

        print(f"batch={batch_index} job_ids={batch_job_ids}")
        print_token_usage(response)

        for scored_job in sorted(
            scored_jobs,
            key=lambda item: item.scores.overall,
            reverse=True,
        ):
            try:
                persist_score(scored_job.job_id, scored_job.scores, response.response_text)
            except psycopg.Error as exc:
                print(f"job_id={scored_job.job_id} database failed: {exc}")
                summary.database_failures += 1
                continue

            summary.scored += 1
            print(
                f"job_id={scored_job.job_id} scored "
                f"job_fit={scored_job.scores.job_fit} "
                f"interview_chances={scored_job.scores.interview_chances} "
                f"compensation={scored_job.scores.compensation} "
                f"location={scored_job.scores.location} "
                f"overall={scored_job.scores.overall}"
            )

    return summary


def main() -> None:
    """CLI entrypoint for Greenhouse scoring."""
    args = parse_args()
    if args.limit is not None and args.limit <= 0:
        print("limit must be greater than 0")
        raise SystemExit(1)
    if args.rate_per_minute <= 0:
        print("rate-per-minute must be greater than 0")
        raise SystemExit(1)

    try:
        summary = score_jobs(
            mode=args.mode,
            limit=resolve_limit(args.mode, args.limit),
            rate_per_minute=args.rate_per_minute,
        )
    except (OSError, ValueError, psycopg.Error) as exc:
        print(str(exc))
        raise SystemExit(1) from exc

    print(
        f"Final summary: selected={summary.selected} scored={summary.scored} "
        f"api_failures={summary.api_failures} parse_failures={summary.parse_failures} "
        f"database_failures={summary.database_failures} "
        f"prompt_tokens={summary.prompt_tokens} "
        f"prompt_cost=${summary.prompt_cost:.6f} "
        f"response_tokens={summary.response_tokens} "
        f"response_cost=${summary.response_cost:.6f} "
        f"total_tokens={summary.total_tokens} "
        f"total_cost=${summary.total_cost:.6f}"
    )
    if not summary.success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
