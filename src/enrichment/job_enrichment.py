import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Any, Optional
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
TAG_BREAK_PATTERN = re.compile(r"(?i)<\s*(br|/p|/div|/li|/ul|/ol|/h[1-6])\b[^>]*>")
TAG_PATTERN = re.compile(r"<[^>]+>")
WHITESPACE_PATTERN = re.compile(r"[ \t\r\f\v]+")
THREE_PLUS_NEWLINES_PATTERN = re.compile(r"\n{3,}")
SALARY_AMOUNT_PATTERN = re.compile(r"\$\s*([0-9][0-9,]{2,})")

_THREAD_LOCAL = threading.local()


@dataclass
class EnrichmentWorkItem:
    """Identifiers needed to fetch and update one job enrichment row."""

    job_id: int
    token: str
    greenhouse_job_id: int


@dataclass
class NormalizedEnrichment:
    """Normalized job enrichment fields ready for database persistence."""

    description: Optional[str]
    min_salary: Optional[int]
    max_salary: Optional[int]
    currency: Optional[str]
    internal_job_id: Optional[str]
    application_questions: Optional[str]


@dataclass
class EnrichmentResult:
    """Result of processing one enrichment work item."""

    job_id: int
    success: bool = False
    not_found: bool = False
    request_failed: bool = False
    parse_failed: bool = False
    database_failed: bool = False
    message: str = ""


@dataclass
class RunSummary:
    """Track the aggregate outcome of a job enrichment run."""

    scheduled: int = 0
    completed: int = 0
    enriched: int = 0
    not_found: int = 0
    request_failures: int = 0
    parse_failures: int = 0
    database_failures: int = 0

    @property
    def failed(self) -> int:
        """Return the total number of failed jobs across failure categories."""
        return self.request_failures + self.parse_failures + self.database_failures

    @property
    def success(self) -> bool:
        """Treat any failed job as a non-successful run."""
        return self.failed == 0


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
    """Parse CLI arguments for the job enrichment worker."""
    parser = argparse.ArgumentParser(
        description="Pull and normalize Greenhouse job enrichment data."
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional limit on how many unenriched jobs to process",
    )
    parser.add_argument(
        "--rate-per-minute",
        type=int,
        default=DEFAULT_RATE_PER_MINUTE,
        help="Maximum number of job-detail pulls to start per 60 seconds",
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


def fetch_pending_enrichment_jobs(
    conn: psycopg.Connection,
    limit: Optional[int],
) -> list[EnrichmentWorkItem]:
    """Load jobs that have an enrichment row but have not been marked enriched yet."""
    query = """
        SELECT ge.job_id, gj.token, gj.greenhouse_job_id
        FROM green_enrich AS ge
        JOIN green_job AS gj
          ON gj.job_id = ge.job_id
        WHERE gj.enriched = FALSE
          AND ge.request_status IS DISTINCT FROM 404
          AND gj.token IS NOT NULL
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
        EnrichmentWorkItem(job_id=row[0], token=row[1], greenhouse_job_id=row[2])
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


def fetch_job_payload(token: str, greenhouse_job_id: int) -> dict[str, Any]:
    """Fetch one Greenhouse individual-job payload and validate its top-level shape."""
    session = get_thread_session()
    response = session.get(
        build_job_api_url(token, greenhouse_job_id),
        headers=GREENHOUSE_API_HEADERS,
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Greenhouse job API did not return a JSON object")
    return payload


def decode_html_text(raw_content: Any) -> Optional[str]:
    """Convert the escaped HTML-rich Greenhouse content field into readable plain text."""
    if not isinstance(raw_content, str) or not raw_content.strip():
        return None

    text = raw_content
    for _ in range(3):
        decoded = unescape(text)
        if decoded == text:
            break
        text = decoded

    text = TAG_BREAK_PATTERN.sub("\n", text)
    text = TAG_PATTERN.sub(" ", text)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = WHITESPACE_PATTERN.sub(" ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = THREE_PLUS_NEWLINES_PATTERN.sub("\n\n", text)
    stripped = text.strip()
    return stripped or None


def extract_structured_salary(payload: dict[str, Any]) -> tuple[Optional[int], Optional[int], Optional[str]]:
    """Use the first usable pay_input_ranges entry as the canonical salary source."""
    pay_input_ranges = payload.get("pay_input_ranges")
    if not isinstance(pay_input_ranges, list):
        return None, None, None

    for pay_range in pay_input_ranges:
        if not isinstance(pay_range, dict):
            continue

        min_cents = pay_range.get("min_cents")
        max_cents = pay_range.get("max_cents")
        currency = pay_range.get("currency_type")

        min_salary = int(min_cents / 100) if isinstance(min_cents, int) and min_cents > 0 else None
        max_salary = int(max_cents / 100) if isinstance(max_cents, int) and max_cents > 0 else None
        currency_text = currency.strip() if isinstance(currency, str) and currency.strip() else None

        if min_salary is not None or max_salary is not None or currency_text is not None:
            return min_salary, max_salary, currency_text

    return None, None, None


def extract_salary_from_description(description: Optional[str]) -> tuple[Optional[int], Optional[int], Optional[str]]:
    """Best-effort fallback salary parsing from plain-text description content."""
    if not description:
        return None, None, None

    matches = SALARY_AMOUNT_PATTERN.findall(description)
    amounts = [int(match.replace(",", "")) for match in matches]
    unique_amounts: list[int] = []
    for amount in amounts:
        if amount not in unique_amounts:
            unique_amounts.append(amount)

    if len(unique_amounts) < 2:
        return None, None, None

    first, second = unique_amounts[0], unique_amounts[1]
    return min(first, second), max(first, second), "USD"


def normalize_question_options(values: Any) -> list[dict[str, Any]]:
    """Normalize the answer options for one question field."""
    if not isinstance(values, list):
        return []

    options: list[dict[str, Any]] = []
    for value in values:
        if not isinstance(value, dict):
            continue

        option: dict[str, Any] = {}
        if "label" in value:
            option["label"] = value.get("label")
        if "value" in value:
            option["value"] = value.get("value")
        if "free_form" in value:
            option["free_form"] = value.get("free_form")
        if "decline_to_answer" in value:
            option["decline_to_answer"] = value.get("decline_to_answer")
        if option:
            options.append(option)

    return options


def normalize_question(question: dict[str, Any], source: str) -> Optional[dict[str, Any]]:
    """Normalize one question object into a compact JSON-friendly structure."""
    normalized: dict[str, Any] = {"source": source}
    label = question.get("label")
    if isinstance(label, str) and label.strip():
        normalized["label"] = label.strip()
    else:
        normalized["label"] = None

    if "required" in question:
        normalized["required"] = bool(question.get("required"))

    if isinstance(question.get("type"), str):
        normalized["type"] = question.get("type")

    fields = question.get("fields")
    if isinstance(fields, list):
        normalized_fields: list[dict[str, Any]] = []
        for field in fields:
            if not isinstance(field, dict):
                continue

            normalized_field: dict[str, Any] = {}
            if isinstance(field.get("name"), str):
                normalized_field["name"] = field.get("name")
            if isinstance(field.get("type"), str):
                normalized_field["type"] = field.get("type")

            options = normalize_question_options(field.get("values"))
            if options:
                normalized_field["options"] = options

            if normalized_field:
                normalized_fields.append(normalized_field)

        if normalized_fields:
            normalized["fields"] = normalized_fields

    answer_options = normalize_question_options(question.get("answer_options"))
    if answer_options:
        normalized["answer_options"] = answer_options

    if normalized.get("label") is None and "fields" not in normalized and "answer_options" not in normalized:
        return None
    return normalized


def normalize_application_questions(payload: dict[str, Any]) -> Optional[str]:
    """Build a compact normalized JSON string from relevant question sections."""
    normalized_questions: list[dict[str, Any]] = []

    def add_questions(raw_questions: Any, source: str) -> None:
        if not isinstance(raw_questions, list):
            return
        for question in raw_questions:
            if not isinstance(question, dict):
                continue
            normalized = normalize_question(question, source)
            if normalized is not None:
                normalized_questions.append(normalized)

    add_questions(payload.get("questions"), "questions")
    add_questions(payload.get("demographic_questions"), "demographic_questions")

    compliance = payload.get("compliance")
    if isinstance(compliance, list):
        for entry in compliance:
            if not isinstance(entry, dict):
                continue
            source = f"compliance:{entry.get('type', 'unknown')}"
            add_questions(entry.get("questions"), source)

    if not normalized_questions:
        return None
    return json.dumps(normalized_questions, ensure_ascii=False, separators=(",", ":"))


def normalize_payload(payload: dict[str, Any]) -> NormalizedEnrichment:
    """Parse the individual job payload into normalized enrichment fields."""
    description = decode_html_text(payload.get("content"))
    min_salary, max_salary, currency = extract_structured_salary(payload)
    if min_salary is None and max_salary is None and currency is None:
        min_salary, max_salary, currency = extract_salary_from_description(description)

    internal_job_id = payload.get("internal_job_id")
    internal_job_id_text = str(internal_job_id) if internal_job_id is not None else None
    application_questions = normalize_application_questions(payload)

    return NormalizedEnrichment(
        description=description,
        min_salary=min_salary,
        max_salary=max_salary,
        currency=currency,
        internal_job_id=internal_job_id_text,
        application_questions=application_questions,
    )


def mark_job_request_status(
    job_id: int,
    request_status: int,
    mark_enriched: bool,
) -> None:
    """Persist a checked request status and optionally mark the job as enriched."""
    with db_connect(autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE green_enrich
                    SET request_status = %s
                    WHERE job_id = %s
                    """,
                    (request_status, job_id),
                )

                if mark_enriched:
                    cur.execute(
                        """
                        UPDATE green_job
                        SET enriched = TRUE
                        WHERE job_id = %s
                        """,
                        (job_id,),
                    )

            conn.commit()
        except Exception:
            conn.rollback()
            raise


def persist_enrichment(job_id: int, normalized: NormalizedEnrichment) -> None:
    """Update enrichment fields and mark the source greenhouse job as enriched atomically."""
    with db_connect(autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE green_enrich
                    SET description = %s,
                        min_salary = %s,
                        max_salary = %s,
                        currency = %s,
                        internal_job_id = %s,
                        application_questions = %s,
                        request_status = %s,
                        enriched_at = NOW()
                    WHERE job_id = %s
                    """,
                    (
                        normalized.description,
                        normalized.min_salary,
                        normalized.max_salary,
                        normalized.currency,
                        normalized.internal_job_id,
                        normalized.application_questions,
                        200,
                        job_id,
                    ),
                )

                cur.execute(
                    """
                    UPDATE green_job
                    SET enriched = TRUE
                    WHERE job_id = %s
                    """,
                    (job_id,),
                )

            conn.commit()
        except Exception:
            conn.rollback()
            raise


def process_work_item(work_item: EnrichmentWorkItem) -> EnrichmentResult:
    """Fetch, normalize, and persist one job enrichment payload."""
    try:
        payload = fetch_job_payload(work_item.token, work_item.greenhouse_job_id)
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else 0
        if status_code == 404:
            try:
                mark_job_request_status(
                    job_id=work_item.job_id,
                    request_status=404,
                    mark_enriched=True,
                )
            except psycopg.Error as db_exc:
                message = f"job_id={work_item.job_id} database failed: {db_exc}"
                print(message)
                return EnrichmentResult(
                    job_id=work_item.job_id,
                    database_failed=True,
                    message=message,
                )

            message = f"job_id={work_item.job_id} request failed: 404 Client Error: Not Found"
            print(message)
            return EnrichmentResult(
                job_id=work_item.job_id,
                not_found=True,
                message=message,
            )
        message = f"job_id={work_item.job_id} request failed: {exc}"
        print(message)
        return EnrichmentResult(
            job_id=work_item.job_id,
            request_failed=True,
            message=message,
        )
    except requests.RequestException as exc:
        message = f"job_id={work_item.job_id} request failed: {exc}"
        print(message)
        return EnrichmentResult(
            job_id=work_item.job_id,
            request_failed=True,
            message=message,
        )

    try:
        normalized = normalize_payload(payload)
    except Exception as exc:
        message = f"job_id={work_item.job_id} parse failed: {exc}"
        print(message)
        return EnrichmentResult(
            job_id=work_item.job_id,
            parse_failed=True,
            message=message,
        )

    try:
        persist_enrichment(work_item.job_id, normalized)
    except psycopg.Error as exc:
        message = f"job_id={work_item.job_id} database failed: {exc}"
        print(message)
        return EnrichmentResult(
            job_id=work_item.job_id,
            database_failed=True,
            message=message,
        )

    return EnrichmentResult(
        job_id=work_item.job_id,
        success=True,
        message=f"job_id={work_item.job_id} enriched",
    )


def consume_finished_futures(
    futures: dict[Future[EnrichmentResult], int],
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
        if result.success:
            summary.enriched += 1
        elif result.not_found:
            summary.not_found += 1
        elif result.request_failed:
            summary.request_failures += 1
        elif result.parse_failed:
            summary.parse_failures += 1
        elif result.database_failed:
            summary.database_failures += 1

    if summary.completed and (
        force_progress or summary.completed % PROGRESS_PRINT_INTERVAL == 0
    ):
        print_progress(summary, total_jobs)


def print_progress(summary: RunSummary, total_jobs: int) -> None:
    """Print a concise progress snapshot during an enrichment run."""
    print(
        f"Progress: completed={summary.completed}/{total_jobs} "
        f"scheduled={summary.scheduled} enriched={summary.enriched} "
        f"not_found={summary.not_found} "
        f"request_failures={summary.request_failures} "
        f"parse_failures={summary.parse_failures} "
        f"database_failures={summary.database_failures}"
    )


def run_enrichment(limit: Optional[int], rate_per_minute: int, max_workers: int) -> int:
    """Run rate-limited concurrent enrichment for unenriched Greenhouse jobs."""
    with db_connect() as conn:
        work_items = fetch_pending_enrichment_jobs(conn, limit)

    if not work_items:
        print("No unenriched green_enrich rows found")
        return 0

    print(
        f"jobs={len(work_items)} rate_per_minute={rate_per_minute} "
        f"max_workers={max_workers}"
    )

    rate_limiter = EvenRateLimiter(rate_per_minute=rate_per_minute)
    summary = RunSummary()
    next_index = 0
    futures: dict[Future[EnrichmentResult], int] = {}

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
                consume_finished_futures(futures, summary, total_jobs=len(work_items))

        consume_finished_futures(
            futures,
            summary,
            total_jobs=len(work_items),
            force_progress=True,
        )

    print(
        f"Final summary: scheduled={summary.scheduled} completed={summary.completed} "
        f"enriched={summary.enriched} not_found={summary.not_found} "
        f"request_failures={summary.request_failures} "
        f"parse_failures={summary.parse_failures} "
        f"database_failures={summary.database_failures}"
    )
    return 0 if summary.success else 1


def main() -> None:
    """CLI entrypoint for Greenhouse job enrichment."""
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
        exit_code = run_enrichment(
            limit=args.limit,
            rate_per_minute=args.rate_per_minute,
            max_workers=args.max_workers,
        )
    except psycopg.Error as exc:
        print(f"Database error in job_enrichment: {exc}")
        raise SystemExit(1) from exc

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
