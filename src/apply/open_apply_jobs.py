import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.error import URLError
from urllib.request import urlopen

import psycopg

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env
from scoring.utility.green_as_json import parse_application_questions

load_shared_env()

COMMON_QUESTIONS_PATH = Path(__file__).resolve().parent / "green_questions" / "common_questions.json"
DEFAULT_CDP_ENDPOINT = "http://127.0.0.1:9222"
DEFAULT_LIMIT = 1
DEFAULT_WAIT_SECONDS = 30
TEXT_FIELD_TYPES = {"input_text", "textarea"}
SINGLE_SELECT_FIELD_TYPES = {"multi_value_single_select"}
MULTI_SELECT_FIELD_TYPES = {"multi_value_multi_select"}


@dataclass
class ReadyApplyJob:
    """Joined apply row ready to open and fill in the browser."""

    job_id: int
    url: str
    company_name: Optional[str]
    title: Optional[str]
    application_questions: list[dict[str, Any]]
    response_text: Optional[str]


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
    """Parse CLI arguments for the Playwright browser opener."""
    parser = argparse.ArgumentParser(
        description="Open and fill queued Greenhouse application URLs in a Chrome profile."
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


def normalize_text(value: Any) -> str:
    """Normalize labels and answers for stable matching."""
    if not isinstance(value, str):
        return ""
    return " ".join(value.split()).strip().lower()


def load_common_question_answers() -> dict[str, str]:
    """Load the hardcoded common-question registry keyed by normalized label."""
    raw_text = COMMON_QUESTIONS_PATH.read_text(encoding="utf-8")
    payload = json.loads(raw_text)
    questions = payload.get("questions", [])
    if not isinstance(questions, list):
        return {}

    answers: dict[str, str] = {}
    for question in questions:
        if not isinstance(question, dict):
            continue
        label = normalize_text(question.get("label"))
        answer = question.get("answer")
        if label and isinstance(answer, str):
            answers[label] = answer
    return answers


def parse_llm_answer_map(response_text: Optional[str], expected_job_id: int) -> dict[str, Any]:
    """Parse the stored Gemini response into a normalized question->answer map."""
    if not response_text or not response_text.strip():
        return {}

    payload = json.loads(response_text)
    if not isinstance(payload, dict):
        raise ValueError("Gemini response must be a JSON object")

    raw_job_id = payload.get("job_id")
    if isinstance(raw_job_id, str) and raw_job_id.strip().isdigit():
        raw_job_id = int(raw_job_id.strip())
    if raw_job_id != expected_job_id:
        raise ValueError("Gemini response job_id does not match the requested job")

    answers = payload.get("answers")
    if not isinstance(answers, list):
        raise ValueError("Gemini response must include an answers array")

    answer_map: dict[str, Any] = {}
    for item in answers:
        if not isinstance(item, dict):
            continue
        question = normalize_text(item.get("question"))
        answer = item.get("answer")
        if question and answer is not None:
            answer_map[question] = answer
    return answer_map


def field_options(field: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the selectable options for a field, if any."""
    options = field.get("options")
    if isinstance(options, list):
        return [option for option in options if isinstance(option, dict)]
    answer_options = field.get("answer_options")
    if isinstance(answer_options, list):
        return [option for option in answer_options if isinstance(option, dict)]
    return []


def resolve_option(field: dict[str, Any], answer: Any) -> Optional[dict[str, Any]]:
    """Find the select/radio option that matches an answer."""
    normalized_answer = normalize_text(answer)
    if not normalized_answer:
        return None

    for option in field_options(field):
        label = normalize_text(option.get("label"))
        value = normalize_text(option.get("value"))
        if label == normalized_answer or value == normalized_answer:
            return option
    return None


def fetch_ready_jobs(conn: psycopg.Connection, limit: int) -> list[ReadyApplyJob]:
    """Load the next queued jobs that still need browser automation."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                gj.job_id,
                gj.url,
                gj.company_name,
                gj.title,
                ge.application_questions,
                ga.response
            FROM green_apply AS ga
            JOIN green_job AS gj
              ON gj.job_id = ga.job_id
            JOIN green_score AS gs
              ON gs.job_id = ga.job_id
            JOIN green_enrich AS ge
              ON ge.job_id = ga.job_id
            WHERE ga.questions IS TRUE
              AND gj.url IS NOT NULL
            ORDER BY gs.overall DESC, ga.job_id ASC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()

    jobs: list[ReadyApplyJob] = []
    for row in rows:
        application_questions = parse_application_questions(row[4])
        if not isinstance(application_questions, list):
            application_questions = []
        jobs.append(
            ReadyApplyJob(
                job_id=row[0],
                url=row[1],
                company_name=row[2],
                title=row[3],
                application_questions=application_questions,
                response_text=row[5],
            )
        )
    return jobs


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


def fill_text_field(page: Any, field: dict[str, Any], question_label: str, answer: Any) -> bool:
    """Fill a text or textarea question."""
    name = field.get("name")
    answer_text = str(answer)

    if isinstance(name, str) and name:
        locator = page.locator(f'[name="{name}"]')
        if locator.count():
            locator.first.fill(answer_text)
            return True

    try:
        page.get_by_label(question_label, exact=True).fill(answer_text)
        return True
    except Exception:
        return False


def fill_single_select(page: Any, field: dict[str, Any], question_label: str, answer: Any) -> bool:
    """Fill a single-select question using the resolved option."""
    option = resolve_option(field, answer)
    if option is None:
        return False

    name = field.get("name")
    option_label = option.get("label")
    option_value = option.get("value")

    if isinstance(name, str) and name:
        select_locator = page.locator(f'select[name="{name}"]')
        if select_locator.count():
            try:
                if isinstance(option_label, str):
                    select_locator.first.select_option(label=option_label)
                elif option_value is not None:
                    select_locator.first.select_option(value=str(option_value))
                else:
                    return False
            except Exception:
                if option_value is None:
                    return False
                select_locator.first.select_option(value=str(option_value))
            return True

        if option_value is not None:
            radio_locator = page.locator(f'input[name="{name}"][value="{option_value}"]')
            if radio_locator.count():
                radio_locator.first.check()
                return True

            checkbox_locator = page.locator(f'input[name="{name}"][value="{option_value}"]')
            if checkbox_locator.count():
                checkbox_locator.first.check()
                return True

    try:
        if isinstance(option_label, str):
            page.get_by_label(option_label, exact=True).check()
            return True
    except Exception:
        pass

    try:
        if isinstance(option_label, str):
            page.get_by_label(question_label, exact=True).select_option(label=option_label)
        return True
    except Exception:
        return False


def fill_multi_select(page: Any, field: dict[str, Any], question_label: str, answer: Any) -> bool:
    """Fill a multi-select question."""
    answers: list[Any]
    if isinstance(answer, list):
        answers = answer
    else:
        answers = [answer]

    name = field.get("name")
    filled_any = False
    for single_answer in answers:
        option = resolve_option(field, single_answer)
        if option is None:
            continue

        option_value = option.get("value")
        option_label = option.get("label")

        if isinstance(name, str) and name:
            if option_value is not None:
                checkbox_locator = page.locator(f'input[name="{name}"][value="{option_value}"]')
                if checkbox_locator.count():
                    checkbox_locator.first.check()
                    filled_any = True
                    continue

        try:
            if isinstance(option_label, str):
                page.get_by_label(option_label, exact=True).check()
                filled_any = True
        except Exception:
            continue

    if filled_any:
        return True

    try:
        page.get_by_label(question_label, exact=True).check()
        return True
    except Exception:
        return False


def fill_question(page: Any, question: dict[str, Any], answer: Any) -> bool:
    """Fill one Greenhouse question from a parsed answer."""
    label = question.get("label")
    if not isinstance(label, str) or not label.strip():
        return False

    fields = question.get("fields")
    if not isinstance(fields, list):
        return False

    for field in fields:
        if not isinstance(field, dict):
            continue
        field_type = field.get("type")
        if field_type in TEXT_FIELD_TYPES:
            if fill_text_field(page, field, label, answer):
                return True
        elif field_type in SINGLE_SELECT_FIELD_TYPES:
            if fill_single_select(page, field, label, answer):
                return True
        elif field_type in MULTI_SELECT_FIELD_TYPES:
            if fill_multi_select(page, field, label, answer):
                return True

    return False


def fill_questions_by_registry(
    page: Any,
    questions: list[dict[str, Any]],
    answer_lookup: dict[str, Any],
) -> tuple[int, list[str]]:
    """Fill the provided questions using a normalized answer lookup."""
    filled = 0
    missing: list[str] = []
    for question in questions:
        label = normalize_text(question.get("label"))
        if not label:
            continue
        if label not in answer_lookup:
            missing.append(question.get("label") if isinstance(question.get("label"), str) else label)
            continue
        if fill_question(page, question, answer_lookup[label]):
            filled += 1
        else:
            missing.append(question.get("label") if isinstance(question.get("label"), str) else label)
    return filled, missing


def fill_job_page(page: Any, job: ReadyApplyJob) -> None:
    """Fill the common questions first, then the LLM-returned response questions."""
    common_answers = load_common_question_answers()
    llm_answers = parse_llm_answer_map(job.response_text, job.job_id)

    common_questions: list[dict[str, Any]] = []
    custom_questions: list[dict[str, Any]] = []

    for question in job.application_questions:
        if not isinstance(question, dict):
            continue
        label = normalize_text(question.get("label"))
        if not label:
            continue
        if label in common_answers:
            common_questions.append(question)
        else:
            custom_questions.append(question)

    common_filled, common_missing = fill_questions_by_registry(page, common_questions, common_answers)
    print(
        f"job_id={job.job_id} common_filled={common_filled} common_missing={len(common_missing)}"
    )
    for missing_label in common_missing:
        print(f"job_id={job.job_id} missing common question: {missing_label}")

    custom_filled, custom_missing = fill_questions_by_registry(page, custom_questions, llm_answers)
    print(
        f"job_id={job.job_id} custom_filled={custom_filled} custom_missing={len(custom_missing)}"
    )
    for missing_label in custom_missing:
        print(f"job_id={job.job_id} missing custom question: {missing_label}")


def open_job_urls(endpoint: str, jobs: list[ReadyApplyJob]) -> None:
    """Attach to Chrome over CDP and open each queued job URL in a new tab."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "playwright is not installed. Install Playwright before running this script."
        ) from exc

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
            print(f"opened job_id={job.job_id} url={job.url}")
            fill_job_page(page, job)


def main() -> None:
    """CLI entrypoint for opening and filling queued apply URLs in Chrome."""
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
