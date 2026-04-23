import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import psycopg

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env
from scoring.utility.green_as_json import parse_application_questions

load_shared_env()

DEFAULT_RATE_PER_MINUTE = 12
MODEL_NAME = "gemini-2.5-flash"
PROMPT_FILE_NAME = "prompt1.txt"
PROMPT_PATH = Path(__file__).resolve().parent / PROMPT_FILE_NAME
ENRICHMENT_DISPLAY_DIR = SRC_ROOT / "scoring" / "enrichment_display"
TEXTAREA_HISTORY_TABLE = "green_apply_answers"
RECENT_APPLICATION_LIMIT = 10
EDITABLE_ANSWER_STYLES = {"text_area", "textarea", "input_text", "input"}

SOURCE_TRIVIAL_QUESTION_LABELS = {
    "first name",
    "last name",
    "preferred first name",
    "please enter your preferred last name/surname (only enter your preferred last name/surname)",
    "email",
    "phone",
    "linkedin profile",
    "website",
    "resume/cv",
    "cover letter",
    "gender",
    "race",
    "do you have a high school diploma, or have you successfully passed a high school equivalency exam such as the ged?",
    "veteranstatus",
    "disabilitystatus",
}

REQUESTED_TRIVIAL_QUESTION_LABELS = {
    "linked in profile",
    "linkedin",
    "linkedin url",
    "country code",
    "phone number",
    "resume",
    "school",
    "degree",
    "discipline",
    "veteran status",
    "veteran",
    "disability status",
    "disabled",
    "are you hispanic?",
    "hispanic",
    "are you a veteran?",
    "are you disabled?",
}


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


@dataclass(frozen=True)
class AcceptedEditableAnswer:
    """A curated editable answer stored for future prompt context."""

    job_id: int
    question_label: str
    answer_style: str
    answer_text: str


@dataclass
class PreparedApplication:
    """A prepared application ready for immediate review and persistence."""

    job: ApplicationPrepJob
    response_text: str
    response_payload: Optional[dict[str, Any]]
    recent_applications: list[dict[str, Any]]
    accepted_editable_answers: list[AcceptedEditableAnswer] = field(default_factory=list)
    count_as_packaged: bool = True


@dataclass
class PrepSummary:
    """Track the aggregate outcome of one application-prep run."""

    selected: int = 0
    packaged: int = 0
    api_failures: int = 0
    parse_failures: int = 0
    review_failures: int = 0
    database_failures: int = 0

    @property
    def failed(self) -> int:
        """Return the total number of failed jobs."""
        return (
            self.api_failures
            + self.parse_failures
            + self.review_failures
            + self.database_failures
        )

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
        "--test",
        action="store_true",
        help="Prepare only the first queued job with packaged_at IS NULL",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Prepare all queued jobs with packaged_at IS NULL",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum number of jobs to prepare during a full run",
    )
    parser.add_argument(
        "--redo",
        action="store_true",
        help="Prepare every queued job again, regardless of packaged completion state",
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


def normalize_question_label(label: Any) -> str:
    """Normalize a question label for stable registry matching."""
    if not isinstance(label, str):
        return ""
    return " ".join(label.split()).strip().lower()


def normalize_style_label(style: Any) -> str:
    """Normalize an answer style label for stable matching."""
    if not isinstance(style, str):
        return ""
    normalized = " ".join(style.split()).strip().lower()
    return normalized.replace("-", "_")


def is_text_area_answer(answer: Any) -> bool:
    """Return True when a Gemini answer targets a long-form text area."""
    if not isinstance(answer, dict):
        return False
    return normalize_style_label(answer.get("style")) in {"text_area", "textarea"}


def is_editable_answer(answer: Any) -> bool:
    """Return True when the answer can be edited in the review buffer."""
    if not isinstance(answer, dict):
        return False
    return normalize_style_label(answer.get("style")) in EDITABLE_ANSWER_STYLES


@lru_cache(maxsize=1)
def load_trivial_question_labels() -> set[str]:
    """Load the labels that should be removed before sending questions to Gemini."""
    labels = set(REQUESTED_TRIVIAL_QUESTION_LABELS)
    discovered_labels: set[str] = set()

    if not ENRICHMENT_DISPLAY_DIR.exists():
        return labels | SOURCE_TRIVIAL_QUESTION_LABELS

    for path in ENRICHMENT_DISPLAY_DIR.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue

        questions = payload.get("application_questions", [])
        if not isinstance(questions, list):
            continue

        for question in questions:
            if not isinstance(question, dict):
                continue
            normalized_label = normalize_question_label(question.get("label"))
            if normalized_label in SOURCE_TRIVIAL_QUESTION_LABELS:
                discovered_labels.add(normalized_label)

    return labels | SOURCE_TRIVIAL_QUESTION_LABELS | discovered_labels


def filter_application_questions(application_questions: Any) -> list[dict[str, Any]]:
    """Keep only non-trivial questions for Gemini to answer."""
    if not isinstance(application_questions, list):
        return []

    trivial_question_labels = load_trivial_question_labels()
    filtered_questions: list[dict[str, Any]] = []

    for question in application_questions:
        if not isinstance(question, dict):
            continue

        normalized_label = normalize_question_label(question.get("label"))
        if not normalized_label:
            continue

        if normalized_label in trivial_question_labels:
            continue

        filtered_questions.append(question)

    return filtered_questions


def ensure_textarea_answer_history_table(conn: psycopg.Connection) -> None:
    """Create the accepted-textarea history table when it is missing."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TEXTAREA_HISTORY_TABLE} (
                answer_id BIGSERIAL PRIMARY KEY,
                job_id INTEGER NOT NULL REFERENCES green_job(job_id) ON DELETE CASCADE,
                question_label TEXT NOT NULL,
                answer_style TEXT NOT NULL DEFAULT 'Text_Area',
                answer_text TEXT NOT NULL,
                prompt TEXT NOT NULL,
                model TEXT NOT NULL,
                accepted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            f"""
            ALTER TABLE {TEXTAREA_HISTORY_TABLE}
            ADD COLUMN IF NOT EXISTS answer_style TEXT NOT NULL DEFAULT 'Text_Area'
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {TEXTAREA_HISTORY_TABLE}_accepted_at_idx
            ON {TEXTAREA_HISTORY_TABLE} (accepted_at DESC, answer_id DESC)
            """
        )


def fetch_recent_applications(
    conn: psycopg.Connection,
    limit: int = RECENT_APPLICATION_LIMIT,
) -> list[dict[str, Any]]:
    """Load the most recent accepted applications for prompt context."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH recent_jobs AS (
                SELECT
                    job_id,
                    MAX(accepted_at) AS last_accepted_at
                FROM {TEXTAREA_HISTORY_TABLE}
                GROUP BY job_id
                ORDER BY last_accepted_at DESC, job_id DESC
                LIMIT %s
            )
            SELECT
                recent_jobs.job_id,
                gj.company_name,
                gj.title,
                gj.location,
                ge.description,
                gaa.question_label,
                gaa.answer_style,
                gaa.answer_text,
                gaa.prompt,
                gaa.model,
                gaa.accepted_at
            FROM recent_jobs
            JOIN green_job AS gj
              ON gj.job_id = recent_jobs.job_id
            LEFT JOIN green_enrich AS ge
              ON ge.job_id = recent_jobs.job_id
            JOIN {TEXTAREA_HISTORY_TABLE} AS gaa
              ON gaa.job_id = recent_jobs.job_id
            ORDER BY recent_jobs.last_accepted_at DESC,
                     recent_jobs.job_id DESC,
                     gaa.accepted_at ASC,
                     gaa.answer_id ASC
            """,
            (limit,),
        )
        rows = cur.fetchall()

    applications: list[dict[str, Any]] = []
    current_job_id: Optional[int] = None
    current_application: Optional[dict[str, Any]] = None

    def serialize_value(value: Any) -> Any:
        if hasattr(value, "isoformat"):
            return value.isoformat(sep=" ", timespec="seconds")
        return value

    for row in rows:
        job_id = row[0]
        if job_id != current_job_id:
            current_job_id = job_id
            current_application = {
                "job_id": job_id,
                "company_name": serialize_value(row[1]),
                "title": serialize_value(row[2]),
                "location": serialize_value(row[3]),
                "description": serialize_value(row[4]),
                "answers": [],
                "last_accepted_at": serialize_value(row[10]),
            }
            applications.append(current_application)

        if current_application is None:
            continue

        current_application["answers"].append(
            {
                "question_label": serialize_value(row[5]),
                "answer_style": serialize_value(row[6]),
                "answer_text": serialize_value(row[7]),
                "prompt": serialize_value(row[8]),
                "model": serialize_value(row[9]),
                "accepted_at": serialize_value(row[10]),
            }
        )

    return applications


def format_recent_applications(entries: list[dict[str, Any]]) -> str:
    """Render recent accepted applications as prompt context."""
    if not entries:
        return "No accepted applications are available yet."

    return json.dumps(entries, ensure_ascii=False, indent=2)


def find_editor_command() -> Optional[str]:
    """Return the first usable terminal editor command."""
    editor_env = os.getenv("EDITOR")
    candidates = ["nvim"]
    if editor_env:
        candidates.append(editor_env)
    candidates.extend(["vim", "vi", "nano"])

    for candidate in candidates:
        if candidate and shutil.which(candidate):
            return candidate
    return None


def extract_editable_answers(response_payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the editable answers from a parsed model payload."""
    answers = response_payload.get("answers")
    if not isinstance(answers, list):
        return []
    return [answer for answer in answers if is_editable_answer(answer)]


def build_review_editor_payload(
    job: ApplicationPrepJob,
    response_payload: dict[str, Any],
    recent_applications: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build the reduced review document opened in nvim."""
    return {
        "job_description": job.description,
        "instructions": [
            "Edit only the editable_answers array.",
            "Leave question_label values unchanged.",
            "Do not modify job_description or recent_applications sections.",
        ],
        "editable_answers": extract_editable_answers(response_payload),
        "recent_applications": recent_applications,
    }


def open_response_in_editor(review_payload: dict[str, Any]) -> dict[str, Any]:
    """Open the reduced review JSON in one editor session and return the edited payload."""
    editor = find_editor_command()
    if editor is None:
        raise RuntimeError("No terminal editor was found. Install nvim or set EDITOR.")

    with tempfile.NamedTemporaryFile(
        mode="w+",
        encoding="utf-8",
        suffix=".json",
        delete=False,
    ) as tmp:
        tmp.write(json.dumps(review_payload, ensure_ascii=False, indent=2))
        tmp.flush()
        temp_path = Path(tmp.name)

    try:
        subprocess.run([editor, str(temp_path)], check=True)
        edited_text = temp_path.read_text(encoding="utf-8")
        edited_payload = json.loads(edited_text)
        if not isinstance(edited_payload, dict):
            raise ValueError("Edited response must be a JSON object")
        return edited_payload
    finally:
        temp_path.unlink(missing_ok=True)


def prompt_for_editable_review(job: ApplicationPrepJob, answer_count: int) -> bool:
    """Ask whether the user wants to edit the staged editable answers."""
    print()
    print(
        f"job_id={job.job_id} has {answer_count} editable answer(s) ready for review."
    )
    while True:
        choice = input("Edit answers now? [y/n]: ").strip().lower()
        if choice == "y":
            return True
        if choice == "n":
            return False
        print("Enter y or n.")


def collect_accepted_editable_answers(
    job_id: int,
    response_payload: dict[str, Any],
) -> list[AcceptedEditableAnswer]:
    """Turn editable answers into rows for the accepted-answer history table."""
    accepted_answers: list[AcceptedEditableAnswer] = []
    answers = response_payload.get("answers")
    if not isinstance(answers, list):
        return accepted_answers

    for answer in answers:
        if not is_editable_answer(answer):
            continue

        question_label = str(answer.get("question label") or "").strip()
        answer_style = str(answer.get("style") or "").strip()
        answer_text_raw = answer.get("answer label")
        if isinstance(answer_text_raw, str):
            answer_text = answer_text_raw
        elif answer_text_raw is None:
            answer_text = ""
        else:
            answer_text = str(answer_text_raw)

        if answer_text.strip():
            accepted_answers.append(
                AcceptedEditableAnswer(
                    job_id=job_id,
                    question_label=question_label,
                    answer_style=answer_style,
                    answer_text=answer_text,
                )
            )

    return accepted_answers


def merge_reviewed_answers(
    original_payload: dict[str, Any],
    reviewed_payload: dict[str, Any],
) -> dict[str, Any]:
    """Merge edited answer labels from the review document back into the Gemini payload."""
    editable_answers = reviewed_payload.get("editable_answers")
    if not isinstance(editable_answers, list):
        raise ValueError("Review payload must include an editable_answers array")

    original_answers = extract_editable_answers(original_payload)
    if len(editable_answers) != len(original_answers):
        raise ValueError(
            "editable_answers count changed during review; please keep the same rows"
        )

    merged_payload = json.loads(json.dumps(original_payload, ensure_ascii=False))
    merged_answers = merged_payload.get("answers")
    if not isinstance(merged_answers, list):
        raise ValueError("Original response must include an answers array")

    editable_index = 0
    for answer in merged_answers:
        if not is_editable_answer(answer):
            continue

        reviewed_answer = editable_answers[editable_index]
        if not isinstance(reviewed_answer, dict):
            raise ValueError("Each editable answer must remain a JSON object")
        reviewed_label = reviewed_answer.get("question label")
        original_label = answer.get("question label")
        if reviewed_label != original_label:
            raise ValueError("question_label values must not change during review")

        answer_text = reviewed_answer.get("answer label")
        if isinstance(answer_text, str):
            answer["answer label"] = answer_text
        elif answer_text is None:
            answer["answer label"] = ""
        else:
            answer["answer label"] = str(answer_text)

        editable_index += 1

    return merged_payload


def review_response_payload(
    job: ApplicationPrepJob,
    response_payload: dict[str, Any],
    recent_applications: list[dict[str, Any]],
) -> tuple[dict[str, Any], int]:
    """Optionally open one editor for the editable answers in a staged response."""
    editable_answers = extract_editable_answers(response_payload)
    if not editable_answers:
        return response_payload, 0

    if not prompt_for_editable_review(job, len(editable_answers)):
        return response_payload, 0

    review_payload = build_review_editor_payload(job, response_payload, recent_applications)

    while True:
        try:
            edited_review_payload = open_response_in_editor(review_payload)
            merged_payload = merge_reviewed_answers(response_payload, edited_review_payload)
            parse_ai_response(
                json.dumps(merged_payload, ensure_ascii=False),
                job.job_id,
            )
            return merged_payload, len(editable_answers)
        except json.JSONDecodeError as exc:
            print(f"job_id={job.job_id} edited JSON is invalid: {exc}")
        except ValueError as exc:
            print(f"job_id={job.job_id} edited response is invalid: {exc}")

        while True:
            retry = input("Reopen nvim and try again? [y/n]: ").strip().lower()
            if retry == "y":
                break
            if retry == "n":
                raise RuntimeError(
                    f"job_id={job.job_id} editable answer review was aborted before a valid save"
                )
            print("Enter y or n.")


def fetch_jobs_to_prepare(
    conn: psycopg.Connection,
    mode: str,
    limit: Optional[int],
) -> list[ApplicationPrepJob]:
    """Load queued jobs that still need application preparation."""
    packaged_clause = ""
    if mode in {"test", "full"}:
        packaged_clause = "WHERE ga.packaged_at IS NULL"

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
        {packaged_clause}
        AND ga.submitted_at IS NULL
        ORDER BY gs.overall DESC, ga.job_id ASC
    """.format(packaged_clause=packaged_clause)

    params: list[Any] = []
    if mode == "test":
        query += " LIMIT 1"
    elif mode == "full" and limit is not None:
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
        "application_questions": filter_application_questions(job.application_questions),
    }


def load_prompt_template() -> str:
    """Load the application-prep prompt template."""
    return PROMPT_PATH.read_text(encoding="utf-8")


def render_prompt(
    prompt_template: str,
    job_payload: dict[str, Any],
    recent_applications: list[dict[str, Any]],
) -> str:
    """Render the final prompt by injecting the job JSON and application context."""
    rendered_job = json.dumps(job_payload, ensure_ascii=False, indent=2)
    rendered_context = format_recent_applications(recent_applications)
    return (
        prompt_template.replace("{RECENT APPLICATIONS HERE}", rendered_context)
        .replace("{JOB JSON HERE}", rendered_job)
    )


def build_client() -> Any:
    """Create the Gemini client from the configured API key."""
    try:
        from google import genai
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "google-genai is required to prepare application answers"
        ) from exc

    api_key = os.getenv("GEMINI_API")
    if not api_key:
        raise ValueError("Missing GEMINI_API environment variable")
    return genai.Client(api_key=api_key)


def request_ai_response(client: Any, prompt: str) -> str:
    """Send the prep prompt to Gemini and return the raw response text."""
    from google.genai import types

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


def build_empty_response(job_id: int) -> str:
    """Create a deterministic empty-answer response when no non-trivial prompts remain."""
    return json.dumps(
        {
            "job_id": job_id,
            "answers": [],
            "confidence": "low",
        },
        ensure_ascii=False,
    )


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


def persist_response(
    conn: psycopg.Connection,
    job_id: int,
    response_text: str,
    accepted_editable_answers: list[AcceptedEditableAnswer],
) -> None:
    """Store the approved response, answer history, and packaged marker."""
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE green_apply
                SET response = %s,
                    prompt = %s,
                    model = %s,
                    packaged_at = NOW()
                WHERE job_id = %s
                """,
                (response_text, PROMPT_FILE_NAME, MODEL_NAME, job_id),
            )

            for accepted_answer in accepted_editable_answers:
                cur.execute(
                    f"""
                    INSERT INTO {TEXTAREA_HISTORY_TABLE} (
                        job_id,
                        question_label,
                        answer_style,
                        answer_text,
                        prompt,
                        model
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        accepted_answer.job_id,
                        accepted_answer.question_label,
                        accepted_answer.answer_style,
                        accepted_answer.answer_text,
                        PROMPT_FILE_NAME,
                        MODEL_NAME,
                    ),
                )


def count_jobs_to_prepare(conn: psycopg.Connection, mode: str) -> int:
    """Count the queued jobs eligible for the selected mode."""
    if mode == "redo":
        query = "SELECT COUNT(*) FROM green_apply AS ga"
    else:
        query = "SELECT COUNT(*) FROM green_apply AS ga WHERE ga.packaged_at IS NULL"

    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()

    return int(row[0]) if row else 0


def prepare_applications(mode: str, limit: Optional[int], rate_per_minute: int) -> PrepSummary:
    """Prepare queued applications with AI-generated answers."""
    prompt_template = load_prompt_template()
    client = build_client()
    rate_limiter = EvenRateLimiter(rate_per_minute=rate_per_minute)
    summary = PrepSummary()

    with db_connect() as conn:
        ensure_textarea_answer_history_table(conn)
        summary.available = count_jobs_to_prepare(conn, mode)
        jobs = fetch_jobs_to_prepare(conn, mode, limit)
        summary.selected = len(jobs)
        if not jobs:
            print("No queued jobs need application prep")
            return summary

        print(
            f"mode={mode} selected={len(jobs)} limit={'all' if limit is None else limit} "
            f"rate_per_minute={rate_per_minute} model={MODEL_NAME}"
        )

        for job in jobs:
            job_payload = build_job_payload(job)
            filtered_questions = job_payload["application_questions"]
            recent_applications = fetch_recent_applications(conn)

            if not filtered_questions:
                empty_response = build_empty_response(job.job_id)
                try:
                    persist_response(conn, job.job_id, empty_response, [])
                except psycopg.Error as exc:
                    print(f"job_id={job.job_id} database failed: {exc}")
                    summary.database_failures += 1
                    continue

                summary.packaged += 1
                print(f"job_id={job.job_id} packaged (no non-trivial questions)")
                continue

            prompt = render_prompt(prompt_template, job_payload, recent_applications)

            try:
                rate_limiter.acquire()
                response_text = request_ai_response(client, prompt)
            except Exception as exc:
                print(f"job_id={job.job_id} api failed: {exc}")
                summary.api_failures += 1
                continue

            try:
                response_payload = parse_ai_response(response_text, job.job_id)
            except Exception as exc:
                print(f"job_id={job.job_id} parse failed: {exc}")
                summary.parse_failures += 1
                try:
                    persist_response(conn, job.job_id, response_text, [])
                except psycopg.Error as db_exc:
                    print(f"job_id={job.job_id} database failed: {db_exc}")
                    summary.database_failures += 1
                    continue
                print(f"job_id={job.job_id} stored raw response after parse failure")
                continue

            accepted_editable_answers: list[AcceptedEditableAnswer] = []
            editable_count = len(extract_editable_answers(response_payload))
            final_response_text = response_text

            if editable_count > 0:
                try:
                    final_response_payload, editable_count = review_response_payload(
                        job,
                        response_payload,
                        recent_applications,
                    )
                except RuntimeError as exc:
                    print(str(exc))
                    summary.review_failures += 1
                    continue

                final_response_text = json.dumps(
                    final_response_payload,
                    ensure_ascii=False,
                    indent=2,
                )
                accepted_editable_answers = collect_accepted_editable_answers(
                    job.job_id,
                    final_response_payload,
                )

            try:
                persist_response(
                    conn,
                    job.job_id,
                    final_response_text,
                    accepted_editable_answers,
                )
            except psycopg.Error as exc:
                print(f"job_id={job.job_id} database failed: {exc}")
                summary.database_failures += 1
                continue

            summary.packaged += 1
            if editable_count > 0:
                print(f"job_id={job.job_id} packaged with {editable_count} editable answer(s)")
            else:
                print(f"job_id={job.job_id} packaged")

    return summary


def main() -> None:
    """CLI entrypoint for application preparation."""
    args = parse_args()
    mode_flags = [args.test, args.full, args.redo]
    if sum(1 for flag in mode_flags if flag) > 1:
        print("Choose only one of --test, --full, or --redo")
        raise SystemExit(1)
    if args.limit is not None and args.limit <= 0:
        print("limit must be greater than 0")
        raise SystemExit(1)
    if args.limit is not None and not args.full:
        print("--limit can only be used with --full")
        raise SystemExit(1)
    if args.rate_per_minute <= 0:
        print("rate-per-minute must be greater than 0")
        raise SystemExit(1)

    if args.test:
        mode = "test"
    elif args.redo:
        mode = "redo"
    else:
        mode = "full"

    try:
        summary = prepare_applications(
            mode=mode,
            limit=resolve_limit(args.limit),
            rate_per_minute=args.rate_per_minute,
        )
    except (OSError, ValueError, psycopg.Error) as exc:
        print(str(exc))
        raise SystemExit(1) from exc

    print(
        f"Final summary: selected={summary.selected} packaged={summary.packaged} "
        f"api_failures={summary.api_failures} parse_failures={summary.parse_failures} "
        f"review_failures={summary.review_failures} "
        f"database_failures={summary.database_failures}"
    )
    if not summary.success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
