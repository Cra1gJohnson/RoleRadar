import argparse
import asyncio
import json
import os
import re
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
import cover
from scoring.utility.green_as_json import parse_application_questions

load_shared_env()

DEFAULT_RATE_PER_MINUTE = 24
DEFAULT_MAX_CONCURRENCY = 8
MODEL_NAME = "gemini-2.5-flash"
INPUT_COST_PER_MILLION_TOKENS = 0.30
OUTPUT_COST_PER_MILLION_TOKENS = 2.50
INPUT_COST_PER_TOKEN = INPUT_COST_PER_MILLION_TOKENS / 1_000_000
OUTPUT_COST_PER_TOKEN = OUTPUT_COST_PER_MILLION_TOKENS / 1_000_000
PROMPT_WITH_COVER_FILE_NAME = "prompt1.txt"
PROMPT_WITHOUT_COVER_FILE_NAME = "prompt2.txt"
APPLY_DIR = Path(__file__).resolve().parent
DEFAULT_RESUME_PDF_PATH = SRC_ROOT.parent / "templates" / "resume" / "CPJohnson_resume.pdf"
ENRICHMENT_DISPLAY_DIR = SRC_ROOT / "scoring" / "enrichment_display"
TEXTAREA_HISTORY_TABLE = "apply_answers"
COVER_HISTORY_TABLE = "apply_cover"
ACCEPTED_ANSWER_CONTEXT_LIMIT = 20
EDITABLE_ANSWER_STYLES = {"text_area", "input_text"}
ANSWER_LABEL_KEY = "answer label"
ANSWER_LABEL_ALIASES = (
    "answer label",
    "answer_label",
    "answerLabel",
    "answer text",
    "answer_text",
    "answered text",
    "answered_text",
    "answeredText",
)

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
    existing_cover_letter: Optional[str]


@dataclass(frozen=True)
class AcceptedEditableAnswer:
    """A curated editable answer stored for future prompt context."""

    job_id: int
    question_label: str
    style: str
    answer_label: str


@dataclass
class PreparedApplication:
    """A prepared application ready for immediate review and persistence."""

    job: ApplicationPrepJob
    response_text: str
    response_payload: Optional[dict[str, Any]]
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
    cover_failures: int = 0
    prompt_tokens: int = 0
    response_tokens: int = 0
    total_tokens: int = 0
    prompt_cost: float = 0.0
    response_cost: float = 0.0
    total_cost: float = 0.0

    @property
    def failed(self) -> int:
        """Return the total number of failed jobs."""
        return (
            self.api_failures
            + self.parse_failures
            + self.review_failures
            + self.database_failures
            + self.cover_failures
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


class AsyncEvenRateLimiter:
    """Async variant of the even dispatch limiter."""

    def __init__(self, rate_per_minute: int) -> None:
        self.dispatch_interval = 60.0 / rate_per_minute
        self.next_dispatch_at = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            sleep_seconds = self.next_dispatch_at - now
            if sleep_seconds > 0:
                await asyncio.sleep(sleep_seconds)
                now = time.monotonic()

            self.next_dispatch_at = max(self.next_dispatch_at + self.dispatch_interval, now)


@dataclass(frozen=True)
class PendingPromptRequest:
    """One prepared Gemini request waiting to be dispatched."""

    job: ApplicationPrepJob
    prompt: str
    prompt_file_name: str
    generate_cover_letter: bool
    has_cover_letter_question: bool


@dataclass(frozen=True)
class PromptRequestResult:
    """One completed Gemini request, successful or failed."""

    request: PendingPromptRequest
    response_text: Optional[str] = None
    prompt_tokens: int = 0
    response_tokens: int = 0
    total_tokens: int = 0
    error: Optional[str] = None


@dataclass(frozen=True)
class GeminiPrepResponse:
    """Raw Gemini response text and token accounting."""

    response_text: str
    prompt_tokens: int = 0
    response_tokens: int = 0
    total_tokens: int = 0


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
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=DEFAULT_MAX_CONCURRENCY,
        help="Maximum number of in-flight Gemini requests",
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


def has_cover_letter_question(application_questions: Any) -> bool:
    """Return True when raw application questions include a cover letter field."""
    if not isinstance(application_questions, list):
        return False

    pattern = re.compile(r"\bcover\W*letter\b", re.IGNORECASE)
    for question in application_questions:
        if not isinstance(question, dict):
            continue

        for key in ("label", "name", "question"):
            value = question.get(key)
            if isinstance(value, str) and pattern.search(value):
                return True

    return False


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


def fetch_accepted_answer_context(
    conn: psycopg.Connection,
    limit: int = ACCEPTED_ANSWER_CONTEXT_LIMIT,
) -> list[dict[str, Any]]:
    """Load recent accepted answers for model-only prompt context."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                question_label,
                answer_label,
                style
            FROM {TEXTAREA_HISTORY_TABLE}
            ORDER BY accepted_at DESC, answer_id DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()

    return [
        {
            "question_label": row[0],
            "answer_label": row[1],
            "style": row[2],
        }
        for row in rows
    ]


def format_accepted_answer_context(entries: list[dict[str, Any]]) -> str:
    """Render accepted answers without leaking response-schema lookalike keys."""
    if not entries:
        return "[]"

    examples = [
        {
            "question_label": entry.get("question_label", ""),
            "accepted_answer": entry.get("answer_label", ""),
            "style": entry.get("style", ""),
        }
        for entry in entries
    ]
    return json.dumps(examples, ensure_ascii=False, indent=2)


def extract_answer_label(answer: dict[str, Any]) -> Any:
    """Return the answer text from the canonical key or known model mistakes."""
    for key in ANSWER_LABEL_ALIASES:
        if key in answer:
            return answer.get(key)
    return ""


def canonicalize_answer(answer: dict[str, Any]) -> dict[str, Any]:
    """Normalize one model answer to the exact response schema expected downstream."""
    canonical_answer = dict(answer)
    answer_label = extract_answer_label(canonical_answer)
    canonical_answer[ANSWER_LABEL_KEY] = (
        answer_label if isinstance(answer_label, str) else str(answer_label or "")
    )

    for key in ANSWER_LABEL_ALIASES:
        if key != ANSWER_LABEL_KEY:
            canonical_answer.pop(key, None)

    return canonical_answer


def canonicalize_response_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize all answer rows to use only the canonical `answer label` key."""
    answers = payload.get("answers")
    if not isinstance(answers, list):
        raise ValueError("Gemini response must include an answers array")

    normalized_payload = json.loads(json.dumps(payload, ensure_ascii=False))
    normalized_payload["answers"] = [
        canonicalize_answer(answer) if isinstance(answer, dict) else answer
        for answer in answers
    ]
    return normalized_payload


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
    include_cover_letter: bool,
) -> dict[str, Any]:
    """Build the reduced review document opened in nvim."""
    review_payload = {
        "instructions": [
            "Edit only the editable_answers array.",
            "Leave question_label values unchanged.",
        ],
        "editable_answers": extract_editable_answers(response_payload),
    }
    if include_cover_letter:
        cover_letter = cover.normalize_cover_letter_payload(response_payload)
        cover_letter["company_name"] = cover_letter["company_name"] or job.company_name or ""
        cover_letter["job_title"] = cover_letter["job_title"] or job.title or ""
        review_payload["instructions"] = [
            "Edit only cover_letter and the editable_answers array.",
            "The cover_letter company_name and job_title are used in the compiled PDF.",
            "Leave question_label values unchanged.",
        ]
        review_payload["cover_letter"] = cover_letter

    return review_payload


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


def prompt_for_editable_review(
    job: ApplicationPrepJob,
    answer_count: int,
    has_cover_letter: bool,
) -> bool:
    """Ask whether the user wants to edit the staged editable answers."""
    print()
    print(f"Job title: {job.title or 'N/A'}")
    print(f"Job URL: {job.url or 'N/A'}")
    print(f"Job ID: {job.job_id}")
    if answer_count:
        print(f"Questions that need approval: {answer_count}")
    else:
        print("No questions need approval")
    print(
        "Cover letter first paragraph needs approval: "
        f"{'yes' if has_cover_letter else 'no'}"
    )

    while True:
        choice = input("Edit the Model response? y or n : ").strip().lower()
        if choice == "y":
            if answer_count == 0 and not has_cover_letter:
                print("No editable model content is available for this response.")
                return False
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
        style = str(answer.get("style") or "").strip()
        answer_label_raw = extract_answer_label(answer)
        if isinstance(answer_label_raw, str):
            answer_label = answer_label_raw
        elif answer_label_raw is None:
            answer_label = ""
        else:
            answer_label = str(answer_label_raw)

        if answer_label.strip():
            accepted_answers.append(
                AcceptedEditableAnswer(
                    job_id=job_id,
                    question_label=question_label,
                    style=style,
                    answer_label=answer_label,
                )
            )

    return accepted_answers


def merge_reviewed_answers(
    original_payload: dict[str, Any],
    reviewed_payload: dict[str, Any],
    include_cover_letter: bool,
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
    if include_cover_letter:
        merged_payload = cover.apply_cover_letter_review(merged_payload, reviewed_payload)
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

        answer_label = extract_answer_label(reviewed_answer)
        if isinstance(answer_label, str):
            answer["answer label"] = answer_label
        elif answer_label is None:
            answer["answer label"] = ""
        else:
            answer["answer label"] = str(answer_label)

        editable_index += 1

    return merged_payload


def review_response_payload(
    job: ApplicationPrepJob,
    response_payload: dict[str, Any],
    include_cover_letter: bool,
) -> tuple[dict[str, Any], int]:
    """Optionally open one editor for the editable answers in a staged response."""
    if include_cover_letter:
        cover_letter = response_payload.get("cover_letter")
        if isinstance(cover_letter, dict):
            company_name = cover_letter.get("company_name")
            job_title = cover_letter.get("job_title")
            if not isinstance(company_name, str) or not company_name.strip():
                cover_letter["company_name"] = job.company_name or ""
            if not isinstance(job_title, str) or not job_title.strip():
                cover_letter["job_title"] = job.title or ""

    editable_answers = extract_editable_answers(response_payload)
    has_cover_letter = include_cover_letter and bool(
        cover.normalize_cover_letter_payload(response_payload)
    )
    if not editable_answers and not has_cover_letter:
        return response_payload, 0

    if not prompt_for_editable_review(job, len(editable_answers), has_cover_letter):
        return response_payload, 0

    review_payload = build_review_editor_payload(
        job,
        response_payload,
        include_cover_letter,
    )

    while True:
        try:
            edited_review_payload = open_response_in_editor(review_payload)
            merged_payload = merge_reviewed_answers(
                response_payload,
                edited_review_payload,
                include_cover_letter,
            )
            parse_ai_response(
                json.dumps(merged_payload, ensure_ascii=False),
                job.job_id,
                include_cover_letter,
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
    where_clauses = ["ga.submitted_at IS NULL"]
    if mode in {"test", "full"}:
        where_clauses.insert(0, "ga.packaged_at IS NULL")

    where_sql = f"WHERE {' AND '.join(where_clauses)}"

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
            ge.application_questions,
            ga.cover_letter
        FROM green_apply AS ga
        JOIN green_job AS gj
          ON gj.job_id = ga.job_id
        JOIN green_enrich AS ge
          ON ge.job_id = ga.job_id
        JOIN green_score AS gs
          ON gs.job_id = ga.job_id
        {where_clause}
        ORDER BY gs.overall DESC, ga.job_id ASC
    """.format(where_clause=where_sql)

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
            existing_cover_letter=row[10],
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


def load_prompt_template(prompt_file_name: str) -> str:
    """Load one application-prep prompt template."""
    return (APPLY_DIR / prompt_file_name).read_text(encoding="utf-8")


def render_prompt(
    prompt_template: str,
    job_payload: dict[str, Any],
    accepted_answer_context: list[dict[str, Any]],
    include_cover_letter: bool,
) -> str:
    """Render the final prompt by injecting job JSON and accepted-answer context."""
    rendered_job = json.dumps(job_payload, ensure_ascii=False, indent=2)
    rendered_answer_context = format_accepted_answer_context(accepted_answer_context)
    rendered = (
        prompt_template.replace("{ACCEPTED ANSWERS HERE}", rendered_answer_context)
        .replace("{JOB JSON HERE}", rendered_job)
    )
    if include_cover_letter:
        rendered = rendered.replace("{COVER LETTER TEMPLATE HERE}", cover.load_cover_template())
    return rendered


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


def extract_usage_metadata(response: Any) -> tuple[int, int]:
    """Extract prompt and response token counts from a Gemini response."""
    metadata = getattr(response, "usage_metadata", None)
    if metadata is None:
        metadata = getattr(response, "usageMetadata", None)
    if metadata is None:
        return 0, 0
    return (
        getattr(metadata, "prompt_token_count", 0) or 0,
        getattr(metadata, "candidates_token_count", 0) or 0,
    )


def extract_total_token_count(response: Any, prompt_tokens: int, response_tokens: int) -> int:
    """Return total tokens from metadata, falling back to prompt plus response."""
    return (
        getattr(getattr(response, "usage_metadata", None), "total_token_count", 0)
        or getattr(getattr(response, "usageMetadata", None), "total_token_count", 0)
        or prompt_tokens + response_tokens
    )


def token_cost(tokens: int, per_token_rate: float) -> float:
    """Calculate the dollar cost for a token count."""
    return tokens * per_token_rate


def response_total_cost(response: PromptRequestResult) -> float:
    """Calculate the total Gemini cost for one prepared application response."""
    return token_cost(response.prompt_tokens, INPUT_COST_PER_TOKEN) + token_cost(
        response.response_tokens,
        OUTPUT_COST_PER_TOKEN,
    )


def request_ai_response(client: Any, prompt: str) -> GeminiPrepResponse:
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
    prompt_tokens, response_tokens = extract_usage_metadata(response)
    return GeminiPrepResponse(
        response_text=response_text,
        prompt_tokens=prompt_tokens,
        response_tokens=response_tokens,
        total_tokens=extract_total_token_count(response, prompt_tokens, response_tokens),
    )


async def request_ai_response_async(client: Any, prompt: str) -> GeminiPrepResponse:
    """Send the prep prompt asynchronously when async client support is available."""
    aio_client = getattr(client, "aio", None)
    if aio_client is None or not hasattr(aio_client, "models"):
        return await asyncio.to_thread(request_ai_response, client, prompt)

    from google.genai import types

    response = await aio_client.models.generate_content(
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
    return GeminiPrepResponse(
        response_text=response_text,
        prompt_tokens=prompt_tokens,
        response_tokens=response_tokens,
        total_tokens=extract_total_token_count(response, prompt_tokens, response_tokens),
    )


def process_prompt_result(
    conn: psycopg.Connection,
    api_result: PromptRequestResult,
    summary: PrepSummary,
) -> None:
    """Review and persist one completed model response."""
    job = api_result.request.job
    generate_cover_letter = api_result.request.generate_cover_letter
    has_cover_letter_question = api_result.request.has_cover_letter_question
    prompt_file_name = api_result.request.prompt_file_name
    response_text = api_result.response_text

    summary.prompt_tokens += api_result.prompt_tokens
    summary.response_tokens += api_result.response_tokens
    summary.total_tokens += api_result.total_tokens
    prompt_cost = token_cost(api_result.prompt_tokens, INPUT_COST_PER_TOKEN)
    response_cost = token_cost(api_result.response_tokens, OUTPUT_COST_PER_TOKEN)
    summary.prompt_cost += prompt_cost
    summary.response_cost += response_cost
    summary.total_cost += prompt_cost + response_cost

    if api_result.error is not None:
        print(f"job_id={job.job_id} api failed: {api_result.error}")
        summary.api_failures += 1
        return

    if response_text is None:
        print(f"job_id={job.job_id} api failed: empty async result")
        summary.api_failures += 1
        return

    try:
        response_payload = parse_ai_response(response_text, job.job_id, generate_cover_letter)
    except Exception as exc:
        print(f"job_id={job.job_id} parse failed: {exc}")
        summary.parse_failures += 1
        return

    try:
        final_response_payload, _reviewed_count = review_response_payload(
            job,
            response_payload,
            generate_cover_letter,
        )
    except RuntimeError as exc:
        print(str(exc))
        summary.review_failures += 1
        return

    final_response_text = json.dumps(
        final_response_payload,
        ensure_ascii=False,
        indent=2,
    )
    accepted_editable_answers = collect_accepted_editable_answers(
        job.job_id,
        final_response_payload,
    )
    cover_letter: Optional[dict[str, str]] = None
    resume_path = str(DEFAULT_RESUME_PDF_PATH.resolve())
    cover_letter_pdf_path: Optional[str] = None
    if generate_cover_letter:
        try:
            cover_letter = cover.normalize_cover_letter_payload(final_response_payload)
            cover_letter_pdf = cover.compile_cover_letter(
                company_name=cover_letter["company_name"] or job.company_name or "",
                job_title=cover_letter["job_title"] or job.title or "",
                first_paragraph=cover_letter["first_paragraph"],
            )
            cover_letter_pdf_path = cover.normalize_pdf_path(cover_letter_pdf)
        except Exception as exc:
            print(f"job_id={job.job_id} cover letter failed: {exc}")
            summary.cover_failures += 1
            return

    try:
        persist_response(
            conn,
            job.job_id,
            final_response_text,
            accepted_editable_answers,
            prompt_file_name=prompt_file_name,
            cover_first_paragraph=(
                cover_letter["first_paragraph"] if cover_letter is not None else None
            ),
            resume_path=resume_path,
            cover_letter_pdf_path=cover_letter_pdf_path,
            clear_cover_letter=not has_cover_letter_question,
        )
    except psycopg.Error as exc:
        print(f"job_id={job.job_id} database failed: {exc}")
        summary.database_failures += 1
        return

    summary.packaged += 1
    print(
        f"Job ID: {job.job_id} Packaged and Ready to submit "
        f"(cost ${response_total_cost(api_result):.6f})"
    )


async def dispatch_and_process_prompt_requests(
    client: Any,
    requests: list[PendingPromptRequest],
    rate_per_minute: int,
    max_concurrency: int,
    conn: psycopg.Connection,
    summary: PrepSummary,
) -> None:
    """Dispatch requests concurrently and review each response as it arrives."""
    if not requests:
        return

    limiter = AsyncEvenRateLimiter(rate_per_minute=rate_per_minute)
    semaphore = asyncio.Semaphore(max_concurrency)

    async def run_one(request: PendingPromptRequest) -> PromptRequestResult:
        async with semaphore:
            await limiter.acquire()
            try:
                response = await request_ai_response_async(client, request.prompt)
            except Exception as exc:
                return PromptRequestResult(
                    request=request,
                    error=str(exc),
                )

            return PromptRequestResult(
                request=request,
                response_text=response.response_text,
                prompt_tokens=response.prompt_tokens,
                response_tokens=response.response_tokens,
                total_tokens=response.total_tokens,
            )

    tasks = [asyncio.create_task(run_one(request)) for request in requests]
    for completed in asyncio.as_completed(tasks):
        result = await completed
        await asyncio.to_thread(process_prompt_result, conn, result, summary)


def parse_ai_response(
    response_text: str,
    expected_job_id: int,
    include_cover_letter: bool,
) -> dict[str, Any]:
    """Validate that the model returned JSON for the expected job."""
    payload = json.loads(response_text)
    if not isinstance(payload, dict):
        raise ValueError("Gemini JSON response must be an object")
    payload = canonicalize_response_payload(payload)
    raw_job_id = payload.get("job_id")
    if isinstance(raw_job_id, str) and raw_job_id.strip().isdigit():
        raw_job_id = int(raw_job_id.strip())
    if raw_job_id != expected_job_id:
        raise ValueError("Gemini response job_id does not match the requested job")
    answers = payload.get("answers")
    if not isinstance(answers, list):
        raise ValueError("Gemini response must include an answers array")
    if include_cover_letter:
        cover.normalize_cover_letter_payload(payload)
    else:
        payload.pop("cover_letter", None)
    return payload


def persist_response(
    conn: psycopg.Connection,
    job_id: int,
    response_text: str,
    accepted_editable_answers: list[AcceptedEditableAnswer],
    prompt_file_name: str,
    cover_first_paragraph: Optional[str] = None,
    resume_path: Optional[str] = None,
    cover_letter_pdf_path: Optional[str] = None,
    clear_cover_letter: bool = False,
) -> None:
    """Store the approved response, cover history, answer history, and packaged marker."""
    cover_letter_value = None if clear_cover_letter else cover_letter_pdf_path
    cover_letter_sql = "%s" if clear_cover_letter or cover_letter_pdf_path is not None else "cover_letter"
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE green_apply
                SET response = %s,
                    prompt = %s,
                    model = %s,
                    resume = COALESCE(%s, resume),
                    cover_letter = {cover_letter_sql},
                    packaged_at = NOW()
                WHERE job_id = %s
                """,
                (
                    response_text,
                    prompt_file_name,
                    MODEL_NAME,
                    resume_path,
                    cover_letter_value,
                    job_id,
                ),
            )

            for accepted_answer in accepted_editable_answers:
                cur.execute(
                    f"""
                    INSERT INTO {TEXTAREA_HISTORY_TABLE} (
                        job_id,
                        question_label,
                        answer_label,
                        style,
                        prompt,
                        model
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        accepted_answer.job_id,
                        accepted_answer.question_label,
                        accepted_answer.answer_label,
                        accepted_answer.style,
                        prompt_file_name,
                        MODEL_NAME,
                    ),
                )

            if cover_first_paragraph is not None:
                cur.execute(
                    f"""
                    INSERT INTO {COVER_HISTORY_TABLE} (
                        job_id,
                        first_paragraph,
                        prompt,
                        model
                    )
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        job_id,
                        cover_first_paragraph,
                        prompt_file_name,
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


def prepare_applications(
    mode: str,
    limit: Optional[int],
    rate_per_minute: int,
    max_concurrency: int,
) -> PrepSummary:
    """Prepare queued applications with AI-generated answers."""
    prompt_templates = {
        PROMPT_WITH_COVER_FILE_NAME: load_prompt_template(PROMPT_WITH_COVER_FILE_NAME),
        PROMPT_WITHOUT_COVER_FILE_NAME: load_prompt_template(PROMPT_WITHOUT_COVER_FILE_NAME),
    }
    client = build_client()
    summary = PrepSummary()

    with db_connect() as conn:
        summary.available = count_jobs_to_prepare(conn, mode)
        jobs = fetch_jobs_to_prepare(conn, mode, limit)
        summary.selected = len(jobs)
        if not jobs:
            print("No queued jobs need application prep")
            return summary

        print(
            f"mode={mode} selected={len(jobs)} limit={'all' if limit is None else limit} "
            f"rate_per_minute={rate_per_minute} max_concurrency={max_concurrency} "
            f"model={MODEL_NAME}"
        )

        accepted_answer_context = fetch_accepted_answer_context(conn)
        pending_requests: list[PendingPromptRequest] = []
        for job in jobs:
            has_cover_letter = has_cover_letter_question(job.application_questions)
            generate_cover_letter = has_cover_letter and not bool(
                isinstance(job.existing_cover_letter, str) and job.existing_cover_letter.strip()
            )
            prompt_file_name = (
                PROMPT_WITH_COVER_FILE_NAME
                if generate_cover_letter
                else PROMPT_WITHOUT_COVER_FILE_NAME
            )
            job_payload = build_job_payload(job)
            prompt = render_prompt(
                prompt_templates[prompt_file_name],
                job_payload,
                accepted_answer_context,
                generate_cover_letter,
            )
            pending_requests.append(
                PendingPromptRequest(
                    job=job,
                    prompt=prompt,
                    prompt_file_name=prompt_file_name,
                    generate_cover_letter=generate_cover_letter,
                    has_cover_letter_question=has_cover_letter,
                )
            )

        asyncio.run(
            dispatch_and_process_prompt_requests(
                client=client,
                requests=pending_requests,
                rate_per_minute=rate_per_minute,
                max_concurrency=max_concurrency,
                conn=conn,
                summary=summary,
            )
        )

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
    if args.max_concurrency <= 0:
        print("max-concurrency must be greater than 0")
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
            max_concurrency=args.max_concurrency,
        )
    except (OSError, ValueError, psycopg.Error) as exc:
        print(str(exc))
        raise SystemExit(1) from exc

    print(
        f"Final summary: selected={summary.selected} packaged={summary.packaged} "
        f"api_failures={summary.api_failures} parse_failures={summary.parse_failures} "
        f"review_failures={summary.review_failures} "
        f"database_failures={summary.database_failures} "
        f"cover_failures={summary.cover_failures} "
        f"input_tokens={summary.prompt_tokens} "
        f"input_cost=${summary.prompt_cost:.6f} "
        f"output_tokens={summary.response_tokens} "
        f"output_cost=${summary.response_cost:.6f} "
        f"total_tokens={summary.total_tokens} "
        f"total_cost=${summary.total_cost:.6f}"
    )
    if not summary.success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
