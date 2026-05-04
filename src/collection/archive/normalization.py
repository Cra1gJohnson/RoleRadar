import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

US_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "IA",
    "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS",
    "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA",
    "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY",
}
US_STATE_NAMES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "hawaii", "idaho", "illinois",
    "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine", "maryland",
    "massachusetts", "michigan", "minnesota", "mississippi", "missouri",
    "montana", "nebraska", "nevada", "new hampshire", "new jersey", "new mexico",
    "new york", "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode island", "south carolina", "south dakota", "tennessee",
    "texas", "utah", "vermont", "virginia", "washington", "west virginia",
    "wisconsin", "wyoming",
}
US_LOCATION_PHRASES = {
    "united states",
    "united states of america",
    "u.s.",
    "u.s",
    "usa",
    "us remote",
    "remote-us",
    "remote - u.s.",
    "remote - u.s",
    "remote - usa",
    "remote - united states",
    "remote, usa",
    "remote, united states",
    "anywhere in the united states",
    "field based - united states",
    "east coast",
    "new england",
    "midwest",
    "washington dc",
    "washington, dc",
    "washington, d.c.",
    "new york city",
    "new york metro area",
    "nyc",
}
AMBIGUOUS_NON_US_LOCATIONS = {"georgia"}


@dataclass(frozen=True)
class NormalizedJobRow:
    """Normalized Greenhouse job data ready for database writes."""

    greenhouse_job_id: int
    company_name: Optional[str]
    title: Optional[str]
    location: Optional[str]
    united_states: bool
    url: Optional[str]
    updated_at: datetime


@dataclass(frozen=True)
class NormalizedBoardPayload:
    """Pure in-memory representation of a normalized Greenhouse board payload.

    `jobs` contains every normalized job in the response.
    `db_jobs` contains every normalized job allowed to reach green_job.
    """

    token: str
    raw_job_ids: list[int]
    jobs: list[NormalizedJobRow]
    db_jobs: list[NormalizedJobRow]
    job_count: int
    company_name: Optional[str]
    filtered_count: int
    failed_count: int
    united_states: bool


def extract_job_count(payload: dict[str, Any]) -> int:
    """Read the returned job count, preferring metadata when present."""
    meta = payload.get("meta")
    if isinstance(meta, dict):
        total = meta.get("total")
        if isinstance(total, int):
            return total

    jobs = payload.get("jobs")
    if isinstance(jobs, list):
        return len(jobs)

    raise ValueError("Greenhouse payload missing jobs list")


def extract_sorted_job_ids(payload: dict[str, Any]) -> list[int]:
    """Collect, validate, and sort the numeric job IDs from the payload."""
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        raise ValueError("Greenhouse payload missing jobs list")

    job_ids: list[int] = []
    for job in jobs:
        if not isinstance(job, dict) or not isinstance(job.get("id"), int):
            raise ValueError("Greenhouse payload contains a job without a valid id")
        job_ids.append(job["id"])

    job_ids.sort()
    return job_ids


def extract_company_name(payload: dict[str, Any], sorted_job_ids: list[int]) -> Optional[str]:
    """Use the company name from the job with the smallest job ID."""
    if not sorted_job_ids:
        return None

    smallest_job_id = sorted_job_ids[0]
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        return None

    for job in jobs:
        if isinstance(job, dict) and job.get("id") == smallest_job_id:
            company_name = job.get("company_name")
            if isinstance(company_name, str):
                stripped = company_name.strip()
                return stripped or None
            return None

    return None


def parse_payload_timestamp(raw_value: Any, token: str, greenhouse_job_id: Any) -> datetime:
    """Parse a Greenhouse ISO 8601 timestamp into a timezone-aware datetime."""
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise ValueError(f"{token} job {greenhouse_job_id}: invalid updated_at value")

    try:
        parsed = datetime.fromisoformat(raw_value)
    except ValueError as exc:
        raise ValueError(
            f"{token} job {greenhouse_job_id}: could not parse updated_at '{raw_value}'"
        ) from exc

    if parsed.tzinfo is None:
        raise ValueError(f"{token} job {greenhouse_job_id}: updated_at must include timezone")
    return parsed


def extract_location_name(job: dict[str, Any]) -> Optional[str]:
    """Normalize the nested Greenhouse location field into a plain string."""
    location = job.get("location")
    if not isinstance(location, dict):
        return None

    location_name = location.get("name")
    if isinstance(location_name, str):
        stripped = location_name.strip()
        return stripped or None
    return None


def normalize_text(value: Any) -> Optional[str]:
    """Normalize optional string fields, converting blanks to None."""
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def is_united_states_location(location_name: Optional[str]) -> bool:
    """Best-effort classification for whether a Greenhouse location is in the U.S."""
    if not location_name:
        return False

    normalized = location_name.strip().lower()
    if not normalized:
        return False

    if normalized in US_LOCATION_PHRASES:
        return True
    if any(phrase in normalized for phrase in US_LOCATION_PHRASES):
        return True
    if normalized in AMBIGUOUS_NON_US_LOCATIONS:
        return False

    code_tokens = set(re.findall(r"\b[A-Z]{2}\b", location_name.upper()))
    if code_tokens & US_STATE_CODES:
        return True

    for state_name in US_STATE_NAMES:
        if re.search(rf"\b{re.escape(state_name)}\b", normalized):
            return True

    return False


def normalize_job(
    job: dict[str, Any],
    token: str,
) -> NormalizedJobRow:
    """Validate and normalize one Greenhouse job from the board payload."""
    greenhouse_job_id = job.get("id")
    if not isinstance(greenhouse_job_id, int):
        raise ValueError(f"{token} job missing valid numeric id")

    updated_at = parse_payload_timestamp(job.get("updated_at"), token, greenhouse_job_id)
    location_name = extract_location_name(job)

    return NormalizedJobRow(
        greenhouse_job_id=greenhouse_job_id,
        company_name=normalize_text(job.get("company_name")),
        title=normalize_text(job.get("title")),
        location=location_name,
        united_states=is_united_states_location(location_name),
        url=normalize_text(job.get("absolute_url")),
        updated_at=updated_at,
    )


def normalize_board_payload(payload: dict[str, Any], token: str) -> NormalizedBoardPayload:
    """Normalize one Greenhouse board payload without touching the database."""
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        raise ValueError("Greenhouse payload missing jobs list")

    raw_job_ids = extract_sorted_job_ids(payload)
    normalized_jobs: list[NormalizedJobRow] = []
    db_jobs: list[NormalizedJobRow] = []
    failed_count = 0
    united_states = False

    for job in jobs:
        if not isinstance(job, dict):
            failed_count += 1
            continue

        try:
            normalized_job = normalize_job(job, token)
        except ValueError:
            failed_count += 1
            continue

        if normalized_job.united_states:
            united_states = True

        db_jobs.append(normalized_job)
        normalized_jobs.append(normalized_job)

    return NormalizedBoardPayload(
        token=token,
        raw_job_ids=raw_job_ids,
        jobs=normalized_jobs,
        db_jobs=db_jobs,
        job_count=extract_job_count(payload),
        company_name=extract_company_name(payload, raw_job_ids),
        filtered_count=0,
        failed_count=failed_count,
        united_states=united_states,
    )
