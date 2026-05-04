from typing import Any, Optional
from urllib.parse import quote

from ats_common import (
    NormalizedJob,
    first_text,
    numeric_compensation,
    parse_epoch_millis,
    text_or_none,
    us_location,
)

ATS = "Lever"
BOARD_API = "https://api.lever.co/v0/postings/{board}?mode=json"


def build_board_url(board: str) -> str:
    return BOARD_API.format(board=quote(board, safe=""))


def extract_jobs(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, list):
        raise ValueError("Lever payload must be a jobs list")
    return [job for job in payload if isinstance(job, dict)]


def extract_job_id(job: dict[str, Any]) -> str:
    job_id = job.get("id")
    if not isinstance(job_id, str) or not job_id.strip():
        raise ValueError("Lever job missing string id")
    return job_id.strip()


def extract_company_name(payload: Any, jobs: list[NormalizedJob]) -> Optional[str]:
    return None


def extract_location(job: dict[str, Any]) -> Optional[str]:
    categories = job.get("categories")
    if isinstance(categories, dict):
        location = text_or_none(categories.get("location"))
        if location:
            return location
        all_locations = categories.get("allLocations")
        if isinstance(all_locations, list):
            joined = ", ".join(
                location
                for location in (text_or_none(value) for value in all_locations)
                if location
            )
            if joined:
                return joined
    return text_or_none(job.get("country"))


def extract_salary_range(job: dict[str, Any]) -> tuple[Optional[int], Optional[int]]:
    salary_range = job.get("salaryRange")
    if not isinstance(salary_range, dict):
        return None, None

    min_value = salary_range.get("min")
    max_value = salary_range.get("max")
    return numeric_compensation(min_value), numeric_compensation(max_value)


def normalize_job(board: str, job: dict[str, Any]) -> NormalizedJob:
    location = extract_location(job)
    min_compensation, max_compensation = extract_salary_range(job)

    return NormalizedJob(
        board=board,
        ats=ATS,
        ats_job_id=extract_job_id(job),
        company_name=None,
        title=text_or_none(job.get("text")),
        location=location,
        url=text_or_none(job.get("hostedUrl")) or text_or_none(job.get("applyUrl")),
        description=first_text(
            job.get("descriptionPlain"),
            job.get("descriptionBodyPlain"),
            job.get("openingPlain"),
            job.get("additionalPlain"),
        ),
        min_compensation=min_compensation,
        max_compensation=max_compensation,
        united_states=us_location(location) or job.get("country") == "US",
        updated_at=parse_epoch_millis(job.get("createdAt")),
    )
