from typing import Any, Optional
from urllib.parse import quote

from ats_common import (
    NormalizedJob,
    first_text,
    numeric_compensation,
    parse_iso_datetime,
    text_or_none,
    us_location,
)

ATS = "Green"
BOARD_API = "https://boards-api.greenhouse.io/v1/boards/{board}/jobs?pay_transparency=true"


def build_board_url(board: str) -> str:
    return BOARD_API.format(board=quote(board, safe=""))


def extract_jobs(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("jobs"), list):
        raise ValueError("Green payload missing jobs list")
    return [job for job in payload["jobs"] if isinstance(job, dict)]


def extract_job_id(job: dict[str, Any]) -> str:
    job_id = job.get("id")
    if not isinstance(job_id, int):
        raise ValueError("Green job missing numeric id")
    return str(job_id)


def extract_company_name(payload: Any, jobs: list[NormalizedJob]) -> Optional[str]:
    for job in jobs:
        if job.company_name:
            return job.company_name
    return None


def extract_compensation(job: dict[str, Any]) -> tuple[Optional[int], Optional[int]]:
    ranges = job.get("pay_input_ranges")
    if not isinstance(ranges, list):
        return None, None

    for pay_range in ranges:
        if not isinstance(pay_range, dict):
            continue
        min_value = numeric_compensation(pay_range.get("min_cents"))
        max_value = numeric_compensation(pay_range.get("max_cents"))
        if min_value is not None:
            min_value = int(min_value / 100)
        if max_value is not None:
            max_value = int(max_value / 100)
        if min_value is not None or max_value is not None:
            return min_value, max_value

    return None, None


def normalize_job(board: str, job: dict[str, Any]) -> NormalizedJob:
    location = job.get("location")
    location_name = None
    if isinstance(location, dict):
        location_name = text_or_none(location.get("name"))
    updated_at = first_text(job.get("updated_at"), job.get("first_published"))
    min_compensation, max_compensation = extract_compensation(job)

    return NormalizedJob(
        board=board,
        ats=ATS,
        ats_job_id=extract_job_id(job),
        company_name=text_or_none(job.get("company_name")),
        title=text_or_none(job.get("title")),
        location=location_name,
        url=text_or_none(job.get("absolute_url")),
        description=None,
        min_compensation=min_compensation,
        max_compensation=max_compensation,
        united_states=us_location(location_name),
        updated_at=parse_iso_datetime(updated_at),
    )
