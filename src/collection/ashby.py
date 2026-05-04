from typing import Any, Optional
from urllib.parse import quote

from ats_common import NormalizedJob, parse_iso_datetime, parse_salary_summary, text_or_none, us_location

ATS = "Ashby"
BOARD_API = "https://api.ashbyhq.com/posting-api/job-board/{board}?includeCompensation=true"


def build_board_url(board: str) -> str:
    return BOARD_API.format(board=quote(board, safe=""))


def extract_jobs(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("jobs"), list):
        raise ValueError("Ashby payload missing jobs list")
    return [job for job in payload["jobs"] if isinstance(job, dict)]


def extract_job_id(job: dict[str, Any]) -> str:
    job_id = job.get("id")
    if not isinstance(job_id, str) or not job_id.strip():
        raise ValueError("Ashby job missing string id")
    return job_id.strip()


def extract_company_name(payload: Any, jobs: list[NormalizedJob]) -> Optional[str]:
    return None


def extract_compensation(job: dict[str, Any]) -> tuple[Optional[int], Optional[int]]:
    compensation = job.get("compensation")
    if not isinstance(compensation, dict):
        return None, None

    min_value, max_value = parse_salary_summary(
        compensation.get("scrapeableCompensationSalarySummary")
    )
    if min_value is not None or max_value is not None:
        return min_value, max_value

    return parse_salary_summary(compensation.get("compensationTierSummary"))


def normalize_job(board: str, job: dict[str, Any]) -> NormalizedJob:
    location = text_or_none(job.get("location"))
    min_compensation, max_compensation = extract_compensation(job)

    return NormalizedJob(
        board=board,
        ats=ATS,
        ats_job_id=extract_job_id(job),
        company_name=None,
        title=text_or_none(job.get("title")),
        location=location,
        url=text_or_none(job.get("jobUrl")) or text_or_none(job.get("applyUrl")),
        description=text_or_none(job.get("descriptionPlain")),
        min_compensation=min_compensation,
        max_compensation=max_compensation,
        united_states=us_location(location),
        updated_at=parse_iso_datetime(job.get("publishedAt")),
    )
