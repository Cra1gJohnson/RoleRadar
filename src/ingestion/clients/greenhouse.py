from collections.abc import Iterable

from ingestion.clients.base import BaseJobClient
from ingestion.schemas import NormalizedJob


class GreenhouseClient(BaseJobClient):
    source = "greenhouse"

    def __init__(self, board_token: str, user_agent: str) -> None:
        super().__init__(user_agent=user_agent)
        self.board_token = board_token

    def fetch_jobs(self) -> Iterable[NormalizedJob]:
        response = self._http.get(f"https://boards-api.greenhouse.io/v1/boards/{self.board_token}/jobs")
        response.raise_for_status()
        payload = response.json()

        for job in payload.get("jobs", []):
            yield NormalizedJob(
                source=self.source,
                company=self.board_token,
                external_id=str(job["id"]),
                title=job["title"],
                location=_join_location(job.get("location")),
                team=_department_name(job),
                employment_type=None,
                workplace_type=None,
                url=job["absolute_url"],
                description=None,
                metadata={
                    "updated_at": job.get("updated_at"),
                    "requisition_id": job.get("requisition_id"),
                    "internal_job_id": job.get("internal_job_id"),
                    "metadata": job.get("metadata", []),
                    "data_compliance": job.get("data_compliance", []),
                },
            )


def _department_name(job: dict) -> str | None:
    departments = job.get("departments") or []
    names = [item.get("name") for item in departments if item.get("name")]
    return ", ".join(names) if names else None


def _join_location(location: dict | None) -> str | None:
    if not location:
        return None
    name = location.get("name")
    return str(name) if name else None

