from collections.abc import Iterable

from ingestion.clients.base import BaseJobClient
from ingestion.schemas import NormalizedJob


class LeverClient(BaseJobClient):
    source = "lever"

    def __init__(self, company_slug: str, user_agent: str) -> None:
        super().__init__(user_agent=user_agent)
        self.company_slug = company_slug

    def fetch_jobs(self) -> Iterable[NormalizedJob]:
        response = self._http.get(f"https://api.lever.co/v0/postings/{self.company_slug}?mode=json")
        response.raise_for_status()
        postings = response.json()

        for job in postings:
            categories = job.get("categories") or {}
            yield NormalizedJob(
                source=self.source,
                company=self.company_slug,
                external_id=str(job["id"]),
                title=job["text"],
                location=categories.get("location"),
                team=categories.get("team") or categories.get("department"),
                employment_type=categories.get("commitment"),
                workplace_type=categories.get("workplaceType"),
                url=job["hostedUrl"],
                description=None,
                metadata={
                    "updated_at": job.get("updatedAt"),
                    "created_at": job.get("createdAt"),
                    "categories": categories,
                    "tags": job.get("tags", []),
                },
            )

