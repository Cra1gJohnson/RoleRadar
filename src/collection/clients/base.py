from collections.abc import Iterable

import httpx

from ingestion.schemas import NormalizedJob


class BaseJobClient:
    source: str

    def __init__(self, user_agent: str) -> None:
        self._http = httpx.Client(
            timeout=30.0,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            follow_redirects=True,
        )

    def fetch_jobs(self) -> Iterable[NormalizedJob]:
        raise NotImplementedError

    def close(self) -> None:
        self._http.close()

