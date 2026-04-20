import asyncio
import argparse
import json
import re
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence
from urllib.parse import quote, urlparse

import httpx
import psycopg
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

YOU_SEARCH_API = "https://ydc-index.io/v1/search"
YOU_SEARCH_MAX_RESULTS = 100
GREENHOUSE_VALIDATE_API = "https://boards-api.greenhouse.io/v1/boards/{token}"
GREENHOUSE_DOMAINS = {"boards.greenhouse.io", "job-boards.greenhouse.io"}
QUERY_TEMPLATES = (
    "site:boards.greenhouse.io {name}",
    "site:job-boards.greenhouse.io {name}",
)
TOKEN_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
GREENHOUSE_API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}


class Settings(BaseSettings):
    db_name: str = Field(alias="DB_NAME")
    db_user: str = Field(alias="DB_USER")
    db_password: str = Field(alias="DB_PASSWORD")
    db_host: str = Field(alias="DB_HOST")
    db_port: int = Field(alias="DB_PORT")
    you_api_key: str = Field(alias="API")

    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parents[2] / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@dataclass(frozen=True)
class DiscoveryName:
    name_id: int
    name: str


@dataclass
class QueryRunResult:
    query: str
    skipped: bool
    success: bool
    results_num: int
    candidate_tokens: int
    valid_tokens: int
    inserted_tokens: int
    error: Optional[str] = None


class SearchRateLimiter:
    def __init__(self, max_requests: int, period_seconds: float) -> None:
        self.max_requests = max_requests
        self.period_seconds = period_seconds
        self.request_times: deque[float] = deque()

    def wait_for_slot(self) -> None:
        now = time.monotonic()
        self._trim(now)
        if len(self.request_times) < self.max_requests:
            self.request_times.append(now)
            return

        sleep_for = self.period_seconds - (now - self.request_times[0])
        if sleep_for > 0:
            print(f"rate-limit waiting {sleep_for:.1f}s before next You.com request")
            time.sleep(sleep_for)

        now = time.monotonic()
        self._trim(now)
        self.request_times.append(now)

    def _trim(self, now: float) -> None:
        while self.request_times and (now - self.request_times[0]) >= self.period_seconds:
            self.request_times.popleft()


def parse_name_range(raw: str) -> tuple[int, int]:
    match = re.fullmatch(r"(\d+)-(\d+)", raw.strip())
    if not match:
        raise ValueError("name_range must be in format start-end, e.g. 0-200")

    start = int(match.group(1))
    end = int(match.group(2))
    if end <= start:
        raise ValueError("name_range end must be greater than start")
    return start, end


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query You.com for Greenhouse board tokens")
    parser.add_argument(
        "name_range",
        help="Range of discovery_name records to use, zero-based start-end (e.g. 0-200)",
    )
    parser.add_argument(
        "--mode",
        choices=("test", "full"),
        default="test",
        help="Run one test query or all selected rows x 2 queries",
    )
    parser.add_argument(
        "--name",
        default=None,
        help="Discovery name used in test mode (must exist in selected range)",
    )
    return parser.parse_args()


def db_connect(settings: Settings) -> psycopg.Connection:
    return psycopg.connect(
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        host=settings.db_host,
        port=settings.db_port,
        autocommit=True,
    )


def fetch_discovery_names_in_range(
    conn: psycopg.Connection,
    start: int,
    end: int,
) -> list[DiscoveryName]:
    limit = end - start
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT name_id, name
            FROM discovery_name
            ORDER BY name_id
            OFFSET %s
            LIMIT %s
            """,
            (start, limit),
        )
        rows = cur.fetchall()

    return [DiscoveryName(name_id=row[0], name=row[1]) for row in rows]


def mark_names_used_now(conn: psycopg.Connection, name_ids: Sequence[int]) -> None:
    if not name_ids:
        return

    with conn.cursor() as cur:
        cur.execute(
            "UPDATE discovery_name SET last_used = NOW() WHERE name_id = ANY(%s)",
            (list(name_ids),),
        )


def update_name_success(conn: psycopg.Connection, name_id: int, success: bool) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE discovery_name SET success = %s WHERE name_id = %s",
            (success, name_id),
        )


def query_already_recorded(conn: psycopg.Connection, query: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM you_query WHERE query = %s", (query,))
        return cur.fetchone() is not None


def insert_you_query(
    conn: psycopg.Connection,
    query: str,
    results_num: int,
    success: bool,
    tokens: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO you_query (query, results_num, success, tokens)
            VALUES (%s, %s, %s, %s)
            """,
            (query, results_num, success, tokens),
        )


def board_token_exists(conn: psycopg.Connection, token: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM board_token WHERE token = %s", (token,))
        return cur.fetchone() is not None


def insert_board_token(conn: psycopg.Connection, token: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO board_token (token, last_used, success)
            VALUES (%s, NOW(), TRUE)
            ON CONFLICT (token) DO NOTHING
            """,
            (token,),
        )


def is_greenhouse_url(url: str) -> bool:
    host = urlparse(url).netloc.lower().split(":")[0]
    return host in GREENHOUSE_DOMAINS


def extract_board_token(url: str) -> Optional[str]:
    parsed = urlparse(url)
    host = parsed.netloc.lower().split(":")[0]
    if host not in GREENHOUSE_DOMAINS:
        return None

    path_parts = [part for part in parsed.path.split("/") if part]
    if not path_parts:
        return None

    token = path_parts[0].strip().lower()
    if not TOKEN_RE.match(token):
        return None

    return token


def extract_result_urls(payload: dict) -> list[str]:
    results = payload.get("results")
    if not isinstance(results, dict):
        return []

    web_results = results.get("web")
    if not isinstance(web_results, list):
        return []

    urls: list[str] = []
    for item in web_results:
        if not isinstance(item, dict):
            continue
        url = item.get("url")
        if isinstance(url, str) and url:
            urls.append(url)
    return urls


async def validate_board_token(http: httpx.AsyncClient, token: str) -> bool:
    response = await http.get(GREENHOUSE_VALIDATE_API.format(token=quote(token, safe="")))
    response.raise_for_status()
    payload = response.json()
    return isinstance(payload, (dict, list))


async def validate_candidate_tokens_async(tokens: Sequence[str]) -> dict[str, bool]:
    if not tokens:
        return {}

    concurrency = min(8, len(tokens))
    semaphore = asyncio.Semaphore(concurrency)

    async def validate_one(
        http: httpx.AsyncClient,
        token: str,
    ) -> tuple[str, bool]:
        async with semaphore:
            try:
                is_valid = await validate_board_token(http, token)
            except (httpx.HTTPError, json.JSONDecodeError, ValueError):
                is_valid = False
            return token, is_valid

    async with httpx.AsyncClient(
        timeout=30.0,
        headers=GREENHOUSE_API_HEADERS,
    ) as http:
        results = await asyncio.gather(*(validate_one(http, token) for token in tokens))

    return {token: is_valid for token, is_valid in results}


def validate_candidate_tokens(tokens: Sequence[str]) -> dict[str, bool]:
    return asyncio.run(validate_candidate_tokens_async(tokens))


def run_search(
    http: httpx.Client,
    rate_limiter: SearchRateLimiter,
    query: str,
) -> dict:
    rate_limiter.wait_for_slot()
    response = http.get(
        YOU_SEARCH_API,
        params={
            "query": query,
            "count": YOU_SEARCH_MAX_RESULTS,
        },
        headers={
            "X-API-KEY": http.headers["X-API-KEY"],
            "Accept": "application/json",
        },
    )
    response.raise_for_status()
    return response.json()


def process_query(
    conn: psycopg.Connection,
    http: httpx.Client,
    rate_limiter: SearchRateLimiter,
    query: str,
) -> QueryRunResult:
    if query_already_recorded(conn, query):
        return QueryRunResult(
            query=query,
            skipped=True,
            success=True,
            results_num=0,
            candidate_tokens=0,
            valid_tokens=0,
            inserted_tokens=0,
        )

    try:
        payload = run_search(http=http, rate_limiter=rate_limiter, query=query)
        urls = extract_result_urls(payload)
        greenhouse_urls = [url for url in urls if is_greenhouse_url(url)]
        candidate_tokens = sorted(
            {
                token
                for token in (extract_board_token(url) for url in greenhouse_urls)
                if token is not None
            }
        )

        token_validity = validate_candidate_tokens(candidate_tokens)

        valid_tokens = 0
        inserted_tokens = 0
        for token in candidate_tokens:
            if not token_validity.get(token, False):
                continue

            valid_tokens += 1
            if not board_token_exists(conn, token):
                insert_board_token(conn, token)
                inserted_tokens += 1

        insert_you_query(
            conn=conn,
            query=query,
            results_num=len(urls),
            success=True,
            tokens=valid_tokens,
        )
        return QueryRunResult(
            query=query,
            skipped=False,
            success=True,
            results_num=len(urls),
            candidate_tokens=len(candidate_tokens),
            valid_tokens=valid_tokens,
            inserted_tokens=inserted_tokens,
        )
    except (httpx.HTTPError, json.JSONDecodeError, ValueError) as exc:
        insert_you_query(conn=conn, query=query, results_num=0, success=False, tokens=0)
        return QueryRunResult(
            query=query,
            skipped=False,
            success=False,
            results_num=0,
            candidate_tokens=0,
            valid_tokens=0,
            inserted_tokens=0,
            error=str(exc),
        )


def print_query_result(current: int, total: int, result: QueryRunResult) -> None:
    if result.skipped:
        print(f"[{current}/{total}] skipped existing query | {result.query}")
        return

    if result.success:
        print(
            f"[{current}/{total}] ok | results={result.results_num} "
            f"candidates={result.candidate_tokens} valid={result.valid_tokens} "
            f"inserted={result.inserted_tokens} | {result.query}"
        )
        return

    print(f"[{current}/{total}] failed | {result.error} | {result.query}")


def run_test_mode(
    conn: psycopg.Connection,
    http: httpx.Client,
    rate_limiter: SearchRateLimiter,
    selected_names: Sequence[DiscoveryName],
    raw_name: Optional[str],
) -> None:
    if not raw_name:
        raise RuntimeError("--name is required in test mode")

    name_row = next((n for n in selected_names if n.name.lower() == raw_name.lower()), None)
    if name_row is None:
        raise RuntimeError(f"Name '{raw_name}' was not found in selected range")

    query = QUERY_TEMPLATES[0].format(name=name_row.name)
    result = process_query(conn=conn, http=http, rate_limiter=rate_limiter, query=query)
    print_query_result(1, 1, result)
    update_name_success(conn, name_row.name_id, result.success)


def run_full_mode(
    conn: psycopg.Connection,
    http: httpx.Client,
    rate_limiter: SearchRateLimiter,
    selected_names: Sequence[DiscoveryName],
) -> None:
    total_queries = len(selected_names) * len(QUERY_TEMPLATES)
    query_counter = 0

    for name_row in selected_names:
        name_success = True
        for template in QUERY_TEMPLATES:
            query_counter += 1
            query = template.format(name=name_row.name)
            result = process_query(conn=conn, http=http, rate_limiter=rate_limiter, query=query)
            print_query_result(query_counter, total_queries, result)
            if not result.success:
                name_success = False
        update_name_success(conn, name_row.name_id, name_success)


def main() -> None:
    args = parse_args()
    settings = Settings()
    start, end = parse_name_range(args.name_range)

    http = httpx.Client(
        timeout=30.0,
        follow_redirects=True,
        headers={
            "User-Agent": "app-copilot-discovery/0.1.0",
            "Accept": "application/json",
            "X-API-KEY": settings.you_api_key,
        },
    )
    rate_limiter = SearchRateLimiter(max_requests=100, period_seconds=60.0)

    try:
        with db_connect(settings) as conn:
            selected_names = fetch_discovery_names_in_range(conn, start, end)
            if not selected_names:
                raise RuntimeError("No discovery_name rows found in selected range")

            mark_names_used_now(conn, [row.name_id for row in selected_names])

            if args.mode == "test":
                run_test_mode(
                    conn=conn,
                    http=http,
                    rate_limiter=rate_limiter,
                    selected_names=selected_names,
                    raw_name=args.name,
                )
            else:
                run_full_mode(
                    conn=conn,
                    http=http,
                    rate_limiter=rate_limiter,
                    selected_names=selected_names,
                )
    finally:
        http.close()


if __name__ == "__main__":
    main()
