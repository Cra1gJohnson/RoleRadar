import argparse
import asyncio
from datetime import datetime
import json
import os
import re
import sys
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence
from urllib.parse import quote, urlparse

import httpx
import psycopg
import requests

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env

load_shared_env()

LOG_DIR = Path(__file__).resolve().parent / "log"
LOG_PATH: Optional[Path] = None
YOU_SEARCH_API = "https://ydc-index.io/v1/search"
YOU_SEARCH_MAX_RESULTS = 100
YOU_SEARCH_TIMEOUT_SECONDS = 30
ATS_VALIDATE_TIMEOUT_SECONDS = 8
VALIDATION_PROGRESS_INTERVAL = 10
DEFAULT_VALIDATION_WORKERS = 24
GREENHOUSE_VALIDATE_API = "https://boards-api.greenhouse.io/v1/boards/{board}"
ASHBY_VALIDATE_API = (
    "https://api.ashbyhq.com/posting-api/job-board/{board}"
    "?includeCompensation=false"
)
LEVER_VALIDATE_API = "https://api.lever.co/v0/postings/{board}?mode=json"
QUERY_TEMPLATES = (
    ("Green", "site:boards.greenhouse.io {name}"),
    ("Green", "site:job-boards.greenhouse.io {name}"),
    ("Ashby", "site:jobs.ashbyhq.com {name}"),
    ("Lever", "site:jobs.lever.co {name}"),
)
ATS_DOMAINS = {
    "Green": {"boards.greenhouse.io", "job-boards.greenhouse.io"},
    "Ashby": {"jobs.ashbyhq.com"},
    "Lever": {"jobs.lever.co"},
}
BOARD_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}


def initialize_log() -> Path:
    """Create this run's log file and return its path."""
    global LOG_PATH
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    LOG_PATH = LOG_DIR / f"you_search_{timestamp}.log"
    LOG_PATH.touch()
    log(f"log_path={LOG_PATH}")
    return LOG_PATH


def log(message: str) -> None:
    """Append one progress line to the run log."""
    if LOG_PATH is None:
        raise RuntimeError("Log file has not been initialized")
    with LOG_PATH.open("a", encoding="utf-8") as log_file:
        log_file.write(f"{message}\n")


@dataclass(frozen=True)
class Keyword:
    name_id: int
    name: str


@dataclass
class QueryRunResult:
    query: str
    ats: str
    success: bool
    skipped: bool = False
    results_num: int = 0
    candidate_boards: int = 0
    valid_boards: int = 0
    inserted_boards: int = 0
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
            log(f"rate-limit waiting {sleep_for:.1f}s before next You.com request")
            time.sleep(sleep_for)

        now = time.monotonic()
        self._trim(now)
        self.request_times.append(now)

    def _trim(self, now: float) -> None:
        while self.request_times and (now - self.request_times[0]) >= self.period_seconds:
            self.request_times.popleft()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query You.com for ATS board identifiers and persist valid boards.\n"\
        "Checks Greenhouse, Ashby, and Lever.\n"
        "Uses keyword , ats_board, and you_search all under one connection -- see DB.\n"\
        "Uses asyncio for board validation, sequential for all else.\n",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "mode",
        choices=("test", "full"),
        help="Run one keyword in test mode or all keywords in full mode",
    )
    parser.add_argument(
        "--name",
        help="Keyword name used in test mode, must be present in keyword table"
        )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run You.com searches even when the search already exists in you_search",
    )
    parser.add_argument(
        "--validation-workers",
        type=int,
        default=DEFAULT_VALIDATION_WORKERS,
        help="Maximum concurrent ATS board validation requests",
    )
    return parser.parse_args()


def db_connect() -> psycopg.Connection:
    return psycopg.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        autocommit=True,
    )


def fetch_all_keywords(conn: psycopg.Connection) -> list[Keyword]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT name_id, name
            FROM keyword
            ORDER BY name_id
            """
        )
        rows = cur.fetchall()
    return [Keyword(name_id=row[0], name=row[1]) for row in rows]


def fetch_keyword_by_name(conn: psycopg.Connection, name: str) -> Optional[Keyword]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT name_id, name
            FROM keyword
            WHERE LOWER(name) = LOWER(%s)
            ORDER BY name_id
            LIMIT 1
            """,
            (name,),
        )
        row = cur.fetchone()

    if row is None:
        return None
    return Keyword(name_id=row[0], name=row[1])


def mark_keywords_used_now(conn: psycopg.Connection, name_ids: Sequence[int]) -> None:
    if not name_ids:
        return

    with conn.cursor() as cur:
        cur.execute(
            "UPDATE keyword SET last_used = NOW() WHERE name_id = ANY(%s)",
            (list(name_ids),),
        )


def update_keyword_result(
    conn: psycopg.Connection,
    name_id: int,
    success: bool,
    inserted_counts: dict[str, int],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE keyword
            SET green_boards = %s,
                ashby_boards = %s,
                lever_boards = %s,
                success = %s
            WHERE name_id = %s
            """,
            (
                inserted_counts["Green"],
                inserted_counts["Ashby"],
                inserted_counts["Lever"],
                success,
                name_id,
            ),
        )


def search_already_recorded(conn: psycopg.Connection, search: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM you_search WHERE search = %s", (search,))
        return cur.fetchone() is not None


def touch_you_search(conn: psycopg.Connection, search: str) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE you_search SET last_used = NOW() WHERE search = %s", (search,))


def upsert_you_search(
    conn: psycopg.Connection,
    search: str,
    results_num: int,
    success: bool,
    boards: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO you_search (search, results_num, success, tokens)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (search)
            DO UPDATE SET
                results_num = EXCLUDED.results_num,
                last_used = NOW(),
                success = EXCLUDED.success,
                tokens = EXCLUDED.tokens
            """,
            (search, results_num, success, boards),
        )


def board_exists(conn: psycopg.Connection, board: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM ats_board WHERE LOWER(board) = LOWER(%s)",
            (board,),
        )
        return cur.fetchone() is not None


def insert_ats_board_if_new(conn: psycopg.Connection, board: str, ats: str) -> bool:
    if board_exists(conn, board):
        return False

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ats_board (board, ats, last_used, success)
            SELECT %s, %s, NOW(), TRUE
            WHERE NOT EXISTS (
                SELECT 1
                FROM ats_board
                WHERE LOWER(board) = LOWER(%s)
            )
            """,
            (board, ats, board),
        )
        return cur.rowcount == 1


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


def extract_board_identifier(url: str, ats: str) -> Optional[str]:
    parsed = urlparse(url)
    host = parsed.netloc.lower().split(":")[0]
    if host not in ATS_DOMAINS[ats]:
        return None

    match = re.match(r"^/([^/?#]+)/?", parsed.path)
    if not match:
        return None

    board = match.group(1).strip()
    if ats == "Green":
        board = board.lower()
    if not BOARD_RE.match(board):
        return None
    return board


def build_validate_url(ats: str, board: str) -> str:
    if ats == "Green":
        return GREENHOUSE_VALIDATE_API.format(board=quote(board, safe=""))
    if ats == "Ashby":
        return ASHBY_VALIDATE_API.format(board=quote(board, safe=""))
    if ats == "Lever":
        return LEVER_VALIDATE_API.format(board=quote(board, safe=""))
    raise ValueError(f"Unsupported ATS: {ats}")


def validate_payload_shape(ats: str, payload: object) -> bool:
    if ats == "Ashby":
        return isinstance(payload, dict) and isinstance(payload.get("jobs"), list)
    if ats == "Lever":
        return isinstance(payload, list)
    return isinstance(payload, (dict, list))


async def validate_board_async(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    ats: str,
    board: str,
) -> tuple[str, bool]:
    async with semaphore:
        try:
            response = await client.get(build_validate_url(ats, board))
            response.raise_for_status()
            return board, validate_payload_shape(ats, response.json())
        except (httpx.HTTPError, json.JSONDecodeError, ValueError):
            return board, False


async def validate_candidate_boards_async(
    ats: str,
    boards: Sequence[str],
    validation_workers: int,
) -> dict[str, bool]:
    validity: dict[str, bool] = {}
    total_boards = len(boards)
    if validation_workers <= 0:
        raise ValueError("--validation-workers must be greater than 0")

    if total_boards:
        workers = min(validation_workers, total_boards)
        log(f"validating ats={ats} candidates={total_boards} workers={workers}")
    else:
        return validity

    timeout = httpx.Timeout(
        ATS_VALIDATE_TIMEOUT_SECONDS,
        connect=ATS_VALIDATE_TIMEOUT_SECONDS,
        read=ATS_VALIDATE_TIMEOUT_SECONDS,
        write=ATS_VALIDATE_TIMEOUT_SECONDS,
        pool=ATS_VALIDATE_TIMEOUT_SECONDS,
    )
    limits = httpx.Limits(
        max_connections=workers,
        max_keepalive_connections=workers,
    )
    semaphore = asyncio.Semaphore(workers)

    async with httpx.AsyncClient(
        headers=API_HEADERS,
        timeout=timeout,
        limits=limits,
        follow_redirects=True,
    ) as client:
        tasks = [
            asyncio.create_task(validate_board_async(client, semaphore, ats, board))
            for board in boards
        ]
        for index, task in enumerate(asyncio.as_completed(tasks), start=1):
            board, is_valid = await task
            validity[board] = is_valid
            if index % VALIDATION_PROGRESS_INTERVAL == 0 or index == total_boards:
                valid_count = sum(1 for value in validity.values() if value)
                log(
                    f"validation progress ats={ats} checked={index}/{total_boards} "
                    f"valid={valid_count}"
                )
    return validity


def validate_candidate_boards(
    ats: str,
    boards: Sequence[str],
    validation_workers: int,
) -> dict[str, bool]:
    return asyncio.run(
        validate_candidate_boards_async(
            ats=ats,
            boards=boards,
            validation_workers=validation_workers,
        )
    )


def run_search(
    session: requests.Session,
    rate_limiter: SearchRateLimiter,
    search: str,
    you_api_key: str,
) -> dict:
    rate_limiter.wait_for_slot()
    response = session.get(
        YOU_SEARCH_API,
        params={
            "query": search,
            "count": YOU_SEARCH_MAX_RESULTS,
        },
        headers={
            "X-API-KEY": you_api_key,
            "Accept": "application/json",
        },
        timeout=YOU_SEARCH_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def process_search(
    conn: psycopg.Connection,
    session: requests.Session,
    rate_limiter: SearchRateLimiter,
    ats: str,
    search: str,
    you_api_key: str,
    force: bool,
    validation_workers: int,
) -> QueryRunResult:
    if search_already_recorded(conn, search):
        if not force:
            return QueryRunResult(query=search, ats=ats, success=True, skipped=True)
        touch_you_search(conn, search)

    try:
        log(f"starting search ats={ats} | {search}")
        payload = run_search(
            session=session,
            rate_limiter=rate_limiter,
            search=search,
            you_api_key=you_api_key,
        )
        urls = extract_result_urls(payload)
        candidate_boards = sorted(
            {
                board
                for board in (extract_board_identifier(url, ats) for url in urls)
                if board is not None
            },
            key=str.lower,
        )
        log(
            f"search results ats={ats} results={len(urls)} "
            f"candidates={len(candidate_boards)} | {search}"
        )
        board_validity = validate_candidate_boards(
            ats,
            candidate_boards,
            validation_workers,
        )

        valid_boards = 0
        inserted_boards = 0
        for board in candidate_boards:
            if not board_validity.get(board, False):
                continue

            valid_boards += 1
            if insert_ats_board_if_new(conn, board, ats):
                inserted_boards += 1

        upsert_you_search(
            conn=conn,
            search=search,
            results_num=len(urls),
            success=True,
            boards=valid_boards,
        )
        return QueryRunResult(
            query=search,
            ats=ats,
            success=True,
            results_num=len(urls),
            candidate_boards=len(candidate_boards),
            valid_boards=valid_boards,
            inserted_boards=inserted_boards,
        )
    except (requests.RequestException, json.JSONDecodeError, ValueError) as exc:
        upsert_you_search(conn=conn, search=search, results_num=0, success=False, boards=0)
        return QueryRunResult(query=search, ats=ats, success=False, error=str(exc))


def print_query_result(current: int, total: int, result: QueryRunResult) -> None:
    if result.skipped:
        log(f"[{current}/{total}] skipped existing | ats={result.ats} | {result.query}")
        return

    if result.success:
        log(
            f"[{current}/{total}] ok | ats={result.ats} results={result.results_num} "
            f"candidates={result.candidate_boards} valid={result.valid_boards} "
            f"inserted={result.inserted_boards} | {result.query}"
        )
        return

    log(f"[{current}/{total}] failed | ats={result.ats} {result.error} | {result.query}")


def process_keyword(
    conn: psycopg.Connection,
    session: requests.Session,
    rate_limiter: SearchRateLimiter,
    keyword: Keyword,
    query_counter: int,
    total_queries: int,
    you_api_key: str,
    force: bool,
    validation_workers: int,
) -> tuple[int, bool, dict[str, int]]:
    keyword_success = True
    inserted_counts = {"Green": 0, "Ashby": 0, "Lever": 0}

    for ats, template in QUERY_TEMPLATES:
        query_counter += 1
        search = template.format(name=keyword.name)
        result = process_search(
            conn=conn,
            session=session,
            rate_limiter=rate_limiter,
            ats=ats,
            search=search,
            you_api_key=you_api_key,
            force=force,
            validation_workers=validation_workers,
        )
        print_query_result(query_counter, total_queries, result)
        inserted_counts[ats] += result.inserted_boards
        if not result.success:
            keyword_success = False

    update_keyword_result(
        conn=conn,
        name_id=keyword.name_id,
        success=keyword_success,
        inserted_counts=inserted_counts,
    )
    log(
        f"keyword={keyword.name!r} green={inserted_counts['Green']} "
        f"ashby={inserted_counts['Ashby']} lever={inserted_counts['Lever']} "
        f"success={keyword_success}"
    )
    return query_counter, keyword_success, inserted_counts


def run_test_mode(
    conn: psycopg.Connection,
    session: requests.Session,
    rate_limiter: SearchRateLimiter,
    keyword: Keyword,
    you_api_key: str,
    force: bool,
    validation_workers: int,
) -> None:
    process_keyword(
        conn=conn,
        session=session,
        rate_limiter=rate_limiter,
        keyword=keyword,
        query_counter=0,
        total_queries=len(QUERY_TEMPLATES),
        you_api_key=you_api_key,
        force=force,
        validation_workers=validation_workers,
    )


def run_full_mode(
    conn: psycopg.Connection,
    session: requests.Session,
    rate_limiter: SearchRateLimiter,
    selected_keywords: Sequence[Keyword],
    you_api_key: str,
    force: bool,
    validation_workers: int,
) -> None:
    total_queries = len(selected_keywords) * len(QUERY_TEMPLATES)
    query_counter = 0

    for keyword in selected_keywords:
        query_counter, _, _ = process_keyword(
            conn=conn,
            session=session,
            rate_limiter=rate_limiter,
            keyword=keyword,
            query_counter=query_counter,
            total_queries=total_queries,
            you_api_key=you_api_key,
            force=force,
            validation_workers=validation_workers,
        )


def main() -> None:
    args = parse_args()
    if args.validation_workers <= 0:
        raise RuntimeError("--validation-workers must be greater than 0")
    initialize_log()
    you_api_key = os.getenv("API")
    if not you_api_key:
        raise RuntimeError("Missing API environment variable")

    with requests.Session() as session:
        session.headers.update(
            {
                "User-Agent": "app-copilot-discovery/0.1.0",
                "Accept": "application/json",
            }
        )
        rate_limiter = SearchRateLimiter(max_requests=100, period_seconds=60.0)

        with db_connect() as conn:
            if args.mode == "test":
                if not args.name:
                    raise RuntimeError("--name is required in test mode")
                keyword = fetch_keyword_by_name(conn, args.name)
                if keyword is None:
                    raise RuntimeError(f"Keyword '{args.name}' was not found")
                mark_keywords_used_now(conn, [keyword.name_id])
                run_test_mode(
                    conn=conn,
                    session=session,
                    rate_limiter=rate_limiter,
                    keyword=keyword,
                    you_api_key=you_api_key,
                    force=args.force,
                    validation_workers=args.validation_workers,
                )
            else:
                selected_keywords = fetch_all_keywords(conn)
                if not selected_keywords:
                    raise RuntimeError("No keyword rows found")
                mark_keywords_used_now(conn, [row.name_id for row in selected_keywords])
                run_full_mode(
                    conn=conn,
                    session=session,
                    rate_limiter=rate_limiter,
                    selected_keywords=selected_keywords,
                    you_api_key=you_api_key,
                    force=args.force,
                    validation_workers=args.validation_workers,
                )


if __name__ == "__main__":
    main()
