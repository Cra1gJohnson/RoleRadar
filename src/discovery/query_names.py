import argparse
import os
import random
import re
import selectors
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence, Set, Tuple, Union
from urllib.parse import quote, urlparse

import psycopg
import requests

GREENHOUSE_BOARD_API = "https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
GREENHOUSE_DOMAINS = {"boards.greenhouse.io", "job-boards.greenhouse.io"}
QUERY_TEMPLATES = (
    "site:boards.greenhouse.io {name}",
    "site:job-boards.greenhouse.io {name}",
)
TOKEN_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
URL_RE = re.compile(r"https?://[^\s<>\"']+")

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


class RunStoppedError(RuntimeError):
    """Raised when the run must stop due to repeated query errors."""


@dataclass(frozen=True)
class DiscoveryName:
    name_id: int
    name: str


@dataclass
class FailedQuery:
    name: str
    query: str
    reason: str


@dataclass
class QueryOutcome:
    had_relevant_urls: bool


@dataclass
class DdgrSessionManager:
    ddgr_bin: str
    max_queries_per_session: int = 25
    proc: Optional[subprocess.Popen[bytes]] = None
    queries_in_session: int = 0

    def close(self) -> None:
        if self.proc is None:
            return
        if self.proc.stdin is not None and not self.proc.stdin.closed:
            try:
                self.proc.stdin.write(b"q\n")
                self.proc.stdin.flush()
            except Exception:
                pass
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)
        self.proc = None
        self.queries_in_session = 0

    def _start_process_with_query(self, query: str) -> str:
        cmd = build_ddgr_command(self.ddgr_bin, query)
        print(f"  starting ddgr session: {' '.join(cmd[:5])} ...")
        self.proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=False,
        )
        self.queries_in_session = 1
        return read_ddgr_until_prompt(self.proc, timeout_seconds=90.0)

    def _send_command_and_read(self, command: str) -> str:
        if self.proc is None or self.proc.poll() is not None:
            raise RuntimeError("ddgr subprocess is not running")
        if self.proc.stdin is None:
            raise RuntimeError("ddgr subprocess stdin is not available")
        self.proc.stdin.write(command.encode("utf-8"))
        self.proc.stdin.flush()
        return read_ddgr_until_prompt(self.proc, timeout_seconds=90.0)

    def run_query_first_page(self, query: str) -> str:
        needs_new_session = (
            self.proc is None
            or self.proc.poll() is not None
            or self.queries_in_session >= self.max_queries_per_session
        )
        if needs_new_session:
            self.close()
            return self._start_process_with_query(query)

        print("  reusing ddgr session for next query")
        self.queries_in_session += 1
        return self._send_command_and_read(f"d {query}\n")

    def next_page(self) -> str:
        return self._send_command_and_read("n\n")


def db_connect() -> psycopg.Connection:
    return psycopg.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        autocommit=True,
    )


def parse_name_range(raw: str) -> Tuple[int, int]:
    match = re.fullmatch(r"(\d+)-(\d+)", raw.strip())
    if not match:
        raise ValueError("name_range must be in format start-end, e.g. 0-200")

    start = int(match.group(1))
    end = int(match.group(2))

    if end <= start:
        raise ValueError("name_range end must be greater than start")

    return start, end


def fetch_discovery_names_in_range(
    conn: psycopg.Connection,
    start: int,
    end: int,
) -> List[DiscoveryName]:
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


def upsert_board_token(conn: psycopg.Connection, token: str, success: bool) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO board_token (token, last_used, success)
            VALUES (%s, NOW(), %s)
            ON CONFLICT (token)
            DO UPDATE SET
                last_used = EXCLUDED.last_used,
                success = EXCLUDED.success
            """,
            (token, success),
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


def validate_board_token(session: requests.Session, token: str) -> bool:
    api_url = GREENHOUSE_BOARD_API.format(token=quote(token, safe=""))
    resp = session.get(api_url, headers=GREENHOUSE_API_HEADERS, timeout=30)
    return resp.status_code == 200


def build_ddgr_command(ddgr_bin: str, query: str) -> List[str]:
    ddgr_path = Path(ddgr_bin)
    if ddgr_path.exists():
        return [sys.executable, str(ddgr_path), "--nocolor", "-x", "-n", "20", query]
    return [ddgr_bin, "--nocolor", "-x", "-n", "20", query]


def parse_urls_from_text(text: str) -> List[str]:
    urls: List[str] = []
    for match in URL_RE.finditer(text):
        url = match.group(0).rstrip(").,]>")
        urls.append(url)
    return urls


def extract_ddgr_error(text: str) -> Optional[str]:
    for line in text.splitlines():
        if "[ERROR]" in line:
            return line.strip()
    return None


def read_ddgr_until_prompt(proc: subprocess.Popen[bytes], timeout_seconds: float = 90.0) -> str:
    if proc.stdout is None:
        raise RuntimeError("ddgr subprocess stdout is not available")

    selector = selectors.DefaultSelector()
    selector.register(proc.stdout, selectors.EVENT_READ)

    prompt_token = b"ddgr (? for help)"
    chunks: List[bytes] = []
    start = time.monotonic()

    try:
        while True:
            if time.monotonic() - start > timeout_seconds:
                raise RuntimeError("Timed out waiting for ddgr page output")

            events = selector.select(timeout=0.5)
            if events:
                data = os.read(proc.stdout.fileno(), 4096)
                if data:
                    chunks.append(data)
                    start = time.monotonic()
                    current = b"".join(chunks)
                    if prompt_token in current:
                        return current.decode("utf-8", errors="replace")
                    continue

            if proc.poll() is not None:
                try:
                    rest = os.read(proc.stdout.fileno(), 4096)
                    if rest:
                        chunks.append(rest)
                        continue
                except OSError:
                    pass
                break
    finally:
        selector.close()

    return b"".join(chunks).decode("utf-8", errors="replace")


def ddgr_subprocess_search(
    query: str,
    pages: Union[int, str],
    ddgr_session: DdgrSessionManager,
) -> List[str]:
    max_pages = 10 if pages == "max" else int(pages)

    all_urls: List[str] = []
    seen: Set[str] = set()

    for page_index in range(1, max_pages + 1):
        print(f"  fetching page {page_index}/{max_pages}")
        if page_index == 1:
            page_text = ddgr_session.run_query_first_page(query)
        else:
            delay = random.uniform(18.0, 48.0)
            print(f"  sleeping {delay:.1f}s before requesting next page")
            time.sleep(delay)
            page_text = ddgr_session.next_page()

        error_line = extract_ddgr_error(page_text)
        if error_line:
            raise RuntimeError(error_line)

        page_urls = parse_urls_from_text(page_text)
        new_count = 0
        for url in page_urls:
            if url in seen:
                continue
            seen.add(url)
            all_urls.append(url)
            new_count += 1
            if len(all_urls) >= 20:
                print("  reached 20 URLs for this query")
                break

        print(f"  page {page_index}: {len(page_urls)} urls parsed, {new_count} new")

        if len(all_urls) >= 20:
            break

        if page_index >= max_pages:
            break

        if pages == "max" and new_count == 0:
            print("  no new URLs; stopping max-page loop")
            break

    return all_urls[:20]


def process_query(
    conn: psycopg.Connection,
    session: requests.Session,
    ddgr_session: DdgrSessionManager,
    name_row: DiscoveryName,
    query: str,
    pages: Union[int, str],
) -> QueryOutcome:
    print(f"Running query: {query}")

    urls = ddgr_subprocess_search(query=query, pages=pages, ddgr_session=ddgr_session)
    greenhouse_urls = [url for url in urls if is_greenhouse_url(url)]

    if not greenhouse_urls:
        print(f"No greenhouse URLs found for: {name_row.name}")
        return QueryOutcome(had_relevant_urls=False)

    tokens: Set[str] = set()
    for url in greenhouse_urls:
        token = extract_board_token(url)
        if token:
            tokens.add(token)
            print(f"  extracted token: {token}")

    if not tokens:
        print(f"No board tokens extracted for: {name_row.name}")
        return QueryOutcome(had_relevant_urls=False)

    for token in sorted(tokens):
        try:
            is_valid = validate_board_token(session, token)
        except requests.RequestException as exc:
            is_valid = False
            print(f"  token validation error for {token}: {exc}")

        if is_valid:
            upsert_board_token(conn, token, True)
            print(f"  valid token saved: {token}")
        else:
            print(f"  invalid token: {token}")

    return QueryOutcome(had_relevant_urls=True)


def random_query_sleep(min_delay: float, max_delay: float) -> None:
    low = max(0.0, min_delay)
    high = max(low, max_delay)
    delay = random.uniform(low, high)
    print(f"Sleeping {delay:.1f}s before next query")
    time.sleep(delay)


def periodic_cooldown_after_queries(
    query_counter: int,
    total_queries: int,
    interval: int = 20,
    min_seconds: float = 720.0,
    max_seconds: float = 1440.0,
) -> None:
    if query_counter <= 0 or query_counter >= total_queries:
        return
    if query_counter % interval != 0:
        return
    delay = random.uniform(min_seconds, max_seconds)
    print(
        f"Periodic cooldown after {query_counter} queries: "
        f"sleeping {delay:.1f} seconds"
    )
    time.sleep(delay)


def register_query_error(
    failed_queries: List[FailedQuery],
    name: str,
    query: str,
    reason: str,
) -> None:
    failed_queries.append(FailedQuery(name=name, query=query, reason=reason))


def print_failed_queries(failed_queries: Sequence[FailedQuery]) -> None:
    print("Failed queries:")
    for idx, item in enumerate(failed_queries, start=1):
        print(f"{idx}. name='{item.name}' query='{item.query}' reason='{item.reason}'")


def run_test_mode(
    conn: psycopg.Connection,
    session: requests.Session,
    ddgr_session: DdgrSessionManager,
    selected_names: Sequence[DiscoveryName],
    raw_name: Optional[str],
    pages: Union[int, str],
) -> None:
    if not raw_name:
        raise RuntimeError("--name is required in test mode")

    name_row = next((n for n in selected_names if n.name.lower() == raw_name.lower()), None)
    if name_row is None:
        raise RuntimeError(f"Name '{raw_name}' was not found in selected range")

    query = QUERY_TEMPLATES[0].format(name=name_row.name)
    try:
        outcome = process_query(conn, session, ddgr_session, name_row, query, pages)
        update_name_success(conn, name_row.name_id, outcome.had_relevant_urls)
    except Exception as exc:
        update_name_success(conn, name_row.name_id, False)
        print(f"FAILED query for '{name_row.name}': {exc}")
        print("run failed")
        raise SystemExit(1) from exc


def run_full_mode(
    conn: psycopg.Connection,
    session: requests.Session,
    ddgr_session: DdgrSessionManager,
    selected_names: Sequence[DiscoveryName],
    pages: Union[int, str],
    min_delay: float,
    max_delay: float,
) -> None:
    failed_queries: List[FailedQuery] = []
    total_queries = len(selected_names) * len(QUERY_TEMPLATES)
    query_counter = 0

    for name_index, name_row in enumerate(selected_names):
        name_success = True
        skip_remaining_for_name = False

        for template_index, template in enumerate(QUERY_TEMPLATES):
            query_counter += 1
            query = template.format(name=name_row.name)
            print(f"[{query_counter}/{total_queries}] {query}")

            try:
                outcome = process_query(conn, session, ddgr_session, name_row, query, pages)
                if not outcome.had_relevant_urls:
                    name_success = False
            except Exception as exc:
                name_success = False
                register_query_error(failed_queries, name_row.name, query, str(exc))

                if len(failed_queries) == 1:
                    print(f"First error encountered for '{name_row.name}'. Waiting 360 seconds and continuing.")
                    time.sleep(1280)
                    skip_remaining_for_name = True
                    break

                print_failed_queries(failed_queries)
                print("run failed")
                update_name_success(conn, name_row.name_id, False)
                raise RunStoppedError("Second query error encountered; stopping run") from exc

            is_last_query_for_name = template_index == (len(QUERY_TEMPLATES) - 1)
            is_last_name = name_index == (len(selected_names) - 1)
            is_last_query_of_run = is_last_query_for_name and is_last_name
            if not is_last_query_of_run:
                random_query_sleep(min_delay, max_delay)
                periodic_cooldown_after_queries(
                    query_counter=query_counter,
                    total_queries=total_queries,
                )

        if skip_remaining_for_name:
            update_name_success(conn, name_row.name_id, False)
            continue

        update_name_success(conn, name_row.name_id, name_success)


def parse_pages(raw: str) -> Union[int, str]:
    raw_value = raw.strip().lower()
    if raw_value == "max":
        return "max"
    if raw_value not in {"1", "2", "3"}:
        raise ValueError("--pages must be one of: 1, 2, 3, max")
    return int(raw_value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query DuckDuckGo for Greenhouse board tokens")
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
    parser.add_argument(
        "--pages",
        default="1",
        help="Pages per query: 1, 2, 3, or max (max stops at no-new-urls or 10 pages)",
    )
    parser.add_argument(
        "--min-delay",
        type=float,
        default=180.0,
        help="Minimum seconds between queries in full mode",
    )
    parser.add_argument(
        "--max-delay",
        type=float,
        default=540.0,
        help="Maximum seconds between queries in full mode",
    )
    parser.add_argument(
        "--ddgr-bin",
        default="./ddgr-2.2/ddgr",
        help="Path to ddgr executable/module file",
    )
    parser.add_argument(
        "--cookie-file",
        default="src/discovery/ddgr_cookies.txt",
        help="No-op in subprocess mode (kept for CLI compatibility)",
    )
    parser.add_argument(
        "--reset-cookies",
        action="store_true",
        help="No-op in subprocess mode (kept for CLI compatibility)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start, end = parse_name_range(args.name_range)
    pages = parse_pages(args.pages)

    if args.reset_cookies or args.cookie_file:
        print("Note: cookie options are currently no-op in subprocess ddgr mode")

    session = requests.Session()
    ddgr_session = DdgrSessionManager(ddgr_bin=args.ddgr_bin, max_queries_per_session=25)

    try:
        with db_connect() as conn:
            selected_names = fetch_discovery_names_in_range(conn, start, end)
            if not selected_names:
                raise RuntimeError("No discovery_name rows found in selected range")

            mark_names_used_now(conn, [row.name_id for row in selected_names])

            if args.mode == "test":
                run_test_mode(
                    conn=conn,
                    session=session,
                    ddgr_session=ddgr_session,
                    selected_names=selected_names,
                    raw_name=args.name,
                    pages=pages,
                )
            else:
                run_full_mode(
                    conn=conn,
                    session=session,
                    ddgr_session=ddgr_session,
                    selected_names=selected_names,
                    pages=pages,
                    min_delay=args.min_delay,
                    max_delay=args.max_delay,
                )
    except RunStoppedError:
        raise SystemExit(1)
    finally:
        ddgr_session.close()


if __name__ == "__main__":
    main()
