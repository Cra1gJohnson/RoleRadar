import argparse
import asyncio
import hashlib
import os
import resource
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import httpx
import psycopg

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env
from delete import delete_missing_jobs, fetch_existing_job_rows, verify_cascade_contract
from normalization import normalize_board_payload
from upsert import upsert_jobs

load_shared_env()

GREENHOUSE_BOARD_API = "https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
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
BOARD_HASH_HEX_LENGTH = 24
DEFAULT_RATE_PER_MINUTE = 200
DEFAULT_CONCURRENCY = 16
DEFAULT_FAILURE_THRESHOLD = 0
DEFAULT_QUEUE_SIZE = 32
PROGRESS_PRINT_INTERVAL = 50

_STOP_TOKEN = object()


@dataclass(frozen=True)
class SnapshotRow:
    """Fields needed from the current board snapshot row."""

    snapshot_id: int
    board_hash: Optional[str]
    status: Optional[str]


@dataclass(frozen=True)
class FetchResult:
    """Outcome of one network fetch."""

    token: str
    payload: Optional[dict[str, Any]]
    request_status: int
    fetch_seconds: float
    error_message: Optional[str] = None


@dataclass
class RunSummary:
    """Track the aggregate outcome of one collection run."""

    scheduled: int = 0
    completed: int = 0
    changed: int = 0
    no_change: int = 0
    aborted_count: int = 0
    request_failures: int = 0
    normalization_failures: int = 0
    database_failures: int = 0
    inserted_count: int = 0
    updated_count: int = 0
    deleted_count: int = 0
    skipped_count: int = 0
    filtered_count: int = 0
    normalized_jobs: int = 0
    queue_peak: int = 0
    request_seconds_total: float = 0.0
    normalize_seconds_total: float = 0.0
    db_seconds_total: float = 0.0
    request_latencies_ms: list[float] = field(default_factory=list)
    normalize_latencies_ms: list[float] = field(default_factory=list)
    db_latencies_ms: list[float] = field(default_factory=list)
    elapsed_seconds: float = 0.0
    cpu_user_seconds: float = 0.0
    cpu_system_seconds: float = 0.0
    max_rss_kb: int = 0

    @property
    def failed(self) -> int:
        """Return the total number of failed boards across failure categories."""
        return self.request_failures + self.normalization_failures + self.database_failures


class AsyncEvenRateLimiter:
    """Spread request starts evenly across time instead of bursting within a minute."""

    def __init__(self, rate_per_minute: int) -> None:
        self.dispatch_interval = 60.0 / rate_per_minute
        self.next_dispatch_at = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until the next evenly spaced dispatch slot is available."""
        async with self.lock:
            now = time.monotonic()
            sleep_seconds = self.next_dispatch_at - now
            if sleep_seconds > 0:
                await asyncio.sleep(sleep_seconds)
                now = time.monotonic()

            self.next_dispatch_at = max(self.next_dispatch_at + self.dispatch_interval, now)


def print_runtime_context() -> None:
    """Print the resolved repo root and database target before a run starts."""
    db_name = os.getenv("DB_NAME") or "<unset>"
    db_user = os.getenv("DB_USER") or "<unset>"
    db_host = os.getenv("DB_HOST") or "<unset>"
    db_port = os.getenv("DB_PORT") or "<unset>"
    cwd = Path.cwd()

    print(
        "Runtime context: "
        f"repo_root={SRC_ROOT} "
        f"cwd={cwd} "
        f"db_target={db_name}@{db_host}:{db_port} "
        f"db_user={db_user}"
    )


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for controller modes."""
    parser = argparse.ArgumentParser(
        description="Async controller for live Greenhouse board collection."
    )
    parser.add_argument(
        "mode",
        choices=("full", "new", "test"),
        help="Run a full scan, unseen-only scan, or a one-board test run",
    )
    parser.add_argument(
        "--rate-per-minute",
        type=int,
        default=DEFAULT_RATE_PER_MINUTE,
        help="Maximum number of board fetches to start per 60 seconds",
    )
    parser.add_argument(
        "--concurrency",
        "--max-workers",
        dest="concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help="Maximum number of concurrent async fetches",
    )
    parser.add_argument(
        "--failure-threshold",
        type=int,
        default=DEFAULT_FAILURE_THRESHOLD,
        help="Stop fetching new boards after this many failures; 0 disables the stop",
    )
    parser.add_argument(
        "--queue-size",
        type=int,
        default=DEFAULT_QUEUE_SIZE,
        help="Maximum number of fetched payloads waiting for normalization and writes",
    )
    return parser.parse_args()


async def db_connect(*, autocommit: bool = False) -> psycopg.AsyncConnection:
    """Create an async PostgreSQL connection using the shared env-based settings."""
    return await psycopg.AsyncConnection.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        autocommit=autocommit,
    )


async def fetch_all_valid_tokens(conn: psycopg.AsyncConnection) -> list[str]:
    """Load every successful board token for a full collection run."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT token
            FROM board_token
            WHERE token IS NOT NULL
              AND success = TRUE
            ORDER BY token
            """
        )
        rows = await cur.fetchall()

    return [row[0] for row in rows]


async def fetch_random_valid_token(conn: psycopg.AsyncConnection) -> Optional[str]:
    """Load one random successful board token for test mode."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT token
            FROM board_token
            WHERE token IS NOT NULL
              AND success = TRUE
            ORDER BY RANDOM()
            LIMIT 1
            """
        )
        row = await cur.fetchone()

    if row is None:
        return None
    return row[0]


async def fetch_unseen_valid_tokens(conn: psycopg.AsyncConnection) -> list[str]:
    """Load successful board tokens that do not yet have a snapshot row."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT bt.token
            FROM board_token AS bt
            WHERE bt.token IS NOT NULL
              AND bt.success = TRUE
              AND NOT EXISTS (
                  SELECT 1
                  FROM greenhouse_board_snapshot AS gbs
                  WHERE gbs.token = bt.token
              )
            ORDER BY bt.token_id, bt.token
            """
        )
        rows = await cur.fetchall()

    return [row[0] for row in rows]


def build_board_api_url(token: str) -> str:
    """Construct the Greenhouse jobs API URL for a board token."""
    return GREENHOUSE_BOARD_API.format(token=quote(token, safe=""))


def compute_board_hash(job_ids: list[int]) -> str:
    """Hash the uninterrupted string of sorted job IDs and cap the hex digest."""
    joined_ids = "".join(str(job_id) for job_id in job_ids)
    digest = hashlib.sha256(joined_ids.encode("utf-8")).hexdigest()
    return digest[:BOARD_HASH_HEX_LENGTH]


def resolve_snapshot_status(job_count: int, existing_status: Optional[str]) -> str:
    """Mark empty boards cold and otherwise preserve status when possible."""
    if job_count == 0:
        return "COLD"
    if existing_status:
        return existing_status
    return "WARM"


async def get_latest_snapshot(conn: psycopg.AsyncConnection, token: str) -> Optional[SnapshotRow]:
    """Fetch the most recent snapshot row for the token, if one exists."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT snapshot_id, board_hash, status
            FROM greenhouse_board_snapshot
            WHERE token = %s
            """,
            (token,),
        )
        row = await cur.fetchone()

    if row is None:
        return None
    return SnapshotRow(snapshot_id=row[0], board_hash=row[1], status=row[2])


async def insert_snapshot(
    conn: psycopg.AsyncConnection,
    token: str,
    request_status: int,
    job_count: int,
    board_hash: str,
    company_name: Optional[str],
    status: str,
) -> int:
    """Insert the first snapshot row for a token and return the snapshot id."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO greenhouse_board_snapshot (
                token,
                fetched_at,
                request_status,
                job_count,
                board_hash,
                company_name,
                status,
                united_states
            )
            VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s)
            RETURNING snapshot_id
            """,
            (
                token,
                request_status,
                job_count,
                board_hash,
                company_name,
                status,
                None,
            ),
        )
        row = await cur.fetchone()

    if row is None:
        raise RuntimeError(f"Failed to insert snapshot for {token}")
    return row[0]


async def update_snapshot(
    conn: psycopg.AsyncConnection,
    snapshot_id: int,
    request_status: int,
    job_count: Optional[int] = None,
    board_hash: Optional[str] = None,
    company_name: Optional[str] = None,
    status: Optional[str] = None,
    united_states: Optional[bool] = None,
) -> None:
    """Update the existing snapshot row in place."""
    assignments = ["fetched_at = NOW()", "request_status = %s"]
    params: list[Any] = [request_status]

    if job_count is not None:
        assignments.append("job_count = %s")
        params.append(job_count)
    if board_hash is not None:
        assignments.append("board_hash = %s")
        params.append(board_hash)
    if company_name is not None:
        assignments.append("company_name = %s")
        params.append(company_name)
    if status is not None:
        assignments.append("status = %s")
        params.append(status)
    if united_states is not None:
        assignments.append("united_states = %s")
        params.append(united_states)

    params.append(snapshot_id)

    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            UPDATE greenhouse_board_snapshot
            SET {", ".join(assignments)}
            WHERE snapshot_id = %s
            """,
            params,
        )


async def record_request_failure(
    conn: psycopg.AsyncConnection,
    token: str,
    request_status: int,
) -> None:
    """Persist the latest request status on an existing snapshot after fetch failure."""
    snapshot = await get_latest_snapshot(conn, token)
    if snapshot is None:
        return
    await update_snapshot(conn, snapshot.snapshot_id, request_status=request_status)


async def fetch_board_payload(client: httpx.AsyncClient, token: str) -> dict[str, Any]:
    """Fetch the board jobs payload and return the decoded JSON object."""
    response = await client.get(
        build_board_api_url(token),
        headers=GREENHOUSE_API_HEADERS,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Greenhouse API did not return a JSON object")
    return payload


async def fetch_worker(
    token_queue: asyncio.Queue[object],
    result_queue: asyncio.Queue[FetchResult | object],
    client: httpx.AsyncClient,
    rate_limiter: AsyncEvenRateLimiter,
    summary: RunSummary,
    stop_event: asyncio.Event,
) -> None:
    """Fetch board payloads and enqueue the raw responses for the writer."""
    while True:
        token_item = await token_queue.get()
        if token_item is _STOP_TOKEN:
            return

        token = str(token_item)
        if stop_event.is_set():
            summary.aborted_count += 1
            continue

        await rate_limiter.acquire()
        fetch_start = time.perf_counter()

        try:
            payload = await fetch_board_payload(client, token)
            request_status = 200
            error_message = None
        except httpx.HTTPStatusError as exc:
            request_status = exc.response.status_code if exc.response is not None else 0
            payload = None
            error_message = str(exc)
        except httpx.RequestError as exc:
            request_status = 0
            payload = None
            error_message = str(exc)

        fetch_seconds = time.perf_counter() - fetch_start
        summary.request_seconds_total += fetch_seconds
        summary.request_latencies_ms.append(fetch_seconds * 1000.0)

        await result_queue.put(
            FetchResult(
                token=token,
                payload=payload,
                request_status=request_status,
                fetch_seconds=fetch_seconds,
                error_message=error_message,
            )
        )
        summary.queue_peak = max(summary.queue_peak, result_queue.qsize())


def _percentile(samples: list[float], percentile: float) -> float:
    """Return a simple percentile from a sorted copy of the samples."""
    if not samples:
        return 0.0

    ordered = sorted(samples)
    index = int(round((len(ordered) - 1) * percentile))
    index = max(0, min(index, len(ordered) - 1))
    return ordered[index]


def print_progress(summary: RunSummary, total_tokens: int) -> None:
    """Print a concise progress snapshot during a full run."""
    print(
        f"Progress: completed={summary.completed}/{total_tokens} scheduled={summary.scheduled} "
        f"changed={summary.changed} no_change={summary.no_change} failed={summary.failed} "
        f"inserted={summary.inserted_count} updated={summary.updated_count} "
        f"deleted={summary.deleted_count} skipped={summary.skipped_count} "
        f"filtered={summary.filtered_count}"
    )


async def process_successful_result(
    conn: psycopg.AsyncConnection,
    result: FetchResult,
    summary: RunSummary,
) -> None:
    """Normalize, compare, and persist one successful board response."""
    normalize_start = time.perf_counter()
    normalized = normalize_board_payload(result.payload or {}, result.token)
    normalize_seconds = time.perf_counter() - normalize_start
    summary.normalize_seconds_total += normalize_seconds
    summary.normalize_latencies_ms.append(normalize_seconds * 1000.0)
    summary.normalization_failures += normalized.failed_count
    summary.filtered_count += normalized.filtered_count
    summary.normalized_jobs += len(normalized.jobs)

    board_hash = compute_board_hash(normalized.raw_job_ids)
    response_job_ids = [job.greenhouse_job_id for job in normalized.db_jobs]

    db_start = time.perf_counter()
    async with conn.transaction():
        existing_snapshot = await get_latest_snapshot(conn, result.token)
        next_status = resolve_snapshot_status(
            job_count=normalized.job_count,
            existing_status=existing_snapshot.status if existing_snapshot else None,
        )

        if existing_snapshot is None:
            snapshot_id = await insert_snapshot(
                conn,
                token=result.token,
                request_status=200,
                job_count=normalized.job_count,
                board_hash=board_hash,
                company_name=normalized.company_name,
                status=next_status,
            )
        else:
            snapshot_id = existing_snapshot.snapshot_id
            await update_snapshot(
                conn,
                snapshot_id=snapshot_id,
                request_status=200,
                job_count=normalized.job_count,
                board_hash=board_hash,
                company_name=normalized.company_name,
                status=next_status,
            )

        existing_jobs = await fetch_existing_job_rows(conn, result.token)
        upsert_summary = await upsert_jobs(
            conn,
            token=result.token,
            snapshot_id=snapshot_id,
            jobs=normalized.db_jobs,
            existing_jobs=existing_jobs,
        )
        delete_summary = await delete_missing_jobs(
            conn,
            token=result.token,
            response_job_ids=response_job_ids,
            existing_jobs=existing_jobs,
        )
        await update_snapshot(
            conn,
            snapshot_id=snapshot_id,
            request_status=200,
            united_states=normalized.united_states,
        )

    db_seconds = time.perf_counter() - db_start
    summary.db_seconds_total += db_seconds
    summary.db_latencies_ms.append(db_seconds * 1000.0)
    summary.inserted_count += upsert_summary.inserted_count
    summary.updated_count += upsert_summary.updated_count
    summary.skipped_count += upsert_summary.skipped_count
    summary.deleted_count += delete_summary.deleted_count

    board_changed = bool(
        upsert_summary.inserted_count
        or upsert_summary.updated_count
        or delete_summary.deleted_count
        or existing_snapshot is None
    )
    if board_changed:
        summary.changed += 1
    else:
        summary.no_change += 1


async def process_result(
    conn: psycopg.AsyncConnection,
    result: FetchResult,
    summary: RunSummary,
) -> None:
    """Persist one fetched board response or record a failed request."""
    if result.payload is None:
        try:
            async with conn.transaction():
                await record_request_failure(conn, result.token, result.request_status)
        except psycopg.Error as exc:
            summary.database_failures += 1
            print(f"{result.token} request-failure write failed: {exc}")
            raise

        summary.request_failures += 1
        return

    try:
        await process_successful_result(conn, result, summary)
    except ValueError as exc:
        summary.normalization_failures += 1
        print(f"{result.token} normalization failed: {exc}")
    except psycopg.Error as exc:
        summary.database_failures += 1
        print(f"{result.token} database failed: {exc}")
        raise


async def writer_loop(
    conn: psycopg.AsyncConnection,
    result_queue: asyncio.Queue[FetchResult | object],
    summary: RunSummary,
    failure_threshold: int,
    stop_event: asyncio.Event,
    worker_tasks: list[asyncio.Task[None]],
    total_tokens: int,
) -> None:
    """Consume fetched board payloads sequentially and persist them."""
    while True:
        item = await result_queue.get()
        if item is _STOP_TOKEN:
            return

        result = item
        try:
            await process_result(conn, result, summary)
        except psycopg.Error:
            stop_event.set()
            for task in worker_tasks:
                task.cancel()
            raise

        summary.completed += 1
        if summary.completed and summary.completed % PROGRESS_PRINT_INTERVAL == 0:
            print_progress(summary, total_tokens)

        if failure_threshold > 0 and summary.failed >= failure_threshold:
            stop_event.set()


async def run_collection(
    tokens: list[str],
    rate_per_minute: int,
    concurrency: int,
    failure_threshold: int,
    queue_size: int,
    total_tokens: int,
) -> RunSummary:
    """Run the async collection pipeline over a fixed token set."""
    summary = RunSummary(scheduled=len(tokens))
    stop_event = asyncio.Event()
    rate_limiter = AsyncEvenRateLimiter(rate_per_minute=rate_per_minute)
    result_queue: asyncio.Queue[FetchResult | object] = asyncio.Queue(
        maxsize=queue_size
    )

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(30.0, connect=10.0),
        limits=httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency),
        follow_redirects=True,
    ) as client:
        conn = await db_connect(autocommit=True)
        try:
            await verify_cascade_contract(conn)

            token_queue: asyncio.Queue[object] = asyncio.Queue()
            for token in tokens:
                await token_queue.put(token)
            for _ in range(concurrency):
                await token_queue.put(_STOP_TOKEN)

            workers: list[asyncio.Task[None]] = []
            writer_task = asyncio.create_task(
                writer_loop(
                    conn=conn,
                    result_queue=result_queue,
                    summary=summary,
                    failure_threshold=failure_threshold,
                    stop_event=stop_event,
                    worker_tasks=workers,
                    total_tokens=total_tokens,
                )
            )

            for _ in range(concurrency):
                workers.append(
                    asyncio.create_task(
                        fetch_worker(
                            token_queue=token_queue,
                            result_queue=result_queue,
                            client=client,
                            rate_limiter=rate_limiter,
                            summary=summary,
                            stop_event=stop_event,
                        )
                    )
                )

            worker_results = await asyncio.gather(*workers, return_exceptions=True)
            unexpected_worker_errors = [
                result
                for result in worker_results
                if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError)
            ]
            if unexpected_worker_errors:
                stop_event.set()
                if not writer_task.done():
                    writer_task.cancel()
                await asyncio.gather(writer_task, return_exceptions=True)
                raise unexpected_worker_errors[0]

            if writer_task.done():
                await writer_task
            else:
                await result_queue.put(_STOP_TOKEN)
                await writer_task
        finally:
            await conn.close()

    return summary


async def load_full_scan_tokens() -> list[str]:
    """Load every eligible token using a short-lived bootstrap connection."""
    async with await db_connect(autocommit=True) as conn:
        return await fetch_all_valid_tokens(conn)


async def load_unseen_scan_tokens() -> list[str]:
    """Load unseen eligible tokens using a short-lived bootstrap connection."""
    async with await db_connect(autocommit=True) as conn:
        return await fetch_unseen_valid_tokens(conn)


async def load_test_token() -> Optional[str]:
    """Load one random test token using a short-lived bootstrap connection."""
    async with await db_connect(autocommit=True) as conn:
        return await fetch_random_valid_token(conn)


def capture_process_metrics(
    summary: RunSummary,
    start_time: float,
    start_usage: resource.struct_rusage,
) -> None:
    """Populate process-level timing and memory metrics on the run summary."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    summary.elapsed_seconds = time.perf_counter() - start_time
    summary.cpu_user_seconds = usage.ru_utime - start_usage.ru_utime
    summary.cpu_system_seconds = usage.ru_stime - start_usage.ru_stime
    summary.max_rss_kb = usage.ru_maxrss


def print_final_summary(summary: RunSummary) -> None:
    """Print a compact final run summary with benchmark metrics."""
    request_p50 = _percentile(summary.request_latencies_ms, 0.50)
    request_p95 = _percentile(summary.request_latencies_ms, 0.95)
    normalize_p50 = _percentile(summary.normalize_latencies_ms, 0.50)
    normalize_p95 = _percentile(summary.normalize_latencies_ms, 0.95)
    db_p50 = _percentile(summary.db_latencies_ms, 0.50)
    db_p95 = _percentile(summary.db_latencies_ms, 0.95)

    print(
        f"Final summary: scheduled={summary.scheduled} completed={summary.completed} "
        f"changed={summary.changed} no_change={summary.no_change} failed={summary.failed} "
        f"request_failures={summary.request_failures} normalization_failures={summary.normalization_failures} "
        f"database_failures={summary.database_failures} inserted={summary.inserted_count} "
        f"updated={summary.updated_count} deleted={summary.deleted_count} "
        f"skipped={summary.skipped_count} filtered={summary.filtered_count} "
        f"normalized_jobs={summary.normalized_jobs} aborted={summary.aborted_count} "
        f"queue_peak={summary.queue_peak} elapsed_seconds={summary.elapsed_seconds:.2f} "
        f"cpu_user_seconds={summary.cpu_user_seconds:.2f} "
        f"cpu_system_seconds={summary.cpu_system_seconds:.2f} "
        f"max_rss_kb={summary.max_rss_kb} "
        f"request_seconds_total={summary.request_seconds_total:.2f} "
        f"normalize_seconds_total={summary.normalize_seconds_total:.2f} "
        f"db_seconds_total={summary.db_seconds_total:.2f} "
        f"request_ms_p50={request_p50:.1f} request_ms_p95={request_p95:.1f} "
        f"normalize_ms_p50={normalize_p50:.1f} normalize_ms_p95={normalize_p95:.1f} "
        f"db_ms_p50={db_p50:.1f} db_ms_p95={db_p95:.1f}"
    )


async def run_test_mode() -> int:
    """Select one random token and run a single board check."""
    token = await load_test_token()
    if token is None:
        print("No successful board_token rows found in database")
        return 1

    print(f"Mode=test token={token}")
    start_time = time.perf_counter()
    start_usage = resource.getrusage(resource.RUSAGE_SELF)
    summary = await run_collection(
        tokens=[token],
        rate_per_minute=DEFAULT_RATE_PER_MINUTE,
        concurrency=1,
        failure_threshold=0,
        queue_size=2,
        total_tokens=1,
    )
    capture_process_metrics(summary, start_time, start_usage)
    print_final_summary(summary)
    return 1 if summary.failed or summary.aborted_count else 0


async def run_full_scan(
    rate_per_minute: int,
    concurrency: int,
    failure_threshold: int,
    queue_size: int,
) -> int:
    """Run an async scan across all valid board tokens."""
    tokens = await load_full_scan_tokens()
    if not tokens:
        print("No successful board_token rows found in database")
        return 1

    print(
        f"Mode=full tokens={len(tokens)} rate_per_minute={rate_per_minute} "
        f"concurrency={concurrency} "
        f"failure_threshold={'disabled' if failure_threshold == 0 else failure_threshold}"
    )
    start_time = time.perf_counter()
    start_usage = resource.getrusage(resource.RUSAGE_SELF)
    summary = await run_collection(
        tokens=tokens,
        rate_per_minute=rate_per_minute,
        concurrency=concurrency,
        failure_threshold=failure_threshold,
        queue_size=queue_size,
        total_tokens=len(tokens),
    )
    capture_process_metrics(summary, start_time, start_usage)
    print_progress(summary, len(tokens))
    print_final_summary(summary)
    return 1 if summary.failed or summary.aborted_count else 0


async def run_unseen_scan(
    rate_per_minute: int,
    concurrency: int,
    failure_threshold: int,
    queue_size: int,
) -> int:
    """Run a smoothly paced scan across tokens with no snapshot row yet."""
    tokens = await load_unseen_scan_tokens()
    if not tokens:
        print("No unseen successful board_token rows found in database")
        return 0

    print(
        f"Mode=new tokens={len(tokens)} rate_per_minute={rate_per_minute} "
        f"concurrency={concurrency} "
        f"failure_threshold={'disabled' if failure_threshold == 0 else failure_threshold}"
    )
    start_time = time.perf_counter()
    start_usage = resource.getrusage(resource.RUSAGE_SELF)
    summary = await run_collection(
        tokens=tokens,
        rate_per_minute=rate_per_minute,
        concurrency=concurrency,
        failure_threshold=failure_threshold,
        queue_size=queue_size,
        total_tokens=len(tokens),
    )
    capture_process_metrics(summary, start_time, start_usage)
    print_progress(summary, len(tokens))
    print_final_summary(summary)
    return 1 if summary.failed or summary.aborted_count else 0


async def async_main() -> int:
    """Async CLI entrypoint for collection operations."""
    args = parse_args()
    print_runtime_context()

    if args.rate_per_minute <= 0:
        print("rate-per-minute must be greater than 0")
        return 1
    if args.concurrency <= 0:
        print("concurrency must be greater than 0")
        return 1
    if args.queue_size <= 0:
        print("queue-size must be greater than 0")
        return 1
    if args.failure_threshold < 0:
        print("failure-threshold must be 0 or greater")
        return 1

    if args.mode == "test":
        return await run_test_mode()
    if args.mode == "new":
        return await run_unseen_scan(
            rate_per_minute=args.rate_per_minute,
            concurrency=args.concurrency,
            failure_threshold=args.failure_threshold,
            queue_size=args.queue_size,
        )
    return await run_full_scan(
        rate_per_minute=args.rate_per_minute,
        concurrency=args.concurrency,
        failure_threshold=args.failure_threshold,
        queue_size=args.queue_size,
    )


def main() -> None:
    """CLI entrypoint for collection operations."""
    try:
        exit_code = asyncio.run(async_main())
    except (OSError, ValueError, psycopg.Error, httpx.HTTPError) as exc:
        print(str(exc))
        raise SystemExit(1) from exc

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
