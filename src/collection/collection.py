import argparse
import asyncio
import hashlib
import json
import os
import resource
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Any, Optional

import psycopg
import requests

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env

from ats_common import API_HEADERS, BoardRow, NormalizedJob
import ashby
import green
import lever

load_shared_env()

ATS_MODULES: dict[str, ModuleType] = {
    "Green": green,
    "Ashby": ashby,
    "Lever": lever,
}
BOARD_HASH_HEX_LENGTH = 24
DEFAULT_RATE_PER_MINUTE = 200
DEFAULT_CONCURRENCY = 50
LOG_DIR = Path(__file__).resolve().parent / "logs"
STOP_SENTINEL = object()


@dataclass(frozen=True)
class SnapshotRow:
    snapshot_id: int
    board_hash: Optional[str]


@dataclass(frozen=True)
class FetchResult:
    board: BoardRow
    payload: Any
    request_status: int
    fetch_seconds: float
    error: Optional[str] = None


@dataclass(frozen=True)
class ExistingJobRow:
    job_id: int
    updated_at: Optional[datetime]
    company_name: Optional[str]
    title: Optional[str]
    location: Optional[str]
    url: Optional[str]
    description: Optional[str]
    min_compensation: Optional[int]
    max_compensation: Optional[int]
    united_states: Optional[bool]


@dataclass
class BoardWriteSummary:
    changed: bool = False
    job_count: int = 0
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    deleted: int = 0
    normalize_seconds: float = 0.0
    db_seconds: float = 0.0
    error: Optional[str] = None


@dataclass
class RunSummary:
    scheduled: int = 0
    completed: int = 0
    changed: int = 0
    unchanged: int = 0
    request_failed: int = 0
    write_failed: int = 0
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    deleted: int = 0
    interrupted: bool = False
    started_at: float = field(default_factory=time.monotonic)


class Logger:
    def __init__(self) -> None:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.path = LOG_DIR / f"collection_{timestamp}.log"
        self.path.touch()

    def write(self, message: str) -> None:
        timestamp = datetime.now().isoformat(timespec="seconds")
        with self.path.open("a", encoding="utf-8") as log_file:
            log_file.write(f"{timestamp} {message}\n")


class AsyncEvenRateLimiter:
    def __init__(self, rate_per_minute: int) -> None:
        self.dispatch_interval = 60.0 / rate_per_minute
        self.next_dispatch_at = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self.lock:
            now = time.monotonic()
            sleep_seconds = self.next_dispatch_at - now
            if sleep_seconds > 0:
                await asyncio.sleep(sleep_seconds)
                now = time.monotonic()
            self.next_dispatch_at = max(self.next_dispatch_at + self.dispatch_interval, now)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect jobs from Green, Ashby, and Lever boards.")
    parser.add_argument(
        "mode",
        choices=("test", "full", "Green", "Ashby", "Lever"),
        help="test collects one random board; full collects all successful boards; ATS modes scope by ATS.",
    )
    parser.add_argument(
        "--rate-per-minute",
        type=int,
        default=DEFAULT_RATE_PER_MINUTE,
        help="Maximum board requests to start per minute.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help="Maximum concurrent board fetch workers.",
    )
    return parser.parse_args()


async def db_connect() -> psycopg.AsyncConnection:
    return await psycopg.AsyncConnection.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        autocommit=True,
    )


async def fetch_boards(conn: psycopg.AsyncConnection, mode: str) -> list[BoardRow]:
    if mode == "test":
        query = """
            SELECT board, ats
            FROM ats_board
            WHERE success = TRUE
            ORDER BY RANDOM()
            LIMIT 1
        """
        params: tuple[Any, ...] = ()
    elif mode == "full":
        query = """
            SELECT board, ats
            FROM ats_board
            WHERE success = TRUE
            ORDER BY ats, board
        """
        params = ()
    else:
        query = """
            SELECT board, ats
            FROM ats_board
            WHERE success = TRUE
              AND ats = %s
            ORDER BY board
        """
        params = (mode,)

    async with conn.cursor() as cur:
        await cur.execute(query, params)
        rows = await cur.fetchall()
    return [BoardRow(board=row[0], ats=row[1]) for row in rows]


def compute_board_hash(job_ids: list[str]) -> str:
    joined_ids = "\n".join(sorted(job_ids))
    digest = hashlib.sha256(joined_ids.encode("utf-8")).hexdigest()
    return digest[:BOARD_HASH_HEX_LENGTH]


def get_ats_module(ats: str) -> ModuleType:
    try:
        return ATS_MODULES[ats]
    except KeyError as exc:
        raise ValueError(f"Unsupported ATS: {ats}") from exc


async def fetch_board_payload(
    limiter: AsyncEvenRateLimiter,
    board: BoardRow,
) -> FetchResult:
    await limiter.acquire()
    return await asyncio.to_thread(fetch_board_payload_sync, board)


def fetch_board_payload_sync(board: BoardRow) -> FetchResult:
    module = get_ats_module(board.ats)
    started = time.monotonic()
    try:
        response = requests.get(
            module.build_board_url(board.board),
            headers=API_HEADERS,
            timeout=30,
        )
        request_status = response.status_code
        response.raise_for_status()
        return FetchResult(
            board=board,
            payload=response.json(),
            request_status=request_status,
            fetch_seconds=time.monotonic() - started,
        )
    except requests.HTTPError as exc:
        response = exc.response
        return FetchResult(
            board=board,
            payload=None,
            request_status=response.status_code if response is not None else 0,
            fetch_seconds=time.monotonic() - started,
            error=str(exc),
        )
    except (requests.RequestException, json.JSONDecodeError, ValueError) as exc:
        return FetchResult(
            board=board,
            payload=None,
            request_status=0,
            fetch_seconds=time.monotonic() - started,
            error=str(exc),
        )


async def fetch_worker(
    input_queue: asyncio.Queue[BoardRow | object],
    output_queue: asyncio.Queue[FetchResult],
    limiter: AsyncEvenRateLimiter,
) -> None:
    while True:
        item = await input_queue.get()
        try:
            if item is STOP_SENTINEL:
                return
            try:
                result = await fetch_board_payload(limiter, item)
            except Exception as exc:
                result = FetchResult(
                    board=item,
                    payload=None,
                    request_status=0,
                    fetch_seconds=0.0,
                    error=str(exc),
                )
            await output_queue.put(result)
        finally:
            input_queue.task_done()


async def get_snapshot(conn: psycopg.AsyncConnection, board: str) -> Optional[SnapshotRow]:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT snapshot_id, board_hash
            FROM board_snapshot
            WHERE board = %s
            """,
            (board,),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    return SnapshotRow(snapshot_id=row[0], board_hash=row[1])


async def upsert_snapshot(
    conn: psycopg.AsyncConnection,
    board: str,
    request_status: int,
    job_count: Optional[int],
    board_hash: Optional[str],
    company_name: Optional[str],
    united_states: Optional[bool],
) -> int:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO board_snapshot (
                board,
                fetched_at,
                request_status,
                job_count,
                board_hash,
                company_name,
                united_states
            )
            VALUES (%s, NOW(), %s, %s, %s, %s, %s)
            ON CONFLICT (board)
            DO UPDATE SET
                fetched_at = NOW(),
                request_status = EXCLUDED.request_status,
                job_count = COALESCE(EXCLUDED.job_count, board_snapshot.job_count),
                board_hash = COALESCE(EXCLUDED.board_hash, board_snapshot.board_hash),
                company_name = COALESCE(EXCLUDED.company_name, board_snapshot.company_name),
                united_states = COALESCE(EXCLUDED.united_states, board_snapshot.united_states)
            RETURNING snapshot_id
            """,
            (board, request_status, job_count, board_hash, company_name, united_states),
        )
        row = await cur.fetchone()
    if row is None:
        raise RuntimeError(f"Failed to upsert snapshot for board={board}")
    return row[0]


async def fetch_existing_jobs(
    conn: psycopg.AsyncConnection,
    board: str,
) -> dict[str, ExistingJobRow]:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT
                job_id,
                ats_job_id,
                updated_at,
                company_name,
                title,
                location,
                url,
                description,
                min_compensation,
                max_compensation,
                united_states
            FROM job
            WHERE board = %s
            """,
            (board,),
        )
        rows = await cur.fetchall()
    return {
        row[1]: ExistingJobRow(
            job_id=row[0],
            updated_at=row[2],
            company_name=row[3],
            title=row[4],
            location=row[5],
            url=row[6],
            description=row[7],
            min_compensation=row[8],
            max_compensation=row[9],
            united_states=row[10],
        )
        for row in rows
    }


async def insert_job(
    conn: psycopg.AsyncConnection,
    snapshot_id: int,
    job: NormalizedJob,
) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO job (
                snapshot_id,
                board,
                ats,
                ats_job_id,
                company_name,
                title,
                location,
                url,
                description,
                min_compensation,
                max_compensation,
                united_states,
                embedded,
                first_fetched_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, NOW(), %s)
            """,
            (
                snapshot_id,
                job.board,
                job.ats,
                job.ats_job_id,
                job.company_name,
                job.title,
                job.location,
                job.url,
                job.description,
                job.min_compensation,
                job.max_compensation,
                job.united_states,
                job.updated_at,
            ),
        )


async def update_job(
    conn: psycopg.AsyncConnection,
    job_id: int,
    snapshot_id: int,
    job: NormalizedJob,
) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE job
            SET snapshot_id = %s,
                ats = %s,
                company_name = %s,
                title = %s,
                location = %s,
                url = %s,
                description = %s,
                min_compensation = %s,
                max_compensation = %s,
                united_states = %s,
                updated_at = %s
            WHERE job_id = %s
            """,
            (
                snapshot_id,
                job.ats,
                job.company_name,
                job.title,
                job.location,
                job.url,
                job.description,
                job.min_compensation,
                job.max_compensation,
                job.united_states,
                job.updated_at,
                job_id,
            ),
        )


async def upsert_jobs(
    conn: psycopg.AsyncConnection,
    snapshot_id: int,
    jobs: list[NormalizedJob],
) -> tuple[int, int, int]:
    if not jobs:
        return 0, 0, 0

    existing_jobs = await fetch_existing_jobs(conn, jobs[0].board)
    inserted = 0
    updated = 0
    skipped = 0

    for job in jobs:
        existing = existing_jobs.get(job.ats_job_id)
        if existing is None:
            await insert_job(conn, snapshot_id, job)
            inserted += 1
            continue

        if not job_changed(existing, job):
            skipped += 1
            continue

        await update_job(conn, existing.job_id, snapshot_id, job)
        updated += 1

    return inserted, updated, skipped


def job_changed(existing: ExistingJobRow, job: NormalizedJob) -> bool:
    return any(
        (
            existing.updated_at != job.updated_at,
            existing.company_name != job.company_name,
            existing.title != job.title,
            existing.location != job.location,
            existing.url != job.url,
            existing.description != job.description,
            existing.min_compensation != job.min_compensation,
            existing.max_compensation != job.max_compensation,
            existing.united_states != job.united_states,
        )
    )


async def delete_missing_jobs(
    conn: psycopg.AsyncConnection,
    board: str,
    response_job_ids: list[str],
) -> int:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            DELETE FROM job
            WHERE board = %s
              AND NOT (ats_job_id = ANY(%s))
            RETURNING job_id
            """,
            (board, response_job_ids),
        )
        rows = await cur.fetchall()
    return len(rows)


def normalize_payload(board: BoardRow, payload: Any) -> tuple[list[str], list[NormalizedJob], Optional[str], bool]:
    module = get_ats_module(board.ats)
    payload_jobs = module.extract_jobs(payload)
    job_ids = [module.extract_job_id(job) for job in payload_jobs]
    jobs = [module.normalize_job(board.board, job) for job in payload_jobs]
    company_name = module.extract_company_name(payload, jobs)
    united_states = any(job.united_states for job in jobs)
    return job_ids, jobs, company_name, united_states


async def process_fetch_result(
    conn: psycopg.AsyncConnection,
    result: FetchResult,
) -> BoardWriteSummary:
    summary = BoardWriteSummary()
    db_started = time.monotonic()

    async with conn.transaction():
        if result.payload is None:
            await upsert_snapshot(
                conn=conn,
                board=result.board.board,
                request_status=result.request_status,
                job_count=None,
                board_hash=None,
                company_name=None,
                united_states=None,
            )
            summary.error = result.error
            summary.db_seconds = time.monotonic() - db_started
            return summary

        normalize_started = time.monotonic()
        job_ids, jobs, company_name, united_states = normalize_payload(result.board, result.payload)
        summary.normalize_seconds = time.monotonic() - normalize_started
        summary.job_count = len(job_ids)
        board_hash = compute_board_hash(job_ids)

        snapshot = await get_snapshot(conn, result.board.board)
        if snapshot is not None and snapshot.board_hash == board_hash:
            await upsert_snapshot(
                conn=conn,
                board=result.board.board,
                request_status=result.request_status,
                job_count=len(job_ids),
                board_hash=board_hash,
                company_name=company_name,
                united_states=united_states,
            )
            summary.skipped = len(jobs)
            summary.db_seconds = time.monotonic() - db_started
            return summary

        summary.changed = True
        snapshot_id = await upsert_snapshot(
            conn=conn,
            board=result.board.board,
            request_status=result.request_status,
            job_count=len(job_ids),
            board_hash=board_hash,
            company_name=company_name,
            united_states=united_states,
        )
        summary.inserted, summary.updated, summary.skipped = await upsert_jobs(conn, snapshot_id, jobs)
        summary.deleted = await delete_missing_jobs(conn, result.board.board, job_ids)
        summary.db_seconds = time.monotonic() - db_started
        return summary


def resource_snapshot() -> tuple[float, float, int]:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime, usage.ru_stime, usage.ru_maxrss


def log_board_result(logger: Logger, result: FetchResult, summary: BoardWriteSummary) -> None:
    cpu_user, cpu_system, max_rss = resource_snapshot()
    status = "changed" if summary.changed else "unchanged"
    if result.payload is None or summary.error:
        status = "failed"
    logger.write(
        f"board={result.board.board} ats={result.board.ats} status={status} "
        f"request_status={result.request_status} jobs={summary.job_count} "
        f"inserted={summary.inserted} updated={summary.updated} skipped={summary.skipped} "
        f"deleted={summary.deleted} fetch_s={result.fetch_seconds:.3f} "
        f"normalize_s={summary.normalize_seconds:.3f} db_s={summary.db_seconds:.3f} "
        f"cpu_user_s={cpu_user:.3f} cpu_system_s={cpu_system:.3f} max_rss_kb={max_rss} "
        f"error={summary.error or result.error or ''}"
    )


async def run_collection(args: argparse.Namespace) -> Logger:
    if args.rate_per_minute <= 0:
        raise RuntimeError("--rate-per-minute must be greater than 0")
    if args.concurrency <= 0:
        raise RuntimeError("--concurrency must be greater than 0")

    logger = Logger()
    summary = RunSummary()
    logger.write(
        f"start mode={args.mode} rate_per_minute={args.rate_per_minute} "
        f"concurrency={args.concurrency}"
    )

    async with await db_connect() as conn:
        boards = await fetch_boards(conn, args.mode)
        summary.scheduled = len(boards)
        logger.write(f"selected_boards={len(boards)}")
        if not boards:
            logger.write("summary scheduled=0 completed=0")
            return logger

        input_queue: asyncio.Queue[BoardRow | object] = asyncio.Queue()
        output_queue: asyncio.Queue[FetchResult] = asyncio.Queue()
        for board in boards:
            await input_queue.put(board)
        worker_count = min(args.concurrency, len(boards))
        for _ in range(worker_count):
            await input_queue.put(STOP_SENTINEL)

        limiter = AsyncEvenRateLimiter(args.rate_per_minute)
        workers = [
            asyncio.create_task(fetch_worker(input_queue, output_queue, limiter))
            for _ in range(worker_count)
        ]
        try:
            while summary.completed < summary.scheduled:
                result = await output_queue.get()
                try:
                    if result.payload is None:
                        summary.request_failed += 1
                    try:
                        write_summary = await process_fetch_result(conn, result)
                    except Exception as exc:
                        write_summary = BoardWriteSummary(error=str(exc))
                        summary.write_failed += 1

                    if write_summary.changed:
                        summary.changed += 1
                    elif result.payload is not None and write_summary.error is None:
                        summary.unchanged += 1

                    summary.inserted += write_summary.inserted
                    summary.updated += write_summary.updated
                    summary.skipped += write_summary.skipped
                    summary.deleted += write_summary.deleted
                    summary.completed += 1
                    log_board_result(logger, result, write_summary)
                finally:
                    output_queue.task_done()
        except (KeyboardInterrupt, asyncio.CancelledError):
            summary.interrupted = True
            logger.write("shutdown requested; cancelling fetch workers")
            for worker in workers:
                worker.cancel()
        finally:
            await asyncio.gather(*workers, return_exceptions=True)

    elapsed = time.monotonic() - summary.started_at
    cpu_user, cpu_system, max_rss = resource_snapshot()
    logger.write(
        f"summary scheduled={summary.scheduled} completed={summary.completed} "
        f"changed={summary.changed} unchanged={summary.unchanged} "
        f"request_failed={summary.request_failed} write_failed={summary.write_failed} "
        f"inserted={summary.inserted} updated={summary.updated} skipped={summary.skipped} "
        f"deleted={summary.deleted} interrupted={summary.interrupted} elapsed_s={elapsed:.3f} "
        f"cpu_user_s={cpu_user:.3f} cpu_system_s={cpu_system:.3f} max_rss_kb={max_rss}"
    )
    return logger


def main() -> None:
    args = parse_args()
    logger: Optional[Logger] = None
    try:
        logger = asyncio.run(run_collection(args))
    except KeyboardInterrupt:
        if logger is not None:
            logger.write("interrupted by keyboard")
    except Exception as exc:
        if logger is not None:
            logger.write(f"fatal error={exc}")
        raise


if __name__ == "__main__":
    main()
