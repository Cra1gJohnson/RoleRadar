import argparse
import sys
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import psycopg

from board_hash import BoardProcessResult, db_connect, process_board_token

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env

load_shared_env()

DEFAULT_RATE_PER_MINUTE = 120
DEFAULT_MAX_WORKERS = 8
DEFAULT_FAILURE_THRESHOLD = 0
PROGRESS_PRINT_INTERVAL = 25


@dataclass
class RunSummary:
    """Track the aggregate outcome of a collection control run."""

    scheduled: int = 0
    completed: int = 0
    changed: int = 0
    no_change: int = 0
    failed: int = 0
    jobs_inserted: int = 0
    jobs_updated: int = 0
    jobs_skipped: int = 0
    jobs_filtered: int = 0


class RollingRateLimiter:
    """Enforce a maximum number of task starts within a rolling time window."""

    def __init__(self, max_calls: int, window_seconds: float) -> None:
        """Initialize the limiter with a rolling-window cap."""
        self.max_calls = max_calls
        self.window_seconds = window_seconds
        self.timestamps: deque[float] = deque()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        """Block until another request can start within the configured cap."""
        while True:
            with self.lock:
                now = time.monotonic()
                self._drop_expired(now)

                if len(self.timestamps) < self.max_calls:
                    self.timestamps.append(now)
                    return

                oldest_allowed = self.timestamps[0] + self.window_seconds
                sleep_seconds = max(oldest_allowed - now, 0.01)

            time.sleep(sleep_seconds)

    def _drop_expired(self, now: float) -> None:
        """Remove timestamps that have moved outside the rolling window."""
        while self.timestamps and now - self.timestamps[0] >= self.window_seconds:
            self.timestamps.popleft()


class EvenRateLimiter:
    """Spread task starts evenly across time instead of bursting within a minute."""

    def __init__(self, rate_per_minute: int) -> None:
        """Initialize the limiter with a fixed interval between dispatches."""
        self.dispatch_interval = 60.0 / rate_per_minute
        self.next_dispatch_at = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        """Wait until the next evenly spaced dispatch slot is available."""
        with self.lock:
            now = time.monotonic()
            sleep_seconds = self.next_dispatch_at - now
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
                now = time.monotonic()

            # If we fell behind, resume from now instead of bursting to catch up.
            self.next_dispatch_at = max(self.next_dispatch_at + self.dispatch_interval, now)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for controller modes."""
    parser = argparse.ArgumentParser(
        description="Top-level controller for Greenhouse board collection."
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
        help="Maximum number of boards to start per rolling 60 seconds",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help="Maximum number of concurrent worker threads",
    )
    parser.add_argument(
        "--failure-threshold",
        type=int,
        default=DEFAULT_FAILURE_THRESHOLD,
        help="Abort scheduling new boards after this many failures; 0 disables the stop",
    )
    return parser.parse_args()


def fetch_all_valid_tokens(conn: psycopg.Connection) -> list[str]:
    """Load every successful board token for a full collection run."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT token
            FROM board_token
            WHERE token IS NOT NULL
              AND success = TRUE
            ORDER BY token
            """
        )
        rows = cur.fetchall()

    return [row[0] for row in rows]


def fetch_random_valid_token(conn: psycopg.Connection) -> Optional[str]:
    """Load one random successful board token for test mode."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT token
            FROM board_token
            WHERE token IS NOT NULL
              AND success = TRUE
            ORDER BY RANDOM()
            LIMIT 1
            """
        )
        row = cur.fetchone()

    if row is None:
        return None
    return row[0]


def fetch_unseen_valid_tokens(conn: psycopg.Connection) -> list[str]:
    """Load successful board tokens that do not yet have a snapshot row."""
    with conn.cursor() as cur:
        cur.execute(
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
        rows = cur.fetchall()

    return [row[0] for row in rows]


def run_single_token(token: str, verbose: bool) -> BoardProcessResult:
    """Run one board check and convert unexpected exceptions into failures."""
    try:
        return process_board_token(token, verbose=verbose)
    except Exception as exc:
        message = f"{token} failed: unexpected controller error: {exc}"
        print(message)
        return BoardProcessResult(token=token, changed=False, failed=True, message=message)


def print_progress(summary: RunSummary, total_tokens: int) -> None:
    """Print a concise progress snapshot during a full run."""
    print(
        f"Progress: completed={summary.completed}/{total_tokens} "
        f"scheduled={summary.scheduled} changed={summary.changed} "
        f"no_change={summary.no_change} failed={summary.failed} "
        f"jobs_inserted={summary.jobs_inserted} jobs_updated={summary.jobs_updated} "
        f"jobs_skipped={summary.jobs_skipped} jobs_filtered={summary.jobs_filtered}"
    )


def consume_finished_futures(
    futures: dict[Future[BoardProcessResult], str],
    summary: RunSummary,
    total_tokens: int,
    force_progress: bool = False,
) -> None:
    """Collect completed worker results and update run counters."""
    done_futures = [future for future in futures if future.done()]

    for future in done_futures:
        token = futures.pop(future)
        try:
            result = future.result()
        except Exception as exc:
            print(f"{token} failed: unexpected future error: {exc}")
            summary.failed += 1
            summary.completed += 1
            continue

        summary.completed += 1
        summary.jobs_inserted += result.inserted_count
        summary.jobs_updated += result.updated_count
        summary.jobs_skipped += result.skipped_count
        summary.jobs_filtered += result.filtered_count
        if result.failed:
            summary.failed += 1
        elif result.changed:
            summary.changed += 1
        else:
            summary.no_change += 1

    if summary.completed and (
        force_progress or summary.completed % PROGRESS_PRINT_INTERVAL == 0
    ):
        print_progress(summary, total_tokens)


def run_test_mode() -> int:
    """Select one random token and run a single board check."""
    with db_connect() as conn:
        token = fetch_random_valid_token(conn)

    if token is None:
        print("No successful board_token rows found in database")
        return 1

    print(f"Mode=test token={token}")
    result = run_single_token(token, verbose=True)
    if result.failed:
        print("Test run failed")
        return 1

    if result.changed:
        print("Test run succeeded with board update")
    else:
        print("Test run succeeded with no board change")
    return 0


def run_full_scan(
    rate_per_minute: int,
    max_workers: int,
    failure_threshold: int,
) -> int:
    """Run a rate-limited concurrent scan across all valid board tokens."""
    with db_connect() as conn:
        tokens = fetch_all_valid_tokens(conn)

    if not tokens:
        print("No successful board_token rows found in database")
        return 1

    print(
        f"Mode=full tokens={len(tokens)} rate_per_minute={rate_per_minute} "
        f"max_workers={max_workers} "
        f"failure_threshold={'disabled' if failure_threshold == 0 else failure_threshold}"
    )

    rate_limiter = RollingRateLimiter(max_calls=rate_per_minute, window_seconds=60.0)
    summary = RunSummary()
    stop_scheduling = False
    next_token_index = 0
    futures: dict[Future[BoardProcessResult], str] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while next_token_index < len(tokens) or futures:
            while (
                not stop_scheduling
                and next_token_index < len(tokens)
                and len(futures) < max_workers
            ):
                if failure_threshold > 0 and summary.failed >= failure_threshold:
                    stop_scheduling = True
                    print(
                        f"Failure threshold reached ({summary.failed}); "
                        "stopping new scheduling"
                    )
                    break

                rate_limiter.acquire()
                token = tokens[next_token_index]
                future = executor.submit(run_single_token, token, False)
                futures[future] = token
                summary.scheduled += 1
                next_token_index += 1

            if not futures:
                break

            done, _ = wait(set(futures.keys()), timeout=0.5, return_when=FIRST_COMPLETED)
            if done:
                consume_finished_futures(futures, summary, total_tokens=len(tokens))

        consume_finished_futures(
            futures,
            summary,
            total_tokens=len(tokens),
            force_progress=True,
        )

    print(
        f"Final summary: scheduled={summary.scheduled} completed={summary.completed} "
        f"changed={summary.changed} no_change={summary.no_change} failed={summary.failed} "
        f"jobs_inserted={summary.jobs_inserted} jobs_updated={summary.jobs_updated} "
        f"jobs_skipped={summary.jobs_skipped} jobs_filtered={summary.jobs_filtered}"
    )

    if failure_threshold > 0 and summary.failed >= failure_threshold:
        print("Full run ended after reaching the failure threshold")
        return 1

    return 0


def run_unseen_scan(
    rate_per_minute: int,
    max_workers: int,
    failure_threshold: int,
) -> int:
    """Run a smoothly paced scan across tokens with no snapshot row yet."""
    with db_connect() as conn:
        tokens = fetch_unseen_valid_tokens(conn)

    if not tokens:
        print("No unseen successful board_token rows found in database")
        return 0

    print(
        f"Mode=new tokens={len(tokens)} rate_per_minute={rate_per_minute} "
        f"max_workers={max_workers} "
        f"failure_threshold={'disabled' if failure_threshold == 0 else failure_threshold}"
    )

    rate_limiter = EvenRateLimiter(rate_per_minute=rate_per_minute)
    summary = RunSummary()
    stop_scheduling = False
    next_token_index = 0
    futures: dict[Future[BoardProcessResult], str] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while next_token_index < len(tokens) or futures:
            while (
                not stop_scheduling
                and next_token_index < len(tokens)
                and len(futures) < max_workers
            ):
                if failure_threshold > 0 and summary.failed >= failure_threshold:
                    stop_scheduling = True
                    print(
                        f"Failure threshold reached ({summary.failed}); "
                        "stopping new scheduling"
                    )
                    break

                rate_limiter.acquire()
                token = tokens[next_token_index]
                future = executor.submit(run_single_token, token, False)
                futures[future] = token
                summary.scheduled += 1
                next_token_index += 1

            if not futures:
                break

            done, _ = wait(set(futures.keys()), timeout=0.5, return_when=FIRST_COMPLETED)
            if done:
                consume_finished_futures(futures, summary, total_tokens=len(tokens))

        consume_finished_futures(
            futures,
            summary,
            total_tokens=len(tokens),
            force_progress=True,
        )

    print(
        f"Final summary: scheduled={summary.scheduled} completed={summary.completed} "
        f"changed={summary.changed} no_change={summary.no_change} failed={summary.failed} "
        f"jobs_inserted={summary.jobs_inserted} jobs_updated={summary.jobs_updated} "
        f"jobs_skipped={summary.jobs_skipped} jobs_filtered={summary.jobs_filtered}"
    )

    if failure_threshold > 0 and summary.failed >= failure_threshold:
        print("Unseen run ended after reaching the failure threshold")
        return 1

    return 0


def main() -> None:
    """CLI entrypoint for collection control operations."""
    args = parse_args()

    if args.rate_per_minute <= 0:
        print("rate-per-minute must be greater than 0")
        raise SystemExit(1)
    if args.max_workers <= 0:
        print("max-workers must be greater than 0")
        raise SystemExit(1)
    if args.failure_threshold < 0:
        print("failure-threshold must be 0 or greater")
        raise SystemExit(1)

    try:
        if args.mode == "test":
            exit_code = run_test_mode()
        elif args.mode == "new":
            exit_code = run_unseen_scan(
                rate_per_minute=args.rate_per_minute,
                max_workers=args.max_workers,
                failure_threshold=args.failure_threshold,
            )
        else:
            exit_code = run_full_scan(
                rate_per_minute=args.rate_per_minute,
                max_workers=args.max_workers,
                failure_threshold=args.failure_threshold,
            )
    except psycopg.Error as exc:
        print(f"Database error in collection_control: {exc}")
        raise SystemExit(1) from exc

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
