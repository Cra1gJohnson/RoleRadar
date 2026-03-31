import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import psycopg

ASSIGNMENT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = Path(__file__).resolve().parents[2]
if str(ASSIGNMENT_ROOT) not in sys.path:
    sys.path.append(str(ASSIGNMENT_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env
from job_enrichment import mark_job_request_status

load_shared_env()

ERRORS_PATH = Path(__file__).resolve().parent / "errors.md"
JOB_ID_PATTERN = re.compile(r"\bjob_id\s*=\s*(\d+)\b")


@dataclass
class BackfillSummary:
    """Track the outcome of one 404 backfill run."""

    parsed_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    failed_count: int = 0

    @property
    def success(self) -> bool:
        """Treat any database failure as a non-successful run."""
        return self.failed_count == 0


def db_connect() -> psycopg.Connection:
    """Create a PostgreSQL connection using the shared env-based settings."""
    return psycopg.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        autocommit=True,
    )


def extract_job_id(raw_line: str) -> int | None:
    """Extract the assignment job_id from one logged 404 error line."""
    match = JOB_ID_PATTERN.search(raw_line)
    if match is None:
        return None
    return int(match.group(1))


def line_is_404_error(raw_line: str) -> bool:
    """Keep only lines that represent checked 404 request failures."""
    return "request failed:" in raw_line and "404" in raw_line


def job_exists(conn: psycopg.Connection, job_id: int) -> bool:
    """Check whether the target enrichment row exists before reporting an update."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM green_enrich
            WHERE job_id = %s
            """,
            (job_id,),
        )
        return cur.fetchone() is not None


def process_errors_file() -> BackfillSummary:
    """Backfill 404 request_status rows from the saved errors.md log."""
    summary = BackfillSummary()
    seen_job_ids: set[int] = set()

    with db_connect() as conn:
        for raw_line in ERRORS_PATH.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or not line_is_404_error(line):
                summary.skipped_count += 1
                continue

            job_id = extract_job_id(line)
            if job_id is None:
                summary.skipped_count += 1
                continue

            if job_id in seen_job_ids:
                summary.skipped_count += 1
                continue
            seen_job_ids.add(job_id)
            summary.parsed_count += 1

            try:
                if not job_exists(conn, job_id):
                    print(f"job_id={job_id} skipped: no green_enrich row found")
                    summary.skipped_count += 1
                    continue

                mark_job_request_status(
                    job_id=job_id,
                    request_status=404,
                    mark_enriched=True,
                )
                print(f"job_id={job_id} marked request_status=404")
                summary.updated_count += 1
            except psycopg.Error as exc:
                print(f"job_id={job_id} failed: database write failed: {exc}")
                summary.failed_count += 1

    return summary


def main() -> None:
    """CLI entrypoint for backfilling 404 request statuses from errors.md."""
    if not ERRORS_PATH.exists():
        print(f"Missing errors file: {ERRORS_PATH}")
        raise SystemExit(1)

    try:
        summary = process_errors_file()
    except psycopg.Error as exc:
        print(f"Database error in error_request: {exc}")
        raise SystemExit(1) from exc
    except OSError as exc:
        print(str(exc))
        raise SystemExit(1) from exc

    print(
        f"parsed={summary.parsed_count} updated={summary.updated_count} "
        f"skipped={summary.skipped_count} failed={summary.failed_count}"
    )
    if not summary.success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
