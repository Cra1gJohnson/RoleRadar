import argparse
import hashlib
import os
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import quote

import psycopg
import requests

from upsert_jobs import (
    UpsertSummary,
    extract_location_name,
    is_united_states_location,
    process_board_payload,
)

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
STATUS_WARM = "WARM"
STATUS_COLD = "COLD"


@dataclass
class SnapshotRow:
    """Fields needed from the current board snapshot row."""

    snapshot_id: int
    board_hash: Optional[str]
    status: Optional[str]


@dataclass
class BoardProcessResult:
    """Summarize the outcome of processing one board token."""

    token: str
    changed: bool
    failed: bool
    message: str
    inserted_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    filtered_count: int = 0


def log_info(message: str, verbose: bool) -> None:
    """Print informational messages only when verbose output is enabled."""
    if verbose:
        print(message)


def parse_args() -> argparse.Namespace:
    """Parse the required board token argument for CLI usage."""
    parser = argparse.ArgumentParser(
        description="Fetch a Greenhouse board, compute its hash, and update greenhouse_board_snapshot."
    )
    parser.add_argument("board_token", help="Greenhouse board token to process")
    return parser.parse_args()


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


def validate_board_token(conn: psycopg.Connection, token: str) -> None:
    """Ensure the token exists in board_token and is marked successful."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT success
            FROM board_token
            WHERE token = %s
            """,
            (token,),
        )
        row = cur.fetchone()

    if row is None:
        raise ValueError("board_token not found in database")
    if row[0] is not True:
        raise ValueError("board_token marked as unsuccessful")


def get_latest_snapshot(conn: psycopg.Connection, token: str) -> Optional[SnapshotRow]:
    """Fetch the most recent snapshot row for the token, if one exists."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT snapshot_id, board_hash, status
            FROM greenhouse_board_snapshot
            WHERE token = %s
            """,
            (token,),
        )
        row = cur.fetchone()

    if row is None:
        return None
    return SnapshotRow(snapshot_id=row[0], board_hash=row[1], status=row[2])


def build_board_api_url(token: str) -> str:
    """Construct the Greenhouse jobs API URL for a board token."""
    return GREENHOUSE_BOARD_API.format(token=quote(token, safe=""))


def fetch_board_payload(session: requests.Session, token: str) -> dict[str, Any]:
    """Fetch the board jobs payload and return the decoded JSON object."""
    response = session.get(
        build_board_api_url(token),
        headers=GREENHOUSE_API_HEADERS,
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Greenhouse API did not return a JSON object")
    return payload


def extract_job_count(payload: dict[str, Any]) -> int:
    """Read the returned job count, preferring metadata when present."""
    meta = payload.get("meta")
    if isinstance(meta, dict):
        total = meta.get("total")
        if isinstance(total, int):
            return total

    jobs = payload.get("jobs")
    if isinstance(jobs, list):
        return len(jobs)

    raise ValueError("Greenhouse payload missing jobs list")


def extract_sorted_job_ids(payload: dict[str, Any]) -> list[int]:
    """Collect, validate, and sort the numeric job IDs from the payload."""
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        raise ValueError("Greenhouse payload missing jobs list")

    job_ids: list[int] = []
    for job in jobs:
        if not isinstance(job, dict) or not isinstance(job.get("id"), int):
            raise ValueError("Greenhouse payload contains a job without a valid id")
        job_ids.append(job["id"])

    job_ids.sort()
    return job_ids


def compute_board_hash(job_ids: list[int]) -> str:
    """Hash the uninterrupted string of sorted job IDs and cap the hex digest."""
    joined_ids = "".join(str(job_id) for job_id in job_ids)
    digest = hashlib.sha256(joined_ids.encode("utf-8")).hexdigest()
    return digest[:BOARD_HASH_HEX_LENGTH]


def extract_company_name(payload: dict[str, Any], sorted_job_ids: list[int]) -> Optional[str]:
    """Use the company name from the job with the smallest job ID."""
    if not sorted_job_ids:
        return None

    smallest_job_id = sorted_job_ids[0]
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        return None

    for job in jobs:
        if isinstance(job, dict) and job.get("id") == smallest_job_id:
            company_name = job.get("company_name")
            if isinstance(company_name, str) and company_name.strip():
                return company_name.strip()
            return None

    return None


def resolve_snapshot_status(job_count: int, existing_status: Optional[str]) -> str:
    """Mark empty boards cold and otherwise preserve status when possible."""
    if job_count == 0:
        return STATUS_COLD
    if existing_status:
        return existing_status
    return STATUS_WARM


def has_united_states_job(payload: dict[str, Any]) -> bool:
    """Return true when at least one job on the board is located in the U.S."""
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        return False

    for job in jobs:
        if not isinstance(job, dict):
            continue
        if is_united_states_location(extract_location_name(job)):
            return True

    return False


def insert_snapshot(
    conn: psycopg.Connection,
    token: str,
    request_status: int,
    job_count: int,
    board_hash: str,
    company_name: Optional[str],
    status: str,
    united_states: bool,
) -> int:
    """Insert the first snapshot row for a token and return the snapshot id."""
    with conn.cursor() as cur:
        cur.execute(
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
                united_states,
            ),
        )
        row = cur.fetchone()

    if row is None:
        raise RuntimeError(f"Failed to insert snapshot for {token}")
    return row[0]


def update_snapshot(
    conn: psycopg.Connection,
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

    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE greenhouse_board_snapshot
            SET {", ".join(assignments)}
            WHERE snapshot_id = %s
            """,
            params,
        )


def record_request_failure(
    conn: psycopg.Connection,
    token: str,
    request_status: int,
) -> None:
    """Persist the latest request status on an existing snapshot after fetch failure."""
    snapshot = get_latest_snapshot(conn, token)
    if snapshot is None:
        return
    update_snapshot(conn, snapshot.snapshot_id, request_status=request_status)


def build_upsert_result(
    token: str,
    changed: bool,
    upsert_summary: UpsertSummary,
    success_message: str,
    verbose: bool,
) -> BoardProcessResult:
    """Convert the upsert outcome into the board-level result contract."""
    if upsert_summary.success:
        return BoardProcessResult(
            token=token,
            changed=changed,
            failed=False,
            message=success_message,
            inserted_count=upsert_summary.inserted_count,
            updated_count=upsert_summary.updated_count,
            skipped_count=upsert_summary.skipped_count,
            filtered_count=upsert_summary.filtered_count,
        )

    message = (
        f"{token}: snapshot updated but job upsert had failures "
        f"(failed={upsert_summary.failed_count})"
    )
    print(message)
    return BoardProcessResult(
        token=token,
        changed=changed,
        failed=True,
        message=message,
        inserted_count=upsert_summary.inserted_count,
        updated_count=upsert_summary.updated_count,
        skipped_count=upsert_summary.skipped_count,
        filtered_count=upsert_summary.filtered_count,
    )


def process_board_token(token: str, verbose: bool = True) -> BoardProcessResult:
    """
    Fetch a Greenhouse board, update its snapshot row, and run job normalization.

    The returned result tells callers whether the board changed and whether any part
    of the end-to-end board processing failed.
    """
    with db_connect() as conn, requests.Session() as session:
        validate_board_token(conn, token)

        try:
            payload = fetch_board_payload(session, token)
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else 0
            record_request_failure(conn, token, status_code)
            message = f"{token} failed: {status_code} {exc}"
            print(message)
            return BoardProcessResult(token=token, changed=False, failed=True, message=message)
        except requests.RequestException as exc:
            record_request_failure(conn, token, 0)
            message = f"{token} failed: 0 {exc}"
            print(message)
            return BoardProcessResult(token=token, changed=False, failed=True, message=message)

        job_count = extract_job_count(payload)
        sorted_job_ids = extract_sorted_job_ids(payload)
        board_hash = compute_board_hash(sorted_job_ids)
        company_name = extract_company_name(payload, sorted_job_ids)
        united_states = has_united_states_job(payload)

        existing_snapshot = get_latest_snapshot(conn, token)
        next_status = resolve_snapshot_status(
            job_count=job_count,
            existing_status=existing_snapshot.status if existing_snapshot else None,
        )

        if existing_snapshot is not None:
            if existing_snapshot.board_hash == board_hash:
                update_snapshot(
                    conn,
                    existing_snapshot.snapshot_id,
                    request_status=200,
                    status=next_status,
                    united_states=united_states,
                )
                message = f"{token}: no board change detected"
                log_info(message, verbose)
                return BoardProcessResult(token=token, changed=False, failed=False, message=message)

            update_snapshot(
                conn,
                existing_snapshot.snapshot_id,
                request_status=200,
                job_count=job_count,
                board_hash=board_hash,
                company_name=company_name,
                status=next_status,
                united_states=united_states,
            )
            message = f"{token}: board snapshot updated"
            log_info(message, verbose)

            upsert_summary = process_board_payload(
                payload=payload,
                token=token,
                snapshot_id=existing_snapshot.snapshot_id,
                check_comp=True,
                verbose=verbose,
            )
            return build_upsert_result(
                token=token,
                changed=True,
                upsert_summary=upsert_summary,
                success_message=message,
                verbose=verbose,
            )

        snapshot_id = insert_snapshot(
            conn,
            token=token,
            request_status=200,
            job_count=job_count,
            board_hash=board_hash,
            company_name=company_name,
            status=next_status,
            united_states=united_states,
        )
        message = f"{token}: board snapshot inserted"
        log_info(message, verbose)

        upsert_summary = process_board_payload(
            payload=payload,
            token=token,
            snapshot_id=snapshot_id,
            check_comp=False,
            verbose=verbose,
        )
        return build_upsert_result(
            token=token,
            changed=True,
            upsert_summary=upsert_summary,
            success_message=message,
            verbose=verbose,
        )


def main() -> None:
    """CLI entrypoint for direct execution and future controller usage."""
    args = parse_args()
    try:
        process_board_token(args.board_token.strip())
    except ValueError as exc:
        print(str(exc))
        raise SystemExit(1) from exc
    except psycopg.Error as exc:
        print(f"Database error while processing {args.board_token}: {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
