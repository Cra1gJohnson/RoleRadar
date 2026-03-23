import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import psycopg


US_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "IA",
    "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS",
    "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA",
    "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY",
}
US_STATE_NAMES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "hawaii", "idaho", "illinois",
    "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine", "maryland",
    "massachusetts", "michigan", "minnesota", "mississippi", "missouri",
    "montana", "nebraska", "nevada", "new hampshire", "new jersey", "new mexico",
    "new york", "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode island", "south carolina", "south dakota", "tennessee",
    "texas", "utah", "vermont", "virginia", "washington", "west virginia",
    "wisconsin", "wyoming",
}
US_LOCATION_PHRASES = {
    "united states",
    "united states of america",
    "u.s.",
    "u.s",
    "usa",
    "us remote",
    "remote-us",
    "remote - u.s.",
    "remote - u.s",
    "remote - usa",
    "remote - united states",
    "remote, usa",
    "remote, united states",
    "anywhere in the united states",
    "field based - united states",
    "east coast",
    "new england",
    "midwest",
    "washington dc",
    "washington, dc",
    "washington, d.c.",
    "new york city",
    "new york metro area",
    "nyc",
}
AMBIGUOUS_NON_US_LOCATIONS = {"georgia"}


@dataclass
class JobRow:
    """Normalized Greenhouse job data ready for database writes."""

    snapshot_id: int
    token: str
    greenhouse_job_id: int
    company_name: Optional[str]
    title: Optional[str]
    location: Optional[str]
    url: Optional[str]
    description: Optional[str]
    updated_at: datetime


@dataclass
class ExistingJobRow:
    """Existing database row fields used for comparison and updates."""

    job_id: int
    updated_at: Optional[datetime]


@dataclass
class UpsertSummary:
    """Summary of job writes performed for one board payload."""

    inserted_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    filtered_count: int = 0
    failed_count: int = 0

    @property
    def success(self) -> bool:
        """Treat any per-row failure as a non-successful run."""
        return self.failed_count == 0


def log_info(message: str, verbose: bool) -> None:
    """Print informational messages only when verbose output is enabled."""
    if verbose:
        print(message)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for direct upsert execution."""
    parser = argparse.ArgumentParser(
        description="Normalize a Greenhouse board payload and upsert jobs."
    )
    parser.add_argument("token", help="Greenhouse board token")
    parser.add_argument("snapshot_id", type=int, help="Snapshot id from greenhouse_board_snapshot")
    parser.add_argument(
        "check_comp",
        help="Comparison mode flag: TRUE to compare existing jobs, FALSE to insert all",
    )

    payload_group = parser.add_mutually_exclusive_group(required=True)
    payload_group.add_argument(
        "--payload-file",
        help="Path to a JSON file containing a Greenhouse board payload",
    )
    payload_group.add_argument(
        "--payload-json",
        help="Raw JSON string containing a Greenhouse board payload",
    )

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


def parse_check_comp(raw_value: str) -> bool:
    """Convert common CLI boolean spellings into a strict boolean."""
    normalized = raw_value.strip().upper()
    if normalized == "TRUE":
        return True
    if normalized == "FALSE":
        return False
    raise ValueError("check_comp must be TRUE or FALSE")


def load_payload_from_args(args: argparse.Namespace) -> dict[str, Any]:
    """Load a payload from the selected CLI source and validate its top-level type."""
    if args.payload_file:
        payload_text = Path(args.payload_file).read_text(encoding="utf-8")
    else:
        payload_text = args.payload_json

    payload = json.loads(payload_text)
    if not isinstance(payload, dict):
        raise ValueError("Payload must be a JSON object")
    return payload


def validate_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Validate the top-level payload shape and return the jobs list."""
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        raise ValueError("Payload missing jobs list")
    return jobs


def parse_payload_timestamp(raw_value: Any, token: str, greenhouse_job_id: Any) -> datetime:
    """Parse a Greenhouse ISO 8601 timestamp into a timezone-aware datetime."""
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise ValueError(f"{token} job {greenhouse_job_id}: invalid updated_at value")

    try:
        parsed = datetime.fromisoformat(raw_value)
    except ValueError as exc:
        raise ValueError(
            f"{token} job {greenhouse_job_id}: could not parse updated_at '{raw_value}'"
        ) from exc

    if parsed.tzinfo is None:
        raise ValueError(f"{token} job {greenhouse_job_id}: updated_at must include timezone")
    return parsed


def extract_location_name(job: dict[str, Any]) -> Optional[str]:
    """Normalize the nested Greenhouse location field into a plain string."""
    location = job.get("location")
    if not isinstance(location, dict):
        return None

    location_name = location.get("name")
    if isinstance(location_name, str):
        stripped = location_name.strip()
        return stripped or None
    return None


def normalize_text(value: Any) -> Optional[str]:
    """Normalize optional string fields, converting blanks to None."""
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def is_united_states_location(location_name: Optional[str]) -> bool:
    """Best-effort classification for whether a Greenhouse location is in the U.S."""
    if not location_name:
        return False

    normalized = location_name.strip().lower()
    if not normalized:
        return False

    if normalized in US_LOCATION_PHRASES:
        return True
    if any(phrase in normalized for phrase in US_LOCATION_PHRASES):
        return True
    if normalized in AMBIGUOUS_NON_US_LOCATIONS:
        return False

    # Check common state-abbreviation formats like "Boston, MA" or "Palo Alto CA".
    code_tokens = set(re.findall(r"\b[A-Z]{2}\b", location_name.upper()))
    if code_tokens & US_STATE_CODES:
        return True

    # Match full state names only when they appear as whole phrases.
    for state_name in US_STATE_NAMES:
        if re.search(rf"\b{re.escape(state_name)}\b", normalized):
            return True

    return False


def normalize_job(
    job: dict[str, Any],
    token: str,
    snapshot_id: int,
) -> JobRow:
    """Validate and normalize one Greenhouse job from the board payload."""
    greenhouse_job_id = job.get("id")
    if not isinstance(greenhouse_job_id, int):
        raise ValueError(f"{token} job missing valid numeric id")

    updated_at = parse_payload_timestamp(job.get("updated_at"), token, greenhouse_job_id)

    return JobRow(
        snapshot_id=snapshot_id,
        token=token,
        greenhouse_job_id=greenhouse_job_id,
        company_name=normalize_text(job.get("company_name")),
        title=normalize_text(job.get("title")),
        location=extract_location_name(job),
        url=normalize_text(job.get("absolute_url")),
        description=None,
        updated_at=updated_at,
    )


def normalize_jobs(
    payload: dict[str, Any],
    token: str,
    snapshot_id: int,
) -> tuple[list[JobRow], int, int]:
    """
    Normalize all jobs in the payload and count per-row validation failures.

    Payload-level shape errors still raise immediately because they indicate the
    caller did not provide a valid Greenhouse board response.
    """
    jobs = validate_payload(payload)
    normalized_jobs: list[JobRow] = []
    filtered_count = 0
    failed_count = 0

    for job in jobs:
        if not isinstance(job, dict):
            print(f"{token}: encountered non-object job payload")
            failed_count += 1
            continue

        location_name = extract_location_name(job)
        if not is_united_states_location(location_name):
            filtered_count += 1
            continue

        try:
            normalized_jobs.append(normalize_job(job, token, snapshot_id))
        except ValueError as exc:
            print(str(exc))
            failed_count += 1

    return normalized_jobs, filtered_count, failed_count


def fetch_existing_jobs(
    conn: psycopg.Connection,
    token: str,
    greenhouse_job_ids: list[int],
) -> dict[int, ExistingJobRow]:
    """Load existing rows for a token keyed by indexed greenhouse_job_id."""
    if not greenhouse_job_ids:
        return {}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT job_id, greenhouse_job_id, updated_at
            FROM greenhouse_job
            WHERE token = %s
              AND greenhouse_job_id = ANY(%s)
            """,
            (token, greenhouse_job_ids),
        )
        rows = cur.fetchall()

    return {
        row[1]: ExistingJobRow(job_id=row[0], updated_at=row[2])
        for row in rows
    }


def insert_job(conn: psycopg.Connection, job: JobRow) -> None:
    """Insert a fully normalized Greenhouse job row."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO greenhouse_job (
                snapshot_id,
                token,
                greenhouse_job_id,
                company_name,
                title,
                location,
                url,
                description,
                first_fetched_at,
                last_changed_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s)
            """,
            (
                job.snapshot_id,
                job.token,
                job.greenhouse_job_id,
                job.company_name,
                job.title,
                job.location,
                job.url,
                job.description,
                job.updated_at,
            ),
        )


def update_job(conn: psycopg.Connection, existing_job_id: int, job: JobRow) -> None:
    """Update all mutable fields for an existing Greenhouse job row."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE greenhouse_job
            SET snapshot_id = %s,
                token = %s,
                greenhouse_job_id = %s,
                company_name = %s,
                title = %s,
                location = %s,
                url = %s,
                description = %s,
                last_changed_at = NOW(),
                updated_at = %s
            WHERE job_id = %s
            """,
            (
                job.snapshot_id,
                job.token,
                job.greenhouse_job_id,
                job.company_name,
                job.title,
                job.location,
                job.url,
                job.description,
                job.updated_at,
                existing_job_id,
            ),
        )


def process_board_payload(
    payload: dict[str, Any],
    token: str,
    snapshot_id: int,
    check_comp: bool,
    verbose: bool = True,
) -> UpsertSummary:
    """
    Normalize a board payload and write jobs into greenhouse_job.

    When check_comp is false, every normalized job is inserted. When check_comp is
    true, rows are compared by (token, greenhouse_job_id) and only changed rows are
    updated.
    """
    summary = UpsertSummary()

    try:
        normalized_jobs, filtered_count, failed_count = normalize_jobs(
            payload,
            token,
            snapshot_id,
        )
    except ValueError as exc:
        print(str(exc))
        summary.failed_count += 1
        return summary

    summary.filtered_count += filtered_count
    summary.failed_count += failed_count

    if not normalized_jobs:
        log_info(
            f"{token}: no valid jobs to process "
            f"(inserted=0 updated=0 skipped=0 filtered={summary.filtered_count} "
            f"failed={summary.failed_count})",
            verbose,
        )
        return summary

    with db_connect() as conn:
        existing_jobs: dict[int, ExistingJobRow] = {}
        if check_comp:
            # Preload the indexed comparison rows once so the per-job loop stays cheap.
            existing_jobs = fetch_existing_jobs(
                conn,
                token,
                [job.greenhouse_job_id for job in normalized_jobs],
            )

        for job in normalized_jobs:
            existing_job = existing_jobs.get(job.greenhouse_job_id) if check_comp else None

            if existing_job is not None and existing_job.updated_at == job.updated_at:
                summary.skipped_count += 1
                continue

            try:
                if existing_job is None:
                    insert_job(conn, job)
                    summary.inserted_count += 1
                else:
                    update_job(conn, existing_job.job_id, job)
                    summary.updated_count += 1
            except psycopg.Error as exc:
                print(
                    f"{token} job {job.greenhouse_job_id}: database write failed: {exc}"
                )
                summary.failed_count += 1

    log_info(
        f"{token}: inserted={summary.inserted_count} "
        f"updated={summary.updated_count} "
        f"skipped={summary.skipped_count} "
        f"filtered={summary.filtered_count} "
        f"failed={summary.failed_count}",
        verbose,
    )
    return summary


def main() -> None:
    """CLI entrypoint for direct execution and debugging."""
    args = parse_args()

    try:
        check_comp = parse_check_comp(args.check_comp)
        payload = load_payload_from_args(args)
        summary = process_board_payload(
            payload=payload,
            token=args.token.strip(),
            snapshot_id=args.snapshot_id,
            check_comp=check_comp,
            verbose=True,
        )
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(str(exc))
        raise SystemExit(1) from exc
    except psycopg.Error as exc:
        print(f"Database error while processing {args.token}: {exc}")
        raise SystemExit(1) from exc

    if not summary.success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
