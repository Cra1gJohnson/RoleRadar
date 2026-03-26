import argparse
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import psycopg


# this script is really slow. it pulls and evaulates line by line. then
# inserts line by line. 
# def should be optimized int the future. 
# not that important rn. but if this is moved to look for a different set of jobs
# then it needs to be optimized. 



SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env

load_shared_env()

TITLE_INCLUDE_PATTERN = re.compile(
    r"(junior|jr\.?|entry|entry[- ]?level|associate.*(engineer|developer|eng|dev)|"
    r"new grad|recent grad(uate)?|software.*(engineer|developer|consultant)|"
    r"backend.*engineer|frontend.*engineer|full\s*stack\s*engineer|"
    r"web.*(developer|engineer)|application\s*developer|solutions\s*engineer|"
    r"sales\s*engineer|technical.*consultant|implementation\s*engineer|"
    r"customer\s*engineer|integration\s*engineer|support\s*engineer|"
    r"product.*engineer|platform.*engineer)"
)
TITLE_EXCLUDE_PATTERN = re.compile(
    r"(senior|sr\.?|staff|principal|lead|manager|director|architect|"
    r"intern(ship)?|retail|part|lpn|nurse|rn|mental|auto body|attendant|"
    r"vehicle|auto|door|executive|estimator|civil|land|president|personal)"
)
COMPANY_EXCLUDE_PATTERN = re.compile(r"(speechify|hibu|nisc)")


@dataclass
class CandidateJobRow:
    """Minimal job fields needed for candidate classification."""

    job_id: int
    title: Optional[str]
    company_name: Optional[str]


@dataclass
class RunSummary:
    """Track the outcome of one candidate filter run."""

    scanned_count: int = 0
    candidate_true_count: int = 0
    candidate_false_count: int = 0
    enrichment_inserted_count: int = 0
    enrichment_skipped_count: int = 0
    failed_count: int = 0

    @property
    def success(self) -> bool:
        """Treat any write failure as a non-successful run."""
        return self.failed_count == 0


def parse_args() -> argparse.Namespace:
    """Parse optional CLI arguments for candidate filtering."""
    parser = argparse.ArgumentParser(
        description="Classify unresolved Greenhouse jobs for enrichment."
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional limit on how many unresolved jobs to process",
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


def fetch_unresolved_jobs(
    conn: psycopg.Connection,
    limit: Optional[int],
) -> list[CandidateJobRow]:
    """Load jobs whose candidate flag has not been classified yet."""
    query = """
        SELECT job_id, title, company_name
        FROM greenhouse_job
        WHERE candidate IS NULL
        ORDER BY job_id
    """
    params: tuple[object, ...] = ()
    if limit is not None:
        query += " LIMIT %s"
        params = (limit,)

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    return [
        CandidateJobRow(job_id=row[0], title=row[1], company_name=row[2])
        for row in rows
    ]


def normalize_match_text(value: Optional[str]) -> str:
    """Lowercase optional text for regex matching."""
    if not isinstance(value, str):
        return ""
    return value.strip().lower()


def classify_candidate(job: CandidateJobRow) -> bool:
    """Apply the canonical likely-job regex logic to one normalized job row."""
    title = normalize_match_text(job.title)
    company_name = normalize_match_text(job.company_name)

    if not title:
        return False
    if not TITLE_INCLUDE_PATTERN.search(title):
        return False
    if TITLE_EXCLUDE_PATTERN.search(title):
        return False
    if COMPANY_EXCLUDE_PATTERN.search(company_name):
        return False
    return True


def update_candidate_flag(
    conn: psycopg.Connection,
    job_id: int,
    candidate_value: bool,
) -> None:
    """Persist the candidate result for one unresolved greenhouse job."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE greenhouse_job
            SET candidate = %s
            WHERE job_id = %s
              AND candidate IS NULL
            """,
            (candidate_value, job_id),
        )


def ensure_job_enrichment_row(conn: psycopg.Connection, job_id: int) -> bool:
    """Insert a placeholder enrichment row when one does not already exist."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO green_job_enrich (job_id)
            VALUES (%s)
            ON CONFLICT (job_id) DO NOTHING
            """,
            (job_id,),
        )
        return cur.rowcount == 1


def process_candidates(limit: Optional[int] = None) -> RunSummary:
    """Classify unresolved jobs and initialize enrichment rows for candidates."""
    summary = RunSummary()

    with db_connect() as conn:
        jobs = fetch_unresolved_jobs(conn, limit)

        for job in jobs:
            summary.scanned_count += 1
            candidate_value = classify_candidate(job)

            try:
                update_candidate_flag(conn, job.job_id, candidate_value)
                if candidate_value:
                    summary.candidate_true_count += 1
                    inserted = ensure_job_enrichment_row(conn, job.job_id)
                    if inserted:
                        summary.enrichment_inserted_count += 1
                    else:
                        summary.enrichment_skipped_count += 1
                else:
                    summary.candidate_false_count += 1
            except psycopg.Error as exc:
                print(f"job_id={job.job_id} failed: database write failed: {exc}")
                summary.failed_count += 1

    return summary


def print_summary(summary: RunSummary) -> None:
    """Print a concise summary for the completed candidate run."""
    print(
        f"scanned={summary.scanned_count} "
        f"candidate_true={summary.candidate_true_count} "
        f"candidate_false={summary.candidate_false_count} "
        f"enrichment_inserted={summary.enrichment_inserted_count} "
        f"enrichment_skipped={summary.enrichment_skipped_count} "
        f"failed={summary.failed_count}"
    )


def main() -> None:
    """CLI entrypoint for Greenhouse candidate classification."""
    args = parse_args()
    if args.limit is not None and args.limit <= 0:
        print("limit must be greater than 0")
        raise SystemExit(1)

    try:
        summary = process_candidates(limit=args.limit)
    except psycopg.Error as exc:
        print(f"Database error in candidate_filter: {exc}")
        raise SystemExit(1) from exc

    print_summary(summary)
    if not summary.success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
