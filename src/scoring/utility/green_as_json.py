import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import psycopg

SRC_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = SRC_ROOT / "scoring" / "enrichment_display"

if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env

load_shared_env()


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for exporting scored job inputs as JSON."""
    parser = argparse.ArgumentParser(
        description=(
            "Join green_job and green_enrich and export scoring-ready job JSON."
        )
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum number of joined rows to export",
    )
    parser.add_argument(
        "--job-id",
        type=int,
        help="Optional single job_id to export",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where one JSON file per job will be written",
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


def parse_application_questions(raw_value: Any) -> Any:
    """Convert stored question JSON text into a JSON value when possible."""
    if raw_value is None:
        return []

    if isinstance(raw_value, (list, dict)):
        return raw_value

    if isinstance(raw_value, str):
        stripped = raw_value.strip()
        if not stripped:
            return []
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            return stripped

    return raw_value


def fetch_jobs(
    conn: psycopg.Connection,
    limit: int | None,
    job_id: int | None,
) -> list[dict[str, Any]]:
    """Load joined job and enrichment rows in a scoring-friendly JSON shape."""
    query = """
        SELECT
            ghj.job_id,
            ghj.company_name,
            ghj.title,
            ghj.location,
            gje.description,
            COALESCE(gje.min_salary, 0) AS min_salary,
            COALESCE(gje.max_salary, 0) AS max_salary,
            gje.application_questions
        FROM green_job AS gj
        JOIN green_enrich AS ge
          ON ge.job_id = gj.job_id
    """

    conditions: list[str] = []
    params: list[Any] = []

    if job_id is not None:
        conditions.append("gj.job_id = %s")
        params.append(job_id)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY gj.job_id"

    if limit is not None:
        query += " LIMIT %s"
        params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()

    return [
        {
            "job_id": row[0],
            "company_name": row[1],
            "title": row[2],
            "location": row[3],
            "description": row[4],
            "min_salary": row[5],
            "max_salary": row[6],
            "application_questions": parse_application_questions(row[7]),
        }
        for row in rows
    ]


def write_output_files(payload: list[dict[str, Any]], output_dir: str) -> None:
    """Write one JSON file per job into the selected output directory."""
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    for job in payload:
        job_id = job["job_id"]
        output_path = target_dir / f"{job_id}.json"
        serialized = json.dumps(job, ensure_ascii=False, indent=2)
        output_path.write_text(serialized + "\n", encoding="utf-8")
        print(f"Wrote {output_path.name}")


def main() -> None:
    """CLI entrypoint for exporting joined Greenhouse job data as JSON."""
    args = parse_args()
    if args.limit is not None and args.limit <= 0:
        print("limit must be greater than 0")
        raise SystemExit(1)

    try:
        with db_connect() as conn:
            payload = fetch_jobs(conn, limit=args.limit, job_id=args.job_id)
        write_output_files(payload, args.output_dir)
    except (psycopg.Error, OSError, ValueError) as exc:
        print(str(exc))
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
