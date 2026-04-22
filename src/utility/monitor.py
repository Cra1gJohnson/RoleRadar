import os
import sys
from pathlib import Path

import psycopg

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env

load_shared_env()


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


def fetch_one_count(conn: psycopg.Connection, query: str) -> int:
    """Run a count query and return the first integer result."""
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()

    if row is None:
        return 0
    return int(row[0])


def print_line(label: str, **counts: int) -> None:
    """Print a single monitor line in a compact key=value format."""
    parts = [f"{key}={value}" for key, value in counts.items()]
    print(f"{label}: " + " ".join(parts))


def main() -> None:
    """CLI entrypoint for the pipeline monitor."""
    try:
        with db_connect() as conn:
            snapshot_count = fetch_one_count(
                conn,
                "SELECT COUNT(*) FROM greenhouse_board_snapshot",
            )
            green_job_count = fetch_one_count(
                conn,
                "SELECT COUNT(*) FROM green_job",
            )

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS total_rows,
                        COUNT(*) FILTER (
                            WHERE request_status IS DISTINCT FROM 404
                        ) AS not_404_rows
                    FROM green_enrich
                    """
                )
                green_enrich_total, green_enrich_not_404 = cur.fetchone()

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS total_rows,
                        COUNT(*) FILTER (WHERE overall > 60) AS above_60_rows
                    FROM green_score
                    """
                )
                green_score_total, green_score_above_60 = cur.fetchone()

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS total_rows,
                        COUNT(*) FILTER (WHERE submitted_at IS NOT NULL) AS submitted_rows,
                        COUNT(*) FILTER (
                            WHERE packaged_at IS NOT NULL
                              AND submitted_at IS NULL
                        ) AS packaged_not_submitted_rows
                    FROM green_apply
                    """
                )
                (
                    green_apply_total,
                    green_apply_submitted,
                    green_apply_packaged_not_submitted,
                ) = cur.fetchone()

            application_count = fetch_one_count(
                conn,
                "SELECT COUNT(*) FROM application",
            )
    except (OSError, psycopg.Error, ValueError) as exc:
        print(str(exc))
        raise SystemExit(1) from exc

    print_line("greenhouse_board_snapshot", total=int(snapshot_count))
    print_line("green_job", total=int(green_job_count))
    print_line(
        "green_enrich",
        total=int(green_enrich_total),
        not_404=int(green_enrich_not_404),
    )
    print_line(
        "green_score",
        total=int(green_score_total),
        above_60=int(green_score_above_60),
    )
    print_line(
        "green_apply",
        total=int(green_apply_total),
        submitted=int(green_apply_submitted),
        packaged_not_submitted=int(green_apply_packaged_not_submitted),
    )
    print_line("application", total=int(application_count))


if __name__ == "__main__":
    main()
