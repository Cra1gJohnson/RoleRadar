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
    )


def ensure_you_search_table(conn: psycopg.Connection) -> None:
    """Create the You.com search ledger table."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS you_search (
                search_id BIGSERIAL PRIMARY KEY,
                search TEXT NOT NULL UNIQUE,
                results_num INTEGER,
                last_used TIMESTAMPTZ DEFAULT NOW(),
                success BOOLEAN DEFAULT TRUE,
                tokens INTEGER
            )
            """
        )


def main() -> None:
    with db_connect() as conn:
        ensure_you_search_table(conn)

    print("Created you_search table.")


if __name__ == "__main__":
    main()
