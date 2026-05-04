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


def ensure_board_snapshot_table(conn: psycopg.Connection) -> None:
    """Create the board snapshot table used by collection."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS board_snapshot (
                snapshot_id BIGSERIAL PRIMARY KEY,
                board TEXT,
                fetched_at TIMESTAMPTZ DEFAULT NOW(),
                request_status INTEGER,
                job_count INTEGER,
                board_hash TEXT,
                company_name TEXT,
                united_states BOOLEAN
            )
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS board_snapshot_board_idx
            ON board_snapshot (board)
            """
        )
        cur.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = 'fk_board_snapshot_board'
                      AND conrelid = 'board_snapshot'::regclass
                ) THEN
                    ALTER TABLE board_snapshot
                    ADD CONSTRAINT fk_board_snapshot_board
                    FOREIGN KEY (board) REFERENCES ats_board(board);
                END IF;
            END
            $$;
            """
        )


def main() -> None:
    with db_connect() as conn:
        ensure_board_snapshot_table(conn)

    print("Created board_snapshot table.")


if __name__ == "__main__":
    main()
