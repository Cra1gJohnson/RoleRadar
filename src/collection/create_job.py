import os
import sys
from pathlib import Path

import psycopg

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env

load_shared_env()

ATS_ENUM_TYPE = "ats_board_ats"


def db_connect() -> psycopg.Connection:
    """Create a PostgreSQL connection using the shared env-based settings."""
    return psycopg.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )


def ensure_ats_enum(conn: psycopg.Connection) -> None:
    """Create the shared ATS enum type when it is not already present."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_type
                    WHERE typname = '{ATS_ENUM_TYPE}'
                ) THEN
                    CREATE TYPE {ATS_ENUM_TYPE} AS ENUM ('Green', 'Ashby', 'Lever');
                END IF;
            END
            $$;
            """
        )


def ensure_job_table(conn: psycopg.Connection) -> None:
    """Create the normalized ATS job table used by collection."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS job (
                job_id BIGSERIAL PRIMARY KEY,
                snapshot_id BIGINT,
                board TEXT,
                ats {ATS_ENUM_TYPE},
                ats_job_id TEXT NOT NULL,
                company_name TEXT,
                title TEXT,
                location TEXT,
                url TEXT,
                description TEXT,
                min_compensation INTEGER,
                max_compensation INTEGER,
                united_states BOOLEAN,
                embedded BOOLEAN DEFAULT FALSE,
                first_fetched_at TIMESTAMPTZ default NOW(),
                updated_at TIMESTAMPTZ
            )
            """
        )
        cur.execute(
            """
            ALTER TABLE job
            ALTER COLUMN ats_job_id TYPE TEXT
            USING ats_job_id::TEXT
            """
        )
        cur.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = 'fk_job_snapshot'
                      AND conrelid = 'job'::regclass
                ) THEN
                    ALTER TABLE job
                    ADD CONSTRAINT fk_job_snapshot
                    FOREIGN KEY (snapshot_id) REFERENCES board_snapshot(snapshot_id);
                END IF;
            END
            $$;
            """
        )
        cur.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = 'fk_job_board'
                      AND conrelid = 'job'::regclass
                ) THEN
                    ALTER TABLE job
                    ADD CONSTRAINT fk_job_board
                    FOREIGN KEY (board) REFERENCES ats_board(board);
                END IF;
            END
            $$;
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS job_board_ats_job_id_idx
            ON job (board, ats_job_id)
            """
        )


def main() -> None:
    with db_connect() as conn:
        ensure_ats_enum(conn)
        ensure_job_table(conn)

    print("Created job table.")


if __name__ == "__main__":
    main()
