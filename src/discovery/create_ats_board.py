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
    """Create the ATS enum type when it is not already present."""
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


def ensure_ats_board_table(conn: psycopg.Connection) -> None:
    """Create the ATS board table used by multi-ATS discovery."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS ats_board (
                board_id BIGSERIAL PRIMARY KEY,
                board TEXT NOT NULL,
                ats {ATS_ENUM_TYPE} NOT NULL,
                last_used TIMESTAMPTZ,
                success BOOLEAN
            )
            """
        )
        # index on board
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ats_board_board_idx
            ON ats_board (board)
            """
        )

def main() -> None:
    with db_connect() as conn:
        ensure_ats_enum(conn)
        ensure_ats_board_table(conn)

    print("Created ats_board table.")


if __name__ == "__main__":
    main()
