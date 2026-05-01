import os
import sys
from pathlib import Path

import psycopg

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env

load_shared_env()


NAMES_PATH = Path(__file__).with_name("discovery_names.txt")


def load_names(path: Path) -> list[str]:
    """Read discovery names from disk, skipping blank lines."""
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def ensure_keyword_table(conn: psycopg.Connection) -> None:
    """Create the keyword table used to track ATS discovery terms."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS keyword (
                name_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                green_boards INTEGER,
                ashby_boards INTEGER,
                lever_boards INTEGER,
                last_used TIMESTAMPTZ,
                success BOOLEAN
            )
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS keyword_name_lower_idx
            ON keyword (LOWER(name))
            """
        )


def persist_names(conn: psycopg.Connection, names: list[str]) -> None:
    """Insert each name once, leaving ATS count and usage fields blank."""
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO keyword (name)
            SELECT %s
            WHERE NOT EXISTS (
                SELECT 1
                FROM keyword
                WHERE LOWER(name) = LOWER(%s)
            )
            """,
            [(name, name) for name in names],
        )


def main() -> None:
    names = load_names(NAMES_PATH)

    with psycopg.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    ) as conn:
        ensure_keyword_table(conn)
        persist_names(conn, names)

    print(f"Processed {len(names)} names from {NAMES_PATH.name} into keyword.")


if __name__ == "__main__":
    main()
