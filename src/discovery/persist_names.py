import os
from pathlib import Path
import psycopg


# Path to the discovery names file (same directory as this script)
file_path = Path(__file__).with_name("discovery_names.txt")

# Read names (one per line), skipping blank lines
names = [line.strip() for line in file_path.read_text(encoding="utf-8").splitlines() if line.strip()]

# Connect to PostgreSQL and insert names into discovery_name
with psycopg.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
) as conn:
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO discovery_name (name)
            SELECT %s
            WHERE NOT EXISTS (
                SELECT 1
                FROM discovery_name
                WHERE LOWER(name) = LOWER(%s)
            )
            """,
            [(name, name) for name in names],
        )

print(f"Processed {len(names)} names from {file_path.name}.")
