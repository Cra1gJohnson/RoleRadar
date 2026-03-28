from __future__ import annotations

import psycopg


def ensure_green_apply_schema(conn: psycopg.Connection) -> None:
    """Create or evolve the green_apply queue table for application prep."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS green_apply (
                job_id bigint PRIMARY KEY
                    REFERENCES greenhouse_job(job_id)
                    ON DELETE CASCADE,
                questions boolean NOT NULL DEFAULT FALSE,
                response text
            )
            """
        )
        cur.execute(
            """
            ALTER TABLE green_apply
            ADD COLUMN IF NOT EXISTS questions boolean NOT NULL DEFAULT FALSE
            """
        )
        cur.execute(
            """
            ALTER TABLE green_apply
            ADD COLUMN IF NOT EXISTS response text
            """
        )
        cur.execute(
            """
            UPDATE green_apply
            SET questions = FALSE
            WHERE questions IS NULL
            """
        )
