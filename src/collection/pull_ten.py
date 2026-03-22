import json
import os
from pathlib import Path
from urllib.parse import quote

import psycopg
import requests


GREENHOUSE_BOARD_API = "https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
GREENHOUSE_API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}
OUTPUT_DIR = Path(__file__).resolve().parent / "greenhouse_board_resp"


def db_connect() -> psycopg.Connection:
    return psycopg.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        autocommit=True,
    )


def fetch_random_tokens(conn: psycopg.Connection, limit: int = 10) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT token
            FROM board_token
            ORDER BY RANDOM()
            LIMIT %s
            """,
            (limit,),
        )
        # there needs to be that common and space for some reason after limit. Bugs otherwise
        
        rows = cur.fetchall()

    tokens = [row[0] for row in rows]
    if len(tokens) != limit:
        raise RuntimeError(f"Expected {limit} board tokens, found {len(tokens)}")
    return tokens


def fetch_board_jobs(session: requests.Session, token: str) -> tuple[str, dict]:
    api_url = GREENHOUSE_BOARD_API.format(token=quote(token, safe=""))
    response = session.get(api_url, headers=GREENHOUSE_API_HEADERS, timeout=30)
    response.raise_for_status()
    return api_url, response.json()


def write_board_response(token: str, api_url: str, payload: dict) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"{token}.md"
    output_path.write_text(
        f"## GET {api_url}\n{json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}\n",
        encoding="utf-8",
    )


def main() -> None:
    with db_connect() as conn, requests.Session() as session:
        for token in fetch_random_tokens(conn):
            api_url, payload = fetch_board_jobs(session, token)
            write_board_response(token, api_url, payload)
            print(f"Wrote {token}.md")


if __name__ == "__main__":
    main()
