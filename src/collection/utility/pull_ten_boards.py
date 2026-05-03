import json
import os
import sys
from pathlib import Path
from urllib.parse import quote

import argparse
import psycopg
import requests

SRC_ROOT = Path(__file__).resolve().parents[2]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env

load_shared_env()

GREENHOUSE_BOARD_API = "https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
ASHBY_BOARD_API = "https://api.ashbyhq.com/posting-api/job-board/{token}?includeCompensation=false"
LEVER_BOARD_API = "https://api.lever.co/v0/postings/{token}?mode=json"
API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}
OUTPUT_DIR = Path(__file__).resolve().parent.parent


def db_connect() -> psycopg.Connection:
    return psycopg.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        autocommit=True,
    )

def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for ats modes."""
    parser = argparse.ArgumentParser(
        description="Utility to pull 10 board json and give model context"
    )
    parser.add_argument(
        "mode",
        choices=('Green', 'Ashby', 'Lever'),
        type = str,
        help="pull 10 boards from which ats",
    )
    return parser.parse_args()


def fetch_random_tokens(conn: psycopg.Connection, ats: str, limit: int = 10) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT board
            FROM ats_board
            WHERE ats = %s
            ORDER BY RANDOM()
            LIMIT %s
            """,
            (ats,limit),
        )
        # there needs to be that common and space for some reason after limit. Bugs otherwise
        
        rows = cur.fetchall()

    tokens = [row[0] for row in rows]
    if len(tokens) != limit:
        raise RuntimeError(f"Expected {limit} board tokens, found {len(tokens)}")
    return tokens


def fetch_board_jobs(session: requests.Session, token: str, ats: str) -> tuple[str, dict]:
    match ats :
        case 'Green':
            api_url = GREENHOUSE_BOARD_API.format(token=quote(token, safe=""))
        case 'Ashby':
            api_url = ASHBY_BOARD_API.format(token=quote(token, safe=""))
        case 'Lever':
            api_url = LEVER_BOARD_API.format(token=quote(token, safe=""))
    response = session.get(api_url, headers=API_HEADERS, timeout=30)
    response.raise_for_status()
    return api_url, response.json()


def write_board_response(token: str, api_url: str, payload: dict, ats : str) -> None:
    output_dir = OUTPUT_DIR / f"{ats}_board_resp"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{token}_{ats}.json"
    output_path.write_text(
        json.dumps(
            {
                "request_url": api_url,
                "payload": payload,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def main() -> None:
    args = parse_args()
    with db_connect() as conn, requests.Session() as session:
        for token in fetch_random_tokens(conn, args.mode):
            api_url, payload = fetch_board_jobs(session, token, args.mode)
            write_board_response(token, api_url, payload, args.mode)
            print(f"Wrote {token}_{args.mode}.json")


if __name__ == "__main__":
    main()
