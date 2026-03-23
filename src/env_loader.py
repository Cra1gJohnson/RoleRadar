import os
from pathlib import Path


ENV_FILE_PATH = Path(__file__).resolve().parents[1] / ".env"


def load_shared_env() -> None:
    """Load .env settings without overriding variables already in the process env."""
    if not ENV_FILE_PATH.exists():
        return

    for raw_line in ENV_FILE_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue

        os.environ[key] = value.strip()
