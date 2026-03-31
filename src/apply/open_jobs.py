import argparse
import json
import os
import sys
import time
from functools import lru_cache
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import urlopen

import psycopg

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env_loader import load_shared_env
from scoring.utility.green_as_json import parse_application_questions

load_shared_env()

COMMON_QUESTIONS_PATH = Path(__file__).resolve().parent / "green_questions" / "common_questions.json"
DEFAULT_CDP_ENDPOINT = "http://127.0.0.1:9222"
DEFAULT_LIMIT = 1
DEFAULT_WAIT_SECONDS = 30
LOCATOR_TIMEOUT_MS = 1000
STANDARD_GREENHOUSE_DOMAINS = {"job-boards.greenhouse.io", "boards.greenhouse.io"}
TEXT_FIELD_TYPES = {"input_text", "textarea"}
SINGLE_SELECT_FIELD_TYPES = {"multi_value_single_select"}
MULTI_SELECT_FIELD_TYPES = {"multi_value_multi_select"}


