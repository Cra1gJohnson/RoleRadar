from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Any, Optional

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

US_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "IA",
    "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS",
    "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA",
    "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY",
}
US_STATE_NAMES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "hawaii", "idaho", "illinois",
    "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine", "maryland",
    "massachusetts", "michigan", "minnesota", "mississippi", "missouri",
    "montana", "nebraska", "nevada", "new hampshire", "new jersey", "new mexico",
    "new york", "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode island", "south carolina", "south dakota", "tennessee",
    "texas", "utah", "vermont", "virginia", "washington", "west virginia",
    "wisconsin", "wyoming",
}
US_LOCATION_PHRASES = {
    "united states",
    "united states of america",
    "u.s.",
    "u.s",
    "usa",
    "us remote",
    "remote-us",
    "remote - u.s.",
    "remote - u.s",
    "remote - usa",
    "remote - united states",
    "remote, usa",
    "remote, united states",
    "anywhere in the united states",
    "field based - united states",
    "east coast",
    "new england",
    "midwest",
    "washington dc",
    "washington, dc",
    "washington, d.c.",
    "new york city",
    "new york metro area",
    "nyc",
}
AMBIGUOUS_NON_US_LOCATIONS = {"georgia"}


@dataclass(frozen=True)
class BoardRow:
    board: str
    ats: str


@dataclass(frozen=True)
class NormalizedJob:
    board: str
    ats: str
    ats_job_id: str
    company_name: Optional[str]
    title: Optional[str]
    location: Optional[str]
    url: Optional[str]
    description: Optional[str]
    min_compensation: Optional[int]
    max_compensation: Optional[int]
    united_states: bool
    updated_at: Optional[datetime]


def parse_iso_datetime(raw_value: Any) -> Optional[datetime]:
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None

    normalized = raw_value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def parse_epoch_millis(raw_value: Any) -> Optional[datetime]:
    if not isinstance(raw_value, int):
        return None
    return datetime.fromtimestamp(raw_value / 1000, tz=timezone.utc)


def text_or_none(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def us_location(location: Optional[str]) -> bool:
    if not location:
        return False

    normalized = location.strip().lower()
    if not normalized:
        return False

    if normalized in US_LOCATION_PHRASES:
        return True
    if any(phrase in normalized for phrase in US_LOCATION_PHRASES):
        return True
    if normalized in AMBIGUOUS_NON_US_LOCATIONS:
        return False

    code_tokens = set(re.findall(r"\b[A-Z]{2}\b", location.upper()))
    if code_tokens & US_STATE_CODES:
        return True

    for state_name in US_STATE_NAMES:
        if re.search(rf"\b{re.escape(state_name)}\b", normalized):
            return True

    return False


def first_text(*values: Any) -> Optional[str]:
    for value in values:
        normalized = text_or_none(value)
        if normalized is not None:
            return normalized
    return None


def numeric_compensation(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, float):
        return int(value) if value > 0 else None
    return None


def parse_salary_summary(summary: Any) -> tuple[Optional[int], Optional[int]]:
    text = text_or_none(summary)
    if text is None:
        return None, None

    matches = re.findall(r"(\$)?\s*(\d+(?:\.\d+)?)\s*([KkMm])?", text)
    values: list[int] = []
    for dollar_sign, raw_number, suffix in matches:
        if not dollar_sign and not suffix:
            continue
        number = float(raw_number)
        multiplier = 1
        if suffix.lower() == "k":
            multiplier = 1_000
        elif suffix.lower() == "m":
            multiplier = 1_000_000
        values.append(int(number * multiplier))

    if not values:
        return None, None
    if len(values) == 1:
        return values[0], values[0]
    return min(values[0], values[1]), max(values[0], values[1])
