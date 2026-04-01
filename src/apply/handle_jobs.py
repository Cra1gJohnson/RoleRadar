import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

from order_jobs import ReadyApplyJob

ANSWERS_PATH = Path(__file__).resolve().parent / "answers.json"
STANDARD_FORM_SELECTOR = "form#application-form"
STANDARD_FALLBACK_FORM_SELECTOR = "form.application--form"


@dataclass(frozen=True)
class AnswerTarget:
    """A single answer from answers.json and the labels that should match it."""

    key: str
    answer: Any
    aliases: tuple[str, ...]


def normalize_text(value: Any) -> str:
    """Normalize text for label matching."""
    if not isinstance(value, str):
        return ""
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def normalize_aliases(values: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    """Normalize aliases and drop duplicates."""
    if not values:
        return ()

    aliases: list[str] = []
    for value in values:
        alias = normalize_text(value)
        if alias and alias not in aliases:
            aliases.append(alias)
    return tuple(aliases)


def answer_variants(answer: Any) -> list[str]:
    """Return ordered answer strings to try for one field."""
    if answer is None:
        return []

    if isinstance(answer, dict):
        variants: list[str] = []
        primary = answer.get("value")
        if isinstance(primary, str) and primary.strip():
            variants.append(primary.strip())

        extra_variants = answer.get("variants")
        if isinstance(extra_variants, list):
            for variant in extra_variants:
                if isinstance(variant, str) and variant.strip():
                    cleaned = variant.strip()
                    if cleaned not in variants:
                        variants.append(cleaned)

        if variants:
            return variants
        return [str(answer)]

    if isinstance(answer, list):
        variants: list[str] = []
        for variant in answer:
            if isinstance(variant, str) and variant.strip():
                cleaned = variant.strip()
                if cleaned not in variants:
                    variants.append(cleaned)
        return variants

    answer_text = str(answer).strip()
    return [answer_text] if answer_text else []


@lru_cache(maxsize=1)
def load_answers() -> dict[str, Any]:
    """Load the single source of truth used by the apply handler."""
    raw_text = ANSWERS_PATH.read_text(encoding="utf-8")
    payload = json.loads(raw_text)
    if not isinstance(payload, dict):
        return {}
    return payload


def load_alias_map(payload: dict[str, Any]) -> dict[str, tuple[str, ...]]:
    """Load normalized alias lists from answers.json."""
    aliases = payload.get("aliases", {})
    if not isinstance(aliases, dict):
        return {}

    alias_map: dict[str, tuple[str, ...]] = {}
    for key, values in aliases.items():
        normalized_key = normalize_text(key)
        if not normalized_key or not isinstance(values, list):
            continue
        normalized_values = normalize_aliases(tuple(value for value in values if isinstance(value, str)))
        if normalized_values:
            alias_map[normalized_key] = normalized_values
    return alias_map


def text_matches(left: str, right: str) -> bool:
    """Return True when two strings are close enough to treat as equivalent."""
    a = normalize_text(left)
    b = normalize_text(right)
    if not a or not b:
        return False
    return a == b or a in b or b in a


def control_option_values(control: Any) -> list[tuple[str, str]]:
    """Read visible option labels and values from a select control."""
    options: list[tuple[str, str]] = []
    try:
        option_locators = control.locator("option")
        count = option_locators.count()
    except Exception:
        return options

    for index in range(count):
        option = option_locators.nth(index)
        try:
            option_text = option.inner_text().strip()
        except Exception:
            option_text = ""
        try:
            option_value = option.get_attribute("value") or ""
        except Exception:
            option_value = ""

        if option_text or option_value:
            options.append((option_text, option_value))

    return options


def candidate_aliases_for_key(key: str, alias_map: dict[str, tuple[str, ...]]) -> tuple[str, ...]:
    """Build a small alias set for one answer key."""
    variants = [key]
    normalized_key = normalize_text(key)

    if "_" in key:
        variants.append(key.replace("_", " "))
    if " " in key:
        variants.append(key.replace(" ", "_"))
    if normalized_key:
        variants.append(normalized_key)

    variants.extend(alias_map.get(normalized_key, ()))
    return normalize_aliases(tuple(variants))


def build_targets(section_name: str, payload: dict[str, Any]) -> list[AnswerTarget]:
    """Turn a JSON section into fill targets."""
    section = payload.get(section_name, {})
    if not isinstance(section, dict):
        return []

    alias_map = load_alias_map(payload)
    targets: list[AnswerTarget] = []

    for key, answer in section.items():
        if answer is None:
            continue

        aliases = candidate_aliases_for_key(str(key), alias_map)
        if not aliases:
            continue

        targets.append(
            AnswerTarget(
                key=str(key),
                answer=answer,
                aliases=aliases,
            )
        )

    return targets


def control_value_kind(control: Any) -> str:
    """Classify the control type we are about to fill."""
    try:
        tag_name = control.evaluate("el => el.tagName.toLowerCase()")
    except Exception:
        tag_name = ""

    try:
        input_type = normalize_text(control.get_attribute("type"))
    except Exception:
        input_type = ""

    try:
        role = normalize_text(control.get_attribute("role"))
    except Exception:
        role = ""

    if tag_name == "textarea":
        return "textarea"
    if tag_name == "select":
        return "select"
    if input_type == "file":
        return "file"
    if role == "combobox" or control.get_attribute("aria-autocomplete"):
        return "combobox"
    if input_type in {"radio", "checkbox"}:
        return input_type
    return "text"


def get_control_from_label(form: Any, alias: str) -> Optional[Any]:
    """Locate a visible control by label text or label association."""
    normalized_alias = normalize_text(alias)
    if not normalized_alias:
        return None

    labels = form.locator("label")
    try:
        count = labels.count()
    except Exception:
        count = 0

    for index in range(count):
        label = labels.nth(index)
        try:
            label_text = normalize_text(label.inner_text())
        except Exception:
            continue

        if not label_text:
            continue
        if label_text == normalized_alias or normalized_alias in label_text or label_text in normalized_alias:
            try:
                control_id = label.get_attribute("for")
            except Exception:
                control_id = None

            if control_id:
                control = form.locator(f'[id="{control_id}"]')
                try:
                    if control.count():
                        return control.first
                except Exception:
                    pass

            try:
                nested = label.locator("input, textarea, select")
                if nested.count():
                    return nested.first
            except Exception:
                pass

    try:
        control = form.get_by_label(alias, exact=True)
        if control.count():
            return control.first
    except Exception:
        pass

    try:
        loose = form.get_by_label(alias, exact=False)
        if loose.count():
            return loose.first
    except Exception:
        pass

    return None


def select_option_from_control(control: Any, answer_values: list[str]) -> bool:
    """Select or type an answer into the located control."""
    kind = control_value_kind(control)
    page = control.page
    candidates = [value for value in answer_values if isinstance(value, str) and value.strip()]
    if not candidates:
        return False

    if kind == "select":
        option_values = control_option_values(control)
        for candidate in candidates:
            try:
                control.select_option(label=candidate)
                return True
            except Exception:
                pass

            try:
                control.select_option(value=candidate)
                return True
            except Exception:
                pass

            for option_label, option_value in option_values:
                if text_matches(candidate, option_label) or text_matches(candidate, option_value):
                    try:
                        if option_value:
                            control.select_option(value=option_value)
                        else:
                            control.select_option(label=option_label)
                        return True
                    except Exception:
                        continue

        return False

    if kind == "combobox":
        for candidate in candidates:
            try:
                control.click(timeout=1500)
            except Exception:
                pass

            try:
                control.fill(candidate, timeout=1500)
            except Exception:
                try:
                    control.press_sequentially(candidate, delay=25)
                except Exception:
                    pass

            candidate_locators = [
                page.get_by_role("option", name=candidate, exact=True),
                page.get_by_role("option", name=candidate, exact=False),
                page.get_by_text(candidate, exact=True),
                page.get_by_text(candidate, exact=False),
                page.locator("[role='option']").filter(has_text=candidate),
            ]
            for option in candidate_locators:
                try:
                    if option.count():
                        option.first.click(timeout=1500)
                        return True
                except Exception:
                    continue

            try:
                option_list = page.locator("[role='option']")
                option_count = option_list.count()
            except Exception:
                option_count = 0

            for index in range(option_count):
                option = option_list.nth(index)
                try:
                    option_text = option.inner_text().strip()
                except Exception:
                    option_text = ""
                if option_text and text_matches(candidate, option_text):
                    try:
                        option.click(timeout=1500)
                        return True
                    except Exception:
                        continue

            try:
                control.press("Enter")
                return True
            except Exception:
                continue

        return False

    if kind in {"radio", "checkbox"}:
        for candidate in candidates:
            try:
                control.check()
                return True
            except Exception:
                try:
                    page.get_by_label(candidate, exact=True).check()
                    return True
                except Exception:
                    continue
        return False

    if kind == "file":
        for candidate in candidates:
            path = Path(candidate).expanduser()
            if not path.exists():
                continue
            try:
                control.set_input_files(str(path))
                return True
            except Exception:
                continue
        return False

    for candidate in candidates:
        try:
            control.fill(candidate)
            return True
        except Exception:
            continue
    return False


def resolve_standard_form(page: Any) -> Any:
    """Scope standard Greenhouse handling to the application form itself."""
    candidates = [
        page.locator(STANDARD_FORM_SELECTOR).first,
        page.locator(STANDARD_FALLBACK_FORM_SELECTOR).first,
        page.locator("form").first,
    ]

    for candidate in candidates:
        try:
            if candidate.locator("input, textarea, select").count():
                return candidate
        except Exception:
            continue

    return page


def fill_targets(form: Any, targets: list[AnswerTarget]) -> tuple[int, list[str]]:
    """Fill a list of answer targets against the form."""
    filled = 0
    missing: list[str] = []

    for target in targets:
        candidate_values = answer_variants(target.answer)
        if not candidate_values:
            continue

        matched = False
        for alias in target.aliases:
            control = get_control_from_label(form, alias)
            if control is None:
                continue
            if select_option_from_control(control, candidate_values):
                filled += 1
                matched = True
                break

        if not matched:
            missing.append(target.key)

    return filled, missing


def handle_standard(page: Any, job: ReadyApplyJob) -> None:
    """Fill a standard Greenhouse application form from answers.json."""
    # load in answers from answers.json
    payload = load_answers()
    # resolve page to start at form, see STANDARD_FORM_SELECTOR
    form = resolve_standard_form(page)

    # this split is not as defined as it should be
    profile_targets = build_targets("profile", payload)
    question_targets = build_targets("questions", payload)

    # first fill with profile answers 
    profile_filled, profile_missing = fill_targets(form, profile_targets)
    
    
    question_filled, question_missing = fill_targets(form, question_targets)

    print(
        f"job_id={job.job_id} standard_greenhouse "
        f"profile_filled={profile_filled} profile_missing={len(profile_missing)} "
        f"question_filled={question_filled} question_missing={len(question_missing)}"
    )

    for key in profile_missing:
        print(f"job_id={job.job_id} missing profile answer: {key}")
    for key in question_missing:
        print(f"job_id={job.job_id} missing question answer: {key}")


def handle_standard_greenhouse_job(page: Any, job: ReadyApplyJob) -> None:
    """Backward-compatible entry point used by the browser router."""
    handle_standard(page, job)


def handle_nonstandard_job(page: Any, job: ReadyApplyJob) -> None:
    """Placeholder for nonstandard job-board handling."""
    print(f"job_id={job.job_id} route=nonstandard")
