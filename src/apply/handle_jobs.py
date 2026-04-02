import argparse
import json
import re
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional
from playwright.sync_api import sync_playwright, TimeoutError



ANSWERS_PATH = Path(__file__).resolve().parent / "answers.json"
STANDARD_FORM_SELECTOR = "form#application-form"
STANDARD_FALLBACK_FORM_SELECTOR = "form.application--form"
DEFAULT_CDP_ENDPOINT = "http://127.0.0.1:9222"

STANDARD_ALIAS_OVERRIDES: dict[str, tuple[str, ...]] = {
    "location": (
        "Candidate Location",
        "Location (City)",
        "Where are you located?",
        "Location",
    ),
    "country": ("Country", "Country Code"),
    "linkedin": (
        "LinkedIn",
        "LinkedIn Profile",
        "LinkedIn URL",
        "Linkedin",
        "Linkedin Profile",
        "Linkedin URL",
        "Linkedin Url",
    ),
    "website": ("Website", "Portfolio", "Personal Website"),
    "school": ("School", "Education"),
    "degree": ("Degree",),
    "discipline": ("Discipline", "Major", "Field of Study"),
    "gender": ("Gender",),
    "veteran_status": ("Veteran Status", "VeteranStatus"),
    "disability_status": ("Disability Status", "DisabilityStatus"),
    "hispanic": ("Are you Hispanic/Latino?", "Are you Hispanic?", "Hispanic"),
    "resume_path": ("Resume/CV", "Resume", "CV"),
    "cover_letter_path": ("Cover Letter",),
}

DEFAULT_RESUME_CANDIDATES = (
    Path("/home/craig/Documents/AppMaterials/Craig_Johnson_Resume.pdf"),
    Path("/home/craig/Downloads/Craig_Johnson_Gen_Resume.pdf"),
    Path("/home/craig/Downloads/Craig_Johnson_PMO_Resume.pdf"),
)

DEFAULT_COVER_LETTER_CANDIDATES = (
    Path("/home/craig/Documents/AppMaterials/Craig_Johnson_Cover_Letter_TREX.pdf"),
    Path("/home/craig/Documents/AppMaterials/Future_Standard_CoverLetter.pdf"),
    Path("/home/craig/Downloads/Columbia_Cover_Letter.pdf"),
    Path("/home/craig/Downloads/Intuitive_Cover_Letter.pdf"),
)


@dataclass(frozen=True)
class AnswerTarget:
    """A single answer and the labels that should match it."""

    key: str
    answer: Any
    aliases: tuple[str, ...]


@dataclass(frozen=True)
class JobsPackageItem:
    """One job entry emitted by `open_jobs.py`."""

    job_id: int
    url: str
    standard_job: bool
    response: Any


@dataclass(frozen=True)
class JobsPackage:
    """The full package passed from `open_jobs.py` to `handle_jobs.py`."""

    jobs: list[JobsPackageItem]


# @dataclass(frozen=True)
# class LLMAnswerTarget:
#     """One model-generated answer tied to a question label."""

#     question_label: str
#     answer_label: Any
#     style: str


# def normalize_text(value: Any) -> str:
#     """Normalize text for label matching."""
#     if not isinstance(value, str):
#         return ""
#     return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


# def normalize_aliases(values: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
#     """Normalize aliases and drop duplicates."""
#     if not values:
#         return ()

#     aliases: list[str] = []
#     for value in values:
#         alias = normalize_text(value)
#         if alias and alias not in aliases:
#             aliases.append(alias)
#     return tuple(aliases)


# def answer_variants(answer: Any) -> list[str]:
#     """Return ordered answer strings to try for one field."""
#     if answer is None:
#         return []

#     if isinstance(answer, dict):
#         variants: list[str] = []
#         primary = answer.get("value")
#         if isinstance(primary, str) and primary.strip():
#             variants.append(primary.strip())

#         extra_variants = answer.get("variants")
#         if isinstance(extra_variants, list):
#             for variant in extra_variants:
#                 if isinstance(variant, str) and variant.strip():
#                     cleaned = variant.strip()
#                     if cleaned not in variants:
#                         variants.append(cleaned)

#         if variants:
#             return variants
#         return [str(answer)]

#     if isinstance(answer, list):
#         variants: list[str] = []
#         for variant in answer:
#             if isinstance(variant, str) and variant.strip():
#                 cleaned = variant.strip()
#                 if cleaned not in variants:
#                     variants.append(cleaned)
#         return variants

#     answer_text = str(answer).strip()
#     return [answer_text] if answer_text else []


# def target_answer_variants(target: AnswerTarget) -> list[str]:
#     """Return answer variants with key-specific fallbacks."""
#     variants = answer_variants(target.answer)
#     normalized_key = normalize_text(target.key)

#     if normalized_key == "veteran status":
#         veteran_variant = "I am not a protected veteran"
#         if veteran_variant not in variants:
#             variants = [veteran_variant, *variants]

#     return variants


# @lru_cache(maxsize=1)
# def load_answers() -> dict[str, Any]:
#     """Load the candidate profile used for standard Greenhouse forms."""
#     raw_text = ANSWERS_PATH.read_text(encoding="utf-8")
#     payload = json.loads(raw_text)
#     if not isinstance(payload, dict):
#         return {}
#     return payload


# def load_alias_map(payload: dict[str, Any]) -> dict[str, tuple[str, ...]]:
#     """Load normalized alias lists from answers.json and built-in overrides."""
#     aliases = payload.get("aliases", {})
#     alias_map: dict[str, tuple[str, ...]] = {}

#     if isinstance(aliases, dict):
#         for key, values in aliases.items():
#             normalized_key = normalize_text(key)
#             if not normalized_key or not isinstance(values, list):
#                 continue
#             normalized_values = normalize_aliases(
#                 tuple(value for value in values if isinstance(value, str))
#             )
#             if normalized_values:
#                 alias_map[normalized_key] = normalized_values

#     for key, values in STANDARD_ALIAS_OVERRIDES.items():
#         normalized_key = normalize_text(key)
#         normalized_values = normalize_aliases(values)
#         if not normalized_key or not normalized_values:
#             continue
#         if normalized_key in alias_map:
#             merged = list(alias_map[normalized_key])
#             for value in normalized_values:
#                 if value not in merged:
#                     merged.append(value)
#             alias_map[normalized_key] = tuple(merged)
#         else:
#             alias_map[normalized_key] = normalized_values

#     return alias_map


# def text_matches(left: str, right: str) -> bool:
#     """Return True when two strings are close enough to treat as equivalent."""
#     a = normalize_text(left)
#     b = normalize_text(right)
#     if not a or not b:
#         return False
#     return a == b or a in b or b in a


# def control_option_values(control: Any) -> list[tuple[str, str]]:
#     """Read visible option labels and values from a select control."""
#     options: list[tuple[str, str]] = []
#     try:
#         option_locators = control.locator("option")
#         count = option_locators.count()
#     except Exception:
#         return options

#     for index in range(count):
#         option = option_locators.nth(index)
#         try:
#             option_text = option.inner_text().strip()
#         except Exception:
#             option_text = ""
#         try:
#             option_value = option.get_attribute("value") or ""
#         except Exception:
#             option_value = ""

#         if option_text or option_value:
#             options.append((option_text, option_value))

#     return options


# def candidate_aliases_for_key(key: str, alias_map: dict[str, tuple[str, ...]]) -> tuple[str, ...]:
#     """Build a small alias set for one answer key."""
#     variants = [key]
#     normalized_key = normalize_text(key)

#     if "_" in key:
#         variants.append(key.replace("_", " "))
#     if " " in key:
#         variants.append(key.replace(" ", "_"))
#     if normalized_key:
#         variants.append(normalized_key)

#     variants.extend(alias_map.get(normalized_key, ()))
#     return normalize_aliases(tuple(variants))


# def build_targets(section_name: str, payload: dict[str, Any]) -> list[AnswerTarget]:
#     """Turn one section of answers.json into fill targets."""
#     section = payload.get(section_name, {})
#     if not isinstance(section, dict):
#         return []

#     alias_map = load_alias_map(payload)
#     targets: list[AnswerTarget] = []

#     for key, answer in section.items():
#         if answer is None:
#             continue

#         aliases = candidate_aliases_for_key(str(key), alias_map)
#         if not aliases:
#             continue

#         targets.append(
#             AnswerTarget(
#                 key=str(key),
#                 answer=answer,
#                 aliases=aliases,
#             )
#         )

#     return targets


# def resolve_existing_file(candidates: list[Path]) -> Optional[str]:
#     """Return the first path that exists as a file."""
#     for candidate in candidates:
#         expanded = candidate.expanduser()
#         if expanded.is_file():
#             return str(expanded)
#         if expanded.is_dir():
#             for pattern in ("*.pdf", "*.doc", "*.docx", "*.txt", "*.rtf"):
#                 matches = sorted(expanded.glob(pattern))
#                 if matches:
#                     return str(matches[0])
#         if not expanded.suffix:
#             for suffix in (".pdf", ".doc", ".docx", ".txt", ".rtf"):
#                 with_suffix = expanded.with_suffix(suffix)
#                 if with_suffix.is_file():
#                     return str(with_suffix)
#     return None


# def build_profile_file_target(
#     key: str,
#     value: Any,
#     aliases: tuple[str, ...],
#     defaults: tuple[Path, ...],
# ) -> Optional[AnswerTarget]:
#     """Build a file-upload target from answers.json or hard-coded defaults."""
#     if isinstance(value, str) and value.strip():
#         raw_candidates = [Path(value.strip())]
#         resolved = resolve_existing_file(raw_candidates)
#         if resolved:
#             return AnswerTarget(key=key, answer=resolved, aliases=aliases)

#         if not Path(value.strip()).suffix:
#             fallback = resolve_existing_file([Path(value.strip()).with_suffix(".pdf")])
#             if fallback:
#                 return AnswerTarget(key=key, answer=fallback, aliases=aliases)

#     resolved_default = resolve_existing_file(list(defaults))
#     if resolved_default:
#         return AnswerTarget(key=key, answer=resolved_default, aliases=aliases)

#     return None


# def parse_optional_string(payload: dict[str, Any], keys: tuple[str, ...]) -> str:
#     """Return the first non-empty string value found for one of the keys."""
#     for key in keys:
#         value = payload.get(key)
#         if isinstance(value, str) and value.strip():
#             return value.strip()
#     return ""


# def parse_llm_answer_targets(
#     response_payload: Any,
#     expected_job_id: Optional[int] = None,
# ) -> list[LLMAnswerTarget]:
#     """Convert the raw AI response into fill targets."""
#     if isinstance(response_payload, str):
#         response_payload = json.loads(response_payload)

#     if not isinstance(response_payload, dict):
#         raise ValueError("LLM response must be a JSON object")

#     if expected_job_id is not None:
#         raw_job_id = response_payload.get("job_id")
#         if isinstance(raw_job_id, str) and raw_job_id.strip().isdigit():
#             raw_job_id = int(raw_job_id.strip())
#         if raw_job_id != expected_job_id:
#             raise ValueError(
#                 f"LLM response job_id {raw_job_id!r} does not match {expected_job_id}"
#             )

#     answers = response_payload.get("answers")
#     if not isinstance(answers, list):
#         raise ValueError("LLM response must include an answers array")

#     targets: list[LLMAnswerTarget] = []
#     for index, answer in enumerate(answers, start=1):
#         if not isinstance(answer, dict):
#             continue

#         question_label = parse_optional_string(
#             answer,
#             ("question label", "question_label", "question"),
#         )
#         answer_label = answer.get("answer label")
#         if answer_label is None:
#             answer_label = answer.get("answer_label")
#         if answer_label is None:
#             answer_label = answer.get("answer")

#         style = parse_optional_string(answer, ("style",))
#         if not question_label or answer_label is None:
#             continue

#         targets.append(
#             LLMAnswerTarget(
#                 question_label=question_label,
#                 answer_label=answer_label,
#                 style=style,
#             )
#         )

#     return targets


# def control_value_kind(control: Any) -> str:
#     """Classify the control type we are about to fill."""
#     try:
#         tag_name = control.evaluate("el => el.tagName.toLowerCase()")
#     except Exception:
#         tag_name = ""

#     try:
#         input_type = normalize_text(control.get_attribute("type"))
#     except Exception:
#         input_type = ""

#     try:
#         role = normalize_text(control.get_attribute("role"))
#     except Exception:
#         role = ""

#     if tag_name == "textarea":
#         return "textarea"
#     if tag_name == "select":
#         return "select"
#     if input_type == "file":
#         return "file"
#     if role == "combobox" or control.get_attribute("aria-autocomplete"):
#         return "combobox"
#     if input_type in {"radio", "checkbox"}:
#         return input_type
#     return "text"


# def get_control_from_label(form: Any, alias: str) -> Optional[Any]:
#     """Locate a visible control by label text or label association."""
#     normalized_alias = normalize_text(alias)
#     if not normalized_alias:
#         return None

#     labels = form.locator("label")
#     try:
#         count = labels.count()
#     except Exception:
#         count = 0

#     for index in range(count):
#         label = labels.nth(index)
#         try:
#             label_text = normalize_text(label.inner_text())
#         except Exception:
#             continue

#         if not label_text:
#             continue
#         if (
#             label_text == normalized_alias
#             or normalized_alias in label_text
#             or label_text in normalized_alias
#         ):
#             try:
#                 control_id = label.get_attribute("for")
#             except Exception:
#                 control_id = None

#             if control_id:
#                 control = form.locator(f'[id="{control_id}"]')
#                 try:
#                     if control.count():
#                         return control.first
#                 except Exception:
#                     pass

#             try:
#                 nested = label.locator("input, textarea, select")
#                 if nested.count():
#                     return nested.first
#             except Exception:
#                 pass

#     try:
#         control = form.get_by_label(alias, exact=True)
#         if control.count():
#             return control.first
#     except Exception:
#         pass

#     try:
#         loose = form.get_by_label(alias, exact=False)
#         if loose.count():
#             return loose.first
#     except Exception:
#         pass

#     return None


# def click_answer_fallback(form: Any, answer_values: list[str], style: str) -> bool:
#     """Try to select an answer by clicking visible label or option text."""
#     page = getattr(form, "page", form)
#     style_normalized = normalize_text(style)
#     candidates = [value for value in answer_values if isinstance(value, str) and value.strip()]
#     if not candidates:
#         return False

#     for candidate in candidates:
#         locators = [
#             page.get_by_label(candidate, exact=True),
#             page.get_by_label(candidate, exact=False),
#             page.get_by_role("option", name=candidate, exact=True),
#             page.get_by_role("option", name=candidate, exact=False),
#             page.get_by_text(candidate, exact=True),
#             page.get_by_text(candidate, exact=False),
#             page.locator("label").filter(has_text=candidate),
#         ]
#         for locator in locators:
#             try:
#                 if locator.count() == 0:
#                     continue
#             except Exception:
#                 continue

#             chosen = locator.first
#             try:
#                 if style_normalized in {"select", "radio", "checkbox"}:
#                     chosen.check()
#                 else:
#                     chosen.click()
#                 return True
#             except Exception:
#                 try:
#                     chosen.click()
#                     return True
#                 except Exception:
#                     continue

#     return False


# def select_option_from_control(control: Any, answer_values: list[str], style: str = "") -> bool:
#     """Select or type an answer into the located control."""
#     kind = control_value_kind(control)
#     page = control.page
#     candidates = [value for value in answer_values if isinstance(value, str) and value.strip()]
#     if not candidates:
#         return False

#     style_normalized = normalize_text(style)

#     if kind == "select":
#         option_values = control_option_values(control)
#         for candidate in candidates:
#             try:
#                 control.select_option(label=candidate)
#                 return True
#             except Exception:
#                 pass

#             try:
#                 control.select_option(value=candidate)
#                 return True
#             except Exception:
#                 pass

#             for option_label, option_value in option_values:
#                 if text_matches(candidate, option_label) or text_matches(candidate, option_value):
#                     try:
#                         if option_value:
#                             control.select_option(value=option_value)
#                         else:
#                             control.select_option(label=option_label)
#                         return True
#                     except Exception:
#                         continue

#         return False

#     if kind == "combobox":
#         for candidate in candidates:
#             try:
#                 control.click(timeout=1500)
#             except Exception:
#                 pass

#             try:
#                 control.fill(candidate, timeout=1500)
#             except Exception:
#                 try:
#                     control.press_sequentially(candidate, delay=25)
#                 except Exception:
#                     pass

#             candidate_locators = [
#                 page.get_by_role("option", name=candidate, exact=True),
#                 page.get_by_role("option", name=candidate, exact=False),
#                 page.get_by_text(candidate, exact=True),
#                 page.get_by_text(candidate, exact=False),
#                 page.locator("[role='option']").filter(has_text=candidate),
#             ]
#             for option in candidate_locators:
#                 try:
#                     if option.count():
#                         option.first.click(timeout=1500)
#                         return True
#                 except Exception:
#                     continue

#             try:
#                 option_list = page.locator("[role='option']")
#                 option_count = option_list.count()
#             except Exception:
#                 option_count = 0

#             for index in range(option_count):
#                 option = option_list.nth(index)
#                 try:
#                     option_text = option.inner_text().strip()
#                 except Exception:
#                     option_text = ""
#                 if option_text and text_matches(candidate, option_text):
#                     try:
#                         option.click(timeout=1500)
#                         return True
#                     except Exception:
#                         continue

#             try:
#                 control.press("Enter")
#                 return True
#             except Exception:
#                 continue

#         return False

#     if kind in {"radio", "checkbox"}:
#         for candidate in candidates:
#             candidate_locators = [
#                 page.get_by_label(candidate, exact=True),
#                 page.get_by_label(candidate, exact=False),
#                 page.get_by_role(kind, name=candidate, exact=True),
#                 page.get_by_role(kind, name=candidate, exact=False),
#                 page.get_by_text(candidate, exact=True),
#                 page.get_by_text(candidate, exact=False),
#             ]
#             for locator in candidate_locators:
#                 try:
#                     if locator.count() == 0:
#                         continue
#                 except Exception:
#                     continue

#                 chosen = locator.first
#                 try:
#                     chosen.check()
#                     return True
#                 except Exception:
#                     try:
#                         chosen.click()
#                         return True
#                     except Exception:
#                         continue

#         return False

#     if kind == "file":
#         for candidate in candidates:
#             path = Path(candidate).expanduser()
#             if not path.exists():
#                 continue
#             try:
#                 control.set_input_files(str(path))
#                 return True
#             except Exception:
#                 continue
#         return False

#     for candidate in candidates:
#         try:
#             control.fill(candidate)
#             if style_normalized != "textarea":
#                 try:
#                     control.press("Enter")
#                 except Exception:
#                     pass
#             return True
#         except Exception:
#             continue
#     return False


# def resolve_standard_form(page: Any) -> Any:
#     """Scope standard Greenhouse handling to the application form itself."""
#     candidates = [
#         page.locator(STANDARD_FORM_SELECTOR).first,
#         page.locator(STANDARD_FALLBACK_FORM_SELECTOR).first,
#         page.locator("form").first,
#     ]

#     for candidate in candidates:
#         try:
#             if candidate.locator("input, textarea, select").count():
#                 return candidate
#         except Exception:
#             continue

#     return page


# def fill_targets(form: Any, targets: list[AnswerTarget]) -> tuple[int, list[str]]:
#     """Fill a list of answer targets against the form."""
#     filled = 0
#     missing: list[str] = []

#     for target in targets:
#         candidate_values = target_answer_variants(target)
#         if not candidate_values:
#             continue

#         matched = False
#         for alias in target.aliases:
#             control = get_control_from_label(form, alias)
#             if control is None:
#                 continue
#             if select_option_from_control(control, candidate_values):
#                 filled += 1
#                 matched = True
#                 break
#             if click_answer_fallback(form, candidate_values, style=""):
#                 filled += 1
#                 matched = True
#                 break

#         if not matched:
#             missing.append(target.key)

#     return filled, missing


# def build_profile_targets(payload: dict[str, Any]) -> list[AnswerTarget]:
#     """Build all standard profile targets, including file uploads."""
#     profile = payload.get("profile", {})
#     if not isinstance(profile, dict):
#         return []

#     targets = [
#         target
#         for target in build_targets("profile", payload)
#         if target.key not in {"resume_path", "cover_letter_path"}
#     ]
#     alias_map = load_alias_map(payload)

#     resume_target = build_profile_file_target(
#         key="resume_path",
#         value=profile.get("resume_path"),
#         aliases=candidate_aliases_for_key("resume_path", alias_map),
#         defaults=DEFAULT_RESUME_CANDIDATES,
#     )
#     if resume_target is not None:
#         targets.append(resume_target)

#     cover_letter_target = build_profile_file_target(
#         key="cover_letter_path",
#         value=profile.get("cover_letter_path"),
#         aliases=candidate_aliases_for_key("cover_letter_path", alias_map),
#         defaults=DEFAULT_COVER_LETTER_CANDIDATES,
#     )
#     if cover_letter_target is not None:
#         targets.append(cover_letter_target)

#     return targets


# def build_profile_file_target(
#     key: str,
#     value: Any,
#     aliases: tuple[str, ...],
#     defaults: tuple[Path, ...],
# ) -> Optional[AnswerTarget]:
#     """Build a file-upload target from answers.json or hard-coded defaults."""
#     if isinstance(value, str) and value.strip():
#         raw_path = Path(value.strip()).expanduser()
#         resolved = resolve_existing_file([raw_path])
#         if resolved:
#             return AnswerTarget(key=key, answer=resolved, aliases=aliases)

#         if not raw_path.suffix:
#             resolved = resolve_existing_file([raw_path.with_suffix(".pdf")])
#             if resolved:
#                 return AnswerTarget(key=key, answer=resolved, aliases=aliases)

#     resolved_default = resolve_existing_file(list(defaults))
#     if resolved_default:
#         return AnswerTarget(key=key, answer=resolved_default, aliases=aliases)

#     return None


# def build_llm_targets(
#     response_payload: Any,
#     expected_job_id: Optional[int] = None,
# ) -> list[LLMAnswerTarget]:
#     """Convert the raw AI response into question/answer targets."""
#     return parse_llm_answer_targets(response_payload, expected_job_id=expected_job_id)


# def answer_llm_question(form: Any, target: LLMAnswerTarget) -> bool:
#     """Fill one model-generated answer into the form."""
#     candidate_values = answer_variants(target.answer_label)
#     if not candidate_values:
#         return False

#     question_aliases = normalize_aliases(
#         (
#             target.question_label,
#             target.question_label.replace("_", " "),
#         )
#     )

#     for alias in question_aliases:
#         control = get_control_from_label(form, alias)
#         if control is None:
#             continue
#         if select_option_from_control(control, candidate_values, style=target.style):
#             return True
#         if click_answer_fallback(form, candidate_values, style=target.style):
#             return True

#     if click_answer_fallback(form, candidate_values, style=target.style):
#         return True

#     return False

# def find_count(locator, value, name="field", timeout=2000) -> int:
#     try:
#         count = locator.count()
#         if count == 0:
#             print(f"Skip: {name} not on page") 
#         return count
#         # locator.first.wait_for(state="visible", timeout=timeout)
#         # locator.first.click()
#         # locator.first.fill(value)

#     except TimeoutError:
#         print(f"Skip: {name} found but not ready in time")
#         return -1

#     except Exception as e:
#         print(f"Skip: {name} failed: {e}")
#         return -1



def handle_standard(job: JobsPackageItem) -> None:
    """Fill a standard Greenhouse application form. currently a placeholder"""
    
    with sync_playwright() as p :
        browser = p.chromium.connect_over_cdp(DEFAULT_CDP_ENDPOINT, slow_mo=100)
        context = browser.contexts[0]
        page = context.new_page()

        page.goto(job.url)
    
        # first name
        page.get_by_role("textbox", name="First Name").fill("Craig")
        
        # last name
        page.get_by_role("textbox", name="Last Name").fill("Johnson")
        
        # email
        page.get_by_role("textbox", name="Email").fill("cjohns65@terpmail.umd.edu")

        # country code
        page.get_by_role("group", name="Phone").get_by_label("Toggle flyout").click()
        page.get_by_role("option", name="United States +").click()

        # phone number
        page.get_by_role("textbox", name="Phone").fill("4105705038")

        # location
        try: 
            if page.locator("div", has_text="Locate me").count() > 0:
                locate = page.get_by_role("button", name="Locate me")
                locate.click()
                page.set_default_timeout(2500)
                page.locator("body").click()
            else :
                pass
        except Exception as exce:
            print(f"no locate me button {exce}")

        # Linked In
        page.get_by_role("textbox", name="LinkedIn Profile").fill("https://www.linkedin.com/in/craig-p-johnson/")

        # Website
        page.get_by_role("textbox", name= "Website").fill("https://www.cpjserve.com")

        # Gender
        try: 
            if page.locator("div", has_text="GenderSelect...").count() > 0:
                page.get_by_text("GenderSelect...").click()
                page.get_by_role("option", name="Male", exact=True).click()
        except Exception as exce:
            print(f"no gender {exce}")
        # hispanic
        try: 
            if page.locator("div", has_text="Are you Hispanic/Latino?").count() > 0:            
                page.get_by_role("combobox", name= "Are you Hispanic/Latino?").click()
                page.get_by_role("option", name="Yes").click()
        except Exception as exce:
            print(f"no hispanic {exce}")
        # veteran
        try: 
            if page.locator("div", has_text="Veteran StatusSelect...").count() > 0:
                page.get_by_text("Veteran StatusSelect...").click()
                page.get_by_role("option", name="I am not a protected veteran").click()
        except Exception as exce:
            print(f"no gender {exce}")
        # disability
        try: 
            if page.locator("div", has_text="Disability StatusSelect...").count() > 0:
                page.get_by_text("Disability StatusSelect...").click()
                page.get_by_role("option", name="No, I do not have a").click()
        except Exception as exce:
            print(f"no gender {exce}")
        
        

        response = json.loads(job.response)
        for answer in response["answers"] :
            try :
                if answer["style"] == "Select" : 
                    page.get_by_role("combobox", name=answer["question label"]).click()
                else :
                    # this will probably fuck me later
                    pass
                # make sure there actually is an answer
                if answer["answer label"] != "" :
                    page.get_by_role("option", name=answer["answer label"]).click()
            except Exception as exec:
                print(f"yeah, ;-( it was {answer["question label"]} and {exec}")

        try:
            upload = page.locator('input[type="file"]').first

            upload.set_input_files("/home/craig/Documents/AppMaterials/Craig_Johnson_Resume.pdf")
        except Exception as exec:
            print(f"resume boinked \n because {exec}")
        
        browser.close()
        # pass
    pass

def handle_modified(page: Any, job: JobsPackageItem) -> None:
    """Placeholder for nonstandard job-board handling."""
    pass


def validate_job_item(item: Any, row_index: int) -> JobsPackageItem:
    """Validate one jobs-package row from open_jobs.py."""
    if not isinstance(item, dict):
        raise ValueError(f"Row {row_index} is not an object: {item!r}")

    job_id = item.get("job_id")
    url = item.get("url")
    standard_job = item.get("standard_job")
    response = item.get("response")

    if not isinstance(job_id, int) or job_id <= 0:
        raise ValueError(f"Row {row_index} has an invalid job_id: {job_id!r}")
    if not isinstance(url, str) or not url.strip():
        raise ValueError(f"Row {row_index} has an invalid url: {url!r}")
    if not isinstance(standard_job, bool):
        raise ValueError(f"Row {row_index} has an invalid standard_job flag: {standard_job!r}")

    return JobsPackageItem(
        job_id=job_id,
        url=url.strip(),
        standard_job=standard_job,
        response=response,
    )


def load_jobs_package(raw: str) -> JobsPackage:
    """Parse the JSON package taken in through receiver"""
    if not raw:
        raise ValueError("No package was supplied through reciever")

    payload = raw
    if not isinstance(payload, dict):
        raise ValueError("Jobs package must be a JSON object")

    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        raise ValueError("Jobs package must include a jobs array")

    clean_jobs: list[JobsPackageItem] = []
    for row_index, item in enumerate(jobs, start=1):
        clean_jobs.append(validate_job_item(item, row_index))

    return JobsPackage(clean_jobs=clean_jobs)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the browser handler."""
    parser = argparse.ArgumentParser(
        description="Open queued Greenhouse jobs in Chromium and fill application answers."
    )
    parser.add_argument(
        "--cdp-endpoint",
        default=DEFAULT_CDP_ENDPOINT,
        help="CDP endpoint exposed by the running Chrome instance",
    )
    return parser.parse_args()


def reciever(package: json) -> None:
    """ opens playwright and delegates jobs. """
    # read json into dataclass
    if not package.jobs:
        print("no jobs ready for application")
        return
    
    for job in package.jobs :
        try:
            if job.standard_job:
                handle_standard(job)
            else :
                handle_modified(job)
        except Exception as exc:
            print(f"job_id={job.job_id} handling failed: {exc}")

   
                
