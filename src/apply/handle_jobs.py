import argparse
import json
import re
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional
from playwright.sync_api import sync_playwright, TimeoutError, expect


# clean path to parent dir. joined with json file
ANSWERS_PATH = Path(__file__).resolve().parent / "answers.json"
# utility to make comon questions global
def load_common():
    with open(ANSWERS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

COMMON = load_common()        


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

def dismiss_cookie_banner(page_or_frame):
      """ Dismiss cookies at the opening of a url """
      buttons = [
          "Accept",
          "Accept all",
          "Agree",
          "I agree",
          "Allow all",
          "Got it",
      ]

      for name in buttons:
          btn = page_or_frame.get_by_role("button", name=name).first
          try:
              if btn.count() > 0:
                  btn.click(timeout=500)
                  return True
          except Exception:
              pass

      return False

def try_combobox(form, check: json) :
    
    for i in range(len(check["answers"])) :
        try:
            if form.get_by_role("combobox", name=check["question"]).count() == 0:
                raise Exception("no combo box")
            school = form.get_by_role("combobox", name=check["question"]).first
            school.wait_for(state="visible",timeout=1000)
            school.clear()
            school.wait_for(state="visible",timeout=1000)
            school.click()
            school.press_sequentially(check["answers"][i], delay=20)
            option = form.get_by_role("option",name=check["answers"][i]).first
            option.wait_for(state="visible", timeout=1000)
            option.click()

            
            return
        except Exception as e:
            if i == len(check["answers"]) - 1 :
                raise e



def try_textbox(form, check: json) :
    try:
        if form.get_by_role("textbox", name=check["question"]).count() == 0:
            raise Exception("no text box")
        question = form.get_by_role("textbox", name=check["question"]).first
        question.wait_for(state="visible", timeout=200)
        question.fill(check["answers"][0], timeout=200)
    except Exception as e:
        raise e

def try_flyout(form, check: json) :
    try:
        if form.get_by_text(check["question"]).count() == 0:
            return Exception("no flyout here")
        locator = form.get_by_text(check["question"]).first
        locator.fill(check["answers"][0], timeout=200)
        #question.wait_for(state="visible")
        
    except Exception as e:
        raise e



def try_question(form, check: json) -> None :
    try:
        if form.get_by_label(check["question"]).count() == 0 :
            return
        
        label = form.locator("label", has_text=check["question"]).first
        text = label.inner_text().strip()
        print(text)
        pattern = re.compile(
            r"^(Gender|Are you Hispanic/Latino\?|Please identify your race|Veteran Status|Disability Status)$"
        )
        
        if pattern.match(text):
            return
        
        try:
            try_textbox(form, check)
            print(f"{check["question"]} -- text-box used")
            return
        except Exception as e:
            pass
        try:
            try_combobox(form, check)
            print(f"{check["question"]} -- combo-box used")
            return
        except Exception as e:
            pass
        try:
            try_flyout(form, check)
            print(f"{check["question"]} -- flyout used")
            return
        except Exception as e:
            pass

    except Exception as e:
        print(f"bonked on get_by_text -- {check["question"]} -- {e} ")

def find_root(page):
    try:
        page.wait_for_load_state("load")
    except Exception:
        pass
    try:
        page.wait_for_function("document.readyState === 'complete'")
    except Exception:
        pass
    try:
        page.locator("iframe").first.wait_for(state="attached", timeout=10000)
    except Exception:
        pass

    def root_has_form(root) -> bool:
        try:
            if root.locator(STANDARD_FORM_SELECTOR).count() > 0:
                return True
            if root.locator(STANDARD_FALLBACK_FORM_SELECTOR).count() > 0:
                return True
            if root.get_by_label("First Name").count() > 0:
                return True
            if root.get_by_label("Resume").count() > 0:
                return True
        except Exception:
            return False
        return False

    def iframe_children(root):
        candidates = []
        try:
            iframes = root.locator("iframe")
            count = iframes.count()
        except Exception:
            return candidates

        for index in range(count):
            iframe = iframes.nth(index)
            try:
                box = iframe.bounding_box()
                area = 0 if box is None else box["width"] * box["height"]
            except Exception:
                area = 0
            candidates.append((area, iframe))

        candidates.sort(key=lambda item: item[0], reverse=True)
        return [iframe for _, iframe in candidates]

    def descend(root, depth: int = 0):
        if depth >= 6:
            return None

        for iframe in iframe_children(root):
            try:
                child_root = iframe.content_frame
            except Exception:
                child_root = None
            if child_root is None:
                continue
            if root_has_form(child_root):
                return iframe
            found = descend(child_root, depth + 1)
            if found is not None:
                return found

        return None

    return descend(page.main_frame) or page.locator("iframe").first

def handle_standard(job: JobsPackageItem, standard: bool) -> None:
    """Fill a standard Greenhouse application form. currently a placeholder"""
    
    with sync_playwright() as p :
        browser = p.chromium.connect_over_cdp(DEFAULT_CDP_ENDPOINT, slow_mo=75)
        context = browser.contexts[0]
        page = context.new_page()
        page.goto(job.url)
        page.set_default_timeout(2000)
        page.wait_for_load_state(state="load")
        
        # Establish root of the application
        if standard:
            form = page.locator("form")
        else:
            form = find_root(page).content_frame
        
        dismiss_cookie_banner(page)
        dismiss_cookie_banner(form)

        # try every question in common
        for check in COMMON["checks"] :
            try_question(form, check)            

        # Backup location flow
        try: 
            text = form.get_by_role("combobox", name="Location (City)").input_value()    
            if text != "" and form.locator("div", has_text="Locate me").count() > 0:
                locate = form.get_by_role("button", name="Locate me")
                locate.wait_for(state="visible")
                locate.click()
            else :
                pass
        except Exception as exce:
            print(f"no locate me button {exce}")
        
        # flyout for phone number
        try:
            form.get_by_role("group", name="Phone").get_by_label("Toggle flyout").click()
            form.get_by_role("option", name="United States +").click()
        except:
            print("country code fail")
        # Optional demographic questions
        try: 
            form.get_by_role("combobox", name="Gender", exact=True).click()
            form.get_by_role("option", name="Male", exact=True).click()
        except Exception as exce:
            print(f"no gender {exce}")
        # # hispanic
        try: 
            form.get_by_role("combobox", name= "Are you Hispanic/Latino?", exact=True).click()
            form.get_by_role("option", name="No").click()

            form.get_by_role("combobox", name="Please identify your race", exact=True).click()
            form.get_by_role("option", name="White", exact=True).click()
        except Exception as exce:
            print(f"no hispanic {exce}")
        # # veteran
        try: 
            form.get_by_role("combobox",name="Veteran Status", exact=True).click()
            form.get_by_role("option", name="I am not a protected veteran").click()
        except Exception as exce:
            print(f"no veteran {exce}")
        # # disability
        try: 
            form.get_by_role("combobox", name="Disability Status", exact=True).click()
            form.get_by_role("option", name="No, I do not have a").click()
        except Exception as exce:
            print(f"no disability {exce}")
        
        # fill llm responses
        response = json.loads(job.response)
        for resp in response["answers"] :
            try :
                pattern = re.compile(
                    r".*(linkedin|website|github).*"
                )
                question = resp["question label"]
                if pattern.match(question.lower()):
                    continue
                if resp["style"] == "Select" : 
                    box = form.get_by_role("combobox", name=resp["question label"])
                    box.wait_for(state="visible")
                    box.click()
                    box.press_sequentially(resp["answer label"], delay=30)
                    if resp["answer label"] != "" :
                        form.get_by_role("option", name=resp["answer label"], exact=True).click()
                elif resp["style"] == "Input":
                    box = form.get_by_label(resp["question label"]).first
                    box.wait_for(state="visible")
                    box.click()
                    box.press_sequentially(resp["answer label"], delay=10, timeout=10000)
                    try :
                        option = form.get_by_role("option", name=resp["answer label"])
                        option.wait_for(state="visible")
                        option.click()
                    except Exception as e:
                        print(f"no click on the Input {e}")
                else :
                    box = form.get_by_role("textbox", name=resp["question label"])
                    box.wait_for(state="visible")
                    box.click()
                    box.press_sequentially(resp["answer label"], delay=10, timeout=10000)
                
            except Exception as exec:
                print(f"yeah, ;-( it was {resp["question label"]} and {exec}")
        # upload files
        try:
            try:
                resume_input = form.get_by_role("group", name="Resume/CV").get_by_label("Attach")
                # cover_input = form.get_by_role("group", name="Cover Letter").get_by_label("Attach")
                secondary = False
            except Exception as e:
                secondary = True
            if secondary :
                try :

                    resume_section = form.locator("div", has_text="Resume").first
                    resume_input = resume_section.locator('input[type="file"]').first
                except Exception as e:
                    print("failed secondary resume")
            
            resume_input.set_input_files("/home/craig/Documents/AppMaterials/Craig_Johnson_Resume.pdf")
        except Exception as exec:
            print(f"resume boinked \n because {exec}")
        
        try:
            try:
                trans_input = form.get_by_role("group", name="transcript").get_by_label("Attach")
                # cover_input = form.get_by_role("group", name="Cover Letter").get_by_label("Attach")
                secondary = False
            except Exception as e:
                secondary = True
            if secondary :
                try :
                    trans_input = form.get_by_role("group", name="transcript").get_by_label("Upload")
                except Exception as e:
                    print("failed secondary transcript")
            
            trans_input.set_input_files("/home/craig/Documents/AppMaterials/Testudo - Unofficial Transcript.pdf")
        except Exception as exec:
            print(f"transcript boinked \n because {exec}")
        
        label = form.locator("label", has_text="First Name").first
        label.evaluate("(el) => el.scrollIntoView({ block: 'center', inline: 'nearest' })")
        #first_name.scroll_into_view_if_needed()

        browser.close()


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
    print(COMMON)
    
    for job in package.jobs :
        try:
            handle_standard(job, job.standard_job)
        except Exception as exc:
            print(f"job_id={job.job_id} handling failed: {exc}")

   
                
