import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_ROOT = REPO_ROOT / "templates"
COVER_DIR = TEMPLATE_ROOT / "cover"
RESUME_DIR = TEMPLATE_ROOT / "resume"
COVER_TEMPLATE_PATH = COVER_DIR / "CPJohnson_Cover.tex"
RESUME_TEX_PATH = RESUME_DIR / "CPJohnson_Resume.tex"
DEFAULT_RESUME_PATH = RESUME_DIR / "CPJohnson_resume.pdf"

COMPANY_PLACEHOLDER = "{\\textbf{Company Name}}"
TITLE_PLACEHOLDER = "Re: Job Title"
FIRST_PARAGRAPH_PLACEHOLDER = (
    "I am writing to express my interest in the Job Title position at Company Name. "
    "This opening paragraph should be customized for each application and should "
    "briefly connect why I am excited about this specific role."
)


def load_cover_template() -> str:
    """Return the reusable LaTeX cover letter template."""
    return COVER_TEMPLATE_PATH.read_text(encoding="utf-8")


def latex_escape(value: str) -> str:
    """Escape user/model text for safe insertion into LaTeX body text."""
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    return "".join(replacements.get(char, char) for char in value)


def normalize_cover_letter_payload(payload: dict[str, Any]) -> dict[str, str]:
    """Extract the model-generated cover letter fields from a response payload."""
    cover_letter = payload.get("cover_letter")
    if not isinstance(cover_letter, dict):
        raise ValueError("Gemini response must include a cover_letter object")

    first_paragraph = cover_letter.get("first_paragraph")
    if not isinstance(first_paragraph, str) or not first_paragraph.strip():
        raise ValueError("cover_letter.first_paragraph must be a non-empty string")

    company_name = cover_letter.get("company_name")
    job_title = cover_letter.get("job_title")
    return {
        "company_name": company_name.strip() if isinstance(company_name, str) else "",
        "job_title": job_title.strip() if isinstance(job_title, str) else "",
        "first_paragraph": first_paragraph.strip(),
    }


def apply_cover_letter_review(
    payload: dict[str, Any],
    reviewed_payload: dict[str, Any],
) -> dict[str, Any]:
    """Merge edited cover letter fields from the review document."""
    reviewed_cover = reviewed_payload.get("cover_letter")
    if not isinstance(reviewed_cover, dict):
        raise ValueError("Review payload must include a cover_letter object")

    first_paragraph = reviewed_cover.get("first_paragraph")
    if not isinstance(first_paragraph, str) or not first_paragraph.strip():
        raise ValueError("cover_letter.first_paragraph must remain non-empty")

    merged_payload = payload
    merged_cover = merged_payload.setdefault("cover_letter", {})
    if not isinstance(merged_cover, dict):
        raise ValueError("Original cover_letter must be a JSON object")

    for key in ("company_name", "job_title", "first_paragraph"):
        value = reviewed_cover.get(key)
        merged_cover[key] = value.strip() if isinstance(value, str) else ""

    return merged_payload


def render_cover_letter_tex(
    company_name: str,
    job_title: str,
    first_paragraph: str,
) -> str:
    """Render a per-job cover letter from the reusable template."""
    rendered = load_cover_template()
    rendered = rendered.replace(
        COMPANY_PLACEHOLDER,
        "{\\textbf{" + latex_escape(company_name or "Hiring Team") + "}}",
        1,
    )
    rendered = rendered.replace(
        TITLE_PLACEHOLDER,
        "Re: " + latex_escape(job_title or "Open Role"),
        1,
    )
    rendered = rendered.replace(
        FIRST_PARAGRAPH_PLACEHOLDER,
        latex_escape(first_paragraph),
        1,
    )
    return rendered


def run_pdflatex(tex_path: Path) -> None:
    """Compile a LaTeX file in its own directory."""
    subprocess.run(
        [
            "pdflatex",
            "-interaction=nonstopmode",
            "-halt-on-error",
            tex_path.name,
        ],
        cwd=tex_path.parent,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def cleanup_latex_build_files(directory: Path, stem: str) -> None:
    """Remove noisy LaTeX build artifacts for one generated file."""
    for suffix in (".aux", ".log", ".out"):
        (directory / f"{stem}{suffix}").unlink(missing_ok=True)


def filename_company_name(company_name: str) -> str:
    """Return a readable filesystem-safe company name for generated cover letters."""
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", company_name).strip("_")
    return normalized or "Hiring_Team"


def compile_cover_letter(
    company_name: str,
    job_title: str,
    first_paragraph: str,
) -> Path:
    """Write and compile the per-job cover letter PDF."""
    COVER_DIR.mkdir(parents=True, exist_ok=True)
    stem = f"CPJohnson_Cover_{filename_company_name(company_name)}"
    tex_path = COVER_DIR / f"{stem}.tex"
    pdf_path = COVER_DIR / f"{stem}.pdf"

    tex_path.write_text(
        render_cover_letter_tex(company_name, job_title, first_paragraph),
        encoding="utf-8",
    )
    try:
        run_pdflatex(tex_path)
    finally:
        cleanup_latex_build_files(COVER_DIR, stem)

    if not pdf_path.exists():
        raise RuntimeError(f"pdflatex did not create {pdf_path}")
    return pdf_path


def materialize_cover_letter_pdf(latex_source: str) -> Path:
    """Compile stored LaTeX into a temporary PDF for upload automation."""
    COVER_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".tex",
        prefix="CPJohnson_Cover_upload_",
        dir=COVER_DIR,
        delete=False,
    ) as tmp:
        tmp.write(latex_source)
        tex_path = Path(tmp.name)

    try:
        run_pdflatex(tex_path)
        pdf_path = tex_path.with_suffix(".pdf")
        if not pdf_path.exists():
            raise RuntimeError(f"pdflatex did not create {pdf_path}")
        return pdf_path
    finally:
        cleanup_latex_build_files(COVER_DIR, tex_path.stem)


def cleanup_materialized_cover_letter(pdf_path: Path) -> None:
    """Remove a temporary cover-letter PDF, source, and build artifacts."""
    tex_path = pdf_path.with_suffix(".tex")
    cleanup_latex_build_files(pdf_path.parent, pdf_path.stem)
    pdf_path.unlink(missing_ok=True)
    tex_path.unlink(missing_ok=True)


def resolve_stored_cover_letter_pdf_path(stored_value: str) -> Path | None:
    """Return the persisted cover-letter PDF path when the stored value is path-based."""
    candidate = stored_value.strip()
    if not candidate:
        return None

    path = Path(candidate).expanduser()
    if path.suffix.lower() == ".pdf":
        return path
    if path.suffix.lower() == ".tex":
        return path.with_suffix(".pdf")
    return None


def resolve_stored_cover_letter_tex_path(stored_value: str) -> Path | None:
    """Return the persisted cover-letter TeX path when the stored value is path-based."""
    pdf_path = resolve_stored_cover_letter_pdf_path(stored_value)
    if pdf_path is None:
        return None
    return pdf_path.with_suffix(".tex")


def resolve_cover_letter_upload_path(stored_value: str) -> tuple[Path, bool]:
    """Return a PDF path suitable for upload and whether it is temporary."""
    pdf_path = resolve_stored_cover_letter_pdf_path(stored_value)
    if pdf_path is not None and pdf_path.exists():
        return pdf_path, False
    return materialize_cover_letter_pdf(stored_value), True


def read_persisted_cover_letter_source(stored_value: str | None) -> str | None:
    """Return LaTeX source for a stored cover letter, supporting legacy inline content."""
    if not isinstance(stored_value, str) or not stored_value.strip():
        return None

    tex_path = resolve_stored_cover_letter_tex_path(stored_value)
    if tex_path is None:
        return stored_value
    if not tex_path.exists():
        raise RuntimeError(f"Stored cover letter source file is missing: {tex_path}")
    return tex_path.read_text(encoding="utf-8")


def cleanup_persisted_cover_letter_artifacts(stored_value: str | None) -> None:
    """Remove persisted cover-letter PDF and TeX files when the stored value is path-based."""
    if not isinstance(stored_value, str) or not stored_value.strip():
        return

    pdf_path = resolve_stored_cover_letter_pdf_path(stored_value)
    if pdf_path is None:
        return

    tex_path = pdf_path.with_suffix(".tex")
    cleanup_latex_build_files(pdf_path.parent, pdf_path.stem)
    pdf_path.unlink(missing_ok=True)
    tex_path.unlink(missing_ok=True)


def ensure_default_resume_pdf() -> Path:
    """Return the fixed resume PDF path, compiling it from LaTeX when needed."""
    if DEFAULT_RESUME_PATH.exists():
        return DEFAULT_RESUME_PATH

    source_pdf = RESUME_TEX_PATH.with_suffix(".pdf")
    if not source_pdf.exists():
        run_pdflatex(RESUME_TEX_PATH)
        cleanup_latex_build_files(RESUME_DIR, RESUME_TEX_PATH.stem)

    if source_pdf.exists() and source_pdf != DEFAULT_RESUME_PATH:
        shutil.copy2(source_pdf, DEFAULT_RESUME_PATH)

    if not DEFAULT_RESUME_PATH.exists():
        raise RuntimeError(f"Could not create default resume PDF at {DEFAULT_RESUME_PATH}")
    return DEFAULT_RESUME_PATH


def normalize_pdf_path(path: Path) -> str:
    """Return an absolute path string for storage in green_apply."""
    return str(path.resolve())
