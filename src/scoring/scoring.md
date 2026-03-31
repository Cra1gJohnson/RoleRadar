# Scoring Directory Guide

## Purpose

- Define the intent of `src/scoring/`.
- Standardize LLM-based job scoring after enrichment is complete.
- Keep scoring logic separate from collection, assignment, and enrichment.

## Directory Scope

- `scoring.md`: operating notes, assumptions, and prompt-ready context.
- `score_job.py`: scoring worker that sends enriched jobs to Gemini and persists score results.
- `prompt1.md`: base scoring prompt template.
- `utility/export_greenhouse_jobs_json.py`: exports joined job + enrichment rows for local inspection and prompt iteration.
- `enrichment_display/`: sample exported JSON files for local inspection and prompt iteration.

## High-Level Workflow

1. Read jobs from `greenhouse_job` joined to `green_job_enrich`.
2. Select only rows where `greenhouse_job.enriched = TRUE` and `green_job_enrich.ranked IS NULL`.
3. Format each selected row into a lean scoring JSON payload.
4. Group jobs into batches of up to 20 rows.
5. Load `prompt1.md` and replace `{JOB JSON HERE}` with a `"jobs"` array containing the current batch.
6. Send one Gemini request per batch using `google-genai` and `GEMINI_API`.
7. Require JSON output from Gemini.
8. Parse the returned job-level subscores.
9. Compute `overall` from the four subscores.
10. Sort the scored batch before persistence.
11. Upsert each result into `green_job_rank`.
12. Mark `green_job_enrich.ranked = TRUE` after a successful rank write.

## Data Entities

- `greenhouse_job`
  - Source table for normalized jobs.
  - Relevant fields:
    - `job_id`
    - `company_name`
    - `title`
    - `location`
    - `enriched`
- `green_job_enrich`
  - Source table for normalized enrichment content.
  - Relevant fields:
    - `job_id`
    - `description`
    - `min_salary`
    - `max_salary`
    - `application_questions`
    - `ranked`
- `green_job_rank`
  - Output table for scoring results.
  - Relevant fields:
    - `job_id`
    - `job_fit`
    - `interview_chances`
    - `compensation`
    - `location`
    - `overall`
    - `ranked_at`
    - `prompt`
    - `model`
    - `response`
    - `applied`
  - `response` stores the full raw Gemini JSON response for the batch that produced the row.

## Operational Rules

- Gemini is called through `google-genai`, not a raw HTTP client.
- API key comes from `GEMINI_API`.
- The hardcoded model is `gemini-2.5-flash-lite`.
- Gemini request starts are evenly rate-limited to `12` per minute by default.
- Input tokens are billed at `$0.10 / 1,000,000`.
- Output tokens are billed at `$0.40 / 1,000,000`.
- `green_job_rank.prompt` stores the prompt file name, currently `prompt1.md`.
- `green_job_rank.response` stores the full raw JSON text returned by Gemini.
- Individual score fields are stored on a `0-100` scale.
- `overall` is computed from the weighted formula currently implemented in `score_job.py`.
- Scoring only marks `green_job_enrich.ranked = TRUE` after a successful DB write to `green_job_rank`.
- Failed API, parse, or database operations leave `green_job_enrich.ranked` unchanged so the job can be retried later.

## score_job.py Behavior

- Supports:
  - `test` mode with a default limit of `1`
  - `full` mode with a default limit of `10`
  - `--limit` to override either default
- Supports `--rate-per-minute` with a default of `12`.
- Selects deterministic jobs ordered by `job_id` and sends them in batches of up to `10`.
- The scoring payload is a reduced subset of the export utility shape and omits `application_questions`.
- Injects the selected batch payload into `prompt1.md`.
- Requires JSON-only model output.
- Validates that the returned scoring fields exist and are integer scores from `0` to `100`.
- Stores the full Gemini JSON response alongside the normalized score columns.
- Upserts each scored row and then marks the enrichment row as ranked.
- Tracks prompt, output, and total token counts when Gemini returns usage metadata.
- Prints concise per-job and final summaries.

## Prompt Context for Future Work

- Keep prompt edits in `prompt1.md` instead of inlining prompt text in code.
- Keep the JSON input shape for scoring small and explicit unless a prompt revision explicitly requires a schema change.
- Treat scoring as a downstream interpretation layer over already-enriched data, not as a replacement for enrichment.

## Revision Notes

- Keep this file updated as scoring prompts, models, or score persistence rules evolve.
