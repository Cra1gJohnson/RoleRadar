# Assignment Directory Guide

## Purpose

- Define the intent of `src/assignment/`.
- Standardize downstream candidate classification and enrichment preparation for Greenhouse jobs.
- Keep ranking-oriented and enrichment-oriented data outside the thin normalized collection tables.

## Directory Scope

- `assignment.md`: operating notes, assumptions, and future prompt context.
- `candidate_filter.py`: classifies unresolved Greenhouse jobs into candidate or non-candidate and initializes job enrichment rows.
- `job_enrichment.py`: job-level enrichment worker for Greenhouse detail pulls and normalized updates.
- `company_enrichment.py`: reserved for future company-level enrichment collection and persistence.
- `greenhouse_job_response/`: sampled individual Greenhouse job responses used as enrichment-context examples.
- `utility/pull_ten_jobs.py`: local sampling utility for pulling ten random candidate job-detail responses.
- `utility/error_request.py`: utility for backfilling 404 request results from `utility/errors.md`.

## High-Level Workflow

1. Read normalized Greenhouse jobs from PostgreSQL (`greenhouse_job`).
2. Select only rows where `candidate IS NULL`.
3. Apply the assignment regex filter using job title and company name.
4. Persist `candidate = TRUE` or `candidate = FALSE` back to `greenhouse_job`.
5. For rows classified `candidate = TRUE`, insert a matching `job_id` into `green_job_enrich` if no row exists yet.
6. Pull individual Greenhouse job detail responses for candidate jobs as needed.
7. As soon as a job-detail GET returns a checked HTTP response, mark `greenhouse_job.enriched = TRUE`.
8. If the response is `404`, record `green_job_enrich.request_status = 404`, leave the other enrichment fields blank, and let later downstream handling decide what to do with the gone job.
9. Use `green_job_enrich` as the job-level enrichment store for later ranking inputs.

## Data Entities

- `greenhouse_job`
  - Thin normalized job table produced by collection.
  - Important downstream fields:
    - `job_id` (primary key)
    - `company_name`
    - `title`
    - `candidate` (nullable classification flag)
    - `enriched` (downstream enrichment completion flag)
- `green_job_enrich`
  - Latest job-level enrichment store for candidate Greenhouse jobs.
  - Current columns:
    - `job_id` (primary key and foreign key to `greenhouse_job.job_id`)
    - `description`
    - `min_salary`
    - `max_salary`
    - `currency`
    - `internal_job_id`
    - `application_questions`
    - `request_status`
    - `enriched_at`
  - Uses `job_id` as the sole primary key in v1.
  - Does not use a separate `enrich_id`.
- Individual Greenhouse job response
  - Pull target for enrichment context:
    - `GET https://boards-api.greenhouse.io/v1/boards/{token}/jobs/{greenhouse_job_id}?pay_transparency=true&questions=true`
  - Response fields that matter for downstream enrichment include:
    - `content`
    - `pay_input_ranges`
    - `internal_job_id`
    - `absolute_url`
    - `company_name`
    - `title`
    - `location`
    - application-question-related response content when present

## Operational Rules

- Greenhouse is the only source in scope for assignment right now.
- The candidate filter operates incrementally by reading only `greenhouse_job` rows with `candidate IS NULL`.
- The regex logic lives in `candidate_filter.py`; a materialized view may exist for debugging or reference but is not the runtime source of truth.
- Candidate classification should favor recall over precision.
- `green_job_enrich` stores the latest job-level enrichment state, not enrichment history.
- Candidate processing must be idempotent:
  - already-classified jobs are skipped
  - existing `green_job_enrich` rows are not modified during candidate classification
- Company enrichment remains out of scope for now.
- Sample job-detail pulls are for local context gathering only and do not mutate the database.
- A checked `404` is treated as a terminal availability result for the current enrichment pass and is stored in `green_job_enrich.request_status`.

## candidate_filter.py Behavior

- Connects to PostgreSQL using the shared env loader pattern used elsewhere in the repo.
- Reads unresolved jobs from `greenhouse_job`.
- Applies the canonical title/company regex used for likely-job selection.
- Updates each unresolved row to `candidate = TRUE` or `candidate = FALSE`.
- Inserts `job_id` into `green_job_enrich` only for rows classified `TRUE`, and only when that `job_id` is not already present.
- Does not modify `enriched`.
- Prints concise run counts:
  - scanned unresolved jobs
  - marked candidate true
  - marked candidate false
  - enrichment rows inserted
  - enrichment rows skipped because they already existed
  - failures

## job_enrichment.py Behavior

- Reads work from `green_job_enrich` joined to `greenhouse_job`.
- Selects rows that are present in `green_job_enrich` and still have `greenhouse_job.enriched = FALSE`.
- Uses `token` and `greenhouse_job_id` to request the individual Greenhouse job endpoint with `pay_transparency=true` and `questions=true`.
- Runs with Python worker threads using an even rate limiter so request starts stay smooth rather than bursty.
- Normalizes the individual job response into:
  - `description`
  - `min_salary`
  - `max_salary`
  - `currency`
  - `internal_job_id`
  - `application_questions`
  - `enriched_at`
- Stores `description` as readable plain text derived from the Greenhouse `content` field.
- Uses `pay_input_ranges` as the primary salary source and falls back to best-effort salary parsing from the plain-text description when needed.
- Stores `application_questions` as normalized JSON built from relevant question-related sections in the payload.
- On successful `200` responses, updates `green_job_enrich`, writes `request_status = 200`, and marks `greenhouse_job.enriched = TRUE`.
- On checked `404` responses, writes `green_job_enrich.request_status = 404`, marks `greenhouse_job.enriched = TRUE`, and leaves the other enrichment fields blank.
- Leaves non-404 request failures eligible for retry by keeping `greenhouse_job.enriched = FALSE`.

## utility/pull_ten_jobs.py Behavior

- Pulls sample individual Greenhouse job responses for local enrichment context.
- Selects random jobs from `greenhouse_job` where `candidate = TRUE`.
- Uses `token` and `greenhouse_job_id` to request the individual Greenhouse job endpoint with `pay_transparency=true` and `questions=true`.
- Writes markdown files to `greenhouse_job_response/` using the filename pattern `<token>_<greenhouse_job_id>.md`.
- Uses the same markdown output style as the existing response examples: a `## GET ...` line followed by pretty-printed JSON.

## utility/error_request.py Behavior

- Reads `utility/errors.md` line by line.
- Parses lines in the format produced by `job_enrichment.py` for checked `404` request failures.
- Extracts `job_id` values from those lines.
- Updates `green_job_enrich.request_status = 404` for each parsed `job_id`.
- Marks the matching `greenhouse_job.enriched = TRUE` for each parsed `job_id`.
- Ignores non-error lines such as progress output.

## company_enrichment.py Behavior

- Reserved for later company-level enrichment implementation.
- No company-level behavior is defined in v1.

## Prompt Context for Future Work

- Use this directory as the source of truth for downstream Greenhouse assignment scope.
- Keep candidate classification separate from job enrichment and separate from later ranking.
- Preserve `greenhouse_job` as the thin normalized source table and store heavier downstream fields in enrichment-specific tables.

## Open Questions

- When should a previously classified job be reset to `candidate = NULL` for re-evaluation?
- Which additional downstream fields should eventually move into company-level enrichment rather than job-level enrichment?
- How should ranking combine job fit, interview probability, compensation, and application friction once enrichment data is available?

## Revision Notes

- Keep this file updated as assignment scripts and enrichment schemas evolve.
