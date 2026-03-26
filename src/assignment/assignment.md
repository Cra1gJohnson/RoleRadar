# Assignment Directory Guide

## Purpose
- Define the intent of `src/assignment/`.
- Standardize downstream candidate classification and enrichment preparation for Greenhouse jobs.
- Keep ranking-oriented and enrichment-oriented data outside the thin normalized collection tables.

## Directory Scope
- `assignment.md`: operating notes, assumptions, and future prompt context.
- `candidate_filter.py`: classifies unresolved Greenhouse jobs into candidate or non-candidate and initializes job enrichment rows.
- `job_enrichment.py`: reserved for future job-level enrichment collection and persistence.
- `company_enrichment.py`: reserved for future company-level enrichment collection and persistence.

## High-Level Workflow
1. Read normalized Greenhouse jobs from PostgreSQL (`greenhouse_job`).
2. Select only rows where `candidate IS NULL`.
3. Apply the assignment regex filter using job title and company name.
4. Persist `candidate = TRUE` or `candidate = FALSE` back to `greenhouse_job`.
5. For rows classified `candidate = TRUE`, insert a matching `job_id` into `green_job_enrich` if no row exists yet.
6. Leave `enriched = FALSE` until a later enrichment step populates job-level details.
7. Use `green_job_enrich` as the job-level enrichment store for later ranking inputs.

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
    - `enriched_at`
  - Uses `job_id` as the sole primary key in v1.
  - Does not use a separate `enrich_id`.

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
- Reserved for later job-level enrichment implementation.
- Intended to populate fields such as `description`, salary data, `internal_job_id`, `application_questions`, and `enriched_at` for rows already initialized in `green_job_enrich`.

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
