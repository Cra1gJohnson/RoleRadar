# Collection Directory Guide

## Purpose
- Define the intent of `src/collection/`.
- Standardize how board monitoring, board snapshots, and job normalization are managed.
- Provide prompt-ready context for future automation work.

## Directory Scope
- `greenhouse_board_resp/`: directory of real Greenhouse board GET responses for context and testing.
- `collection.md`: operating notes, assumptions, and future prompt context.
- `collection_control.py`: control script for collection scheduling and monitoring.
- `board_hash.py`: board snapshot fetcher and hash tracker for Greenhouse boards.
- `upsert_jobs.py`: job normalization and upsert runner for `greenhouse_job`.
- `pull_ten.py`: utility script for fetching sample board responses during development.

## High-Level Workflow
1. Read candidate Greenhouse board tokens from PostgreSQL (`board_token`).
2. Select boards to monitor based on polling cadence and prior collection results.
3. Fetch `GET https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs`.
4. Compute a board hash from the sorted list of `jobs[].id` values.
5. Persist a board snapshot in PostgreSQL (`greenhouse_board_snapshot`).
6. Detect whether the latest board hash differs from the previous successful snapshot.
7. Normalize jobs from changed board snapshots.
8. Upsert normalized jobs into PostgreSQL (`greenhouse_job`).
9. Record monitoring state so the next poll cycle can prioritize boards correctly.

## Collection Targets
- Greenhouse board jobs API:
  - `https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs`

## Hashing Rules
- Use the Greenhouse `jobs[].id` field as the canonical board membership identifier.
- Sort all job IDs before hashing so the hash is stable even if board ordering changes.
- Board hash changes indicate board membership changes such as:
  - new job posted
  - existing job removed
- Store `job_count` separately for monitoring and dead-board analysis.

## Data Entities
- `board_token`
  - Source table from discovery.
  - Stores unique Greenhouse board tokens used as inputs to collection.
- `greenhouse_board_snapshot`
  - Stores one fetched board snapshot per monitored request.
  - Current columns:
    - `snapshot_id` (primary key)
    - `token` (foreign key to `board_token.token` and unique constraint)
    - `fetched_at` (timestamp of snapshot fetch)
    - `request_status` (HTTP status code or request result)
    - `job_count` (count of jobs returned in the response)
    - `board_hash` (hash derived from sorted job IDs)
    - `company_name` (best available board-level company label)
    - `status` (`board_priority`; values include `HOT`, `WARM`, `COLD`, `DEAD`)
    - `united_states` (whether the latest normalization pass found at least one U.S. job)
  - Used to track board changes over time and decide whether job normalization should run.
  - Only one snapshot per board right now.
- `greenhouse_job`
  - Stores normalized jobs derived from Greenhouse board snapshots.
  - Current columns:
    - `job_id` (primary key)
    - `snapshot_id` (foreign key to `greenhouse_board_snapshot.snapshot_id`)
    - `token` (foreign key to `board_token.token`)
    - `greenhouse_job_id` (Greenhouse API job id)
    - `company_name`
    - `title`
    - `location`
    - `url`
    - `first_fetched_at`
    - `updated_at` (latest `updated_at` value from the Greenhouse payload)
    - `candidate` (nullable downstream enrichment candidate flag)
    - `enriched` (downstream enrichment completion flag)
  - Intended to represent normalized job state for downstream use while preserving linkage to the source snapshot and board token.
  - Keep this table thin; job descriptions and other heavier enrichment data should live downstream in enrichment-specific tables.

## PostgreSQL Configuration (To Be Filled)
- Database:
- User:
- Host:
- Port:
- Schema:

## Table Definitions
- Board token input table:
  - Table name: `board_token`
  - Key columns: `token`
- Board snapshot table:
  - Table name: `greenhouse_board_snapshot`
  - Key columns: `snapshot_id`, `token`, `fetched_at`, `request_status`, `job_count`, `board_hash`, `company_name`, `status`, `united_states`
- Greenhouse job table:
  - Table name: `greenhouse_job`
  - Key columns: `job_id`, `snapshot_id`, `token`, `greenhouse_job_id`, `company_name`, `title`, `location`, `url`, `first_fetched_at`, `updated_at`, `candidate`, `enriched`

## Operational Rules
- Prefer idempotent writes wherever possible.
- Keep board fetching separate from job normalization and job upsert behavior.
- Treat board hashing as a board-level change detector, not as a replacement for normalized job storage.
- Only trigger normalization/upsert when a new board snapshot represents a meaningful board change.
- Keep request status and fetch time for every board poll attempt.
- Maintain traceability from normalized jobs back to the source `greenhouse_board_snapshot`.
- Maintain traceability from normalized jobs back to the originating `board_token`.
- Preserve downstream progress markers on existing jobs unless a later workflow explicitly resets them.

## Prompt Context for Future Work
- Use this directory as the source of truth for Greenhouse board collection scope.
- Keep the system split into three responsibilities:
  - collection scheduling and monitoring
  - board snapshot fetching and hashing
  - job normalization and upsert
- Prefer storing board-level evidence in `greenhouse_board_snapshot` and normalized job-level data in `greenhouse_job`.
- Optimize for repeated polling across thousands of boards without unnecessary reprocessing.
- Keep `greenhouse_job` minimal so enrichment-specific data can live in downstream tables.

## collection_control.py Behavior
- Owns monitoring cadence and polling decisions.
- Selects which board tokens should be fetched next.
- Tracks collection progress across the board population.
- Provides the control layer for repeated board monitoring runs.

## board_hash.py Behavior
- Reads Greenhouse board tokens from PostgreSQL.
- Requests board job payloads from the Greenhouse board jobs API.
- Extracts all `jobs[].id` values from the JSON response.
- Sorts job IDs before hashing so the board hash is order-independent.
- Inserts a new row into `greenhouse_board_snapshot` for each fetched board.
- Records request status, fetch time, board hash, job count, company name, and board status.
- Marks empty boards as `COLD`.
- Acts as the board snapshot layer for change detection.

## upsert_jobs.py Behavior
- Reads board snapshots that require job normalization.
- Takes parameters: payload (json), token (board_token), check_comp (bool).
- `check_comp` indicates if there is an existing snapshot.
- If there is an existing snapshot then jobs must be checked to see if they are up to date.
- Normalizes Greenhouse job payloads into the shape expected by `greenhouse_job`.
- Inserts new jobs and updates existing jobs using upsert behavior.
- Preserves linkage back to the originating `greenhouse_board_snapshot` with token.
- Writes the thin job-level fields currently defined in `greenhouse_job`, including `company_name`, `title`, `location`, `url`, `first_fetched_at`, `updated_at`, `candidate`, and `enriched`.
- Leaves downstream enrichment data outside the collection layer.
- Serves as the normalized jobs layer for downstream querying and analytics.

## Open Questions
- Should Greenhouse-native identifiers such as `internal_job_id` or `requisition_id` also be stored in `greenhouse_job`?
- How should the board-level `status` values `HOT`, `WARM`, `COLD`, and `DEAD` be assigned beyond the current empty-board => `COLD` rule?
- How should dead boards or long-inactive boards be deprioritized over time?
- What retry and backoff policy should apply to failed board requests?

## Revision Notes
- Keep this file updated as the collection schema and scripts evolve.
- Update this file again as `greenhouse_job` gains or drops normalized identifiers or lifecycle fields.
