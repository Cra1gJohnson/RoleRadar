# Collection Directory Guide

## Purpose

- Define the intent of `src/collection/`.
- Standardize how board monitoring, board snapshots, and job normalization are managed.
- Provide prompt-ready context for future automation work.

## Directory Scope

- `greenhouse_board_resp/`: directory of real Greenhouse board GET responses for context and testing.
- `collection.md`: operating notes, assumptions, and future prompt context.
- `collection_control.py`: control script for collection scheduling and monitoring.
- `create_board_snapshot.py`: creates the board snapshot table.
- `create_job.py`: creates the normalized job table.
- `normalization.py`: pure board-response normalization helpers with no database access.
- `upsert.py`: async comparison and upsert helpers for `green_job`.
- `delete.py`: async stale-row deletion helpers for `green_job`.
- `utility/pull_ten_boards.py`: utility script for fetching sample board responses during development.

## High-Level Workflow

1. Read candidate Greenhouse board tokens from PostgreSQL (`board_token`).
2. Select boards to monitor based on polling cadence and prior collection results.
3. Fetch `GET https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs`.
4. Extract and sort the returned `jobs[].id` values, compute the board hash, and compare it with `board_snapshot.board_hash`.
5. For unchanged boards, update the snapshot poll metadata and skip full normalization, job upsert, and stale-row deletion.
6. For changed or first-seen boards, normalize the returned board payload in memory.
7. Persist or update a board snapshot in PostgreSQL (`board_snapshot`).
8. Compare all normalized response jobs to the current `green_job` rows for the same board.
9. Upsert all valid response jobs into PostgreSQL (`green_job`) with a per-job `united_states` location flag.
10. Delete `green_job` rows for that board that are no longer present in the normalized response.
11. Record monitoring state so the next poll cycle can prioritize boards correctly.
12. Write the final collection summary line to `src/collection/logs/`.

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
- `board_snapshot`
  - Stores one fetched board snapshot per monitored request.
  - Current columns:
    - `snapshot_id` (primary key)
    - `board` (unique board identifier; foreign key to `ats_board.board`)
    - `fetched_at` (timestamp of snapshot fetch)
    - `request_status` (HTTP status code or request result)
    - `job_count` (count of jobs returned in the response)
    - `board_hash` (hash derived from sorted job IDs)
    - `company_name` (best available board-level company label)
    - `united_states` (whether the latest normalization pass found at least one U.S. job)
  - Used to track board changes over time and store the latest fetch metadata.
  - Only one snapshot per board right now.
- `green_job`
  - Stores normalized jobs derived from Greenhouse board snapshots.
  - Current columns:
    - `job_id` (primary key)
    - `snapshot_id` (foreign key to `board_snapshot.snapshot_id`)
    - `board` (foreign key to `ats_board.board`)
    - `ats` (`ats_board_ats`; values include `Green`, `Ashby`, `Lever`)
    - `ats_job_id` (source ATS job id)
    - `company_name`
    - `title`
    - `location`
    - `url`
    - `first_fetched_at`
    - `updated_at` (latest `updated_at` value from the Greenhouse payload)
    - `candidate` (nullable downstream enrichment candidate flag)
    - `enriched` (downstream enrichment completion flag)
    - `united_states` (best-effort job-level location classification)
    - `description`
    - `min_compensation`
    - `max_compensation`
  - Intended to represent normalized job state for downstream use while preserving linkage to the source snapshot and board.
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
  - Table name: `board_snapshot`
  - Key columns: `snapshot_id`, `board`, `fetched_at`, `request_status`, `job_count`, `board_hash`, `company_name`, `united_states`
- Greenhouse job table:
  - Table name: `green_job`
  - Key columns: `job_id`, `snapshot_id`, `board`, `ats`, `ats_job_id`, `company_name`, `title`, `location`, `united_states`, `url`, `first_fetched_at`, `updated_at`, `candidate`, `enriched`, `description`, `min_compensation`, `max_compensation`

## Operational Rules

- Prefer idempotent writes wherever possible.
- Keep board fetching separate from job normalization and job table writes.
- Treat board hashing as the fast unchanged-board gate before full normalization and job synchronization.
- Normalize successful board responses only when the board hash is new or changed.
- Keep all returned job IDs in the board hash and write all valid normalized jobs into `green_job`.
- Set `green_job.united_states` for every written job based on its normalized location.
- Keep request status and fetch time for every board poll attempt.
- Maintain traceability from normalized jobs back to the source `board_snapshot`.
- Maintain traceability from normalized jobs back to the originating `ats_board`.
- Preserve downstream progress markers on existing jobs unless a later workflow explicitly resets them.
- Delete stale `green_job` rows when they are no longer present in the normalized live-board response.
- Keep final summary logs one-line only: the file contains the final summary and no progress output.

## Prompt Context for Future Work

- Use this directory as the source of truth for Greenhouse board collection scope.
- Keep the system split into four responsibilities:
  - collection scheduling and monitoring
  - board snapshot fetching and hashing
  - pure job normalization
  - database upsert/delete synchronization
- Prefer storing board-level evidence in `board_snapshot` and normalized job-level data in `green_job`.
- Optimize for repeated polling across thousands of boards without unnecessary reprocessing.
- Keep `green_job` minimal so enrichment-specific data can live in downstream tables.

## collection_control.py Behavior

- Owns monitoring cadence, polling decisions, and async fetch orchestration.
- Uses a bounded queue so responses can be normalized and written sequentially.
- Keeps one PostgreSQL connection open for the duration of the run.
- Updates `board_snapshot` on every successful or failed fetch attempt.
- Short-circuits successful unchanged responses by comparing the freshly computed board hash against the stored snapshot hash before full normalization and job synchronization.
- Feeds normalized work items into the database writer stage so upsert and delete remain sequential.
- Prints the final summary and writes the same single line to `logs/collection_summary_YYYYMMDD_HHMMSS_<mode>.log`.

## normalization.py Behavior

- Converts a raw Greenhouse board payload into a pure in-memory normalized shape.
- Validates `jobs[].id` values and derives the response job id list used for board hashing.
- Normalizes the job fields needed for `green_job`.
- Computes `united_states` for every normalized job.
- Keeps all valid normalized jobs eligible for database writes.
- Does not access PostgreSQL.

## upsert.py Behavior

- Compares normalized response jobs against existing `green_job` rows for the same board.
- Inserts new jobs and updates jobs present in both the database and the live response.
- Leaves unchanged rows untouched when the upstream `updated_at` value matches.
- Updates same-timestamp rows when the stored `united_states` flag differs from the current location classification.
- Uses the current snapshot id for the rows it writes.
- Receives all valid normalized jobs from the collection controller.

## delete.py Behavior

- Loads the current `green_job` rows for a board.
- Deletes rows whose `ats_job_id` is no longer present in the normalized response.
- Verifies the `green_job` child-table cascade contract before destructive deletes run.

## Open Questions

- Should Greenhouse-native identifiers such as `internal_job_id` or `requisition_id` also be stored in `green_job`?
- How should the board-level `status` values `HOT`, `WARM`, `COLD`, and `DEAD` be assigned beyond the current empty-board => `COLD` rule?
- How should dead boards or long-inactive boards be deprioritized over time?
- What retry and backoff policy should apply to failed board requests?

## Revision Notes

- Keep this file updated as the collection schema and scripts evolve.
- Update this file again as `green_job` gains or drops normalized identifiers or lifecycle fields.
