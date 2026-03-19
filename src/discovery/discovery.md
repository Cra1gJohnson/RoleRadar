# Discovery Directory Guide

## Purpose
- Define the intent of `src/discovery/`.
- Standardize how discovery inputs are managed and queried.
- Provide prompt-ready context for future automation work.

## Directory Scope
- `discovery_names.txt`: source list of discovery search terms.
- `discovery.md`: operating notes, assumptions, and future prompt context.

## High-Level Workflow
1. Load discovery terms from `discovery_names.txt`.
2. Persist terms in PostgreSQL (`discovery_name`) with query timestamps.
3. Run search discovery for Greenhouse and Lever targets.
4. Extract normalized identifiers (`board_token`, `site_slug`).
5. Save extracted identifiers into PostgreSQL for downstream API usage.

## Search Targets
- Greenhouse:
  - `site:boards.greenhouse.io "<term>"`
  - `site:job-boards.greenhouse.io "<term>"`
- Lever:
  - `site:jobs.lever.co "<term>"`

## Extraction Rules
- Greenhouse URL patterns:
  - `https://boards.greenhouse.io/{board_token}`
  - `https://job-boards.greenhouse.io/{board_token}`
- Lever URL pattern:
  - `https://jobs.lever.co/{site_slug}`

## Data Entities
- `discovery_name`
  - Main table storing each discovery term from `discovery_names.txt`.
  - Current columns:
    - `name_id` (primary key)
    - `name` (discovery term)
    - `last_used` (timestamp of most recent query use)
    - `success` (boolean query success flag)
  - `last_used` and `success` are used during discovery search execution to track usage time and whether queries returned useful results.
- `board_token`
  - Stores unique Greenhouse board tokens.
  - Current columns:
    - `token_id` (primary key)
    - `token` (unique)
    - `last_used` (timestamp)
    - `success` (boolean)
- `site_slug`
  - Stores unique Lever site slugs.
  - Includes source URL and timestamps.

## PostgreSQL Configuration (To Be Filled)
- Database:
- User:
- Host:
- Port:
- Schema:

## Table Definitions
- Discovery names table:
  - Table name: `discovery_name`
  - Key columns: `name_id`, `name`, `last_used`, `success`
- Board token table:
  - Table name: `board_token`
  - Key columns: `token_id`, `token`, `last_used`, `success`
- Site slug table:
  - Table name:
  - Key columns:

## Operational Rules
- Treat discovery terms as case-insensitive for matching.
- De-duplicate extracted tokens/slugs before insert.
- Maintain `created_at` and `updated_at` timestamps.
- Record `last_queried_at` for each discovery term.

## Prompt Context for Future Work
- Use this directory as the source of truth for discovery scope.
- Prefer idempotent database writes (`upsert` behavior).
- Keep extraction logic strict to approved URL patterns.
- Log query term, query time, and result count for traceability.

## query_names.py Behavior
- Test mode:
  - Requires a positional range argument (example: `0-200`).
  - Loads all names in that range and updates `last_used = NOW()` for all selected rows before running.
  - Runs one ddgr query using `site:boards.greenhouse.io <name>` for the provided `--name`.
- Full mode:
  - Uses all names selected by the range argument and runs both Greenhouse query templates per name.
  - Example with range `0-200`: `400` total queries.
  - Sleeps random `30-90` seconds between queries.
  - Applies a periodic cooldown of random `120-240` seconds after every 20 queries.
- URL handling:
  - Uses local `ddgr` (`ddgr-2.2`) through subprocess execution (interactive mode).
  - Reuses a persistent ddgr subprocess session across queries and rotates to a fresh session after every 25 queries.
  - Supports `--pages 1|2|3|max`:
    - `1|2|3` fetches up to that many pages.
    - `max` fetches until no new URLs or 10 pages (hard cap).
  - Sleeps random `3-8` seconds between additional pages of the same query.
  - Parses expanded URLs from ddgr output (`-x`) and keeps up to 20 unique URLs per query.
  - Keeps Greenhouse domains only.
  - Extract `{board_token}` from:
    - `https://boards.greenhouse.io/{board_token}/...`
    - `https://job-boards.greenhouse.io/{board_token}/...`
- Validation and persistence:
  - Validate token with `GET https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs`.
  - Insert/update valid tokens in `board_token` using upsert on unique `token`.
- Name usage updates:
  - At run start, all selected names are timestamped with `last_used = NOW()`.
  - In full mode, if any query for a name errors or returns no relevant URLs, final `discovery_name.success = false`.
- Error protocol:
  - On first query error: wait 20 seconds, mark current name as failed, continue to next name.
  - On second query error in the run: stop immediately, print failed names/queries, and mark run failed.
  - Previously persisted valid tokens remain stored.
- Runtime output:
  - Streams query progress to stdout, including page fetch events, URL counts, extracted tokens, validation status, and errors.
- Compatibility note:
  - `--cookie-file` and `--reset-cookies` are retained for CLI compatibility but are currently no-op in subprocess ddgr mode.

## Open Questions
- Should search providers be limited to Google only?
- What query rate limits or backoff strategy should be enforced?
- What retention policy should apply to stale tokens/slugs?
- Should failed queries be retried automatically?

## Revision Notes
- Keep this file updated as schema and workflow evolve.
- Add concrete SQL/table details once PostgreSQL config is finalized.
