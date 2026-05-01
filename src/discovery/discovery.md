# Discovery Directory Guide

## Purpose
- Define the intent of `src/discovery/`.
- Standardize how discovery inputs are managed and queried.
- Provide prompt-ready context for future automation work.

## Directory Scope
- `discovery_names.txt`: source list of discovery search terms.
- `discovery.md`: operating notes, assumptions, and future prompt context.
- `create_ats_board.py`: creates the shared ATS board table and enum.
- `query_names.py`: ddgr-based discovery runner for Greenhouse queries.
- `you_query.py`: You.com Search API-based discovery runner for ATS board discovery.

## High-Level Workflow
1. Load discovery terms from `discovery_names.txt`.
2. Persist terms in PostgreSQL (`keyword`) with query timestamps and ATS board counts.
3. Run search discovery for Greenhouse and Lever targets.
4. Record sent You.com searches in PostgreSQL (`you_search`) so they are not resent.
5. Extract normalized identifiers (`board_token`, `site_slug`).
6. Save extracted identifiers into PostgreSQL for downstream API usage.

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
- `keyword`
  - Main table storing each discovery term from `discovery_names.txt`.
  - Current columns:
    - `name_id` (primary key)
    - `name` (discovery term)
    - `green_boards` (count of discovered Greenhouse boards)
    - `ashby_boards` (count of discovered Ashby boards)
    - `lever_boards` (count of discovered Lever boards)
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
- `ats_board`
  - Stores unique ATS board identifiers for Greenhouse, Ashby, and Lever discovery.
  - Current columns:
    - `board_id` (primary key)
    - `board` (board token, job board name, or site slug)
    - `ats` (`Green`, `Ashby`, or `Lever`)
    - `last_used` (timestamp)
    - `success` (boolean)
- `you_search`
  - Stores each You.com search string exactly once.
  - Current columns:
    - `search_id` (primary key)
    - `search` (unique)
    - `results_num` (count of search result URLs returned)
    - `last_used` (timestamp)
    - `success` (boolean search success flag)
    - `tokens` (count of valid ATS board identifiers found for the search)
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
- Keyword table:
  - Table name: `keyword`
  - Key columns: `name_id`, `name`, `green_boards`, `ashby_boards`, `lever_boards`, `last_used`, `success`
- Board token table:
  - Table name: `board_token`
  - Key columns: `token_id`, `token`, `last_used`, `success`
- ATS board table:
  - Table name: `ats_board`
  - Key columns: `board_id`, `board`, `ats`, `last_used`, `success`
- You search table:
  - Table name: `you_search`
  - Key columns: `search_id`, `search`, `results_num`, `last_used`, `success`, `tokens`
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




## you_query.py Behavior
- Uses the You.com Search API endpoint `GET https://ydc-index.io/v1/search`.
- Reads the API key from `.env` variable `API`.
- Opens one PostgreSQL connection for the run and reads selected rows from `keyword`.
- Supports `test` and `full` as the first positional argument; `test` requires `--name`, while `full` runs every keyword.
- Checks `you_search` before each You.com API request and skips already-recorded searches unless `--force` is set.
- `--force` still checks `you_search` and updates `last_used`, then reruns the API call.
- Runs one search per selected keyword for each configured ATS site query:
  - `site:boards.greenhouse.io {name}`
  - `site:job-boards.greenhouse.io {name}`
  - `site:jobs.ashbyhq.com {name}`
  - `site:jobs.lever.co {name}`
- Runs You.com searches sequentially so search logging, validation, and board inserts are persisted immediately and predictably on the single open PostgreSQL connection.
- Requests the maximum supported result count for each query (`count=100`).
- Extracts the board identifier from approved ATS domains:
  - `https://boards.greenhouse.io/{board}/...`
  - `https://job-boards.greenhouse.io/{board}/...`
  - `https://jobs.ashbyhq.com/{board}/...`
  - `https://jobs.lever.co/{board}/...`
- Sorts extracted board identifiers before validation and persistence.
- Validates candidate boards with browser-like headers:
  - Greenhouse: `GET https://boards-api.greenhouse.io/v1/boards/{board}`
  - Ashby: `GET https://api.ashbyhq.com/posting-api/job-board/{board}?includeCompensation=false`
  - Lever: `GET https://api.lever.co/v0/postings/{board}?mode=json`
- Runs ATS board validation with `httpx.AsyncClient` using bounded concurrency controlled by `--validation-workers`.
- Inserts each valid board into `ats_board` only when no existing row has the same board identifier, regardless of ATS.
- Updates each `keyword` row with the count of newly inserted Greenhouse, Ashby, and Lever boards from that keyword's queries.
- Upserts `you_search` with the latest result count, success flag, and valid-board count for each search.
- Enforces a hard cap of 100 You.com search requests per rolling 60-second window.
- Writes concise progress lines for successful queries, counts, inserts, and failures to `src/discovery/log/you_search_<timestamp>.log`.

## Open Questions
- Should search providers be limited to Google only?
- What query rate limits or backoff strategy should be enforced?
- What retention policy should apply to stale tokens/slugs?
- Should failed queries be retried automatically?

## Revision Notes
- Keep this file updated as schema and workflow evolve.
- Add concrete SQL/table details once PostgreSQL config is finalized.
