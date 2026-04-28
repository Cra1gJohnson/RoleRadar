# current working Role Radar

## Purpose

- Maintain the current working version of Role Radar.
- Discover public ATS board tokens from web search results.
- Collect, enrich, score, review, and apply to jobs through a PostgreSQL-backed workflow.

## High-Level Workflow

1. Use the You.com search API to discover ATS board tokens from strategic `discovery names`.
2. Normalize discovered URLs and persist valid `tokens`.
3. Use tokens to make asynchronous requests to ATS public API endpoints.
4. Normalize response JSON and store live jobs in PostgreSQL.
5. Run collection daily so the database maintains a current snapshot of valid jobs.
6. Filter jobs roughly by title using regex in PostgreSQL.
7. Enrich filtered jobs by requesting full descriptions and application questions.
8. Score enriched jobs with a lightweight LLM using a candidate profile.
9. Review and approve scored jobs.
10. Package approved jobs with a larger LLM to answer application questions for user review.
11. Open approved application URLs and use Playwright to fill applications automatically.
12. Store application information back in PostgreSQL.

## External Services

- You.com provides web search for token discovery. This replaces the discontinued Google Custom Search JSON API.
- Public ATS APIs provide board and job data. Tokens are keywords that allow access to large application-tracking-system endpoints.
- Playwright fills applications through a Google Chrome instance started with a `remote_debugging_port`.

## Run Order

Run scripts in this order to exercise the full workflow:

1. `src/discovery/persist_names.py`
2. `src/discovery/you_query.py --mode full 0-250`
3. `src/collection/collection_control.py full|test`
4. `src/enrichment/candidate_filter.py`
5. `src/enrichment/job_enrichment.py`
6. `src/scoring/score_job.py full --limit <int>`
7. `src/apply/order_jobs.py`
8. `src/apply/prepare_app.py --full --limit <int>`
9. `src/execute.sh`
10. `src/apply/open_jobs.py --limit`
