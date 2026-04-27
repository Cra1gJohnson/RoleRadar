# Apply Directory Guide

## Purpose

- Define the interactive application queue in `src/apply/`.
- Let the user quickly review scored jobs by score threshold and move approved jobs into an application queue.

## Directory Scope

- `apply.md`: operating notes and queue semantics.
- `order_jobs.py`: keyboard-only CLI for threshold selection and job approval.
- `prepare_app.py`: AI-powered application-question prep for queued jobs.
- `cover.py`: cover-letter and resume material helper used by `prepare_app.py`.
- `open_jobs.py`: Playwright browser opener and URL router for queued application URLs.
- `handle_jobs.py`: Playwright browser runner that consumes the `open_jobs.py` package JSON, uploads stored material paths, and fills standard Greenhouse forms.
- `utility/dump_apply_html.py`: fetches a queued apply URL and writes the raw HTML response to `green_questions/`.
- `utility/reset_green_apply.py`: clears queued apply rows back to a blank replay state for one or more job ids.
- `utility/reset_green_score_viewed.py`: clears the `green_score.viewed` flag for one or more un-applied jobs so they can be reviewed again.
- `utility/backfill_application.py`: backfills submitted apply rows into the persistent `application` table.
- `prompt1.txt`: prompt template used when the application includes a cover-letter upload field.
- `prompt2.txt`: prompt template used when the application does not include a cover-letter upload field.
- `apply.sh`: browser launcher used later by application automation.
- `green_apply_schema.py`: shared helper for creating and evolving the apply queue table.

## High-Level Workflow

1. Load scored jobs from `green_job`, `green_enrich`, and `green_score`.
2. Ask the user which quality tier they want to review today.
3. Use one of four thresholds: `60+`, `70+`, `80+`, or `90+`.
4. Count how many jobs match the selected threshold and still have `gs.viewed = FALSE` and `gs.applied = FALSE`.
5. Display matching jobs one at a time in descending score order.
6. Show `company_name`, `title`, `location`, `min_salary`, `url`, and `overall` for each job.
7. Ask the user to approve or skip each job using only keyboard input.
8. Insert approved jobs into `green_apply`.
9. Mark `green_score.viewed = TRUE` for both yes and no decisions so the job does not reappear in the review queue.
10. Mark `green_score.applied = TRUE` only when the answer is yes.
11. Leave `green_score.applied` as `NULL` on no decisions so the row stays un-applied.
12. Run `prepare_app.py` later to fill in AI answers for queued jobs where `packaged_at IS NULL`.
13. If the application includes a cover-letter field and `green_apply.cover_letter` is empty, review the generated cover-letter first paragraph plus any editable `Text_Area` or `Input_Text` answers in the terminal before the response is stored.
14. Compile the reviewed cover letter into `CPJohnson_Cover_<company name>.pdf`, store that PDF path in `green_apply.cover_letter`, and keep the matching `.tex` file beside it until submission is persisted.
15. Store the approved response JSON in `green_apply`, then insert the accepted cover paragraph and any accepted editable answers into history tables for future prompt context.
16. Start Chrome with `src/execute.sh`, then run `open_jobs.py` to attach over CDP, open the next queued application URL, and route it to the standard or nonstandard Playwright flow.

## Data Entities

- `green_job`
  - Thin normalized source table.
  - Relevant fields:
    - `job_id`
    - `company_name`
    - `title`
    - `location`
    - `url`
- `green_enrich`
  - Joined for display context.
  - Relevant fields:
    - `job_id`
    - `min_salary`
- `green_score`
  - Source of application priority.
  - Relevant fields:
    - `job_id`
    - `overall`
    - `applied`
    - `viewed`
- `application`
  - Persistent application record for submitted applications across all sources.
  - Intended to become the source of truth for application history and operational metrics.
  - Relevant fields:
    - `app_id`
    - `source`
    - `source_job_id`
    - `internal_job_id`
    - `company_name`
    - `title`
    - `location`
    - `url`
    - `first_fetched_at`
    - `description`
    - `min`
    - `max`
    - `currency`
    - `application_questions`
    - `enriched_at`
    - `overall`
    - `score_prompt`
    - `score_model`
    - `scored_at`
    - `apply_prompt`
    - `apply_model`
    - `apply_response`
    - `resume`
    - `cover_letter`
    - `packaged_at`
    - `submitted_at`
    - `time_to_submit`
  - Metrics use cases:
    - Prompt and model comparisons for scoring and application packaging
    - Time between fetch, enrich, score, package, and submit steps
    - Submission timing and completion analysis
- `green_apply`
  - Thin queue table for jobs approved for application.
  - Current v1 shape:
    - `job_id` primary key
    - foreign key to `green_job.job_id`
    - `submitted_at` timestamp set after the browser flow is confirmed complete
    - `response` approved AI response JSON for application-question answers
    - `packaged_at` timestamp set after application prep is written
    - `prompt` prompt file name used for packaging, such as `prompt1.txt`
    - `model` model name used for packaging
    - `resume` absolute path to the selected resume PDF, currently `templates/resume/CPJohnson_resume.pdf`
    - `cover_letter` absolute path to the generated cover-letter PDF, or `NULL` when no cover-letter field exists
    - `time_to_submit` placeholder duration column, currently left `NULL`
- `apply_answers`
  - Accepted editable-answer history for future prompt context.
  - Relevant fields:
    - `job_id`
    - `question_label`
    - `answer_label`
    - `style`
    - `prompt`
    - `model`
    - `accepted_at`
- `apply_cover`
  - Accepted cover-letter first-paragraph history for future prompt context.
  - Relevant fields:
    - `job_id`
    - `first_paragraph`
    - `prompt`
    - `model`
    - `accepted_at`

## Operational Rules

- Selection is threshold-based and inclusive: a `70+` choice means `overall >= 70`.
- Only jobs with `gs.applied = FALSE` and `gs.viewed = FALSE` are eligible.
- The join aliases should remain `gj`, `ge`, and `gs` for readability and consistency.
- Approved jobs must be written to `green_apply` and marked `green_score.applied = TRUE` and `green_score.viewed = TRUE` in the same transaction.
- Rejected jobs must be marked `green_score.viewed = TRUE` and leave `green_score.applied = NULL`.
- Application-question preparation must store the approved response JSON, prompt name, model name, resume path, cover-letter PDF path, and `packaged_at` only after a successful write.
- Accepted editable answers from approved prep runs must be inserted into `apply_answers` so later prompts can load the most recent examples.
- `apply_answers` is a permanent table; `prepare_app.py` does not create or alter it.
- Accepted cover-letter first paragraphs from approved prep runs must be inserted into `apply_cover`.
- `apply_cover` is a permanent table; `prepare_app.py` does not create or alter it.
- After `handle_jobs.py` finishes a job, `open_jobs.py` should prompt for `y/n` confirmation and set `green_apply.submitted_at` only on `y`.
- The tool should be usable from the terminal without a mouse.
- URLs should be displayed as terminal hyperlinks when the terminal supports it, with plain-text fallback otherwise.

## order_jobs.py Behavior

- Prompts the user to choose one of four score bands.
- Reports the number of matching jobs before review starts.
- Renders each job with the fields needed to make a quick decision.
- Accepts `y` or `n` for each job.
- On `y`, inserts the job into `green_apply` and marks the score row as viewed and applied.
- On `n`, marks the score row as viewed, leaves `applied` null, and moves on.
- Prints a concise final summary with threshold, available jobs, reviewed jobs, approvals, skips, and failures.

## prepare_app.py Behavior

- Supports `--test`, `--full`, `--limit`, and `--redo` modes from the CLI.
- Supports optional `--rate-per-minute` and `--max-concurrency` controls for request throughput.
- `--test` prepares the first queued job where `green_apply.packaged_at IS NULL`.
- `--full` prepares all queued jobs where `green_apply.packaged_at IS NULL`.
- `--limit` only applies to `--full` and caps how many queued jobs are processed.
- `--redo` prepares every row in `green_apply`, regardless of the current `packaged_at` flag.
- Joins `green_job`, `green_enrich`, and `green_score` for application context.
- Uses a regex against raw `application_questions` labels to detect whether a `Cover Letter` field is present.
- Loads `prompt1.txt` only when a cover-letter field is present and `green_apply.cover_letter` is still empty; otherwise loads `prompt2.txt`.
- Filters out trivial questions using the labels found in `src/scoring/enrichment_display/`.
- Injects up to 20 recently accepted `apply_answers` rows into the prompt for model-only context.
- Injects the full reusable LaTeX cover-letter template only for jobs using `prompt1.txt`.
- Dispatches Gemini requests asynchronously (rate-limited), so multiple queued jobs can be prepared in parallel.
- Reviews each model response linearly as it arrives while later requests can continue in flight.
- Prints the job title, URL, ID, editable-question count, and cover-letter approval marker before asking whether to edit the model response.
- Opens one `nvim` review buffer containing the cover-letter company name, job title, first paragraph, and editable `Text_Area` and `Input_Text` answers only for `prompt1.txt` requests.
- Normalizes model answer rows before review/storage so malformed keys such as `answer_text` or `answered text` become the canonical response key `answer label`.
- Uses `cover.py` to compile the cover letter only when `prompt1.txt` is used and stores the generated PDF path in `green_apply.cover_letter`.
- Preserves an existing `green_apply.cover_letter` path when the application still has a cover-letter field and `prompt2.txt` is used.
- Clears `green_apply.cover_letter` when the application has no cover-letter field.
- Uses `templates/resume/CPJohnson_resume.pdf` for every prepared application and stores that absolute path in `green_apply.resume`.
- Stores accepted cover-letter first paragraphs in `apply_cover` only when the application has a cover-letter field.
- Stores accepted `Text_Area` and `Input_Text` answers in `apply_answers`.
- Prints per-application Gemini token cost after packaging and aggregate input/output token costs in the final summary using Gemini 2.5 Flash paid text rates.
- Leaves failed API calls eligible for retry by keeping `packaged_at IS NULL`.

## Prompt Context for Future Work

- Keep this directory focused on application queueing and later application automation.
- Keep the queue table thin so additional application metadata can be added later by downstream scripts.
- Use `application` for persistent application history, metrics, and eventual source-of-truth reporting across all application sources.
- Use `green_score.applied` as the immediate queue completion flag.
- Use `src/execute.sh` to launch Chrome with `Profile 2`, then attach Playwright to `http://127.0.0.1:9222`.
- `handle_jobs.py` reads the jobs package from `stdin`, connects over CDP, uploads `green_apply.resume`, uploads the stored `green_apply.cover_letter` PDF when present, and still tolerates older rows that store inline LaTeX, then fills standard Greenhouse forms from `answers.json` plus the stored AI response.
- `answers.json` can use `{"value": "...", "variants": [...]}` for answers that need multiple select-friendly forms, such as `United States` and `US`.
- `open_jobs.py` emits the JSON jobs package that `handle_jobs.py` consumes.
- `utility/reset_green_apply.py` can be used to clear `green_apply` rows for a replay, leaving `job_id` intact and resetting the rest of the row state.
- `utility/reset_green_score_viewed.py` can be used to make one or more rejected jobs visible in `order_jobs.py` again.
- `utility/backfill_application.py` can be used to move submitted rows from `green_apply` into `application` when a persistent record is missing.
- `utility/backfill_application.py` treats `green_job.greenhouse_job_id` as the persistent `application.source_job_id` value so submitted rows can be compared against the source-system identifier.

## open_jobs.py Behavior

- Waits for the Chrome CDP endpoint started by `src/execute.sh`.
- Loads queued jobs from `green_apply` where `packaged_at IS NOT NULL`.
- Excludes rows whose `green_enrich.request_status = 404`.
- Opens each job URL in the existing Chrome profile.
- Includes `green_apply.resume` and `green_apply.cover_letter` in the jobs package for upload automation.
- When a submitted row is inserted into `application`, the matching cover-letter `.tex` content is copied into `application.cover_letter` and the persisted `.tex` and `.pdf` files are removed.
- Redirects noisy `handle_jobs.py` browser-fill output to `src/apply/logs/handle_jobs_<timestamp>_<job_id>.log`.
- Classifies URLs as `standard_greenhouse` or `nonstandard` using the job-board host.
- Dispatches standard Greenhouse pages to one Playwright hook and nonstandard pages to another.
- Probes each URL before handing it to `handle_jobs.py`; on any non-`200` status, writes that status back to `green_enrich.request_status` and skips the job.
- Prints the job title, job_id, and URL after the browser handling step.
- Prompts `y/n` for each job in order and sets `green_apply.submitted_at` only when the user confirms submission.
- Leaves the route hooks as the integration point for the next browser automation scripts.

## dump_apply_html.py Behavior

- Reads queued jobs from `green_apply` where `packaged_at IS NOT NULL`.
- Joins `green_job` to get the live application URL.
- Fetches the page HTML with a browser-like HTTP session.
- Writes one UTF-8 `.txt` snapshot per job into `src/apply/green_questions/`.
- Includes the job metadata at the top of the file so the HTML is easier to inspect.

## Revision Notes

- Update this file when the approval workflow, threshold bands, or queue table shape changes.
