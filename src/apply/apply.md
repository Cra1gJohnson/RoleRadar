# Apply Directory Guide

## Purpose

- Define the interactive application queue in `src/apply/`.
- Let the user quickly review ranked jobs by score threshold and move approved jobs into an application queue.

## Directory Scope

- `apply.md`: operating notes and queue semantics.
- `order_jobs.py`: keyboard-only CLI for threshold selection and job approval.
- `prepare_app.py`: AI-powered application-question prep for queued jobs.
- `open_apply_jobs.py`: Playwright browser opener and form filler for queued application URLs.
- `prompt1.txt`: prompt template used for application-question answers.
- `apply.sh`: browser launcher used later by application automation.
- `green_apply_schema.py`: shared helper for creating and evolving the apply queue table.

## High-Level Workflow

1. Load ranked jobs from `green_job`, `green_enrich`, and `green_score`.
2. Ask the user which quality tier they want to review today.
3. Use one of four thresholds: `60+`, `70+`, `80+`, or `90+`.
4. Count how many jobs match the selected threshold and still have `gs.applied = FALSE`.
5. Display matching jobs one at a time in descending score order.
6. Show `company_name`, `title`, `location`, `min_salary`, `url`, and `overall` for each job.
7. Ask the user to approve or skip each job using only keyboard input.
8. Insert approved jobs into `green_apply`.
9. Mark `green_score.applied = TRUE` after approval so the job leaves the queue.
10. Run `prepare_app.py` later to fill in AI answers for queued jobs where `questions = FALSE`.
11. Store the raw AI response in `green_apply.response` and flip `green_apply.questions = TRUE` after a successful prep write.
12. Start Chrome with `src/execute.sh`, then run `open_apply_jobs.py` to attach over CDP, open the next queued application URL, and fill the application fields.

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
- `green_apply`
  - Thin queue table for jobs approved for application.
  - Current v1 shape:
    - `job_id` primary key
    - foreign key to `green_job.job_id`
    - `questions` boolean completion flag for application-question prep
    - `response` raw AI response text for application-question answers

## Operational Rules

- Selection is threshold-based and inclusive: a `70+` choice means `overall >= 70`.
- Only jobs with `gs.applied = FALSE` are eligible.
- The join aliases should remain `gj`, `ge`, and `gs` for readability and consistency.
- Approved jobs must be written to `green_apply` and marked `green_score.applied = TRUE` in the same transaction.
- Application-question preparation must store the AI response in `green_apply.response` and set `green_apply.questions = TRUE` only after a successful write.
- The tool should be usable from the terminal without a mouse.
- URLs should be displayed as terminal hyperlinks when the terminal supports it, with plain-text fallback otherwise.

## order_jobs.py Behavior

- Prompts the user to choose one of four score bands.
- Reports the number of matching jobs before review starts.
- Renders each job with the fields needed to make a quick decision.
- Accepts `y` or `n` for each job.
- On `y`, inserts the job into `green_apply` and marks it applied.
- On `n`, leaves the row unchanged and moves on.
- Prints a concise final summary with threshold, available jobs, reviewed jobs, approvals, skips, and failures.

## prepare_app.py Behavior

- Reads jobs from `green_apply` where `questions = FALSE`.
- Joins `green_job`, `green_enrich`, and `green_score` for application context.
- Loads `prompt1.txt` and injects one job at a time.
- Filters out repeated common questions from `src/apply/green_questions/common_questions.json`.
- Keeps only free-text fields such as `input_text` and `textarea` for the AI request.
- Sends the reduced prompt to the AI API and stores the raw response in `green_apply.response`.
- Sets `green_apply.questions = TRUE` after a successful response write.
- Leaves failed jobs eligible for retry by keeping `questions = FALSE`.

## Prompt Context for Future Work

- Keep this directory focused on application queueing and later application automation.
- Keep the queue table thin so additional application metadata can be added later by downstream scripts.
- Use `green_score.applied` as the immediate queue completion flag.
- Use `src/execute.sh` to launch Chrome with `Profile 2`, then attach Playwright to `http://127.0.0.1:9222`.
- Fill common questions first, then custom text responses from `green_apply.response`.

## open_apply_jobs.py Behavior

- Waits for the Chrome CDP endpoint started by `src/execute.sh`.
- Loads queued jobs from `green_apply` where `questions = TRUE`.
- Opens each job URL in a new tab within the existing Chrome profile.
- Fills hardcoded common questions first using `src/apply/green_questions/common_questions.json`.
- Fills remaining custom text questions using the parsed JSON stored in `green_apply.response`.
- Reports missing common or custom answers in the terminal so the form can be reviewed before submission.

## Revision Notes

- Update this file when the approval workflow, threshold bands, or queue table shape changes.
