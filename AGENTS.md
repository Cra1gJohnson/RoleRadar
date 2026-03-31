# Repository Guidelines

## Project Structure & Module Organization

- `src/collection/` ingests and normalizes job-board data.
- `src/discovery/` stores search terms and board-token discovery utilities.
- `src/enrichment/` pulls job details and writes enrichment artifacts.
- `src/scoring/` ranks enriched jobs and contains sample `enrichment_display/*.json` payloads.
- `src/apply/` prepares approved jobs for application flow, including `prepare_app.py` and `green_questions/`.
- `src/*.md` files are the canonical operating notes for each stage. Keep them in sync with code changes.

## Build, Test, and Development Commands

- `python3 src/scoring/score_job.py --help` shows scoring options and confirms dependencies.
- `python3 src/apply/prepare_app.py --help` checks the application-prep worker.
- `python3 -m json.tool src/apply/green_questions/common_questions.json` validates JSON syntax.
- `python3 -m compileall src` performs a fast syntax check across Python files.
- There is no top-level build system or formal test runner in this repo.

## Coding Style & Naming Conventions

- Use Python 3 and keep edits small and script-oriented.
- Prefer ASCII unless a file already uses Unicode.
- Follow existing naming patterns: snake_case for Python files and database fields, lowercase underscore names for artifacts such as `green_questions/common_questions.json`.
- Keep Markdown concise and directive; update the relevant `*.md` guide when behavior changes.
- Use `apply_patch` for manual edits.

## Testing Guidelines

- No dedicated test framework is configured.
- Validate changes with targeted script runs, JSON parsing, and `compileall`.
- For data files, confirm repeated-question mappings and label text exactly match source JSON labels.

## Commit & Pull Request Guidelines

- Use Conventional Commits: `type(scope): description`.
- Prefer scopes that match the affected area, such as `scoring`, `apply`, or `discovery`.
- Example: `fix(apply): add repeated green questions mapping`.
- Pull requests should summarize the workflow change, list touched paths, and mention any manual verification performed.

## Agent-Specific Instructions

- Always attempt to keep one connection open to the db for the duration of a script. This keeps writes fast for large numbers.
- Treat `src/scoring/enrichment_display/` as the source of truth for repeated application questions.
- Only hardcode answers for questions repeated multiple times across that directory.
- Do not overwrite unrelated user changes in a dirty worktree.
