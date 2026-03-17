# Job Ingestion

Python ingestion service for collecting public job listings from Greenhouse and Lever job boards and storing them in PostgreSQL.

## Setup

1. Create a virtual environment and install the project:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

2. Copy the example environment file and adjust the values:

```bash
cp .env.example .env
```

3. Run the ingestion:

```bash
ingest-jobs
```

The command will:

- create the database schema if it does not already exist
- fetch jobs from the configured Greenhouse and Lever sources
- upsert jobs into PostgreSQL
- mark jobs as inactive when they disappear from a source in a given run

## Data model

Jobs are stored in a single relational table keyed by `(source, external_id)`.
This keeps the ingestion pipeline simple while preserving source-specific identifiers and metadata as structured JSON.

