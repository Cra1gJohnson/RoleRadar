from ingestion.config import Settings
from ingestion.database import create_schema, create_session_factory
from ingestion.pipeline import run_ingestion


def main() -> None:
    settings = Settings()
    create_schema(settings.database_url)
    session_factory = create_session_factory(settings.database_url)
    processed = run_ingestion(settings, session_factory)
    print(f"Processed {processed} jobs.")
