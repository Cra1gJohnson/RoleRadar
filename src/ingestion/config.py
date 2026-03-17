from functools import cached_property

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = Field(alias="DATABASE_URL")
    user_agent: str = Field(default="app-copilot-ingestion/0.1.0", alias="USER_AGENT")
    greenhouse_board_tokens: str = Field(default="", alias="GREENHOUSE_BOARD_TOKENS")
    lever_companies: str = Field(default="", alias="LEVER_COMPANIES")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @cached_property
    def greenhouse_boards(self) -> list[str]:
        return _split_csv(self.greenhouse_board_tokens)

    @cached_property
    def lever_company_slugs(self) -> list[str]:
        return _split_csv(self.lever_companies)


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]

