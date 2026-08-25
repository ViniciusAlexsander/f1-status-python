from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR = Path(__file__).resolve().parents[2]
API_DIR = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    ocblacktop_api_base_url: str = "https://api.ocblacktop.com/v1"
    ocblacktop_api_key: str
    cors_allow_origins: str = "http://localhost:5173"
    formula1_subscription_token: str | None = None
    livetiming_auth_file: Path | None = None
    livetiming_signalr_connection_url: str = (
        "wss://livetiming.formula1.com/signalrcore"
    )
    livetiming_signalr_negotiate_url: str = (
        "https://livetiming.formula1.com/signalrcore/negotiate"
    )
    livetiming_signalr_topics: str = "TimingData,SessionData"
    redis_url: str = Field(..., alias="REDIS_URL")
    port: int = 8000

    model_config = SettingsConfigDict(
        env_file=(ROOT_DIR / ".env", API_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def cors_origins(self) -> list[str]:
        return [
            origin.strip()
            for origin in self.cors_allow_origins.split(",")
            if origin.strip()
        ]

    @property
    def signalr_topics(self) -> list[str]:
        return [
            topic.strip()
            for topic in self.livetiming_signalr_topics.split(",")
            if topic.strip()
        ]


@lru_cache
def get_settings() -> Settings:
    return Settings()
