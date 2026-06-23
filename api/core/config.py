from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    ocblacktop_api_base_url: str = "https://api.ocblacktop.com/v1"
    ocblacktop_api_key: str
    cors_allow_origins: str = "http://localhost:5173"
    port: int = 8000

    model_config = SettingsConfigDict(env_file="../.env")

    @property
    def cors_origins(self) -> list[str]:
        return [
            origin.strip()
            for origin in self.cors_allow_origins.split(",")
            if origin.strip()
        ]

@lru_cache
def get_settings() -> Settings:
    return Settings()