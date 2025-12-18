import os
from pydantic import BaseModel


class Settings(BaseModel):
    api_prefix: str = "/api"
    environment: str = os.getenv("ENV", "development")
    # future: database_url, redis_url, etc.


def get_settings() -> Settings:
    return Settings()
