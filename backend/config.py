import os
from typing import Optional
from pydantic import BaseModel

class Settings(BaseModel):
    api_prefix: str = "/api"
    environment: str = os.getenv("ENV", "development")
    db_type: str = os.getenv("DB_TYPE", "excel")
    excel_path: str = os.getenv("EXCEL_PATH", "backend/database/prodwatch_database.xlsx")
    database_url: Optional[str] = os.getenv("DATABASE_URL")
    dashscope_api_key: Optional[str] = os.getenv("DASHSCOPE_API_KEY")

def get_settings() -> Settings:
    return Settings()
