from __future__ import annotations

import os
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # --- Airflow (TEST CLONE ONLY) ---
    airflow_base_url: str = "http://localhost:8080"
    airflow_username: str = "admin"
    airflow_password: str = "admin"

    # --- LLM ---
    llm_provider: str = "openrouter"
    openrouter_api_key: str = ""
    openrouter_model: str = "anthropic/claude-3.5-sonnet"

    # --- Local log volume ---
    log_volume_path: str = ""

    # --- Database ---
    database_url: str = "sqlite:///data/dag_doctor.db"

    # --- App ---
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    log_level: str = "INFO"


def get_settings() -> Settings:
    settings = Settings()
    # Ensure data directory exists
    if settings.database_url.startswith("sqlite:///"):
        db_path = settings.database_url.replace("sqlite:///", "")
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
    return settings
