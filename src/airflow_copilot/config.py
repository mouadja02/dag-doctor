from __future__ import annotations

import os

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    airflow_base_url: str = "http://localhost:8080"
    airflow_username: str = "admin"
    airflow_password: str = "admin"

    llm_provider: str = "openrouter"
    openrouter_api_key: str = ""
    openrouter_model: str = "anthropic/claude-3.5-sonnet"

    log_volume_path: str = ""

    database_url: str = "sqlite:///data/dag_doctor.db"

    app_host: str = "0.0.0.0"
    app_port: int = 8000
    log_level: str = "INFO"

    demo_mode: bool = False
    llm_enabled: bool = True

    secret_key: str = "dev-secret-key-change-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 480

    allowed_origins: list[str] = ["http://localhost:8501", "http://localhost:3000"]

    rate_limit_requests: int = 100
    rate_limit_period_seconds: int = 60

    llm_redaction_level: str = "standard"

    retention_days: int = 90

    slack_webhook_url: str = ""
    jira_api_url: str = ""
    jira_email: str = ""
    jira_api_token: str = ""
    jira_project_key: str = "DD"

    github_token: str = ""
    github_repo: str = ""


def get_settings() -> Settings:
    settings = Settings()
    if settings.database_url.startswith("sqlite:///"):
        db_path = settings.database_url.replace("sqlite:///", "")
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
    return settings
