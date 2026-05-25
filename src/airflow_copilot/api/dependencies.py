"""FastAPI dependency injection — replaces import-time global singletons."""

from __future__ import annotations

from airflow_copilot.airflow_client import AirflowClient
from airflow_copilot.config import get_settings
from airflow_copilot.llm import get_llm_provider
from airflow_copilot.orm import AirflowEnvironment


def get_airflow_client(env: AirflowEnvironment | None = None) -> AirflowClient:
    """Create an Airflow client for a specific environment."""
    if env:
        return AirflowClient(
            base_url=env.base_url,
            username=env.username,
            password=env.password_encrypted,
        )
    settings = get_settings()
    return AirflowClient(
        base_url=settings.airflow_base_url,
        username=settings.airflow_username,
        password=settings.airflow_password,
    )


def get_llm():
    return get_llm_provider()
