"""Pydantic models for Airflow entities and internal analysis results."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class DAGRun(BaseModel):
    dag_id: str
    dag_run_id: str
    logical_date: datetime | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    state: str = ""
    run_type: str = ""
    conf: dict[str, Any] = Field(default_factory=dict)


class TaskInstance(BaseModel):
    task_id: str
    dag_id: str
    dag_run_id: str
    logical_date: datetime | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    duration: float | None = None
    state: str = ""
    try_number: int = 1
    max_tries: int = 1
    operator: str = ""
    hostname: str = ""


class FailedTask(BaseModel):
    dag_id: str
    dag_run_id: str
    task_id: str
    logical_date: datetime | None = None
    try_number: int = 1
    operator: str = ""
    error_log: str = ""
    log_file_path: str = ""


class FailureClassification(BaseModel):
    failure_type: str
    confidence: float = Field(ge=0.0, le=1.0)
    details: dict[str, Any] = Field(default_factory=dict)


class LLMExplanation(BaseModel):
    summary: str
    root_cause: str
    confidence: float = Field(ge=0.0, le=1.0)
    remediation_steps: list[str] = Field(default_factory=list)
    what_not_to_do: list[str] = Field(default_factory=list)


class AnalysisResult(BaseModel):
    id: int | None = None
    dag_id: str
    dag_run_id: str
    task_id: str
    logical_date: datetime | None = None
    try_number: int = 1
    classification: FailureClassification | None = None
    explanation: LLMExplanation | None = None
    report_markdown: str = ""
    created_at: datetime | None = None
