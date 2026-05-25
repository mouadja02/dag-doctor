"""SQLAlchemy ORM models for dag-doctor enterprise schema."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Organization(Base):
    __tablename__ = "organizations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(250), nullable=False)
    slug = Column(String(100), unique=True, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    is_active = Column(Boolean, default=True)

    users = relationship("User", back_populates="organization")
    environments = relationship("AirflowEnvironment", back_populates="organization")


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(320), unique=True, nullable=False, index=True)
    hashed_password = Column(String(128), nullable=False)
    display_name = Column(String(200), default="")
    role = Column(String(50), default="viewer", nullable=False)
    org_id = Column(Integer, ForeignKey("organizations.id"), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    organization = relationship("Organization", back_populates="users")


class AirflowEnvironment(Base):
    __tablename__ = "airflow_environments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    org_id = Column(Integer, ForeignKey("organizations.id"), nullable=False)
    name = Column(String(250), nullable=False)
    env_type = Column(String(20), default="dev", nullable=False)
    base_url = Column(String(500), nullable=False)
    username = Column(String(250), default="")
    password_encrypted = Column(String(500), default="")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    organization = relationship("Organization", back_populates="environments")


class AnalysisRecord(Base):
    __tablename__ = "analyses"

    id = Column(Integer, primary_key=True, autoincrement=True)
    org_id = Column(Integer, ForeignKey("organizations.id"), nullable=True)
    env_id = Column(Integer, ForeignKey("airflow_environments.id"), nullable=True)
    dag_id = Column(String(250), nullable=False)
    dag_run_id = Column(String(250), nullable=False)
    task_id = Column(String(250), nullable=False)
    logical_date = Column(String(100), nullable=True)
    try_number = Column(Integer, default=1)
    failure_type = Column(String(100), default="unknown")
    confidence = Column(Float, default=0.0)
    severity = Column(String(20), default="medium")
    summary = Column(Text, default="")
    root_cause = Column(Text, default="")
    remediation_steps = Column(Text, default="")
    what_not_to_do = Column(Text, default="")
    report_markdown = Column(Text, default="")
    evidence_json = Column(Text, default="[]")
    classifier_details = Column(Text, default="{}")
    model_metadata = Column(Text, default="{}")
    llm_prompt = Column(Text, default="")
    llm_response_id = Column(String(250), default="")
    token_count = Column(Integer, default=0)
    redaction_summary = Column(Text, default="{}")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    def to_result(self):
        from airflow_copilot.models import (
            AnalysisResult,
            EvidenceItem,
            FailureClassification,
            LLMExplanation,
        )
        from datetime import datetime as dt

        try:
            evidence_raw = json.loads(self.evidence_json or "[]")
            evidence = [EvidenceItem(**e) for e in evidence_raw]
        except (json.JSONDecodeError, TypeError):
            evidence = []

        try:
            cdetails = json.loads(self.classifier_details or "{}")
        except (json.JSONDecodeError, TypeError):
            cdetails = {}

        try:
            steps_raw = json.loads(self.remediation_steps or "[]")
            steps = steps_raw if isinstance(steps_raw, list) else [str(steps_raw)]
        except (json.JSONDecodeError, TypeError):
            steps = [self.remediation_steps] if self.remediation_steps else []

        try:
            dont_raw = json.loads(self.what_not_to_do or "[]")
            dont = dont_raw if isinstance(dont_raw, list) else [str(dont_raw)]
        except (json.JSONDecodeError, TypeError):
            dont = [self.what_not_to_do] if self.what_not_to_do else []

        logical = None
        if self.logical_date:
            try:
                logical = dt.fromisoformat(self.logical_date)
            except (ValueError, TypeError):
                pass

        return AnalysisResult(
            id=self.id,
            dag_id=self.dag_id or "",
            dag_run_id=self.dag_run_id or "",
            task_id=self.task_id or "",
            logical_date=logical,
            try_number=int(self.try_number) if self.try_number else 1,
            classification=FailureClassification(
                failure_type=self.failure_type or "unknown",
                confidence=float(self.confidence) if self.confidence else 0.0,
                details=cdetails,
            ),
            explanation=LLMExplanation(
                summary=self.summary or "",
                root_cause=self.root_cause or "",
                confidence=float(self.confidence) if self.confidence else 0.0,
                remediation_steps=steps,
                what_not_to_do=dont,
            ),
            report_markdown=self.report_markdown or "",
            severity=self.severity or "medium",
            evidence=evidence,
            owner="",
            created_at=self.created_at,
        )


class AuditEvent(Base):
    __tablename__ = "audit_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    org_id = Column(Integer, ForeignKey("organizations.id"), nullable=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    action = Column(String(100), nullable=False)
    resource_type = Column(String(100), default="")
    resource_id = Column(String(250), default="")
    details_json = Column(Text, default="{}")
    ip_address = Column(String(50), default="")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class RetentionPolicy(Base):
    __tablename__ = "retention_policies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    org_id = Column(
        Integer, ForeignKey("organizations.id"), unique=True, nullable=False
    )
    max_days = Column(Integer, default=90)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class AnalysisJob(Base):
    __tablename__ = "analysis_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dag_id = Column(String(250), nullable=False)
    dag_run_id = Column(String(250), nullable=False)
    task_id = Column(String(250), nullable=False)
    try_number = Column(Integer, default=1)
    status = Column(String(20), default="queued", nullable=False)
    progress = Column(String(100), default="")
    result_id = Column(Integer, ForeignKey("analyses.id"), nullable=True)
    error_message = Column(Text, default="")
    provider = Column(String(50), default="fallback")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
