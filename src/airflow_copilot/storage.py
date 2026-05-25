"""Storage layer — SQLite-backed persistence for analysis results."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import cast

from sqlalchemy import Column, DateTime, Float, Integer, String, Text, create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from airflow_copilot.config import get_settings
from airflow_copilot.models import AnalysisResult, FailureClassification, LLMExplanation


class Base(DeclarativeBase):
    pass


class AnalysisRecord(Base):
    __tablename__ = "analyses"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dag_id = Column(String(250), nullable=False)
    dag_run_id = Column(String(250), nullable=False)
    task_id = Column(String(250), nullable=False)
    logical_date = Column(String(100), nullable=True)
    try_number = Column(Integer, default=1)
    failure_type = Column(String(100), default="unknown")
    confidence = Column(Float, default=0.0)
    summary = Column(Text, default="")
    root_cause = Column(Text, default="")
    remediation_steps = Column(Text, default="")
    what_not_to_do = Column(Text, default="")
    report_markdown = Column(Text, default="")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    def to_result(self) -> AnalysisResult:
        import json

        steps_str: str = cast(str, self.remediation_steps) or ""
        dont_str: str = cast(str, self.what_not_to_do) or ""
        log_date: str = cast(str, self.logical_date) or ""
        dag: str = cast(str, self.dag_id)
        dag_run: str = cast(str, self.dag_run_id)
        task: str = cast(str, self.task_id)
        ftype: str = cast(str, self.failure_type)
        _conf = self.confidence
        _tn = self.try_number
        conf: float = float(_conf) if _conf is not None else 0.0  # type: ignore[arg-type]
        summary: str = cast(str, self.summary) or ""
        rc: str = cast(str, self.root_cause) or ""
        report: str = cast(str, self.report_markdown) or ""
        tn: int = int(_tn) if _tn is not None else 1  # type: ignore[arg-type]

        try:
            steps = json.loads(steps_str)
        except (json.JSONDecodeError, TypeError):
            steps = [steps_str] if steps_str else []

        try:
            dont = json.loads(dont_str)
        except (json.JSONDecodeError, TypeError):
            dont = [dont_str] if dont_str else []

        logical = None
        if log_date:
            try:
                logical = datetime.fromisoformat(log_date)
            except ValueError:
                pass

        return AnalysisResult(
            id=cast(int, self.id),
            dag_id=dag,
            dag_run_id=dag_run,
            task_id=task,
            logical_date=logical,
            try_number=tn,
            classification=FailureClassification(
                failure_type=ftype,
                confidence=conf,
            ),
            explanation=LLMExplanation(
                summary=summary,
                root_cause=rc,
                confidence=conf,
                remediation_steps=steps,
                what_not_to_do=dont,
            ),
            report_markdown=report,
            created_at=cast(datetime, self.created_at),
        )


class Storage:
    def __init__(self) -> None:
        settings = get_settings()
        self.engine = create_engine(
            settings.database_url,
            connect_args={"check_same_thread": False}
            if "sqlite" in settings.database_url
            else {},
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def save(self, result: AnalysisResult) -> int:
        import json

        record = AnalysisRecord(
            dag_id=result.dag_id,
            dag_run_id=result.dag_run_id,
            task_id=result.task_id,
            logical_date=result.logical_date.isoformat()
            if result.logical_date
            else None,
            try_number=result.try_number,
            failure_type=result.classification.failure_type
            if result.classification
            else "unknown",
            confidence=result.classification.confidence
            if result.classification
            else 0.0,
            summary=result.explanation.summary if result.explanation else "",
            root_cause=result.explanation.root_cause if result.explanation else "",
            remediation_steps=json.dumps(result.explanation.remediation_steps)
            if result.explanation
            else "[]",
            what_not_to_do=json.dumps(result.explanation.what_not_to_do)
            if result.explanation
            else "[]",
            report_markdown=result.report_markdown,
        )
        with self.Session() as session:
            session.add(record)
            session.commit()
            return cast(int, record.id)

    def get_all(self, limit: int = 50) -> list[AnalysisResult]:
        with self.Session() as session:
            records = (
                session.query(AnalysisRecord)
                .order_by(AnalysisRecord.created_at.desc())
                .limit(limit)
                .all()
            )
            return [r.to_result() for r in records]

    def get_by_run(self, dag_id: str, run_id: str) -> list[AnalysisResult]:
        with self.Session() as session:
            records = (
                session.query(AnalysisRecord)
                .filter(
                    AnalysisRecord.dag_id == dag_id,
                    AnalysisRecord.dag_run_id == run_id,
                )
                .order_by(AnalysisRecord.created_at.desc())
                .all()
            )
            return [r.to_result() for r in records]

    def get(self, record_id: int) -> AnalysisResult | None:
        with self.Session() as session:
            record = session.get(AnalysisRecord, record_id)
            return record.to_result() if record else None
