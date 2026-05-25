"""Incident memory — link new failures to prior reports and known fixes."""

from __future__ import annotations

from airflow_copilot.database import get_session
from airflow_copilot.orm import AnalysisRecord


def find_similar_incidents(
    dag_id: str,
    task_id: str,
    failure_type: str,
    limit: int = 5,
) -> list[dict]:
    """Find prior incidents matching the same DAG, task, and failure type.

    Returns a list of prior reports ordered by recency.
    """
    with get_session() as session:
        records = (
            session.query(AnalysisRecord)
            .filter(
                AnalysisRecord.dag_id == dag_id,
                AnalysisRecord.task_id == task_id,
                AnalysisRecord.failure_type == failure_type,
            )
            .order_by(AnalysisRecord.created_at.desc())
            .limit(limit + 1)
            .all()
        )

    results = []
    for r in records:
        results.append(
            {
                "id": r.id,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "root_cause": (r.root_cause or "")[:300],
                "severity": r.severity,
                "remediation_steps": _parse_remediation(r.remediation_steps or ""),
            }
        )

    return results


def get_known_fix(dag_id: str, task_id: str, failure_type: str) -> dict | None:
    """Return the most recent successful remediation steps for a given failure pattern.

    Useful for "recommend the fix that worked last time."
    """
    with get_session() as session:
        records = (
            session.query(AnalysisRecord)
            .filter(
                AnalysisRecord.dag_id == dag_id,
                AnalysisRecord.task_id == task_id,
                AnalysisRecord.failure_type == failure_type,
            )
            .order_by(AnalysisRecord.created_at.desc())
            .limit(3)
            .all()
        )

    for r in records:
        steps = _parse_remediation(r.remediation_steps or "")
        if steps:
            return {
                "from_report_id": r.id,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "steps": steps,
                "source": "prior_incident_memory",
            }

    return None


def _parse_remediation(steps_raw: str) -> list[str]:
    """Parse remediation_steps from JSON string or plain text."""
    import json

    try:
        steps = json.loads(steps_raw)
        if isinstance(steps, list):
            return [str(s) for s in steps if s]
        return [str(steps)]
    except (json.JSONDecodeError, TypeError):
        if steps_raw.strip():
            return [steps_raw.strip()]
        return []
