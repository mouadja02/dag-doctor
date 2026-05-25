"""Ownership intelligence — infer owner from DAG tags, Airflow metadata, and DAG owners."""

from __future__ import annotations

from airflow_copilot.models import DAGRun


def infer_owner(dag_run: DAGRun, dag_owner: str = "") -> str:
    if dag_owner:
        return dag_owner
    return infer_owner_from_dag_id(dag_run.dag_id or "")


def infer_owner_from_dag_id(dag_id: str) -> str:
    tag_prefix_map = {
        "finance": "finance-team",
        "marketing": "marketing-team",
        "sales": "sales-team",
        "engineering": "eng-team",
        "ml": "ml-team",
        "analytics": "analytics-team",
        "etl": "data-engineering",
        "data": "data-engineering",
        "demo": "platform-team",
        "test": "qa-team",
    }
    for tag, owner in tag_prefix_map.items():
        if tag in dag_id.lower():
            return owner
    return "unassigned"


def get_owner_stats() -> dict:
    from airflow_copilot.database import get_session
    from airflow_copilot.orm import AnalysisRecord

    with get_session() as session:
        session.expire_on_commit = False
        records = (
            session.query(AnalysisRecord)
            .order_by(AnalysisRecord.created_at.desc())
            .limit(200)
            .all()
        )
        owners: dict[str, int] = {}
        for r in records:
            owner = infer_owner_from_dag_id(r.dag_id or "")
            owners[owner] = owners.get(owner, 0) + 1

    return {
        "owner_counts": dict(sorted(owners.items(), key=lambda x: x[1], reverse=True)),
        "top_owner": max(owners, key=owners.get) if owners else "N/A",
    }
