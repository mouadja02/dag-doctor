"""Historical clustering — group recurring failures by signature and show trends."""

from __future__ import annotations

import hashlib
from collections import defaultdict
from datetime import datetime, timezone

from airflow_copilot.database import get_session
from airflow_copilot.orm import AnalysisRecord


def _build_signature(
    dag_id: str, task_id: str, failure_type: str, error_keywords: str = ""
) -> str:
    parts = [dag_id, task_id, failure_type]
    if error_keywords:
        parts.append(error_keywords[:200])
    return hashlib.md5("|".join(parts).encode()).hexdigest()[:12]


def get_clusters(org_id: int | None = None, limit: int = 20) -> list[dict]:
    with get_session() as session:
        session.expire_on_commit = False
        query = (
            session.query(AnalysisRecord)
            .order_by(AnalysisRecord.created_at.desc())
            .limit(5000)
        )
        if org_id:
            query = query.filter(AnalysisRecord.org_id == org_id)
        records = query.all()

        if not records:
            return []

        groups: dict[str, list] = defaultdict(list)
        for r in records:
            sig = _build_signature(
                r.dag_id or "",
                r.task_id or "",
                r.failure_type or "unknown",
                r.root_cause or "",
            )
            groups[sig].append(
                {
                    "id": r.id,
                    "dag_id": r.dag_id,
                    "task_id": r.task_id,
                    "failure_type": r.failure_type,
                    "severity": r.severity,
                    "created_at": r.created_at,
                    "root_cause": r.root_cause,
                }
            )

        clusters = []
        for sig, group in groups.items():
            if len(group) < 2:
                continue

            sorted_group = sorted(
                group,
                key=lambda r: (
                    r["created_at"] or datetime.min.replace(tzinfo=timezone.utc)
                ),
                reverse=True,
            )
            first = sorted_group[-1]
            last = sorted_group[0]

            cluster = {
                "signature": sig,
                "dag_id": first["dag_id"],
                "task_id": first["task_id"],
                "failure_type": first["failure_type"],
                "count": len(group),
                "first_seen": first["created_at"].isoformat()
                if first.get("created_at")
                else None,
                "last_seen": last["created_at"].isoformat()
                if last.get("created_at")
                else None,
                "severity": first["severity"],
                "trend": "flat",
                "recent_reports": [
                    {
                        "id": r["id"],
                        "created_at": r["created_at"].isoformat()
                        if r.get("created_at")
                        else None,
                        "severity": r["severity"],
                    }
                    for r in sorted_group[:5]
                ],
                "root_cause_snapshot": (first["root_cause"] or "")[:200],
            }

            if len(group) >= 3:
                recent = sorted_group[:3]
                older = (
                    sorted_group[3:6] if len(sorted_group) >= 6 else sorted_group[3:]
                )
                if older and recent:
                    recent_dates = [
                        r["created_at"] for r in recent if r.get("created_at")
                    ]
                    older_dates = [
                        r["created_at"] for r in older if r.get("created_at")
                    ]
                    if recent_dates and older_dates:
                        if max(recent_dates) > max(older_dates):
                            cluster["trend"] = "rising"
                        elif max(recent_dates) < max(older_dates):
                            cluster["trend"] = "declining"

            clusters.append(cluster)

        clusters.sort(key=lambda c: c["last_seen"] or "", reverse=True)
        return clusters[:limit]


def get_cluster_detail(signature: str) -> dict | None:
    clusters = get_clusters(limit=10000)
    for c in clusters:
        if c["signature"] == signature:
            return {"total_analyses": c["count"], **c}
    return None


def get_recurrence_summary() -> dict:
    clusters = get_clusters(limit=5000)
    if not clusters:
        return {
            "total_clusters": 0,
            "total_recurring_failures": 0,
            "most_frequent": None,
            "rising_clusters": 0,
        }

    total_failures = sum(c["count"] for c in clusters)
    most_frequent = max(clusters, key=lambda c: c["count"])
    rising = sum(1 for c in clusters if c["trend"] == "rising")

    return {
        "total_clusters": len(clusters),
        "total_recurring_failures": total_failures,
        "most_frequent": {
            "dag_id": most_frequent["dag_id"],
            "task_id": most_frequent["task_id"],
            "failure_type": most_frequent["failure_type"],
            "count": most_frequent["count"],
        },
        "rising_clusters": rising,
    }
