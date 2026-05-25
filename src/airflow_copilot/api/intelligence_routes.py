"""Intelligence API routes — clustering, incident memory, ownership, prevention."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from airflow_copilot.clustering import (
    get_cluster_detail,
    get_clusters,
    get_recurrence_summary,
)
from airflow_copilot.incident_memory import find_similar_incidents, get_known_fix
from airflow_copilot.ownership import get_owner_stats
from airflow_copilot.prevention import get_preventive_recommendations

router = APIRouter(prefix="/intelligence", tags=["intelligence"])


@router.get("/clusters")
def list_clusters(limit: int = Query(20, ge=1, le=100)):
    return {"clusters": get_clusters(limit=limit)}


@router.get("/clusters/{signature}")
def cluster_detail(signature: str):
    detail = get_cluster_detail(signature)
    if not detail:
        raise HTTPException(status_code=404, detail="Cluster not found")
    return detail


@router.get("/recurrence")
def recurrence_summary():
    return get_recurrence_summary()


@router.get("/similar")
def similar_incidents(
    dag_id: str,
    task_id: str,
    failure_type: str,
    limit: int = Query(5, ge=1, le=20),
):
    return {
        "similar_incidents": find_similar_incidents(
            dag_id, task_id, failure_type, limit
        ),
        "known_fix": get_known_fix(dag_id, task_id, failure_type),
    }


@router.get("/ownership")
def ownership_stats():
    return get_owner_stats()


@router.get("/prevention")
def prevention_recommendations(dag_id: str, task_id: str, failure_type: str):
    return get_preventive_recommendations(dag_id, task_id, failure_type)
