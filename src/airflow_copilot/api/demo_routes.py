"""Demo API routes — serve pre-seeded incident data when Airflow is unavailable."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from airflow_copilot.demo_fixtures import (
    analyze_incident,
    generate_ticket_payload,
    get_incident,
    get_incidents,
    get_metrics,
)

router = APIRouter(prefix="/demo", tags=["demo"])


@router.get("/status")
async def demo_status():
    return {"demo_mode": True, "reason": "Demo mode active — using pre-seeded fixtures"}


@router.get("/incidents")
async def list_demo_incidents():
    return {"count": len(get_incidents()), "failed_runs": get_incidents()}


@router.get("/incidents/{incident_id}")
async def get_demo_incident(incident_id: str):
    incident = get_incident(incident_id)
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")
    return incident


@router.post("/analyze")
async def analyze_demo_incident(incident_id: str):
    result = analyze_incident(incident_id)
    if not result:
        raise HTTPException(status_code=404, detail="Incident not found")
    return result


@router.post("/ticket")
async def create_demo_ticket(incident_id: str):
    payload = generate_ticket_payload(incident_id)
    if not payload:
        raise HTTPException(status_code=404, detail="Incident not found")
    return payload


@router.post("/reset")
async def reset_demo():
    return {"status": "ok", "message": "Demo data reset"}


@router.get("/metrics")
async def demo_metrics():
    return get_metrics()
