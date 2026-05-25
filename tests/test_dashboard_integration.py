"""Integration tests for demo API endpoints using TestClient."""

from __future__ import annotations

import pytest

from fastapi.testclient import TestClient

from airflow_copilot.api.demo_routes import router
from fastapi import FastAPI


@pytest.fixture
def demo_client():
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


class TestDemoStatus:
    def test_demo_status(self, demo_client):
        resp = demo_client.get("/demo/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["demo_mode"] is True


class TestDemoIncidents:
    def test_list_incidents(self, demo_client):
        resp = demo_client.get("/demo/incidents")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 5
        assert len(data["failed_runs"]) == 5

    def test_get_incident(self, demo_client):
        resp = demo_client.get("/demo/incidents/sql-001")
        assert resp.status_code == 200
        data = resp.json()
        assert data["dag_id"] == "demo_sql_error"

    def test_get_incident_404(self, demo_client):
        resp = demo_client.get("/demo/incidents/nonexistent")
        assert resp.status_code == 404


class TestDemoAnalyze:
    def test_analyze_incident(self, demo_client):
        resp = demo_client.post("/demo/analyze", params={"incident_id": "sql-001"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["dag_id"] == "demo_sql_error"
        assert data["classification"] is not None
        assert data["explanation"] is not None
        assert data["severity"] in ("high", "medium", "low")

    def test_analyze_incident_404(self, demo_client):
        resp = demo_client.post("/demo/analyze", params={"incident_id": "nonexistent"})
        assert resp.status_code == 404


class TestDemoTicket:
    def test_create_ticket(self, demo_client):
        resp = demo_client.post("/demo/ticket", params={"incident_id": "sql-001"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ticket_id"] == "JIRA-DD-0001"

    def test_create_ticket_404(self, demo_client):
        resp = demo_client.post("/demo/ticket", params={"incident_id": "nonexistent"})
        assert resp.status_code == 404


class TestDemoReset:
    def test_reset(self, demo_client):
        resp = demo_client.post("/demo/reset")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


class TestDemoMetrics:
    def test_metrics(self, demo_client):
        resp = demo_client.get("/demo/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert data["failed_today"] == 5
        assert data["avg_diagnosis_time_seconds"] > 0
