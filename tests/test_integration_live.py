"""End-to-end integration tests against the live Docker stack.

These tests validate the full pipeline against a running dag-doctor + Airflow instance.
LLM calls are mocked to avoid API costs. Tests skip gracefully if the stack is unavailable.

Run with: make test-integration
"""

from __future__ import annotations

import os
import time

import httpx
import pytest

API_URL = os.getenv("INTEGRATION_API_URL", "http://localhost:8000")
DASHBOARD_URL = os.getenv("INTEGRATION_DASHBOARD_URL", "http://localhost:8501")


def _stack_available() -> bool:
    """Check if the dag-doctor stack is running and healthy."""
    try:
        with httpx.Client(timeout=5.0) as client:
            resp = client.get(f"{API_URL}/health")
            return resp.status_code == 200
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _stack_available(),
    reason="Docker stack not running. Start with: make full-up",
)


@pytest.fixture(scope="module")
def api_client():
    return httpx.Client(base_url=API_URL, timeout=30.0)


class TestHealthEndpoint:
    def test_health_returns_ok(self, api_client):
        resp = api_client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] in ("ok", "degraded")
        assert isinstance(data["airflow_connected"], bool)
        assert isinstance(data["database_connected"], bool)

    def test_readiness_probe(self, api_client):
        resp = api_client.get("/health/ready")
        assert resp.status_code == 200
        assert resp.json()["ready"] is True

    def test_liveness_probe(self, api_client):
        resp = api_client.get("/health/live")
        assert resp.status_code == 200
        assert resp.json()["alive"] is True

    def test_metrics_endpoint(self, api_client):
        resp = api_client.get("/metrics")
        assert resp.status_code == 200
        assert "dagdoctor" in resp.text


class TestFailedRunsIntegration:
    @pytest.fixture(scope="class")
    def failed_runs(self, api_client):
        resp = api_client.get("/airflow/failed-runs", params={"limit": 50})
        assert resp.status_code == 200
        data = resp.json()
        assert "count" in data
        assert "failed_runs" in data
        return data["failed_runs"]

    def test_at_least_one_failed_run(self, failed_runs):
        assert len(failed_runs) > 0, "Expected at least 1 failed DAG run from Airflow"

    def test_run_has_required_fields(self, failed_runs):
        run = failed_runs[0]
        assert run["dag_id"]
        assert run["dag_run_id"]
        assert run["state"] == "failed"


class TestAnalyzePipeline:
    @pytest.fixture(scope="class")
    def failed_run_info(self, api_client):
        """Get a failed DAG run and its first failed task."""
        resp = api_client.get("/airflow/failed-runs", params={"limit": 5})
        runs = resp.json().get("failed_runs", [])
        assert runs, "No failed runs available"

        run = runs[0]
        dag_id = run["dag_id"]
        run_id = run["dag_run_id"]

        detail_resp = api_client.get(f"/airflow/failed-runs/{dag_id}/{run_id}")
        assert detail_resp.status_code == 200
        detail = detail_resp.json()

        failed_tis = [
            ti for ti in detail.get("task_instances", []) if ti.get("state") == "failed"
        ]
        assert failed_tis, f"No failed tasks found in run {dag_id}/{run_id}"

        return {
            "dag_id": dag_id,
            "dag_run_id": run_id,
            "task_id": failed_tis[0]["task_id"],
            "try_number": failed_tis[0].get("try_number", 1),
        }

    def test_analyze_sync(self, api_client, failed_run_info):
        params = {
            "dag_id": failed_run_info["dag_id"],
            "run_id": failed_run_info["dag_run_id"],
            "task_id": failed_run_info["task_id"],
            "try_number": failed_run_info["try_number"],
        }
        resp = api_client.post("/analyze", params=params, timeout=120.0)
        assert resp.status_code == 200
        data = resp.json()
        assert data["dag_id"] == failed_run_info["dag_id"]
        assert data["id"] is not None
        assert len(data["report_markdown"]) > 100
        assert data["classification"]["failure_type"]

    def test_analyze_async(self, api_client, failed_run_info):
        params = {
            "dag_id": failed_run_info["dag_id"],
            "run_id": failed_run_info["dag_run_id"],
            "task_id": failed_run_info["task_id"],
            "try_number": failed_run_info["try_number"],
        }
        resp = api_client.post("/analyze/async", params=params, timeout=30.0)
        assert resp.status_code == 200
        data = resp.json()
        assert data["job_id"] > 0
        assert data["status"] == "queued"

        job_id = data["job_id"]
        for _ in range(30):
            time.sleep(2)
            job_resp = api_client.get(f"/jobs/{job_id}")
            job = job_resp.json()
            if job.get("status") in ("succeeded", "failed"):
                break

        assert job.get("status") == "succeeded", (
            f"Job failed: {job.get('error_message')}"
        )
        assert job.get("result_id") is not None


class TestReportsIntegration:
    @pytest.fixture(scope="class")
    def reports(self, api_client):
        resp = api_client.get("/reports", params={"limit": 50})
        assert resp.status_code == 200
        return resp.json().get("reports", [])

    def test_reports_exist(self, reports):
        assert len(reports) > 0, "Expected at least 1 stored report"

    def test_reports_metrics(self, api_client):
        resp = api_client.get("/reports/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_reports"] > 0
        assert data.get("top_failure_category")


class TestIntelligenceEndpoints:
    def test_clusters_list(self, api_client):
        resp = api_client.get("/intelligence/clusters", params={"limit": 10})
        assert resp.status_code == 200
        data = resp.json()
        assert "clusters" in data
        for cluster in data["clusters"]:
            assert "signature" in cluster
            assert "dag_id" in cluster
            assert "count" in cluster
            assert cluster["count"] >= 2

    def test_recurrence_summary(self, api_client):
        resp = api_client.get("/intelligence/recurrence")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_clusters" in data
        assert data["total_recurring_failures"] >= 0

    def test_similar_incidents(self, api_client):
        resp = api_client.get(
            "/intelligence/similar",
            params={
                "dag_id": "demo_auth_error",
                "task_id": "broken_auth_task",
                "failure_type": "permissions_auth",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "similar_incidents" in data

    def test_ownership_stats(self, api_client):
        resp = api_client.get("/intelligence/ownership")
        assert resp.status_code == 200
        data = resp.json()
        assert "owner_counts" in data
        assert "top_owner" in data

    def test_prevention_recommendations(self, api_client):
        resp = api_client.get(
            "/intelligence/prevention",
            params={
                "dag_id": "demo_sql_error",
                "task_id": "broken_sql_task",
                "failure_type": "sql_error",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "recommendations" in data
        assert "suggested_checks" in data
        assert len(data["recommendations"]) > 0


class TestDemoModeEndpoints:
    def test_demo_incidents(self, api_client):
        resp = api_client.get("/demo/incidents")
        if resp.status_code == 404:
            pytest.skip("Demo mode not enabled")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] >= 1
