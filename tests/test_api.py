"""Integration tests for the FastAPI routes with mocked Airflow API."""

from __future__ import annotations


import pytest
from fastapi.testclient import TestClient

from airflow_copilot.main import app
from airflow_copilot.database import init_db, get_session
from airflow_copilot.orm import AnalysisRecord

TEST_TOKEN = "eyJ.xxx.yyy"

AIRFLOW_URL = "http://localhost:8080"


def _mock_airflow_api(httpx_mock):
    """Register all Airflow API endpoints that the client will call."""
    httpx_mock.add_response(
        url=f"{AIRFLOW_URL}/api/v2/version",
        json={"version": "3.0.1", "git_version": ".dev0"},
    )
    httpx_mock.add_response(
        url=f"{AIRFLOW_URL}/auth/token",
        method="POST",
        json={"access_token": TEST_TOKEN},
    )
    httpx_mock.add_response(
        url=f"{AIRFLOW_URL}/api/v2/dags",
        json={
            "dags": [
                {"dag_id": "demo_sql_error"},
                {"dag_id": "demo_timeout"},
            ],
            "total_entries": 2,
        },
    )
    httpx_mock.add_response(
        url=f"{AIRFLOW_URL}/api/v2/dags/demo_sql_error/dagRuns?limit=50&state=failed",
        json={
            "dag_runs": [
                {
                    "dag_id": "demo_sql_error",
                    "dag_run_id": "manual__2026-05-25T00:00:00",
                    "logical_date": "2026-05-25T00:00:00Z",
                    "start_date": "2026-05-25T00:01:00Z",
                    "end_date": "2026-05-25T00:02:00Z",
                    "state": "failed",
                    "run_type": "manual",
                    "conf": {},
                }
            ],
            "total_entries": 1,
        },
    )
    httpx_mock.add_response(
        url=f"{AIRFLOW_URL}/api/v2/dags/demo_timeout/dagRuns?limit=50&state=failed",
        json={"dag_runs": [], "total_entries": 0},
    )
    httpx_mock.add_response(
        url=f"{AIRFLOW_URL}/api/v2/dags/demo_sql_error/dagRuns/manual__2026-05-25T00:00:00/taskInstances",
        json={
            "task_instances": [
                {
                    "task_id": "broken_query",
                    "dag_id": "demo_sql_error",
                    "dag_run_id": "manual__2026-05-25T00:00:00",
                    "logical_date": "2026-05-25T00:00:00Z",
                    "start_date": "2026-05-25T00:01:00Z",
                    "end_date": "2026-05-25T00:02:00Z",
                    "duration": 60.0,
                    "state": "failed",
                    "try_number": 1,
                    "max_tries": 2,
                    "operator": "PythonOperator",
                    "hostname": "worker-1",
                }
            ],
            "total_entries": 1,
        },
    )
    httpx_mock.add_response(
        url=f"{AIRFLOW_URL}/api/v2/dags/demo_sql_error/dagRuns/manual__2026-05-25T00:00:00",
        json={
            "dag_id": "demo_sql_error",
            "dag_run_id": "manual__2026-05-25T00:00:00",
            "logical_date": "2026-05-25T00:00:00Z",
            "start_date": "2026-05-25T00:01:00Z",
            "end_date": "2026-05-25T00:02:00Z",
            "state": "failed",
            "run_type": "manual",
            "conf": {},
        },
    )
    httpx_mock.add_response(
        url=(
            f"{AIRFLOW_URL}/api/v2/dags/demo_sql_error/dagRuns/"
            f"manual__2026-05-25T00:00:00/taskInstances/broken_query/logs/1"
        ),
        json={
            "content": [
                {"event": "DAG bundles loaded", "level": "info"},
                {
                    "event": "Task failed with exception",
                    "level": "error",
                    "error_detail": [
                        {
                            "exc_type": "ProgrammingError",
                            "exc_value": "column 'market_cap_usd' does not exist",
                            "exc_notes": [],
                            "syntax_error": None,
                            "is_cause": False,
                            "frames": [
                                {
                                    "filename": "/opt/airflow/dags/sql_error_dag.py",
                                    "lineno": 13,
                                    "name": "broken_sql_task",
                                }
                            ],
                        }
                    ],
                },
            ]
        },
    )


@pytest.fixture
def client():
    init_db()
    return TestClient(app)


@pytest.fixture(autouse=True)
def clean_db():
    init_db()
    with get_session() as session:
        session.query(AnalysisRecord).delete()
        session.commit()
    yield


@pytest.mark.httpx_mock(
    assert_all_responses_were_requested=False,
    assert_all_requests_were_expected=False,
)
class TestHealth:
    def test_health_ok(self, client, httpx_mock):
        _mock_airflow_api(httpx_mock)
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["airflow_connected"] is True

    def test_health_degraded_when_airflow_down(self, client, httpx_mock):
        httpx_mock.add_response(
            url=f"{AIRFLOW_URL}/api/v2/version",
            status_code=503,
        )
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "degraded"


@pytest.mark.httpx_mock(
    assert_all_responses_were_requested=False,
    assert_all_requests_were_expected=False,
)
class TestFailedRuns:
    def test_lists_failed_runs(self, client, httpx_mock):
        _mock_airflow_api(httpx_mock)
        resp = client.get("/airflow/failed-runs")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1
        run = data["failed_runs"][0]
        assert run["dag_id"] == "demo_sql_error"
        assert run["state"] == "failed"

    def test_handles_airflow_unavailable(self, client, httpx_mock):
        httpx_mock.add_response(
            url=f"{AIRFLOW_URL}/api/v2/auth/token",
            method="POST",
            status_code=503,
        )
        httpx_mock.add_response(
            url=f"{AIRFLOW_URL}/auth/token",
            method="POST",
            status_code=503,
        )
        resp = client.get("/airflow/failed-runs")
        assert resp.status_code == 502


@pytest.mark.httpx_mock(
    assert_all_responses_were_requested=False,
    assert_all_requests_were_expected=False,
)
class TestFailedRunDetail:
    def test_gets_run_detail_with_task_instances(self, client, httpx_mock):
        _mock_airflow_api(httpx_mock)
        resp = client.get(
            "/airflow/failed-runs/demo_sql_error/manual__2026-05-25T00:00:00"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["dag_run"]["dag_id"] == "demo_sql_error"
        assert data["failed_task_count"] == 1
        assert len(data["task_instances"]) == 1

    def test_404_for_unknown_run(self, client, httpx_mock):
        _mock_airflow_api(httpx_mock)
        httpx_mock.add_response(
            url=f"{AIRFLOW_URL}/api/v2/dags/unknown/dagRuns/nonexistent",
            status_code=404,
            json={"detail": "Not found"},
        )
        resp = client.get("/airflow/failed-runs/unknown/nonexistent")
        assert resp.status_code == 404


@pytest.mark.httpx_mock(
    assert_all_responses_were_requested=False,
    assert_all_requests_were_expected=False,
)
class TestAnalyze:
    def test_analyze_full_pipeline(self, client, httpx_mock):
        _mock_airflow_api(httpx_mock)
        params = {
            "dag_id": "demo_sql_error",
            "run_id": "manual__2026-05-25T00:00:00",
            "task_id": "broken_query",
            "try_number": 1,
        }
        resp = client.post("/analyze", params=params)
        assert resp.status_code == 200
        data = resp.json()
        assert data["dag_id"] == "demo_sql_error"
        assert data["classification"]["failure_type"] == "sql_error"
        assert data["id"] is not None
        assert len(data["report_markdown"]) > 200

    def test_404_when_log_not_found(self, client, httpx_mock):
        _mock_airflow_api(httpx_mock)
        httpx_mock.add_response(
            url=(
                f"{AIRFLOW_URL}/api/v2/dags/demo_sql_error/dagRuns/"
                f"manual__2026-05-25T00:00:00/taskInstances/missing_task/logs/1"
            ),
            status_code=404,
            json={"detail": "Not found"},
        )
        resp = client.post(
            "/analyze",
            params={
                "dag_id": "demo_sql_error",
                "run_id": "manual__2026-05-25T00:00:00",
                "task_id": "missing_task",
                "try_number": 1,
            },
        )
        assert resp.status_code == 404


@pytest.mark.httpx_mock(
    assert_all_responses_were_requested=False,
    assert_all_requests_were_expected=False,
)
class TestReports:
    def test_lists_stored_reports(self, client, httpx_mock):
        _mock_airflow_api(httpx_mock)
        client.post(
            "/analyze",
            params={
                "dag_id": "demo_sql_error",
                "run_id": "manual__2026-05-25T00:00:00",
                "task_id": "broken_query",
                "try_number": 1,
            },
        )
        resp = client.get("/reports")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] >= 1
        report = data["reports"][0]
        assert report["dag_id"] == "demo_sql_error"
        assert report["id"] is not None

    def test_gets_report_by_id(self, client, httpx_mock):
        _mock_airflow_api(httpx_mock)
        create_resp = client.post(
            "/analyze",
            params={
                "dag_id": "demo_sql_error",
                "run_id": "manual__2026-05-25T00:00:00",
                "task_id": "broken_query",
                "try_number": 1,
            },
        )
        report_id = create_resp.json()["id"]

        resp = client.get(f"/reports/{report_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["dag_id"] == "demo_sql_error"
        assert data["id"] == report_id

    def test_404_for_unknown_report(self, client):
        resp = client.get("/reports/99999")
        assert resp.status_code == 404
