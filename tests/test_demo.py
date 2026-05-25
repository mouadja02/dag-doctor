"""Smoke tests for demo fixtures and demo API endpoints."""

from __future__ import annotations

from airflow_copilot.demo_fixtures import (
    analyze_incident,
    generate_ticket_payload,
    get_incident,
    get_incidents,
    get_metrics,
    DEMO_INCIDENTS,
)


class TestDemoFixtures:
    def test_has_five_incidents(self):
        assert len(DEMO_INCIDENTS) == 5

    def test_each_incident_has_required_fields(self):
        required = {
            "id",
            "dag_id",
            "task_id",
            "run_id",
            "classification",
            "explanation",
            "evidence",
            "severity",
        }
        for inc in DEMO_INCIDENTS:
            assert required.issubset(inc.keys()), f"Missing fields in {inc['id']}"

    def test_each_incident_has_classification(self):
        for inc in DEMO_INCIDENTS:
            c = inc["classification"]
            assert c["failure_type"] in {
                "sql_error",
                "missing_dependency",
                "permissions_auth",
                "timeout",
                "infrastructure_resource",
            }
            assert 0 <= c["confidence"] <= 1

    def test_each_incident_has_evidence(self):
        for inc in DEMO_INCIDENTS:
            assert len(inc["evidence"]) >= 1
            assert inc["evidence"][0]["source_line"]

    def test_each_incident_has_severity(self):
        for inc in DEMO_INCIDENTS:
            assert inc["severity"] in ("high", "medium", "low")

    def test_get_incidents_returns_list(self):
        incidents = get_incidents()
        assert len(incidents) == 5
        assert "created_at" in incidents[0]

    def test_get_incident_by_valid_id(self):
        inc = get_incident("sql-001")
        assert inc is not None
        assert inc["dag_id"] == "demo_sql_error"

    def test_get_incident_by_invalid_id(self):
        assert get_incident("nonexistent") is None

    def test_analyze_incident_returns_full_result(self):
        result = analyze_incident("sql-001")
        assert result is not None
        assert result["dag_id"] == "demo_sql_error"
        assert result["classification"] is not None
        assert result["explanation"] is not None
        assert result["evidence"] is not None
        assert result["severity"] in ("high", "medium", "low")
        assert "report_markdown" in result

    def test_analyze_incident_unknown_id(self):
        assert analyze_incident("nonexistent") is None

    def test_generate_ticket_returns_payload(self):
        payload = generate_ticket_payload("sql-001")
        assert payload is not None
        assert payload["ticket_id"] == "JIRA-DD-0001"
        assert payload["platform"] == "Jira"
        assert payload["status"] == "created"
        p = payload["payload"]
        assert p["title"]
        assert p["description"]
        assert "dag-demo_sql_error" in p["labels"]
        assert p["assignee"] == "alice"

    def test_generate_ticket_unknown_id(self):
        assert generate_ticket_payload("nonexistent") is None

    def test_get_metrics(self):
        m = get_metrics()
        assert m["failed_today"] == 5
        assert m["recurring_candidates"] == 1
        assert m["total_reports"] == 5
        assert m["avg_diagnosis_time_seconds"] > 0

    def test_demo_mode_not_enabled_by_default(self):
        import os

        assert os.getenv("DEMO_MODE", "").lower() not in ("1", "true", "yes")
