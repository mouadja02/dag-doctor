"""Preventive recommendations — suggest tests, checks, and tuning based on failure patterns."""

from __future__ import annotations

from airflow_copilot.database import get_session
from airflow_copilot.orm import AnalysisRecord


def get_preventive_recommendations(
    dag_id: str, task_id: str, failure_type: str
) -> dict:
    """Generate preventive recommendations for a given failure pattern.

    Based on the failure type and historical data, suggests:
    - Tests to add
    - Dependency checks
    - Timeout tuning
    - Schema contracts (for recurring schema drift)
    """
    recommendations: list[str] = []
    checks: list[str] = []

    if failure_type == "sql_error":
        recommendations.append(
            "Add a schema validation step before the extract task. "
            "Use `SHOW COLUMNS FROM {table}` or `DESCRIBE {table}` to verify column existence."
        )
        checks.append("Verify column names in SQL queries against current schema")
        checks.append(
            "Add integration test that runs the actual SQL query against a test schema"
        )

        # Schema contract for recurring schema drift
        with get_session() as session:
            repeat_count = (
                session.query(AnalysisRecord)
                .filter(
                    AnalysisRecord.dag_id == dag_id,
                    AnalysisRecord.task_id == task_id,
                    AnalysisRecord.failure_type == "sql_error",
                )
                .count()
            )
        if repeat_count >= 2:
            recommendations.append(
                "Consider adding a schema contract (dbt source freshness check, "
                "Snowflake schema validation, or column-level assertions) to catch "
                "schema drift before it breaks the pipeline."
            )

    elif failure_type == "missing_dependency":
        recommendations.append(
            "Add missing dependency to requirements.txt or the Airflow worker image."
        )
        checks.append("Verify requirements.txt matches all DAG imports")
        checks.append("Add `pip freeze` validation step in CI before deployment")

    elif failure_type == "timeout":
        recommendations.append(
            "Review historical task duration metrics and set execution_timeout "
            "to p95 duration + 50% buffer."
        )
        checks.append("Monitor upstream API latency metrics")
        checks.append("Add retry with exponential backoff for transient slowdowns")

    elif failure_type == "permissions_auth":
        recommendations.append(
            "Add credential validation step at DAG start (test connection). "
            + "Implement automatic alert N days before credential expiration."
        )
        checks.append("Verify connection credentials in Airflow UI")

    elif failure_type == "infrastructure_resource":
        recommendations.append(
            "Rewrite task to use chunked/streaming processing. "
            "Set memory limits and request appropriate worker resources."
        )
        checks.append("Profile memory usage of the task per input size")

    known_fix = None
    from airflow_copilot.incident_memory import get_known_fix

    prior_fix = get_known_fix(dag_id, task_id, failure_type)
    if prior_fix:
        known_fix = prior_fix

    return {
        "dag_id": dag_id,
        "task_id": task_id,
        "failure_type": failure_type,
        "recommendations": recommendations,
        "suggested_checks": checks,
        "known_fix_from_prior_incident": known_fix,
    }
