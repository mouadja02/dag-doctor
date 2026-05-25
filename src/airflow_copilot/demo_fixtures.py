"""Demo fixtures — pre-seeded incident data for offline demo mode."""

from __future__ import annotations

from datetime import datetime, timezone

DEMO_INCIDENTS = [
    {
        "id": "sql-001",
        "dag_id": "demo_sql_error",
        "task_id": "extract_market_data",
        "run_id": "manual__2025-03-15T08-00-00",
        "logical_date": "2025-03-15T08:00:00",
        "state": "failed",
        "owner": "alice",
        "status": "open",
        "classification": {
            "failure_type": "sql_error",
            "confidence": 0.92,
            "details": {
                "exception_type": "ProgrammingError",
                "sql_error": "ProgrammingError: Column 'market_cap_usd' does not exist",
                "signals_found": {"sql_error": 0.85, "python_exception": 0.50},
            },
        },
        "evidence": [
            {
                "source_line": "snowflake.connector.errors.ProgrammingError: 000904 (42000): SQL compilation error: error line 1 at position 76\ninvalid identifier 'MARKET_CAP_USD'",
                "context_lines": [
                    "Traceback (most recent call last):",
                    '  File "/opt/airflow/dags/demo_sql_error.py", line 22, in query_market_data',
                    "    cursor.execute('SELECT market_cap_usd FROM crypto_prices')",
                    "snowflake.connector.errors.ProgrammingError: 000904 (42000): SQL compilation error: error line 1 at position 76",
                ],
                "signal_type": "sql_error",
            },
        ],
        "explanation": {
            "summary": "The task failed because the SQL query references a column 'market_cap_usd' that no longer exists in the 'crypto_prices' table. This is a schema drift issue.",
            "root_cause": "A recent schema migration renamed or removed the 'market_cap_usd' column in the 'crypto_prices' table without updating the downstream DAG. The Snowflake query compiler rejected the unknown identifier at parse time.",
            "confidence": 0.92,
            "remediation_steps": [
                "Verify the current schema of 'crypto_prices' table (SHOW COLUMNS or DESCRIBE TABLE)",
                "Update the SQL query to use the correct column name (check migration changelog)",
                "Add a schema validation step before the extract task to catch drift early",
            ],
            "what_not_to_do": [
                "Do not add the column back without understanding why it was removed",
                "Do not skip the error — other downstream DAGs may also be affected",
            ],
        },
        "report_markdown": "Full report generated for demo_sql_error...",
        "severity": "medium",
        "is_recurring": False,
        "recurrence_count": 0,
    },
    {
        "id": "dep-002",
        "dag_id": "demo_import_error",
        "task_id": "run_web3_pipeline",
        "run_id": "manual__2025-03-15T09-30-00",
        "logical_date": "2025-03-15T09:30:00",
        "state": "failed",
        "owner": "bob",
        "status": "open",
        "classification": {
            "failure_type": "missing_dependency",
            "confidence": 0.95,
            "details": {
                "exception_type": "ModuleNotFoundError",
                "missing_module": "nonexistent_package_xyz",
                "signals_found": {"missing_dependency": 0.95},
            },
        },
        "evidence": [
            {
                "source_line": "ModuleNotFoundError: No module named 'nonexistent_package_xyz'",
                "context_lines": [
                    "Traceback (most recent call last):",
                    '  File "/opt/airflow/dags/demo_import_error.py", line 13, in <module>',
                    "    import nonexistent_package_xyz",
                    "ModuleNotFoundError: No module named 'nonexistent_package_xyz'",
                ],
                "signal_type": "missing_dependency",
            },
        ],
        "explanation": {
            "summary": "The DAG failed to import because a required Python package 'nonexistent_package_xyz' is not installed in the Airflow worker environment.",
            "root_cause": "The DAG references an external package that was either not listed in the requirements file, not installed during worker provisioning, or the package name was misspelled.",
            "confidence": 0.95,
            "remediation_steps": [
                "Check if 'nonexistent_package_xyz' should be in requirements.txt or the Airflow image Dockerfile",
                "If the package is deprecated/renamed, update the import to the new name",
                "Rebuild the Airflow worker image with the corrected dependencies",
            ],
            "what_not_to_do": [
                "Do not modify the DAG to skip the import — the task logic depends on it",
                "Do not install the package directly on the running worker without updating the image",
            ],
        },
        "report_markdown": "Full report generated for demo_import_error...",
        "severity": "low",
        "is_recurring": False,
        "recurrence_count": 0,
    },
    {
        "id": "auth-003",
        "dag_id": "demo_auth_error",
        "task_id": "load_to_snowflake",
        "run_id": "manual__2025-03-16T06-15-00",
        "logical_date": "2025-03-16T06:15:00",
        "state": "failed",
        "owner": "carol",
        "status": "open",
        "classification": {
            "failure_type": "permissions_auth",
            "confidence": 0.95,
            "details": {
                "exception_type": "DatabaseError",
                "auth_error_source": "Snowflake",
                "signals_found": {"permissions_auth": 0.90, "python_exception": 0.50},
            },
        },
        "evidence": [
            {
                "source_line": "snowflake.connector.errors.DatabaseError: 250001 (08001): Incorrect username or password",
                "context_lines": [
                    "Traceback (most recent call last):",
                    '  File "/opt/airflow/dags/demo_auth_error.py", line 18, in load_data',
                    "    conn = snowflake.connector.connect(user=USER, password=PASSWORD, account=ACCOUNT)",
                    "snowflake.connector.errors.DatabaseError: 250001 (08001): Incorrect username or password",
                ],
                "signal_type": "Snowflake",
            },
        ],
        "explanation": {
            "summary": "The Snowflake connection failed due to expired or incorrect credentials. This is likely a credential rotation issue where the Airflow connection was not updated after a password change.",
            "root_cause": "The Snowflake service account password was rotated per policy but the corresponding Airflow connection 'snowflake_default' still holds the old credential.",
            "confidence": 0.95,
            "remediation_steps": [
                "Update the Snowflake password in the Airflow connection 'snowflake_default'",
                "Verify the connection works from the Airflow UI (Admin > Connections > Test)",
                "Check if the Snowflake account has a credential rotation policy — set a reminder to update Airflow connection on next rotation",
            ],
            "what_not_to_do": [
                "Do not hardcode credentials in the DAG file",
                "Do not disable the task — data loading will silently stop working",
            ],
        },
        "report_markdown": "Full report generated for demo_auth_error...",
        "severity": "high",
        "is_recurring": True,
        "recurrence_count": 3,
        "recurrence_signature": "snowflake_credential_expiry",
    },
    {
        "id": "time-004",
        "dag_id": "demo_timeout",
        "task_id": "slow_api_call",
        "run_id": "manual__2025-03-16T11-00-00",
        "logical_date": "2025-03-16T11:00:00",
        "state": "failed",
        "owner": "dave",
        "status": "open",
        "classification": {
            "failure_type": "timeout",
            "confidence": 0.90,
            "details": {
                "exception_type": "AirflowTaskTimeout",
                "signals_found": {"timeout": 0.90},
            },
        },
        "evidence": [
            {
                "source_line": "airflow.exceptions.AirflowTaskTimeout: Task timed out after 5.0 seconds. Received SIGTERM.",
                "context_lines": [
                    "Traceback (most recent call last):",
                    '  File "/opt/airflow/dags/demo_timeout.py", line 22, in slow_operation',
                    "    time.sleep(120)",
                    "airflow.exceptions.AirflowTaskTimeout: Task timed out after 5.0 seconds. Received SIGTERM.",
                ],
                "signal_type": "timeout",
            },
        ],
        "explanation": {
            "summary": "The task exceeded its 5-second execution_timeout while waiting for an external API call. The upstream service was experiencing latency.",
            "root_cause": "The task has an execution_timeout of 5 seconds but the downstream API consistently takes longer than that. The API may be degraded or the timeout value is too aggressive for normal operation.",
            "confidence": 0.90,
            "remediation_steps": [
                "Check the upstream API status and latency metrics in your observability platform",
                "Increase execution_timeout to a realistic value based on p95 API latency + buffer",
                "Add retry logic with exponential backoff for transient API slowdowns",
            ],
            "what_not_to_do": [
                "Do not blindly increase the timeout to a very high value without understanding the root cause",
                "Do not remove the timeout entirely — it protects against hanging tasks",
            ],
        },
        "report_markdown": "Full report generated for demo_timeout...",
        "severity": "high",
        "is_recurring": False,
        "recurrence_count": 0,
    },
    {
        "id": "oom-005",
        "dag_id": "demo_oom_error",
        "task_id": "process_large_dataset",
        "run_id": "manual__2025-03-17T14-00-00",
        "logical_date": "2025-03-17T14:00:00",
        "state": "failed",
        "owner": "eve",
        "status": "open",
        "classification": {
            "failure_type": "infrastructure_resource",
            "confidence": 0.90,
            "details": {
                "exception_type": "MemoryError",
                "signals_found": {
                    "infrastructure_resource": 0.90,
                    "python_exception": 0.50,
                },
            },
        },
        "evidence": [
            {
                "source_line": "MemoryError: Unable to allocate 8.0 GiB for an array",
                "context_lines": [
                    "Traceback (most recent call last):",
                    '  File "/opt/airflow/dags/demo_oom_error.py", line 20, in process_data',
                    "    df = pd.read_sql('SELECT * FROM large_table', conn)",
                    "MemoryError: Unable to allocate 8.0 GiB for an array",
                ],
                "signal_type": "oom",
            },
        ],
        "explanation": {
            "summary": "The task exhausted available worker memory while loading a large dataset into a pandas DataFrame. The worker's memory allocation was insufficient for the full table scan.",
            "root_cause": "The task attempted to load an entire database table into memory using `pd.read_sql` without chunking or filtering. The worker node has insufficient RAM for the full dataset size.",
            "confidence": 0.90,
            "remediation_steps": [
                "Rewrite the task to use chunked reads (pd.read_sql with chunksize parameter)",
                "Add a WHERE clause to filter data before loading (e.g., process incremental data)",
                "Increase worker memory allocation if chunked reads are not feasible",
                "Consider using a Spark or Polars-based approach for large datasets",
            ],
            "what_not_to_do": [
                "Do not just increase worker memory without fixing the data loading strategy — the dataset will continue to grow",
                "Do not SELECT * without a LIMIT or filtering clause",
            ],
        },
        "report_markdown": "Full report generated for demo_oom_error...",
        "severity": "high",
        "is_recurring": False,
        "recurrence_count": 0,
    },
]

NOW = datetime.now(timezone.utc)


def get_incidents():
    """Return demo incidents with current timestamps."""
    incidents = []
    for i, inc in enumerate(DEMO_INCIDENTS):
        copy = dict(inc)
        copy["created_at"] = NOW.isoformat()
        copy["reported_at"] = NOW.isoformat()
        incidents.append(copy)
    return incidents


def get_incident(incident_id: str):
    """Return a single demo incident by ID."""
    for inc in DEMO_INCIDENTS:
        if inc["id"] == incident_id:
            copy = dict(inc)
            copy["created_at"] = NOW.isoformat()
            return copy
    return None


def analyze_incident(incident_id: str):
    """Return pre-computed analysis for a demo incident (no LLM call)."""
    incident = get_incident(incident_id)
    if not incident:
        return None

    return {
        "id": incident_id,
        "dag_id": incident["dag_id"],
        "dag_run_id": incident["run_id"],
        "task_id": incident["task_id"],
        "logical_date": incident.get("logical_date"),
        "try_number": 1,
        "classification": incident["classification"],
        "explanation": incident["explanation"],
        "retrieved_log": "",
        "report_markdown": (
            f"# Airflow Failure Report\n\n"
            f"## Summary\n\n"
            f"**{incident['classification']['failure_type'].replace('_', ' ').title()}**: "
            f"{incident['explanation']['summary']}\n\n"
            f"## Evidence from Logs\n\n"
            f"```\n{incident['evidence'][0]['source_line']}\n```\n\n"
            f"## Likely Root Cause\n\n"
            f"{incident['explanation']['root_cause']}\n\n"
            f"## Suggested Safe Remediation\n\n"
            f"**Severity**: {'🔴' if incident['severity'] == 'high' else '🟡' if incident['severity'] == 'medium' else '🟢'} "
            f"{incident['severity'].upper()}\n\n"
        ),
        "severity": incident["severity"],
        "evidence": incident["evidence"],
        "owner": incident["owner"],
        "created_at": NOW.isoformat(),
        "is_recurring": incident.get("is_recurring", False),
        "recurrence_count": incident.get("recurrence_count", 0),
    }


def get_metrics():
    """Return aggregate KPI metrics from demo incidents."""
    total = len(DEMO_INCIDENTS)
    category_counts: dict[str, int] = {}
    for inc in DEMO_INCIDENTS:
        ft = inc["classification"]["failure_type"]
        category_counts[ft] = category_counts.get(ft, 0) + 1

    top_category = (
        max(category_counts, key=lambda k: category_counts[k])
        if category_counts
        else "N/A"
    )
    recurring = sum(1 for inc in DEMO_INCIDENTS if inc.get("is_recurring"))

    return {
        "failed_today": total,
        "avg_diagnosis_time_seconds": 42,
        "top_failure_category": top_category.replace("_", " ").title(),
        "recurring_candidates": recurring,
        "total_reports": total,
    }


def generate_ticket_payload(incident_id: str):
    """Generate a mock Jira ticket payload for demo purposes."""
    incident = get_incident(incident_id)
    if not incident:
        return None

    classification = incident["classification"]
    explanation = incident.get("explanation", {})

    ticket_id = f"JIRA-DD-{incident_id.split('-')[-1].zfill(4)}"

    return {
        "ticket_id": ticket_id,
        "platform": "Jira",
        "status": "created",
        "payload": {
            "title": f"[{classification['failure_type'].replace('_', ' ').title()}] {incident['dag_id']} - {incident['task_id']}",
            "description": (
                f"## Root Cause\n{explanation.get('root_cause', 'N/A')}\n\n"
                f"## Evidence\n```\n{incident['evidence'][0]['source_line']}\n```\n\n"
                f"## Classification\n- Type: {classification['failure_type']}\n- Confidence: {classification['confidence']:.0%}\n\n"
                f"## Remediation\n"
                + "\n".join(f"- {s}" for s in explanation.get("remediation_steps", []))
                + f"\n\n## DAG Details\n"
                f"- DAG: {incident['dag_id']}\n"
                f"- Task: {incident['task_id']}\n"
                f"- Run: {incident['run_id']}\n"
                f"- Severity: {incident['severity'].upper()}"
            ),
            "labels": [
                classification["failure_type"],
                f"severity-{incident['severity']}",
                f"dag-{incident['dag_id']}",
            ],
            "assignee": incident.get("owner", "unassigned"),
            "priority": {"high": "P1", "medium": "P2", "low": "P3"}.get(
                incident["severity"], "P2"
            ),
            "airflow_run_url": f"https://airflow.example.com/dags/{incident['dag_id']}/grid?dag_run_id={incident['run_id']}",
        },
    }
