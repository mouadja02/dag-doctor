# Demo Guide

## Quick Start (Offline Demo Mode)

Run dag-doctor in demo mode with pre-seeded incident fixtures — no Airflow required.

```bash
make demo
```

Then open http://localhost:8501

**What happens**: The dashboard shows 5 pre-seeded incidents (SQL schema drift,
missing dependency, expired credential, upstream timeout, out-of-memory failure).
Everything works offline — no Airflow instance, no LLM API key needed.

## Demo Flow

1. **KPI Cards**: See aggregate metrics at a glance (Failed Today, Avg Diagnosis Time, Top Category, Recurring Candidates).

2. **Incident Queue**: Browse 5 incidents in a table with status, DAG name, task, owner, failure type, confidence, and severity.

3. **Incident Detail**: Click the magnifying glass on any incident to open the detail panel.

4. **Analyze**: Click "Analyze with dag-doctor" to see the full diagnosis:
   - Failure classification with confidence
   - Evidence snippet from the logs
   - Plain-English root cause
   - Safe remediation steps with [SAFE] / [REVIEW] tags
   - "What NOT to Do" warning panel

5. **Create Ticket**: Click "Create Ticket" to see the mocked Jira payload — title,
   description, labels, assignee, priority, and Airflow deep link.

6. **Download Report**: Export the incident report as Markdown.




## Resetting Demo Data

```bash
make reset-demo
```

Or click "Reset Demo Data" in the dashboard sidebar.

## Expected Results by Incident

| Incident | DAG | Classification | Confidence | Severity |
|----------|-----|---------------|------------|----------|
| sql-001 | `demo_sql_error` | SQL Error | 92% | Medium |
| dep-002 | `demo_import_error` | Missing Dependency | 95% | Low |
| auth-003 | `demo_auth_error` | Permissions/Auth | 95% | High |
| time-004 | `demo_timeout` | Timeout | 90% | High |
| oom-005 | `demo_oom_error` | OOM | 90% | High |

## Setup Options

| Goal | Command |
|------|---------|
| Demo (offline fixtures) | `make demo` |
| Local dev (with Airflow) | `make dev` |
| Docker (with Airflow) | `make docker-up` |
| Run tests | `make test` |
| Lint | `make lint` |
| Clean DB | `make clean` |
