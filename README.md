# dag-doctor

> **Stop digging through Airflow logs. Let an AI data engineer explain your failed DAG.**

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## The Problem

Data engineers waste hours digging through Airflow logs, scrolling past thousands of lines to find the one error that broke a DAG. SQL syntax errors, Python tracebacks, schema mismatches, timeouts, missing dependencies — the failure modes are endless, and the signals are buried in noise.

**dag-doctor** is an AI incident assistant that connects to your Airflow instance (dev/test only), fetches failed DAG runs, reads the logs, classifies the failure, explains the root cause in plain English, and generates a clean Markdown incident report — in seconds, not hours.

## Architecture

```
                   ┌──────────────────────────────────┐
                   │        Streamlit Dashboard        │
                   │           (port 8501)             │
                   │  ┌──────────┐  ┌──────────────┐  │
                   │  │ Browse   │  │ View Report  │  │
                   │  │ Failures │  │ & Download   │  │
                   │  └──────────┘  └──────────────┘  │
                   └──────────────┬───────────────────┘
                                  │ HTTP
                                  ▼
┌─────────────────────────────────────────────────────┐
│                  FastAPI Backend                     │
│                    (port 8000)                       │
│  ┌───────────┐  ┌────────┐  ┌─────────┐  ┌───────┐ │
│  │ Airflow   │  │ Log    │  │Failure  │  │ LLM   │ │
│  │ Client    │  │ Parser │  │Classifier│  │Layer  │ │
│  └─────┬─────┘  └───┬────┘  └────┬────┘  └───┬───┘ │
│        │            │            │            │     │
│        │            ▼            ▼            │     │
│        │     ┌─────────────┐  ┌──────────┐   │     │
│        │     │  Report     │  │ Storage  │   │     │
│        │     │  Generator  │  │ (SQLite) │   │     │
│        │     └─────────────┘  └──────────┘   │     │
└────────┼─────────────────────────────────────┼─────┘
         │                                     │
         ▼                                     ▼
┌──────────────────┐              ┌──────────────────┐
│  Airflow REST API│              │    OpenRouter    │
│  (Test Clone)    │              │    (LLM API)     │
│  localhost:8080  │              └──────────────────┘
│  Airflow 3.0.1   │
└──────────────────┘
```

### Pipeline Flow

```
Fetch Failed DAG Runs  →  Parse Task Logs  →  Classify Failure
                                                      ↓
Generate Report  ←  LLM Root Cause + Remediation  ←────┘
```

## Quick Start

### Prerequisites

- Python 3.10+
- Docker & Docker Compose (optional, for containerized dev)
- A **test clone** of Apache Airflow (never connect to production)
- An [OpenRouter API key](https://openrouter.ai/) (free tier available)

### 1. Clone & Configure

```bash
git clone https://github.com/mouadja02/dag-doctor.git
cd dag-doctor
cp .env.example .env
# Edit .env with your OpenRouter API key and test Airflow URL
```

### 2. Install & Run (Local)

```bash
pip install -e ".[dev]"
uvicorn airflow_copilot.main:app --reload &
streamlit run dashboard/app.py
```

### 3. Install & Run (Docker)

```bash
docker compose up --build
# API:    http://localhost:8000/docs
# Dashboard: http://localhost:8501
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check |
| `GET` | `/airflow/failed-runs` | List recent failed DAG runs |
| `GET` | `/airflow/failed-runs/{dag_id}/{run_id}` | Detail for a specific failed run |
| `POST` | `/analyze` | Run full analysis pipeline on a DAG run |
| `GET` | `/reports` | List stored analysis reports |
| `GET` | `/reports/{report_id}` | Get a specific report |

## Demo Flow

1. Start the test Airflow instance
2. Copy a broken DAG from `demo_dags/` into the test Airflow's `dags/` folder
3. Wait for the DAG to fail
4. Open the dag-doctor dashboard at `http://localhost:8501`
5. Click "Refresh" to see failed runs
6. Click a failed run to view the full incident report
7. Copy or download the Markdown report

### Demo DAGs Included

| DAG | Failure Type | What It Tests |
|-----|-------------|---------------|
| `sql_error_dag` | SQL Error | SQLAlchemy missing column |
| `python_exception_dag` | Python Exception | Runtime ValueError |
| `timeout_dag` | Timeout | `execution_timeout` exceeded |
| `import_error_dag` | Missing Dependency | `ModuleNotFoundError` |
| `auth_error_dag` | Permission/Auth | Bad Snowflake credentials |

## Sample Report

```markdown
# Airflow Failure Report

## Summary
The task `fetch_on_chain_data` in DAG `btc_updater_batch1` failed due to a SQL error:
column `market_cap_usd` does not exist in the source table.

## Failed DAG
| Field | Value |
|-------|-------|
| DAG ID | btc_updater_batch1 |
| Run ID | scheduled__2026-05-24T06:45:00 |
| Task ID | fetch_on_chain_data |
| Execution Date | 2026-05-24T06:45:00+00:00 |
| Try Number | 1 |

## Failure Classification
- **Type**: SQL Error
- **Confidence**: 0.95

## Likely Root Cause
The upstream data source renamed `market_cap_usd` to `market_cap` in a recent
schema change. The DAG's SQL query references the old column name.

## Suggested Safe Remediation
1. Update the column reference from `market_cap_usd` to `market_cap`
2. Verify the new column exists and has the same data type
3. Test the query in a Snowflake worksheet before deploying
4. Update any downstream consumers of this column

## What NOT to Do
- Do not ALTER the source table to add back the old column
- Do not rename the new column — other pipelines may depend on it
```

## Safety Boundaries

> **CRITICAL: This tool is designed for DEV/TEST Airflow instances only.**

| Rule | Enforcement |
|------|-------------|
| Never connect to production Airflow | Set `AIRFLOW_BASE_URL` to test clone only |
| Never mutate Airflow state | Only GET requests; no trigger/clear/backfill |
| Never auto-apply fixes | All remediation suggestions are read-only |
| Never expose credentials | Log parser redacts API keys and passwords |
| Never execute generated code | SQL/shell suggestions are display-only |

## Roadmap

- [ ] Slack webhook integration — auto-post incident reports to channels
- [ ] GitHub issue auto-creation — one-click "File Issue" from report
- [ ] Historical failure clustering — detect recurring patterns across runs
- [ ] Airflow plugin UI — embed dag-doctor directly in the Airflow dashboard
- [ ] Safe auto-remediation for known fix patterns (opt-in, with approval gate)
- [ ] Postgres support for multi-user deployments
- [ ] Custom classifier training on your own failure history

## Development

```bash
pip install -e ".[dev]"
pytest
```

## License

MIT — see [LICENSE](LICENSE) file.
