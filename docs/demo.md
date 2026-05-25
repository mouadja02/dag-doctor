# Demo Guide

## Prerequisites

1. A **test clone** of your Airflow instance running locally (Airflow 3.x)
2. dag-doctor services running (API + Dashboard)
3. OpenRouter API key configured (optional; fallback works without it)

## Step-by-Step Demo

### 1. Start the test Airflow instance

```bash
cd /path/to/airflow-self-hosted-test
docker compose up -d
```

### 2. Add demo DAGs to test Airflow

```bash
cp dag-doctor/demo_dags/*.py /path/to/airflow-self-hosted-test/dags/
```

Wait ~30 seconds for Airflow to pick up the new DAGs.

### 3. Trigger each demo DAG (manually)

In the Airflow UI (http://localhost:8080):
- Unpause each `demo_*` DAG
- Trigger a manual run
- Wait for the DAG to fail (1-5 seconds)

Or via CLI:
```bash
docker compose exec airflow-scheduler airflow dags unpause demo_sql_error
docker compose exec airflow-scheduler airflow dags trigger demo_sql_error
```

### 4. Start dag-doctor

```bash
cd dag-doctor
docker compose up --build
# Or locally:
pip install -e ".[dev]"
uvicorn airflow_copilot.main:app --reload &
streamlit run dashboard/app.py
```

### 5. Open the dashboard

Navigate to http://localhost:8501

1. Click **Refresh** to see the failed DAG runs
2. Click **Analyze** on a failed task
3. View the classification, root cause, and remediation steps
4. Download the Markdown incident report

## Expected Results by DAG

| Demo DAG | Failure Type | Classification |
|----------|-------------|----------------|
| `demo_sql_error` | SQL Error | `sql_error` (~0.95) |
| `demo_python_exception` | Python Exception | `python_exception` (~0.50) |
| `demo_timeout` | Timeout | `timeout` (~0.90) |
| `demo_import_error` | Missing Dependency | `missing_dependency` (~0.95) |
| `demo_auth_error` | Auth/Permissions | `permissions_auth` (~0.90) |
