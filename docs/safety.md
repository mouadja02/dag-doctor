# Safety Boundaries

## Non-Negotiable Rules

### 1. Never Connect to Production Airflow
- `AIRFLOW_BASE_URL` must always point to a dev/test clone
- The copilot makes no distinction between environments — it assumes everything it sees is safe to analyze
- If you accidentally point to production, you risk exposing sensitive log data to the LLM provider

### 2. Read-Only Access Only
- The AirflowClient uses **only** HTTP GET requests
- No POST/PATCH/DELETE operations against the Airflow API
- No DAG triggering, clearing, backfilling, or state mutation

### 3. No Auto-Remediation
- All suggestions are **display-only**
- The system never executes SQL, shell commands, or API calls based on LLM output
- Generated remediation steps must be manually reviewed and applied by a human

### 4. Credential Redaction
- The log parser automatically redacts:
  - API keys (e.g., `sk-...`)
  - Authorization headers
  - Password/key=value patterns
- Credentials are redacted **before** any log content reaches the LLM

### 5. No Hardcoded Secrets
- All configuration via environment variables
- `.env` is in `.gitignore`
- `.env.example` contains only placeholders

## What If You Find a Security Issue?

Report it privately. Do not open a public GitHub issue for security vulnerabilities.
