# dag-doctor — Implementation Plan

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Streamlit    │────▶│  FastAPI      │────▶│  SQLite      │
│  Dashboard    │     │  Backend      │     │  (reports)   │
│  :8501        │     │  :8000        │     └──────────────┘
└──────────────┘     └──────┬───────┘
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
        ┌──────────┐ ┌──────────┐ ┌──────────────┐
        │ Airflow  │ │ Log File │ │  OpenRouter  │
        │ REST API │ │ Reader   │ │  (LLM)       │
        │ :8080    │ │ (volume) │ │              │
        └──────────┘ └──────────┘ └──────────────┘
              │
              ▼
   ┌─────────────────────┐
   │ Airflow Test Clone  │
   │ (localhost:8080)    │
   │ Airflow 3.0.1       │
   └─────────────────────┘
```

## Status: MVP COMPLETE (25/25 tests passing)

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| LLM Provider | OpenRouter | OpenAI-compatible API, model flexibility, no extra SDK |
| Log Access | API first, file system fallback | API works always; volume read is faster for large logs |
| Demo DAGs | In copilot repo (`demo_dags/`) | Clean separation from Airflow test repo |
| Storage | SQLite for MVP | Zero setup, trivially swappable to Postgres later |
| Auth | Airflow SimpleAuthManager (Basic Auth) | Matches self-hosted config; admin:admin |

## Phase 1: Repo Skeleton & Config ✅
- [x] `pyproject.toml` with dependencies
- [x] `docker-compose.yml` (api, dashboard)
- [x] `.env.example`
- [x] `.gitignore`
- [x] Directory structure
- [x] README with hook, architecture, setup, safety, roadmap
- [x] `src/airflow_copilot/__init__.py`
- [x] `src/airflow_copilot/config.py` — Pydantic BaseSettings

## Phase 2: Airflow Client (Read-Only) ✅
- [x] `src/airflow_copilot/models.py` — Pydantic models
- [x] `src/airflow_copilot/airflow_client.py` — httpx client
- [x] Endpoints: list failed runs, get task instances, get task log

## Phase 3: Log Parser & Classifier ✅
- [x] `src/airflow_copilot/log_parser.py` — regex extraction
- [x] `src/airflow_copilot/classifier.py` — rule-based classification
- [x] Test fixture log files (5 failure types)
- [x] Unit tests (7 log parser + 9 classifier = 16 tests)

## Phase 4: LLM Explanation Layer ✅
- [x] `src/airflow_copilot/llm.py` — OpenRouter + fallback providers
- [x] Structured input/output via Pydantic
- [x] Graceful fallback when no API key

## Phase 5: Incident Report Generator ✅
- [x] `src/airflow_copilot/report_generator.py` — Markdown template
- [x] Unit tests (9 tests)

## Phase 6: FastAPI Backend ✅
- [x] `src/airflow_copilot/main.py` — FastAPI app
- [x] `src/airflow_copilot/api/routes.py` — 6 endpoints
- [x] `src/airflow_copilot/storage.py` — SQLAlchemy + SQLite
- [x] CORS middleware

## Phase 7: Streamlit Dashboard ✅
- [x] `dashboard/app.py` — single-page dashboard
- [x] Failed runs table → detail view → Markdown report
- [x] Download report button

## Phase 8: Demo DAGs ✅
- [x] `demo_dags/sql_error_dag.py`
- [x] `demo_dags/python_exception_dag.py`
- [x] `demo_dags/timeout_dag.py`
- [x] `demo_dags/import_error_dag.py`
- [x] `demo_dags/auth_error_dag.py`

## Safety Boundaries

- Never connect to production Airflow
- Never mutate Airflow state (no PATCH/POST to Airflow)
- Never auto-apply fixes
- Never log credentials
- Read-only GET access only
- `.env` in `.gitignore`

## Roadmap (Post-MVP)

- Slack webhook integration
- GitHub issue auto-creation
- Historical failure clustering
- Airflow plugin UI
- Safe auto-remediation for known fix patterns
- Postgres support
