# Phase 0: Demo Hardening — Implementation Plan

Status date: 2026-05-25

## Overview

Phase 0 transforms dag-doctor from a developer-facing MVP into an executive-demo-ready
incident copilot. All changes are additive to the core pipeline; no existing behavior is
broken.

## Architecture Changes

```
Existing:
  Streamlit ──▶ FastAPI ──▶ Airflow Client ──▶ Airflow REST API
                      ──▶ LLM (OpenRouter)
                      ──▶ SQLite

Phase 0 additions:
  Streamlit ──▶ FastAPI ──▶ Demo Routes (when Airflow offline)
                      ──▶ Demo Fixtures (5 seeded incidents)
                      ──▶ /reports/metrics (KPI data)
                      ──▶ EvidenceItem model (log excerpts)
```

## Task List

### T1: Fix Dockerfile + Add Makefile

- [ ] Fix `Dockerfile` build order: copy `pyproject.toml` + `src/` before `pip install .`
- [ ] Add `HEALTHCHECK` for API container
- [ ] Create `Makefile` with targets: `dev`, `docker-up`, `docker-down`, `demo`, `test`, `lint`, `clean`, `reset-demo`

### T2: Demo Fixtures + Demo API Routes

- [ ] Create `src/airflow_copilot/demo_fixtures.py`: 5 seeded incidents mirroring existing demo DAGs
- [ ] Create `src/airflow_copilot/api/demo_routes.py`: `GET /demo/status`, `GET /demo/incidents`, `GET /demo/incidents/{id}`, `POST /demo/analyze`, `POST /demo/reset`

### T3: Dashboard Redesign

- [ ] KPI summary cards row (Failed Today, Avg Diag Time, Top Category, Recurring)
- [ ] Incident queue table with Status, DAG, Task, Owner, Class, Confidence, Action
- [ ] Incident detail view replacing expander pattern
- [ ] Evidence code block with exact log excerpts
- [ ] Safe remediation panel with severity badge and "What NOT to Do" warning
- [ ] Mock "Create Ticket" button (toast + expandable payload display)
- [ ] Demo mode indicator and settings panel in sidebar

### T4: Evidence & Remediation Panels

- [ ] Evidence display with signal type label and context lines
- [ ] Severity badges (High/Medium/Low) for remediation
- [ ] [SAFE] / [REVIEW] prefix tagging on remediation steps

### T5: API Changes

- [ ] Add `EvidenceItem` model to `models.py`
- [ ] Add `severity` field to `AnalysisResult`
- [ ] `/analyze` response includes evidence array
- [ ] New `GET /reports/metrics` endpoint for KPI data
- [ ] Update `report_generator.py` with real evidence + severity labels

### T6: App Settings

- [ ] Add `DEMO_MODE` and `LLM_ENABLED` to `Settings` config
- [ ] Conditionally include demo routes in `main.py`
- [ ] Settings UI in dashboard sidebar

### T7: Smoke Tests

- [ ] `tests/test_demo.py`: demo fixtures, demo endpoints, demo/reset, no LLM calls
- [ ] `tests/test_dashboard_integration.py`: API happy path in demo mode

### T8: Demo Script & Verification

- [ ] Update `docs/demo.md` with fixture-based offline flow
- [ ] Verify `make demo` starts everything correctly
- [ ] Verify all tests pass (existing 36 + new)

## Execution Order

```
Day 1: T1 + T2 + T5 (parallel, independent)
Day 2: T3 + T4 + T6 (depends on T2 + T5)
Day 3: T7 + T8 (depends on all above)
```

## Exit Criteria

- [ ] Demo runs offline from fixtures (no Airflow required)
- [ ] Docker startup path documented and verified
- [ ] No raw JSON or stack trace walls in the dashboard
- [ ] Each demo scenario produces clear root cause, evidence, and safe next action
- [ ] All tests pass (existing + new smoke tests)
