# dag-doctor Enterprise Roadmap

Status date: 2026-05-25

## Executive Summary

dag-doctor is currently a strong MVP for explaining failed Airflow DAGs from a dev or test Airflow instance. It already has a coherent pipeline: fetch failed DAG runs, parse task logs, classify failures, generate an LLM or fallback explanation, store the report, and show it in a Streamlit dashboard.

The next goal is to turn the MVP into an enterprise-grade incident copilot for data engineering teams. That means moving from "single-user demo utility" to "trusted workflow system" with secure tenancy, auditability, operational reliability, measurable diagnosis quality, integrations, and a polished executive demo.

## Current Version Assessment

### What Exists

- FastAPI backend exposing health, failed run browsing, task analysis, and report retrieval endpoints.
- Airflow 3.x REST client with JWT authentication and read-only DAG/task/log fetching after authentication.
- Regex-based log parser for SQL, timeout, auth, import, connection, OOM, and schema signals.
- Rule-based classifier with confidence scores.
- Optional OpenRouter LLM explanation layer with fallback templates when no API key is configured.
- Markdown incident report generator.
- SQLite persistence for generated analyses.
- Streamlit dashboard for browsing failed runs, launching analysis, viewing reports, and downloading Markdown.
- Demo DAGs for SQL, Python exception, timeout, missing dependency, and auth failures.
- Test suite: 36 tests passing.
- Coverage snapshot: 89% total backend coverage. Lowest coverage is the LLM layer at 71%.

### Enterprise Gaps

- No app authentication, user identity, RBAC, SSO, or tenant isolation.
- CORS is wide open.
- Global backend clients are initialized at import time, making runtime configuration, testing, and multi-tenant operation harder.
- Analysis is synchronous inside the API request path; no job queue, retries, cancellation, progress state, or durable task orchestration.
- SQLite has no migration system and is not suitable for shared enterprise deployments.
- Stored records do not preserve full structured evidence, classifier details, model metadata, prompts, token usage, or audit events.
- LLM safety is basic: simple truncation, plain JSON parsing, limited redaction, no prompt-injection controls, no model evaluation harness, no PII policy, no private model mode.
- Airflow support is narrow: Airflow 3.x JWT path is assumed, and failed run discovery scans DAGs one by one.
- Dashboard is useful but not executive-grade: no incident queue, trend analytics, team workflow, SLA impact, recurring issue detection, or business value framing.
- Deployment is dev-oriented. Docker build path should be verified and hardened; the current Dockerfile installs the package before copying `src/`.
- No observability: metrics, traces, structured logs, health detail, readiness probes, and audit logs are missing.
- No enterprise integrations yet: Slack, Teams, GitHub, Jira, PagerDuty, Datadog, OpenTelemetry, dbt, Snowflake, BigQuery, or lineage tools.

## Product North Star

dag-doctor should become the incident command center for Airflow failures:

1. Detect a failed pipeline.
2. Explain the root cause with evidence.
3. Estimate blast radius and business impact.
4. Recommend safe next actions.
5. Create or update the team workflow artifact.
6. Learn from recurrence.

The executive story should be: "We reduced Airflow incident triage from 30-60 minutes of log reading to a 60-second evidence-backed diagnosis, while keeping humans in control of all fixes."

## Demo Goal For CEO, CTO, And CMO

The demo should not look like a developer toy. It should tell a business story:

- A production-like data pipeline fails.
- dag-doctor detects the failure and shows an incident queue.
- The app explains root cause, evidence, impact, and safe remediation.
- It identifies whether the failure is recurring.
- It creates a Slack/Jira/GitHub artifact.
- It shows governance: no secrets leaked, no state mutated, every AI action audited.
- It closes with measurable value: mean time to diagnosis, recurring failure rate, and affected pipelines.

## Roadmap Overview

### Phase 0: Demo Hardening

Goal: Make the existing app reliable, credible, and polished enough for an executive demo.

Deliverables:

- Fix Docker packaging order and verify `docker compose up --build`.
- Add a one-command demo script or Makefile for starting API, dashboard, and demo Airflow.
- Add seeded demo mode so the dashboard can work even if Airflow is unavailable.
- Replace raw `st.json` views with a polished incident detail layout.
- Add an incident queue with status, DAG, task, owner, failure class, confidence, duration, and "Analyze" action.
- Add business-facing summary cards:
  - Failed DAGs today.
  - Average diagnosis time.
  - Top failure category.
  - Recurring failure candidates.
- Add a "Root cause evidence" panel that shows the relevant traceback, SQL error, or missing dependency line.
- Add a "Safe remediation" panel with severity and risk labels.
- Add a "Create ticket" mocked button for demo value even before real integration is built.
- Add demo fixtures for five polished scenarios:
  - SQL schema drift.
  - Missing Python dependency.
  - Expired Snowflake credential.
  - Timeout from upstream API latency.
  - Out-of-memory worker failure.
- Add basic app settings to disable external LLM calls during demos if needed.
- Add smoke tests for dashboard/API happy path.

Exit criteria:

- Demo can run offline from fixtures.
- Docker or local startup path is documented and verified.
- No raw stack trace wall as the first thing executives see.
- Every demo scenario produces a clear root cause, evidence snippet, and safe next action.

### Phase 1: Enterprise Foundations

Goal: Build the trust layer required before any enterprise team connects real Airflow metadata or logs.

Deliverables:

- Authentication:
  - OAuth/OIDC support.
  - SSO-ready identity model.
  - Session management.
- Authorization:
  - RBAC roles: viewer, responder, admin.
  - Per-Airflow-environment permissions.
  - Read-only enforcement visible in the UI.
- Multi-tenancy:
  - Organization/workspace model.
  - Environment model: dev, staging, prod mirror.
  - Tenant-scoped reports, settings, users, and integrations.
- Security:
  - Replace wildcard CORS with configured allowed origins.
  - Add request validation and rate limiting.
  - Add secret scanning beyond basic regex: connection URIs, JWTs, cloud keys, private keys, emails, and IP-sensitive values.
  - Add configurable "do not send to LLM" policies.
  - Add audit log for every analysis, report view, export, and integration action.
- Database:
  - Move from SQLite to Postgres for shared deployments.
  - Add Alembic migrations.
  - Store structured JSON evidence, classifier details, model metadata, prompts, response IDs, token counts, and redaction summaries.
- Configuration:
  - Per-environment settings instead of import-time globals.
  - Secrets via environment variables or secret manager.
  - Health/readiness endpoints split by dependency.
- Compliance posture:
  - Data retention settings.
  - Report deletion/export.
  - Basic SOC2-aligned audit trail design.

Exit criteria:

- Multiple teams can use the app without seeing each other's data.
- Security posture can be explained confidently to a CTO.
- Every AI-generated answer is traceable to inputs, model config, and evidence.

### Phase 2: Reliable Analysis Engine

Goal: Make diagnosis quality measurable, repeatable, and robust across real-world Airflow failures.

Deliverables:

- Background analysis jobs:
  - Queue-based workflow using Celery, Dramatiq, RQ, or equivalent.
  - Job states: queued, running, succeeded, failed, cancelled.
  - Retries with backoff for Airflow and LLM calls.
  - Idempotency keys for repeated analyze requests.
- Evidence extraction:
  - Preserve source log offsets.
  - Extract traceback frame, SQL query fragment, exception type, operator, connection ID when available, and upstream failed task.
  - Add provider-specific extractors for Postgres, Snowflake, BigQuery, dbt, Spark, KubernetesPodOperator, and PythonOperator.
- Classification improvements:
  - Hierarchical taxonomy: category, subtype, likely owner, severity.
  - Confidence calibration using test fixtures and real labeled examples.
  - Multi-signal ranking instead of only highest weighted score.
  - Unknown/fallback workflow that asks for human labeling.
- LLM reliability:
  - Use strict structured output schema where provider supports it.
  - Add JSON repair and validation.
  - Add prompt-injection defenses: quote logs as untrusted evidence, never follow instructions from logs.
  - Add model/provider abstraction for OpenAI, Azure OpenAI, Anthropic, Bedrock, Vertex AI, OpenRouter, and local/private models.
  - Add cost and token budget controls.
- Evaluation harness:
  - Golden dataset of failed task logs.
  - Metrics: classification accuracy, root cause usefulness, hallucination rate, unsafe suggestion rate, redaction recall.
  - Regression tests for every new failure pattern.

Exit criteria:

- Diagnosis quality is measured, not assumed.
- Analysis jobs are durable and observable.
- The team can show a trend line proving quality improvement.

### Phase 3: Workflow Integrations

Goal: Put dag-doctor inside the tools data teams already use.

Deliverables:

- Slack and Microsoft Teams:
  - Post incident summaries.
  - Interactive buttons: assign, acknowledge, open report, create ticket.
  - Thread updates when analysis completes.
- GitHub and Jira:
  - One-click issue creation.
  - Include root cause, evidence, remediation, affected DAG, and links.
  - De-duplicate against existing open issues for recurring failures.
- PagerDuty/Opsgenie:
  - Attach analysis to incidents.
  - Escalate only high-confidence/high-impact failures.
- Airflow plugin:
  - Add "Analyze with dag-doctor" action inside Airflow task instance view.
  - Deep links from dag-doctor back to Airflow DAG run and task logs.
- Data stack integrations:
  - dbt artifacts for model ownership and lineage.
  - OpenLineage/Marquez for downstream impact.
  - Snowflake/BigQuery metadata checks for schema drift.
  - Git provider lookup to find DAG owner and recent code changes.
- Webhooks/API:
  - Stable public API for external automation.
  - Webhook events for analysis completed, recurring failure detected, ticket created.

Exit criteria:

- dag-doctor reduces handoff friction, not just log reading.
- Incidents move from detection to owner/ticket/action in one workflow.

### Phase 4: Enterprise Operations

Goal: Run dag-doctor like a serious internal platform or commercial SaaS.

Deliverables:

- Deployment:
  - Production Docker image.
  - Helm chart.
  - Kubernetes readiness/liveness probes.
  - Horizontal scaling for API and workers.
  - External Postgres and Redis support.
- Observability:
  - Structured JSON logs.
  - Prometheus metrics.
  - OpenTelemetry traces.
  - Dashboards for analysis latency, queue depth, failure rates, provider errors, token cost, and redaction events.
- Reliability:
  - Circuit breakers for LLM and Airflow APIs.
  - Timeout budgets.
  - Graceful degradation to rule-based analysis.
  - Backup and restore for Postgres.
- Governance:
  - Admin dashboard for providers, retention, redaction, users, audit logs, and integrations.
  - Approval gates for any future remediation action.
  - Environment allowlist and production connection warnings.
- Performance:
  - Concurrent Airflow fetching with pagination.
  - Caching DAG and owner metadata.
  - Log chunking and summarization pipeline for large logs.
  - Bulk analysis for incident storms.
- Testing:
  - CI pipeline with unit, integration, lint, type check, security scan, and Docker build.
  - Contract tests against Airflow 2.x and 3.x API variants.
  - Load tests for large DAG fleets.

Exit criteria:

- The app can support many teams, many Airflow instances, and high incident volume.
- Operators can debug dag-doctor itself quickly.

### Phase 5: Differentiated Intelligence

Goal: Move from "explain this failure" to "prevent repeated pipeline failures."

Deliverables:

- Historical clustering:
  - Group recurring failures by signature, DAG, owner, error text, and code path.
  - Show first seen, last seen, frequency, and trend.
- Incident memory:
  - Link new failures to prior reports and known fixes.
  - Recommend the fix that worked last time.
- Blast radius:
  - Show downstream DAGs, dashboards, datasets, and business processes likely affected.
  - Severity score based on schedule, data freshness SLA, and lineage.
- Ownership intelligence:
  - Infer owner from DAG tags, code owners, Git history, dbt metadata, or Airflow owners.
  - Route reports automatically.
- Preventive recommendations:
  - Suggest tests to add.
  - Suggest missing dependency checks.
  - Suggest timeout tuning based on runtime history.
  - Suggest schema contracts for recurring schema drift.
- Safe remediation design:
  - Start with generated pull request suggestions, not direct mutation.
  - Require human approval.
  - Include tests and rollback notes.

Exit criteria:

- dag-doctor can show measurable reduction in repeated failures.
- The product has a clear moat beyond wrapping an LLM around logs.

## CEO, CTO, CMO Demo Script

### Opening

"This is dag-doctor, an AI incident assistant for Airflow. When a data pipeline fails, it reads the failed run, extracts the evidence, identifies likely root cause, and generates a safe action plan. The goal is to reduce triage time from tens of minutes to under a minute."

### Demo Flow

1. Show the incident queue with multiple failed DAGs.
2. Select a SQL schema drift failure.
3. Show the classification, confidence, and evidence line.
4. Show the plain-English root cause.
5. Show safe remediation and "what not to do."
6. Show recurring failure detection for the same column/schema issue.
7. Click "Create Jira/GitHub issue" or show the mocked integration payload.
8. Show audit panel: who analyzed it, when, which model, what evidence, what was redacted.
9. Show metrics: diagnosis time, top failure categories, repeated failures.

### Message By Audience

CEO:

- Less downtime for data-dependent decisions.
- Faster incident response without adding headcount.
- Repeatable knowledge capture across teams.

CTO:

- Read-only by design.
- Auditable AI output.
- Tenant isolation, RBAC, private model option, and measurable evals.

CMO:

- Clear positioning: "AI incident copilot for data pipelines."
- Concrete ROI story: faster diagnosis, fewer repeated failures, safer remediation.
- Differentiation: Airflow-aware, evidence-backed, workflow-integrated.

## Priority Backlog

### P0: Must Have Before Executive Demo

- Fix and verify Docker/local startup path.
- Add fixture-backed demo mode.
- Build polished incident queue and detail view.
- Add evidence snippets to reports and dashboard.
- Add recurring failure mock or first version.
- Add demo integration action for ticket creation.
- Add demo metrics cards.
- Add a crisp demo script and sample data reset command.

### P1: Must Have Before Pilot With Internal Team

- Authentication and RBAC.
- Postgres plus migrations.
- Background analysis jobs.
- Audit log.
- Stronger redaction.
- Configurable LLM provider.
- Structured evidence storage.
- Slack or Jira integration.
- CI with Docker build and coverage gate.

### P2: Must Have Before Enterprise Rollout

- Multi-tenancy.
- SSO/OIDC.
- Observability stack.
- Helm chart.
- Airflow 2.x and 3.x compatibility.
- Evaluation harness.
- Admin settings UI.
- Retention controls.
- Load testing.

### P3: Differentiators

- Historical clustering.
- Incident memory.
- Blast radius and lineage.
- Ownership inference.
- Pull request suggestions with approval gates.
- Cost controls and private model mode.

## Technical Architecture Target

Recommended architecture:

```text
Browser UI
  -> API Gateway / FastAPI
  -> Auth, RBAC, tenant context
  -> Incident API
  -> Postgres
  -> Redis / Queue
  -> Analysis Workers
      -> Airflow connectors
      -> Log/evidence extractors
      -> Classifier
      -> LLM provider abstraction
      -> Redaction and policy engine
      -> Report generator
  -> Integrations service
      -> Slack / Teams
      -> GitHub / Jira
      -> PagerDuty
  -> Observability
      -> Metrics
      -> Traces
      -> Audit logs
```

## Data Model Additions

Core entities to add:

- Organization.
- User.
- Role.
- AirflowEnvironment.
- DagRunSnapshot.
- TaskFailure.
- AnalysisJob.
- EvidenceItem.
- ClassificationResult.
- LLMRun.
- IncidentReport.
- IntegrationAction.
- AuditEvent.
- FailureCluster.
- OwnerMapping.

Fields to capture:

- Tenant and environment IDs.
- Airflow instance URL alias, not raw secret-bearing config.
- DAG ID, run ID, task ID, try number.
- Operator, queue, pool, hostname, duration, retry metadata.
- Extracted exception type, traceback frames, SQL/query snippets, dependency names.
- Redaction summary and data policy decision.
- Classifier version and model version.
- LLM provider, model, prompt version, token counts, latency, response ID.
- User actions: view, export, ticket creation, acknowledgement, assignment.

## Quality Metrics

Product metrics:

- Mean time to diagnosis.
- Percentage of failures automatically classified.
- Percentage of reports accepted by engineers.
- Repeated failure rate.
- Incidents with owner identified.
- Tickets created from analysis.

Technical metrics:

- API latency.
- Analysis job duration.
- Queue depth.
- Airflow API error rate.
- LLM provider error rate.
- Token cost per analysis.
- Redaction count by type.
- Unknown classification rate.

AI quality metrics:

- Classification accuracy.
- Root cause usefulness score.
- Hallucination rate.
- Unsafe recommendation rate.
- Evidence citation coverage.
- Secret redaction recall.

## Immediate Implementation Sequence

1. Fix Dockerfile and add CI Docker build.
2. Add fixture-backed demo mode.
3. Redesign dashboard around incident queue and incident detail.
4. Add evidence snippets to API response and reports.
5. Add Postgres/Alembic foundation.
6. Add analysis job table and background worker.
7. Add audit events and model metadata capture.
8. Add Slack/Jira or GitHub integration.
9. Add evaluation fixtures and quality gates.
10. Add recurring failure clustering.

## Key Risks

- Sending sensitive logs to external LLM providers without tenant policy controls.
- Executive demo depending on live Airflow availability.
- Overpromising auto-remediation before trust and approval gates exist.
- Treating regex classification as production-grade intelligence without evaluations.
- Scaling failed-run discovery by scanning every DAG serially.
- Losing confidence because the UI looks like a developer console rather than an incident product.

## Recommended Positioning

Short version:

"dag-doctor is an AI incident copilot for Airflow that turns failed DAG logs into evidence-backed root cause reports and safe next actions."

Enterprise version:

"dag-doctor helps data platform teams reduce Airflow incident triage time with secure, auditable AI analysis, workflow integrations, and recurring failure intelligence."

