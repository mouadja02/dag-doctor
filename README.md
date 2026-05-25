# dag-doctor

> **AI incident copilot for Apache Airflow — stop digging through logs.**

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker](https://img.shields.io/badge/docker-ready-blue.svg)](https://www.docker.com/)

## The Problem

Data engineers waste hours digging through Airflow logs, scrolling past thousands of lines to find the one error that broke a DAG. SQL syntax errors, Python tracebacks, schema mismatches, timeouts, missing dependencies — the failure modes are endless, and the signals are buried in noise.

**dag-doctor** is an AI incident copilot that connects to your Airflow instance (dev/test only), fetches failed DAG runs, reads the logs, classifies the failure, explains the root cause in plain English, and generates a clean Markdown incident report — in seconds, not hours.

## Architecture

```
                    ┌─────────────────────────────────────────┐
                    │        Enterprise Streamlit Dashboard    │
                    │              (port 8501)                 │
                    │  ┌──────────┐  ┌──────────┐  ┌────────┐ │
                    │  │ Overview │  │ Incidents│  │Reports │ │
                    │  │  (KPIs)  │  │ (Queue)  │  │ (Store) │ │
                    │  └──────────┘  └──────────┘  └────────┘ │
                    │  ┌──────────┐  ┌──────────────────────┐  │
                    │  │Intelligence│  │   Charts & Analytics   │ │
                    │  │(Clusters)  │  │   (Altair/Vega)      │  │
                    │  └──────────┘  └──────────────────────┘  │
                    └──────────────────┬──────────────────────┘
                                       │ HTTP
                                       ▼
    ┌─────────────────────────────────────────────────────────────────────┐
    │                        FastAPI Backend                              │
    │                        (port 8000)                                   │
    │  ┌───────────┐  ┌────────┐  ┌──────────┐  ┌─────────┐  ┌─────────┐  │
    │  │ Airflow   │  │ Log    │  │ Failure  │  │ LLM     │  │ Auth    │  │
    │  │ Client    │  │ Parser │  │Classifier│  │ Layer   │  │ (JWT)   │  │
    │  └─────┬─────┘  └───┬────┘  └────┬─────┘  └────┬────┘  └─────────┘  │
    │        │            │            │             │                     │
    │        │            ▼            ▼             │                     │
    │        │     ┌─────────────┐  ┌──────────┐    │                     │
    │        │     │  Report     │  │ Storage  │    │                     │
    │        │     │  Generator  │  │(Postgres)│    │                     │
    │        │     └─────────────┘  └──────────┘    │                     │
    └────────┼─────────────────────────────────────────┼─────────────────────┘
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
Fetch Failed DAG Runs → Parse Task Logs → Classify Failure → LLM Root Cause
                                                                     ↓
                                             Generate Report ← Remediation Steps
```

## Dashboard Features

### Multi-Page Enterprise Console

The dashboard has been rebuilt as a multi-page Streamlit app with enterprise-grade UX:

| Page | Purpose |
|------|---------|
| **Overview** | KPI cards, trend charts, recent incidents at a glance |
| **Incidents** | Full incident queue with search, filter, sort, and detail panel |
| **Intelligence** | Recurring failure clusters, ownership analytics, prevention |
| **Reports** | Stored reports browser with filtering by type and DAG |

### Data Visualizations

- **Incidents over time** — area/line chart showing daily failure trends
- **Failure category breakdown** — donut chart by failure type
- **Severity distribution** — stacked bar chart (high/medium/low)
- **Ownership distribution** — horizontal bar chart by team/owner

### Enterprise UX

- Modern **Plus Jakarta Sans** font with **Space Mono** for code
- Material Design icons throughout
- Status badges with severity colors
- Human-readable timestamps ("2 hours ago")
- Master-detail layout for incident inspection
- Query-parameter deep-linking (`?incident=...`)
- Cached API calls with `@st.cache_data(ttl=300)`

## Quick Start

### Prerequisites

- Python 3.10+ (for local dev)
- Docker & Docker Compose 2.0+ (recommended)
- A **test clone** of Apache Airflow (see [Airflow Self-Hosted Setup](#airflow-self-hosted-setup))
- An [OpenRouter API key](https://openrouter.ai/) (free tier available)

### Option A: Docker Compose (Recommended)

```bash
# 1. Clone the repository
git clone https://github.com/mouadja02/dag-doctor.git
cd dag-doctor

# 2. Configure environment
cp .env.example .env
# Edit .env with your OpenRouter API key and Airflow URL

# 3. Build and start all services
docker compose up --build -d

# 4. Access the services
# Dashboard:  http://localhost:8501
# API Docs:   http://localhost:8000/docs
# Health:     http://localhost:8000/health
```

**Services started:**
- `api` — FastAPI backend on port 8000
- `dashboard` — Streamlit multi-page app on port 8501
- `dd-postgres` — PostgreSQL for incident storage

### Option B: Local Development

```bash
# 1. Install dependencies
pip install -e ".[dev]"

# 2. Start the API
uvicorn airflow_copilot.main:app --reload --port 8000

# 3. In another terminal, start the dashboard
streamlit run dashboard/app.py --server.port 8501
```

### Option C: Demo Mode (No Airflow Required)

```bash
# Set demo mode in .env or inline
DEMO_MODE=true docker compose up --build -d dashboard

# Or locally:
DEMO_MODE=true streamlit run dashboard/app.py
```

## Airflow Self-Hosted Setup

For local development, we use a self-hosted Airflow instance. This is a separate Docker Compose deployment located in `airflow-self-hosted/`.

### Step 1: Start the Airflow Network

```bash
# Create the shared Docker network (used by both Airflow and dag-doctor)
docker network create airflow_network

# Start Airflow
cd airflow-self-hosted
cp .env.example .env  # Edit with your credentials
docker compose up -d --build

# Wait for initialization (2-3 minutes)
docker compose logs -f airflow-init

# Verify Airflow is running
docker compose ps
# Access: http://localhost:8080 (user: admin / password: from .env)
```

**Airflow services:**
- `airflow-webserver` — UI on port 8080
- `airflow-scheduler` — DAG runner
- `airflow-apiserver` — REST API on port 8080
- `postgres` — Airflow metadata DB

### Step 2: Configure dag-doctor to Connect

```bash
cd ../  # Back to dag-doctor root

# Edit .env
AIRFLOW_BASE_URL=http://airflow-apiserver:8080
AIRFLOW_USERNAME=airflow
AIRFLOW_PASSWORD=airflow

# Start dag-doctor (it joins the external airflow_network)
docker compose up --build -d
```

### Architecture: Two Docker Compose Projects

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Docker Host                                  │
│                                                                      │
│  ┌─────────────────────────┐        ┌─────────────────────────────┐ │
│  │   airflow-self-hosted  │        │       dag-doctor             │ │
│  │   (docker-compose.yml) │        │   (docker-compose.yml)       │ │
│  │                        │        │                              │ │
│  │  ┌──────────────────┐  │        │  ┌──────────┐  ┌──────────┐  │ │
│  │  │ airflow-web    │  │        │  │   API    │  │ Dashboard│  │ │
│  │  │ server:8080    │  │        │  │  :8000   │  │  :8501   │  │ │
│  │  └────────┬───────┘  │        │  └────┬─────┘  └──────────┘  │ │
│  │           │          │        │       │                      │ │
│  │  ┌────────┴───────┐  │        │       │ joins                │ │
│  │  │airflow_network │◀─┼────────┼───────┘ external: true        │ │
│  │  │ (bridge)       │  │        │                              │ │
│  │  └────────────────┘  │        │  ┌──────────┐                │ │
│  │                      │        │  │dd-postgres│               │ │
│  │                      │        │  │  :5432   │               │ │
│  │                      │        │  └──────────┘               │ │
│  └──────────────────────┘        └──────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

**Key connection:** `dag-doctor` services join the `airflow_network` created by `airflow-self-hosted` so the API can reach Airflow at `http://airflow-apiserver:8080`.

## Project Structure

```
dag-doctor/
├── dashboard/                          # Streamlit multi-page dashboard
│   ├── app.py                          # Entry point (st.navigation)
│   ├── utils.py                        # Shared helpers (API, caching, icons)
│   ├── app_pages/                      # Page modules
│   │   ├── overview.py                 # KPIs + charts
│   │   ├── incidents.py                # Queue + detail panel
│   │   ├── intelligence.py             # Clusters + ownership
│   │   └── reports.py                  # Stored reports
│   └── .streamlit/
│       └── config.toml                 # Theme (fonts, colors, dark/light)
├── src/airflow_copilot/               # FastAPI backend
│   ├── main.py                         # App entry point
│   ├── api/                            # Route modules
│   │   ├── routes.py                   # Core endpoints
│   │   ├── intelligence_routes.py      # Clusters, ownership
│   │   ├── demo_routes.py              # Demo mode fixtures
│   │   └── auth_routes.py             # JWT authentication
│   ├── airflow_client.py               # Airflow REST API client
│   ├── classifier.py                   # Failure type classifier
│   ├── log_parser.py                   # Log signal extractor
│   ├── llm.py                          # LLM provider layer
│   ├── report_generator.py            # Markdown report builder
│   ├── clustering.py                   # Recurring failure detection
│   ├── ownership.py                    # Team/owner attribution
│   └── demo_fixtures.py               # Demo incident data
├── airflow-self-hosted/               # Local Airflow deployment
│   ├── docker-compose.yml             # Airflow services
│   ├── Dockerfile                     # Custom Airflow image
│   ├── dags/                          # Production DAGs
│   └── README.md                      # Airflow setup guide
├── docker-compose.yml                 # dag-doctor services
├── Dockerfile                         # dag-doctor image
├── pyproject.toml                     # Dependencies
├── .env.example                       # Configuration template
└── README.md                          # This file
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check (Airflow + DB status) |
| `GET` | `/health/ready` | Kubernetes readiness probe |
| `GET` | `/health/live` | Kubernetes liveness probe |
| `GET` | `/airflow/failed-runs` | List recent failed DAG runs |
| `GET` | `/airflow/failed-runs/{dag_id}/{run_id}` | Detail for a specific failed run |
| `POST` | `/analyze` | Run full analysis pipeline on a DAG run |
| `POST` | `/analyze/async` | Queue async analysis (returns job_id) |
| `GET` | `/jobs/{job_id}` | Poll async analysis status |
| `GET` | `/reports` | List stored analysis reports |
| `GET` | `/reports/{report_id}` | Get a specific report |
| `DELETE` | `/reports/{report_id}` | Delete a report |
| `GET` | `/intelligence/clusters` | Recurring failure clusters |
| `GET` | `/intelligence/ownership` | Ownership statistics |
| `GET` | `/intelligence/similar` | Find similar incidents |
| `POST` | `/integrations/notify` | Notify Slack/Jira/GitHub |
| `POST` | `/integrations/github` | Create GitHub issue from report |

## Demo Flow

1. Start dag-doctor in demo mode (`DEMO_MODE=true`)
2. Open the dashboard at `http://localhost:8501`
3. Browse the Overview page — KPIs and charts load instantly
4. Click **Incidents** — 10 pre-seeded demo incidents appear
5. Use the search/filter bar to narrow by DAG name or severity
6. Click **Inspect** on any incident to open the detail panel
7. Click **Analyze** to see the AI-generated root cause and remediation
8. Navigate to **Intelligence** to see ownership and cluster analytics
9. Go to **Reports** to browse stored analysis history

### Demo Mode

When `DEMO_MODE=true`, the app serves pre-computed analysis fixtures without calling the LLM or Airflow. Perfect for:
- UI/UX development
- Sales demos
- CI/CD screenshots
- Offline presentations

## Configuration

### Environment Variables

Create `.env` from `.env.example`:

```bash
# Airflow Connection (TEST CLONE ONLY — never production)
AIRFLOW_BASE_URL=http://localhost:8080
AIRFLOW_USERNAME=airflow
AIRFLOW_PASSWORD=airflow

# LLM Provider (OpenRouter recommended)
LLM_PROVIDER=openrouter
OPENROUTER_API_KEY=sk-or-v1-your-key-here
OPENROUTER_MODEL=anthropic/claude-3.5-sonnet

# Database (SQLite for dev, PostgreSQL for shared)
DATABASE_URL=sqlite:///data/dag_doctor.db
# DATABASE_URL=postgresql://dagdoctor:dagdoctor@localhost:5432/dagdoctor

# Authentication
SECRET_KEY=change-me-in-production

# Demo Mode
# DEMO_MODE=true

# Integrations (optional)
# SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
# JIRA_API_URL=https://your-domain.atlassian.net
# JIRA_EMAIL=user@example.com
# JIRA_API_TOKEN=your-token
# JIRA_PROJECT_KEY=DD
```

### Theme Customization

Edit `dashboard/.streamlit/config.toml`:

```toml
[theme.light]
font = "'Plus Jakarta Sans':https://fonts.googleapis.com/..."
codeFont = "'Space Mono':https://fonts.googleapis.com/..."
primaryColor = "#2563EB"
backgroundColor = "#FFFFFF"
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

- [x] Multi-page Streamlit dashboard with enterprise UX
- [x] Data visualizations (Altair/Vega charts)
- [x] Docker Compose deployment
- [x] PostgreSQL support
- [x] JWT authentication
- [x] Demo mode with pre-seeded fixtures
- [x] Recurring failure clustering
- [x] Ownership analytics
- [ ] Slack webhook integration
- [ ] Jira/GitHub auto-ticketing
- [ ] Airflow plugin UI embed
- [ ] Safe auto-remediation with approval gate
- [ ] Custom classifier training

## Development

```bash
# Install in editable mode
pip install -e ".[dev]"

# Run tests
pytest

# Run linting
ruff check .
ruff format .

# Start local dev stack
uvicorn airflow_copilot.main:app --reload --port 8000
streamlit run dashboard/app.py --server.port 8501
```

## License

MIT — see [LICENSE](LICENSE) file.
