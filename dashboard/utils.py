"""dag-doctor — shared utilities and API helpers for the Streamlit dashboard."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Literal

import requests
import streamlit as st

API_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
DEMO_MODE = os.getenv("DEMO_MODE", "").lower() in ("1", "true", "yes")
AUTH_ENABLED = os.getenv("AUTH_ENABLED", "").lower() in ("1", "true", "yes")

SEVERITY_ICON = {
    "high": ":material/error:",
    "medium": ":material/warning:",
    "low": ":material/info:",
}

STATUS_ICON = {
    "failed": ":material/error:",
    "open": ":material/error:",
    "success": ":material/check_circle:",
}

FAILURE_ICON = {
    "sql_error": ":material/storage:",
    "python_exception": ":material/bug_report:",
    "timeout": ":material/timer:",
    "permissions_auth": ":material/lock:",
    "missing_dependency": ":material/package_2:",
    "infrastructure_resource": ":material/dns:",
    "schema_data_quality": ":material/monitoring:",
    "upstream_dependency": ":material/link:",
    "unknown": ":material/help:",
}

TREND_ICON = {
    "rising": ":material/trending_up:",
    "declining": ":material/trending_down:",
    "flat": ":material/trending_flat:",
}


def _sev_color(
    sev: str,
) -> Literal[
    "red", "orange", "yellow", "blue", "green", "violet", "gray", "grey", "primary"
]:
    if sev == "high":
        return "red"
    if sev == "medium":
        return "orange"
    if sev == "low":
        return "blue"
    return "gray"


def _status_color(
    status: str,
) -> Literal[
    "red", "orange", "yellow", "blue", "green", "violet", "gray", "grey", "primary"
]:
    if status in ("failed", "open"):
        return "red"
    if status == "success":
        return "green"
    return "gray"


def _trend_color(
    trend: str,
) -> Literal[
    "red", "orange", "yellow", "blue", "green", "violet", "gray", "grey", "primary"
]:
    if trend == "rising":
        return "red"
    if trend == "declining":
        return "green"
    return "gray"


def _auth_headers() -> dict:
    token = st.session_state.get("auth_token")
    if token:
        return {"Authorization": f"Bearer {token}"}
    return {}


@st.cache_data(ttl="5m")
def api_get(path: str, params: dict | None = None) -> dict:
    """Cached GET request to the dag-doctor API."""
    try:
        headers = _auth_headers()
        r = requests.get(f"{API_URL}{path}", params=params, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.RequestException:
        return {}


@st.cache_data(ttl="5m")
def api_post(path: str, params: dict | None = None) -> dict:
    """Cached POST request to the dag-doctor API."""
    try:
        headers = _auth_headers()
        r = requests.post(
            f"{API_URL}{path}", params=params, headers=headers, timeout=60
        )
        r.raise_for_status()
        return r.json()
    except requests.RequestException:
        return {}


@st.cache_data(ttl="2m")
def get_run_detail(dag_id: str, run_id: str) -> dict:
    """Cached fetch for a single DAG run detail."""
    return api_get(f"/airflow/failed-runs/{dag_id}/{run_id}")


def init_session():
    """Initialize session-state defaults."""
    defaults = {
        "selected_run": None,
        "selected_task": None,
        "analysis_result": None,
        "ticket_payload": None,
        "demo_mode": DEMO_MODE,
        "auth_token": None,
        "user": None,
        "airflow_available": None,
        "active_tab": "queue",
        "incidents_filters": {},
        "reports_filters": {},
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


def format_time_ago(iso_timestamp: str | None) -> str:
    """Convert an ISO timestamp to a human-readable 'ago' string."""
    if not iso_timestamp:
        return "—"
    try:
        dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = now - dt
        if delta.days > 1:
            return f"{delta.days} days ago"
        if delta.days == 1:
            return "1 day ago"
        hours = delta.seconds // 3600
        if hours > 1:
            return f"{hours} hours ago"
        if hours == 1:
            return "1 hour ago"
        minutes = delta.seconds // 60
        if minutes > 1:
            return f"{minutes} minutes ago"
        return "just now"
    except Exception:
        return str(iso_timestamp)[:19]


def render_login():
    """Render the login screen when auth is required."""
    with st.container(horizontal_alignment="center"):
        st.title(":material/ecg_heart: dag-doctor")
        st.caption("Sign in to your account")
        with st.form("login_form", clear_on_submit=False):
            email = st.text_input("Email")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button(
                "Sign in", type="primary", use_container_width=True
            )
            if submitted:
                try:
                    r = requests.post(
                        f"{API_URL}/auth/login",
                        json={"email": email, "password": password},
                        timeout=10,
                    )
                    if r.status_code == 200:
                        data = r.json()
                        st.session_state.auth_token = data["access_token"]
                        st.session_state.user = data
                        st.rerun()
                    else:
                        st.error("Invalid email or password")
                except requests.RequestException:
                    st.error("Cannot connect to API server")


def render_sidebar():
    """Render the shared sidebar across all pages."""
    with st.sidebar:
        st.title(":material/ecg_heart: dag-doctor")
        user = st.session_state.get("user", {})
        if user:
            st.caption(
                f"**{user.get('role', 'viewer')}** — {user.get('org_name', 'N/A')}"
            )
            if st.button(
                "Sign out", use_container_width=True, icon=":material/logout:"
            ):
                st.session_state.auth_token = None
                st.session_state.user = None
                st.rerun()
            st.space("small")

        if DEMO_MODE:
            st.badge("Demo mode", icon=":material/science:", color="blue")
            if st.button(
                "Reset demo data", use_container_width=True, icon=":material/refresh:"
            ):
                api_post("/demo/reset")
                for k in (
                    "selected_run",
                    "selected_task",
                    "analysis_result",
                    "ticket_payload",
                ):
                    st.session_state[k] = None
                # Clear caches so fresh demo data is fetched
                st.cache_data.clear()
                st.rerun()
            st.caption("LLM calls are disabled in demo mode.")
            return

        health = api_get("/health")
        if health.get("airflow_connected"):
            st.success("Airflow connected", icon=":material/check_circle:")
            st.session_state.airflow_available = True
        else:
            st.warning("Airflow unavailable", icon=":material/warning:")
            st.session_state.airflow_available = False

        if health.get("database_connected"):
            st.caption("DB: connected")
        else:
            st.caption("DB: disconnected")

        st.space("small")
        st.caption("dag-doctor v0.1.0")


def get_incidents_data() -> list[dict]:
    """Fetch incidents from demo or live API."""
    if DEMO_MODE:
        data = api_get("/demo/incidents")
        return data.get("failed_runs", [])
    else:
        if st.session_state.get("airflow_available"):
            data = api_get("/airflow/failed-runs", {"limit": 50})
            return data.get("failed_runs", [])
        return []


def get_metrics_data() -> dict:
    """Fetch metrics from demo or live API."""
    if DEMO_MODE:
        return api_get("/demo/metrics")
    else:
        return api_get("/reports/metrics")


def get_reports_data() -> list[dict]:
    """Fetch stored reports."""
    data = api_get("/reports")
    return data.get("reports", [])


def get_clusters_data() -> list[dict]:
    """Fetch intelligence clusters."""
    data = api_get("/intelligence/clusters", {"limit": 20})
    return data.get("clusters", [])


def get_ownership_data() -> dict:
    """Fetch ownership statistics."""
    return api_get("/intelligence/ownership")
