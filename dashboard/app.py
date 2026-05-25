"""dag-doctor Streamlit dashboard."""

from __future__ import annotations

import os

import streamlit as st
import requests

API_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

st.set_page_config(
    page_title="dag-doctor",
    page_icon="🩺",
    layout="wide",
)

st.title("🩺 dag-doctor")
st.caption("AI copilot for failed Airflow DAGs — explain, classify, fix.")


def api_get(path: str, params: dict | None = None) -> dict:
    try:
        r = requests.get(f"{API_URL}{path}", params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        st.error(f"API error: {e}")
        return {}


def api_post(path: str, params: dict | None = None) -> dict:
    try:
        r = requests.post(f"{API_URL}{path}", params=params, timeout=60)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        st.error(f"API error: {e}")
        return {}


with st.sidebar:
    st.header("Connection")

    health = api_get("/health")
    if health.get("status") == "ok":
        st.success("Airflow connected")
    else:
        st.warning("Airflow unavailable — check connection")

    st.divider()
    st.caption("dag-doctor v0.1.0")
    st.caption("[GitHub](https://github.com/mouadja02/dag-doctor)")

st.header("Failed DAG Runs")

if st.button("Refresh", type="primary"):
    st.rerun()

runs_data = api_get("/airflow/failed-runs", {"limit": 50})
failed_runs = runs_data.get("failed_runs", [])

if not failed_runs:
    st.info(
        "No failed DAG runs found. Connect to a test Airflow instance with failed runs."
    )
else:
    for run in failed_runs:
        dag_id = run["dag_id"]
        run_id = run["dag_run_id"]
        state = run.get("state", "unknown")
        logical = run.get("logical_date", "N/A")

        with st.expander(f"{dag_id} — {run_id}"):
            st.json(run)

            detail = api_get(f"/airflow/failed-runs/{dag_id}/{run_id}")
            task_instances = detail.get("task_instances", [])
            st.metric("Total Tasks", len(task_instances))
            st.metric("Failed Tasks", detail.get("failed_task_count", 0))

            failed_tis = [ti for ti in task_instances if ti.get("state") == "failed"]
            if failed_tis:
                st.subheader("Failed Tasks")
                for ti in failed_tis:
                    task_id = ti["task_id"]
                    try_num = ti.get("try_number", 1)

                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.markdown(f"**{task_id}** (try {try_num})")
                    with col2:
                        if st.button(
                            "Analyze", key=f"analyze_{dag_id}_{task_id}_{run_id}"
                        ):
                            with st.spinner("Analyzing failure..."):
                                result = api_post(
                                    "/analyze",
                                    {
                                        "dag_id": dag_id,
                                        "run_id": run_id,
                                        "task_id": task_id,
                                        "try_number": try_num,
                                    },
                                )
                                if result:
                                    st.success("Analysis complete!")
                                    st.session_state[f"result_{dag_id}_{task_id}"] = (
                                        result
                                    )
                                    st.rerun()

                    result_key = f"result_{dag_id}_{task_id}"
                    if result_key in st.session_state:
                        result = st.session_state[result_key]

                        if result.get("classification"):
                            cf = result["classification"]
                            st.markdown(
                                f"**Failure Type**: {cf['failure_type']} (confidence: {cf['confidence']:.0%})"
                            )

                        if result.get("explanation"):
                            exp = result["explanation"]
                            st.markdown("---")
                            st.markdown("### Summary")
                            st.markdown(exp["summary"])
                            st.markdown("### Root Cause")
                            st.markdown(exp["root_cause"])
                            st.markdown("### Remediation Steps")
                            for step in exp.get("remediation_steps", []):
                                st.markdown(f"- {step}")

                        if result.get("report_markdown"):
                            st.markdown("---")
                            st.markdown("### Full Report")
                            st.markdown(result["report_markdown"])
                            st.download_button(
                                "Download Report",
                                result["report_markdown"],
                                file_name=f"incident_report_{dag_id}_{task_id}.md",
                                mime="text/markdown",
                            )

st.divider()
with st.expander("Stored Reports"):
    reports_data = api_get("/reports")
    reports = reports_data.get("reports", [])
    if not reports:
        st.info("No stored reports yet.")
    else:
        for rep in reports:
            st.markdown(
                f"- [{rep['dag_id']}] {rep['task_id']} — "
                f"{rep['failure_type']} ({rep.get('created_at', 'N/A')})"
            )
