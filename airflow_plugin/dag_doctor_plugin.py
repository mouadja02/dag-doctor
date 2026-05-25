"""
Airflow plugin for dag-doctor — adds "Analyze with dag-doctor" to task instance views.

Install: Copy this file to your Airflow plugins/ directory.
Configure with environment variables:
  DAG_DOCTOR_API_URL=http://localhost:8000
  DAG_DOCTOR_DASHBOARD_URL=http://localhost:8501
"""

from __future__ import annotations

import os

try:
    from airflow.plugins_manager import AirflowPlugin
    from airflow.utils.decorators import apply_defaults

    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

import httpx

API_URL = os.getenv("DAG_DOCTOR_API_URL", "http://localhost:8000")
DASHBOARD_URL = os.getenv("DAG_DOCTOR_DASHBOARD_URL", "http://localhost:8501")


def analyze_with_dag_doctor(
    dag_id: str, run_id: str, task_id: str, try_number: int = 1
) -> dict | None:
    """Call the dag-doctor API to analyze a failed task. Returns analysis result."""
    try:
        with httpx.Client(timeout=60.0) as client:
            resp = client.post(
                f"{API_URL}/analyze/async",
                params={
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "task_id": task_id,
                    "try_number": try_number,
                },
            )
            if resp.status_code == 200:
                return resp.json()
            return None
    except Exception:
        return None


def get_report_url(report_id: int) -> str:
    """Build a deep link to a dag-doctor report."""
    return f"{DASHBOARD_URL}/?report={report_id}"


def get_analyze_url(dag_id: str, run_id: str, task_id: str) -> str:
    """Build a deep link to the dag-doctor dashboard for a specific task."""
    return f"{DASHBOARD_URL}/?dag={dag_id}&run={run_id}&task={task_id}"


if AIRFLOW_AVAILABLE:
    from airflow.www.views import AirflowBaseView

    class DagDoctorView(AirflowBaseView):
        """View that redirects to the dag-doctor dashboard."""

        @apply_defaults
        def __init__(self):
            super().__init__()

        def index(self):
            import flask

            return flask.redirect(DASHBOARD_URL)

    # Menu link and operator links
    appbuilder_menu_items = [
        {
            "name": "dag-doctor",
            "href": DASHBOARD_URL,
            "category": "Admin",
        },
    ]

    # Operator extra links — appears on each task instance in the Airflow UI
    from airflow.models.taskinstance import TaskInstance

    def _dag_doctor_link(task_instance: TaskInstance) -> str:
        return f'<a href="{get_analyze_url(task_instance.dag_id, task_instance.run_id, task_instance.task_id)}" target="_blank">Analyze with dag-doctor</a>'

    class DagDoctorPlugin(AirflowPlugin):
        name = "dag_doctor"
        appbuilder_menu_items = appbuilder_menu_items
        task_instance_links = [
            {
                "name": "Analyze with dag-doctor",
                "href_template": DASHBOARD_URL
                + "/?dag={{ dag_id }}&run={{ run_id }}&task={{ task_id }}",
                "html": _dag_doctor_link,
            },
        ]
