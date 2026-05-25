"""Airflow REST API client — read-only access to test instances only.

Authenticates via JWT Bearer tokens (Airflow 3.x FastAPI).
"""

from __future__ import annotations

import logging
import time

import httpx

from airflow_copilot.config import get_settings
from airflow_copilot.models import DAGRun, TaskInstance

logger = logging.getLogger(__name__)


class AirflowClient:
    """Read-only client for the Airflow 3.x REST API.

    Safety: Only performs GET requests. Never mutates Airflow state.
    """

    def __init__(self) -> None:
        settings = get_settings()
        self.base_url = settings.airflow_base_url.rstrip("/")
        self.username = settings.airflow_username
        self.password = settings.airflow_password
        self._token: str | None = None
        self._token_expiry: float = 0.0

        self.client = httpx.Client(
            base_url=self.base_url,
            timeout=30.0,
            headers={"Content-Type": "application/json"},
        )

    def _ensure_auth(self) -> None:
        """Acquire or refresh JWT token if needed."""
        if self._token and time.time() < self._token_expiry - 60:
            return
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate via POST /auth/token to get a JWT."""
        resp = self.client.post(
            "/auth/token",
            json={"username": self.username, "password": self.password},
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._token_expiry = time.time() + 3600  # default 1h expiry
        self.client.headers["Authorization"] = f"Bearer {self._token}"

    def _get(self, path: str, **params) -> dict:
        self._ensure_auth()
        logger.debug("GET %s params=%s", path, params)
        resp = self.client.get(path, params=params)
        resp.raise_for_status()
        return resp.json()

    def get_dags(self) -> list[str]:
        """List all DAG IDs."""
        data = self._get("/api/v2/dags")
        return [d["dag_id"] for d in data.get("dags", [])]

    def get_failed_dag_runs(self, limit: int = 50) -> list[DAGRun]:
        """Fetch recent failed DAG runs across all DAGs."""
        all_failed: list[DAGRun] = []
        dag_ids = self.get_dags()

        for dag_id in dag_ids:
            if len(all_failed) >= limit:
                break
            try:
                data = self._get(
                    f"/api/v2/dags/{dag_id}/dagRuns",
                    limit=limit,
                    state="failed",
                )
                runs = data.get("dag_runs", [])
                for r in runs:
                    all_failed.append(self._parse_dag_run(r))
            except Exception as exc:
                logger.warning("Failed to fetch runs for DAG %s: %s", dag_id, exc)

        all_failed.sort(
            key=lambda r: r.logical_date or "",
            reverse=True,
        )
        return all_failed[:limit]

    def get_dag_run(self, dag_id: str, run_id: str) -> DAGRun | None:
        """Fetch a specific DAG run."""
        data = self._get(f"/api/v2/dags/{dag_id}/dagRuns/{run_id}")
        return self._parse_dag_run(data)

    def get_task_instances(self, dag_id: str, run_id: str) -> list[TaskInstance]:
        """Fetch task instances for a DAG run."""
        path = f"/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances"
        data = self._get(path)
        tis = data.get("task_instances", [])
        return [self._parse_task_instance(ti) for ti in tis]

    def get_task_instance(
        self, dag_id: str, run_id: str, task_id: str
    ) -> TaskInstance | None:
        """Fetch a specific task instance."""
        path = f"/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}"
        data = self._get(path)
        return self._parse_task_instance(data)

    def get_task_log(
        self, dag_id: str, run_id: str, task_id: str, try_number: int
    ) -> str:
        """Fetch the log for a specific task attempt.

        Airflow 3.x returns structured JSON log entries.
        We extract error details and plain-text events.
        """
        path = (
            f"/api/v2/dags/{dag_id}/dagRuns/{run_id}"
            f"/taskInstances/{task_id}/logs/{try_number}"
        )
        try:
            data = self._get(path)
            return self._parse_structured_log(data)
        except Exception as exc:
            logger.error("Failed to fetch task log: %s", exc)
            return ""

    def get_task_log_path(
        self, dag_id: str, run_id: str, task_id: str, try_number: int
    ) -> str:
        """Extract the log file path from the structured log response."""
        path = (
            f"/api/v2/dags/{dag_id}/dagRuns/{run_id}"
            f"/taskInstances/{task_id}/logs/{try_number}"
        )
        try:
            data = self._get(path)
            entries = data if isinstance(data, list) else data.get("content", [])
            for entry in entries:
                if isinstance(entry, dict):
                    sources = entry.get("sources", [])
                    if sources:
                        return str(sources[0])
            return ""
        except Exception:
            return ""

    def _parse_structured_log(self, data: dict | list) -> str:
        """Parse Airflow 3.x structured log response into plain text.

        Formats the JSON error_detail into a proper Python traceback
        that the regex-based log parser can extract exception types from.
        """
        entries = (
            data
            if isinstance(data, list)
            else data.get("content", data.get("logs", []))
        )
        lines: list[str] = []
        log_path = ""

        for entry in entries:
            if not isinstance(entry, dict):
                continue

            # Extract log file path
            sources = entry.get("sources", [])
            if sources and not log_path:
                log_path = str(sources[0])

            event = entry.get("event", "")
            error_detail = entry.get("error_detail")

            if error_detail and isinstance(error_detail, list):
                lines.append(self._format_traceback(error_detail))
            elif event and not event.startswith("::"):
                # Skip GitHub Actions-style group markers
                lines.append(event)

        if log_path:
            lines.append(f"Log file: {log_path}")

        return "\n".join(lines)

    def _format_traceback(self, error_details: list) -> str:
        """Format Airflow 3.x structured error_detail into a Python traceback."""
        parts: list[str] = []
        parts.append("Traceback (most recent call last):")

        for err in error_details:
            frames = err.get("frames", [])
            for frame in frames:
                filename = frame.get("filename", "<unknown>")
                lineno = frame.get("lineno", "?")
                name = frame.get("name", "?")
                parts.append(f'  File "{filename}", line {lineno}, in {name}')

            exc_type = err.get("exc_type", "")
            exc_value = err.get("exc_value", "")

            if frames and exc_type:
                # Capture the full dotted exception path from exc_value
                # e.g., "snowflake.connector.errors.ProgrammingError: ..."
                parts.append(f"{exc_type}: {exc_value}")
            elif exc_type:
                parts.append(f"{exc_type}: {exc_value}")

        return "\n".join(parts)

    def health_check(self) -> bool:
        """Verify connectivity to the Airflow API server."""
        try:
            resp = self.client.get("/api/v2/version")
            resp.raise_for_status()
            return True
        except Exception:
            return False

    def _parse_dag_run(self, data: dict) -> DAGRun:
        return DAGRun(
            dag_id=data.get("dag_id", ""),
            dag_run_id=data.get("dag_run_id", ""),
            logical_date=data.get("logical_date"),
            start_date=data.get("start_date"),
            end_date=data.get("end_date"),
            state=data.get("state", ""),
            run_type=data.get("run_type", ""),
            conf=data.get("conf", {}) or {},
        )

    def _parse_task_instance(self, data: dict) -> TaskInstance:
        return TaskInstance(
            task_id=data.get("task_id", ""),
            dag_id=data.get("dag_id", ""),
            dag_run_id=data.get("dag_run_id", ""),
            logical_date=data.get("logical_date"),
            start_date=data.get("start_date"),
            end_date=data.get("end_date"),
            duration=data.get("duration"),
            state=data.get("state", ""),
            try_number=data.get("try_number", 1),
            max_tries=data.get("max_tries", 1),
            operator=data.get("operator", ""),
            hostname=data.get("hostname", ""),
        )
