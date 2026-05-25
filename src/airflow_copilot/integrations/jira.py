"""Jira integration — create issues from incident reports."""

from __future__ import annotations

import base64
import logging

import httpx

from airflow_copilot.config import get_settings

logger = logging.getLogger(__name__)


def create_jira_issue(
    incident: dict,
    jira_url: str | None = None,
    email: str | None = None,
    api_token: str | None = None,
    project_key: str | None = None,
) -> dict | None:
    """Create a Jira issue from an incident report.

    Args:
        incident: AnalysisResult as dict with classification, explanation, evidence.
        jira_url: Jira instance URL (e.g., https://your-domain.atlassian.net).
        email: Jira account email.
        api_token: Jira API token.
        project_key: Jira project key.

    Returns:
        Dict with issue_key and issue_url on success, None on failure.
    """
    settings = get_settings()
    url = jira_url or settings.jira_api_url
    user_email = email or settings.jira_email
    token = api_token or settings.jira_api_token
    key = project_key or settings.jira_project_key

    if not url or not user_email or not token:
        logger.warning("Jira integration not fully configured")
        return None

    try:
        classification = incident.get("classification", {}) or {}
        explanation = incident.get("explanation", {}) or {}
        evidence = incident.get("evidence", [])
        failure_type = (
            classification.get("failure_type", "unknown").replace("_", " ").title()
        )
        severity = incident.get("severity", "medium").upper()

        evidence_text = "\n".join(e.get("source_line", "") for e in (evidence or []))

        description = (
            f"*DAG:* {incident.get('dag_id', 'N/A')}\n"
            f"*Task:* {incident.get('task_id', 'N/A')}\n"
            f"*Run:* {incident.get('dag_run_id', 'N/A')}\n\n"
            f"*Root Cause:* {explanation.get('root_cause', 'N/A')}\n\n"
            f"*Evidence:*\n{{code}}\n{evidence_text}\n{{code}}\n\n"
            f"*Classification:* {failure_type} ({classification.get('confidence', 0):.0%})\n"
            f"*Severity:* {severity}\n\n"
            f"*Remediation:*\n"
            + "\n".join(f"- {s}" for s in explanation.get("remediation_steps", []))
        )

        auth = base64.b64encode(f"{user_email}:{token}".encode()).decode()
        headers = {
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        payload = {
            "fields": {
                "project": {"key": key},
                "summary": f"[dag-doctor][{failure_type}] {incident.get('dag_id', '')} — {incident.get('task_id', '')}",
                "description": description,
                "issuetype": {"name": "Bug"},
                "labels": [
                    "dag-doctor",
                    failure_type.lower().replace(" ", "-"),
                    f"severity-{incident.get('severity', 'medium')}",
                ],
                "priority": {"name": "High" if severity == "HIGH" else "Medium"},
            }
        }

        api_url = f"{url.rstrip('/')}/rest/api/2/issue"
        with httpx.Client(timeout=15.0) as client:
            resp = client.post(api_url, json=payload, headers=headers)
            if resp.status_code in (200, 201):
                data = resp.json()
                issue_key = data.get("key", "unknown")
                logger.info(
                    "Created Jira issue %s for %s", issue_key, incident.get("dag_id")
                )
                return {
                    "issue_key": issue_key,
                    "issue_url": f"{url.rstrip('/')}/browse/{issue_key}",
                }
            else:
                logger.error("Jira API error %s: %s", resp.status_code, resp.text)
                return None

    except Exception as e:
        logger.error("Failed to create Jira issue: %s", e)
        return None
