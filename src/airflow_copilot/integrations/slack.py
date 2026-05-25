"""Slack integration — post incident summaries via incoming webhook."""

from __future__ import annotations

import logging

from slack_sdk.webhook import WebhookClient

from airflow_copilot.config import get_settings

logger = logging.getLogger(__name__)


def post_incident_to_slack(
    incident: dict,
    webhook_url: str | None = None,
) -> bool:
    """Post an incident summary to a Slack incoming webhook.

    Args:
        incident: AnalysisResult as dict, with dag_id, task_id, classification, etc.
        webhook_url: Slack incoming webhook URL. Falls back to SLACK_WEBHOOK_URL env var.

    Returns:
        True if posted successfully, False otherwise.
    """
    url = webhook_url or get_settings().slack_webhook_url
    if not url:
        logger.warning("No Slack webhook URL configured")
        return False

    try:
        client = WebhookClient(url)

        classification = incident.get("classification", {}) or {}
        explanation = incident.get("explanation", {}) or {}
        failure_type = (
            classification.get("failure_type", "unknown").replace("_", " ").title()
        )
        confidence = classification.get("confidence", 0)
        severity = incident.get("severity", "medium").upper()

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"dag-doctor: {failure_type} detected",
                },
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*DAG:*\n{incident.get('dag_id', 'N/A')}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Task:*\n{incident.get('task_id', 'N/A')}",
                    },
                    {"type": "mrkdwn", "text": f"*Confidence:*\n{confidence:.0%}"},
                    {"type": "mrkdwn", "text": f"*Severity:*\n{severity}"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Root Cause:*\n{explanation.get('root_cause', 'N/A')}",
                },
            },
        ]

        resp = client.send_dict({"blocks": blocks})
        if resp.status_code == 200:
            logger.info(
                "Posted incident to Slack: %s/%s",
                incident.get("dag_id"),
                incident.get("task_id"),
            )
            return True
        else:
            logger.error("Slack webhook failed: %s %s", resp.status_code, resp.body)
            return False

    except Exception as e:
        logger.error("Failed to post to Slack: %s", e)
        return False
