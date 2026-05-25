"""Audit logging — records user actions for compliance and traceability."""

from __future__ import annotations

import json
import logging

from airflow_copilot.database import get_session
from airflow_copilot.orm import AuditEvent

logger = logging.getLogger(__name__)


def log_event(
    action: str,
    user_id: int | None = None,
    org_id: int | None = None,
    resource_type: str = "",
    resource_id: str = "",
    details: dict | None = None,
    ip_address: str = "",
) -> None:
    """Record an audit event. Non-blocking — errors are logged, not raised."""
    try:
        with get_session() as session:
            event = AuditEvent(
                org_id=org_id,
                user_id=user_id,
                action=action,
                resource_type=resource_type,
                resource_id=str(resource_id),
                details_json=json.dumps(details or {}),
                ip_address=ip_address,
            )
            session.add(event)
    except Exception as e:
        logger.error("Failed to write audit event: %s", e)
