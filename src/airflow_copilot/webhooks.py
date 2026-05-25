"""Webhook system — configure and dispatch outbound webhooks for analysis events."""

from __future__ import annotations

import logging

import httpx


logger = logging.getLogger(__name__)

_WEBHOOK_REGISTRY: list[dict] = []


def register_webhook(url: str, events: list[str] | None = None, secret: str = ""):
    """Register a webhook endpoint to receive event notifications."""
    _WEBHOOK_REGISTRY.append(
        {
            "url": url,
            "events": events or ["analysis.completed"],
            "secret": secret,
        }
    )


def list_webhooks() -> list[dict]:
    """List registered webhooks (masked secrets)."""
    return [
        {"url": w["url"], "events": w["events"], "has_secret": bool(w["secret"])}
        for w in _WEBHOOK_REGISTRY
    ]


def clear_webhooks():
    """Clear all registered webhooks."""
    _WEBHOOK_REGISTRY.clear()


def dispatch_event(event_type: str, payload: dict):
    """Dispatch an event to all registered webhooks that listen for this event type."""
    for wh in _WEBHOOK_REGISTRY:
        if event_type in wh["events"]:
            _send_webhook(wh["url"], event_type, payload, wh.get("secret", ""))


def _send_webhook(url: str, event_type: str, payload: dict, secret: str = ""):
    """Send a webhook POST to the given URL."""
    headers = {"Content-Type": "application/json", "X-dag-doctor-Event": event_type}
    if secret:
        import hashlib
        import hmac

        body = httpx._compat.json_dumps(payload)  # type: ignore[attr-defined]
        signature = hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()
        headers["X-dag-doctor-Signature"] = f"sha256={signature}"

    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(url, json=payload, headers=headers)
            if resp.status_code >= 400:
                logger.warning(
                    "Webhook %s returned %s: %s", url, resp.status_code, resp.text[:200]
                )
            else:
                logger.info("Webhook %s dispatched: %s", url, event_type)
    except Exception as e:
        logger.error("Failed to dispatch webhook to %s: %s", url, e)
