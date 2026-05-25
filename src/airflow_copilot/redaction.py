"""Enhanced log redaction — connection URIs, JWTs, cloud keys, private keys, emails, IPs."""

from __future__ import annotations

import json
import re

_CONNECTION_URI = re.compile(
    r"(?:postgresql|mysql|mongodb|redshift|snowflake|bigquery|jdbc|sqlite)://[^\s\"']+",
    re.IGNORECASE,
)
_JWT_PATTERN = re.compile(
    r"eyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+",
)
_AWS_KEY = re.compile(r"AKIA[0-9A-Z]{16}", re.IGNORECASE)
_PRIVATE_KEY = re.compile(
    r"-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----",
)
_EMAIL = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
_IP_ADDR = re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b")

REDACTION_LEVELS = {
    "strict": ["uri", "jwt", "aws_key", "private_key", "email", "ip"],
    "standard": ["uri", "jwt", "aws_key", "private_key"],
    "lenient": ["uri", "aws_key"],
}


def redact_text(text: str, level: str = "standard") -> tuple[str, dict[str, int]]:
    """Redact sensitive content from text. Returns (redacted_text, summary_counts)."""
    to_redact = REDACTION_LEVELS.get(level, REDACTION_LEVELS["standard"])
    counts: dict[str, int] = {}

    if "uri" in to_redact:
        before = text
        text = _CONNECTION_URI.sub("[REDACTED_CONNECTION_URI]", text)
        counts["connection_uris"] = len(_CONNECTION_URI.findall(before))

    if "jwt" in to_redact:
        before = text
        text = _JWT_PATTERN.sub("[REDACTED_JWT]", text)
        counts["jwts"] = len(_JWT_PATTERN.findall(before))

    if "aws_key" in to_redact:
        before = text
        text = _AWS_KEY.sub("[REDACTED_AWS_KEY]", text)
        counts["aws_keys"] = len(_AWS_KEY.findall(before))

    if "private_key" in to_redact:
        before = text
        text = _PRIVATE_KEY.sub("[REDACTED_PRIVATE_KEY_BLOCK]", text)
        counts["private_keys"] = len(_PRIVATE_KEY.findall(before))

    if "email" in to_redact:
        before = text
        text = _EMAIL.sub("[REDACTED_EMAIL]", text)
        counts["emails"] = len(_EMAIL.findall(before))

    if "ip" in to_redact:
        before = text
        text = _IP_ADDR.sub("[REDACTED_IP]", text)
        counts["ips"] = len(_IP_ADDR.findall(before))

    return text, counts


def apply_llm_redaction(log_text: str, level: str = "standard") -> tuple[str, dict]:
    """Apply redaction before sending any content to an LLM."""
    redacted, summary = redact_text(log_text, level)

    redacted = re.sub(
        r"(?:api[_-]?key|password|secret|token)\s*[=:]\s*\S+",
        "[REDACTED_CREDENTIAL]",
        redacted,
        flags=re.IGNORECASE,
    )
    redacted = re.sub(r"sk-[a-zA-Z0-9]{20,}", "[REDACTED_API_KEY]", redacted)
    redacted = re.sub(r"Authorization:\s*\S+", "Authorization: [REDACTED]", redacted)

    return redacted, summary


def generate_redaction_summary(log_text: str, level: str = "standard") -> str:
    """Return a JSON summary of what was redacted (for audit trail)."""
    _, counts = redact_text(log_text, level)
    return json.dumps(counts)
