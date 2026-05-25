"""Failure classifier — rule-based classification of Airflow task failures."""

from __future__ import annotations

from typing import Any

from airflow_copilot.log_parser import ParsedLog
from airflow_copilot.models import FailureClassification


FAILURE_TYPES = [
    "sql_error",
    "python_exception",
    "timeout",
    "permissions_auth",
    "missing_dependency",
    "infrastructure_resource",
    "schema_data_quality",
    "upstream_dependency",
    "unknown",
]


def classify(parsed: ParsedLog, task_state: str = "failed") -> FailureClassification:
    """Classify a parsed log into a failure type with confidence score.

    Uses weighted keyword/signal matching from the parsed log.
    Returns the highest-scoring classification.
    """
    scores: dict[str, float] = {}

    if parsed.sql_error:
        scores["sql_error"] = scores.get("sql_error", 0.0) + 0.85
    if parsed.exception_type:
        scores["python_exception"] = scores.get("python_exception", 0.0) + 0.50
    if parsed.has_timeout:
        scores["timeout"] = scores.get("timeout", 0.0) + 0.90
    if parsed.has_permission_error:
        scores["permissions_auth"] = scores.get("permissions_auth", 0.0) + 0.90
    if parsed.has_import_error:
        scores["missing_dependency"] = scores.get("missing_dependency", 0.0) + 0.95
    if parsed.has_connection_error and not parsed.has_permission_error:
        scores["infrastructure_resource"] = (
            scores.get("infrastructure_resource", 0.0) + 0.80
        )
    if parsed.has_oom:
        scores["infrastructure_resource"] = (
            scores.get("infrastructure_resource", 0.0) + 0.90
        )
    if parsed.has_schema_mismatch:
        scores["schema_data_quality"] = scores.get("schema_data_quality", 0.0) + 0.85

    # Boost based on exception type name content
    ex_lower = parsed.exception_type.lower()
    if any(
        kw in ex_lower
        for kw in ("sql", "programming", "operational", "integrity", "data")
    ):
        scores["sql_error"] = scores.get("sql_error", 0.0) + 0.40
    if "import" in ex_lower or "module" in ex_lower:
        scores["missing_dependency"] = scores.get("missing_dependency", 0.0) + 0.40
    if "timeout" in ex_lower:
        scores["timeout"] = scores.get("timeout", 0.0) + 0.40
    if "permission" in ex_lower or "auth" in ex_lower or "access" in ex_lower:
        scores["permissions_auth"] = scores.get("permissions_auth", 0.0) + 0.40
    if "connection" in ex_lower or "network" in ex_lower or "dns" in ex_lower:
        scores["infrastructure_resource"] = (
            scores.get("infrastructure_resource", 0.0) + 0.40
        )

    if not scores:
        return FailureClassification(
            failure_type="unknown",
            confidence=0.3,
            details={"reason": "no clear signal detected in logs"},
        )

    best_type = max(scores, key=lambda k: scores[k])
    confidence = min(scores[best_type], 1.0)

    signals: dict[str, float] = {
        k: round(v, 2) for k, v in sorted(scores.items(), key=lambda x: -x[1])
    }
    details: dict[str, Any] = {"signals_found": signals}
    if parsed.exception_type:
        details["exception_type"] = parsed.exception_type
    if parsed.missing_module:
        details["missing_module"] = parsed.missing_module
    if parsed.sql_error:
        details["sql_error"] = parsed.sql_error

    return FailureClassification(
        failure_type=best_type,
        confidence=round(confidence, 2),
        details=details,
    )
