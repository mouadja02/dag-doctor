"""Shared utilities for LLM providers — prompt building, response parsing, fallback."""

from __future__ import annotations

import json
import logging

from airflow_copilot.models import FailureClassification, LLMExplanation

logger = logging.getLogger(__name__)


def build_prompt(
    dag_id: str,
    task_id: str,
    classification: FailureClassification,
    log_snippet: str,
) -> str:
    return f"""Analyze this Airflow task failure:

DAG ID: {dag_id}
Task ID: {task_id}
Failure Type: {classification.failure_type}
Confidence: {classification.confidence:.0%}
Exception Type: {classification.details.get("exception_type", "unknown")}

Error log snippet:
---
{log_snippet}
---

Return a JSON object with these fields:
{{"summary": "1-2 sentence summary of what failed",
 "root_cause": "1-3 sentences explaining the likely root cause",
 "remediation_steps": ["step 1", "step 2", ...],
 "what_not_to_do": ["dangerous action 1", ...]}}"""


def parse_response(text: str, confidence: float) -> LLMExplanation:
    try:
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]
        data = json.loads(text)
        return LLMExplanation(
            summary=data.get("summary", ""),
            root_cause=data.get("root_cause", ""),
            confidence=max(confidence, 0.5),
            remediation_steps=data.get("remediation_steps", []),
            what_not_to_do=data.get("what_not_to_do", []),
        )
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        logger.warning("Failed to parse LLM response: %s", e)
        return LLMExplanation(
            summary="LLM analysis completed but response could not be parsed.",
            root_cause=text[:500],
            confidence=0.3,
            remediation_steps=["Review the full log manually."],
            what_not_to_do=[],
        )


def fallback_explanation(classification: FailureClassification) -> LLMExplanation:
    from airflow_copilot.llm import _fallback_explanation as _fallback

    return _fallback(classification)
