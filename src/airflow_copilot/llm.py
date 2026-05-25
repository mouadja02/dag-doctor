"""LLM provider abstraction — supports OpenRouter, with graceful fallback."""

from __future__ import annotations

import json
import logging

from airflow_copilot.config import get_settings
from airflow_copilot.models import FailureClassification, LLMExplanation

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """You are an expert Airflow data engineering incident analyst.
Analyze the failed task details and produce a structured explanation.

Rules:
1. Be specific — mention column names, table names, file paths, error types.
2. Root cause should be 1-3 sentences of plain English.
3. Remediation steps must be safe — never suggest dropping tables, deleting data,
   or running destructive operations without backups.
4. "What not to do" lists dangerous or counterproductive actions to avoid.
5. Respond ONLY with a JSON object matching the requested schema."""


class LLMProvider:
    """Base class for LLM providers."""

    def explain(
        self,
        dag_id: str,
        task_id: str,
        error_log: str,
        classification: FailureClassification,
    ) -> LLMExplanation:
        raise NotImplementedError


class OpenRouterProvider(LLMProvider):
    """OpenRouter provider using the OpenAI-compatible API."""

    def __init__(self) -> None:
        settings = get_settings()
        self.api_key = settings.openrouter_api_key
        self.model = settings.openrouter_model

        from openai import OpenAI

        self.client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=self.api_key,
        )

    def explain(
        self,
        dag_id: str,
        task_id: str,
        error_log: str,
        classification: FailureClassification,
    ) -> LLMExplanation:
        snippet = error_log[:3000]
        user_prompt = _build_prompt(dag_id, task_id, classification, snippet)

        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.3,
                max_tokens=2000,
            )
            text = resp.choices[0].message.content or ""
            return _parse_response(text, classification.confidence)
        except Exception as e:
            logger.error("OpenRouter call failed: %s", e)
            return _fallback_explanation(classification)


class FallbackProvider(LLMProvider):
    """Fallback provider that returns rule-based explanations when no LLM key is configured."""

    def explain(
        self,
        dag_id: str,
        task_id: str,
        error_log: str,
        classification: FailureClassification,
    ) -> LLMExplanation:
        return _fallback_explanation(classification)


def get_llm_provider() -> LLMProvider:
    """Factory: returns the configured LLM provider or fallback."""
    settings = get_settings()
    if settings.llm_provider == "openrouter" and settings.openrouter_api_key:
        return OpenRouterProvider()
    logger.warning(
        "No LLM provider configured — using rule-based fallback. "
        "Set OPENROUTER_API_KEY in .env for AI-powered explanations."
    )
    return FallbackProvider()


def _build_prompt(
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


def _parse_response(text: str, confidence: float) -> LLMExplanation:
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


def _fallback_explanation(classification: FailureClassification) -> LLMExplanation:
    ftype = classification.failure_type
    templates = {
        "sql_error": LLMExplanation(
            summary="A SQL error occurred during task execution.",
            root_cause=(
                "The task issued a SQL query that failed, likely due to a "
                "missing column, table, syntax error, or constraint violation. "
                "Check the query against the current database schema."
            ),
            confidence=classification.confidence,
            remediation_steps=[
                "Review the SQL query in the task logs for syntax errors.",
                "Verify all referenced columns and tables exist in the target database.",
                "Test the query manually in a SQL client before re-running the DAG.",
                "Check if a recent schema migration changed column names or types.",
            ],
            what_not_to_do=[
                "Do not ALTER tables to match old queries — update the query instead.",
                "Do not drop and recreate tables without backups.",
            ],
        ),
        "python_exception": LLMExplanation(
            summary="A Python exception occurred during task execution.",
            root_cause=(
                "The task code raised an unhandled exception. Review the traceback "
                "in the logs to identify the exact line and error type."
            ),
            confidence=classification.confidence,
            remediation_steps=[
                "Locate the traceback in the task logs to find the failing line.",
                "Add input validation to handle edge cases gracefully.",
                "Consider adding retries with exponential backoff for transient errors.",
            ],
            what_not_to_do=[
                "Do not blindly wrap the code in try/except without understanding the root cause.",
            ],
        ),
        "timeout": LLMExplanation(
            summary="The task exceeded its execution timeout and was killed.",
            root_cause=(
                "The task took longer than its configured execution_timeout. "
                "This could be due to a large data volume, slow external API, "
                "database lock contention, or an infinite loop."
            ),
            confidence=classification.confidence,
            remediation_steps=[
                "Increase the execution_timeout if the task legitimately needs more time.",
                "Add data chunking/pagination to process large datasets incrementally.",
                "Check for deadlocks or long-running transactions in the target database.",
                "Profile the task to identify slow operations (API calls, DB queries).",
            ],
            what_not_to_do=[
                "Do not remove the timeout entirely — set a reasonable upper bound.",
                "Do not increase parallelism without checking resource constraints.",
            ],
        ),
        "permissions_auth": LLMExplanation(
            summary="The task failed due to a permissions or authentication error.",
            root_cause=(
                "The task attempted to access a resource without proper credentials "
                "or permissions. Check the Airflow connection, API keys, or service "
                "account used by this task."
            ),
            confidence=classification.confidence,
            remediation_steps=[
                "Verify the Airflow connection credentials are correct and not expired.",
                "Check if the service account/token has the required permissions.",
                "Rotate API keys if they may have been revoked or expired.",
                "Ensure environment variables are properly set in the Airflow config.",
            ],
            what_not_to_do=[
                "Do not hardcode credentials in DAG code — use Airflow Connections or Variables.",
                "Do not grant blanket admin access to fix a permission error.",
            ],
        ),
        "missing_dependency": LLMExplanation(
            summary="The task failed because a required Python package or module is missing.",
            root_cause=(
                "The task code imports a module that is not installed in the "
                "Airflow worker environment. This often happens after adding new "
                "DAGs that depend on packages not in requirements.txt."
            ),
            confidence=classification.confidence,
            remediation_steps=[
                "Install the missing package in the Airflow environment.",
                "Add the package to requirements.txt for reproducible builds.",
                "Rebuild and restart the Airflow containers if using Docker.",
                "Consider adding a pre-flight check task that validates imports.",
            ],
            what_not_to_do=[
                "Do not pip install packages manually on running containers — they will be lost on restart.",
            ],
        ),
        "infrastructure_resource": LLMExplanation(
            summary="The task failed due to an infrastructure or resource issue.",
            root_cause=(
                "The task encountered a connection error, out-of-memory condition, "
                "or other infrastructure-level failure. Check the worker logs, "
                "available memory, and network connectivity."
            ),
            confidence=classification.confidence,
            remediation_steps=[
                "Check the Airflow worker health and resource usage (CPU, RAM, disk).",
                "Verify network connectivity to external services (database, API endpoints).",
                "Check DNS resolution if connecting to hosts by name.",
                "Consider reducing parallelism if running out of memory.",
            ],
            what_not_to_do=[
                "Do not restart services blindly — check logs and metrics first.",
            ],
        ),
        "schema_data_quality": LLMExplanation(
            summary="The task failed due to a schema mismatch or data quality issue.",
            root_cause=(
                "The data being processed does not match the expected schema — "
                "a column is missing, has the wrong type, or contains unexpected values."
            ),
            confidence=classification.confidence,
            remediation_steps=[
                "Compare the actual upstream schema against the DAG's expectations.",
                "Add schema validation checks before processing (e.g., Great Expectations, dbt tests).",
                "Check if a recent upstream change renamed or dropped a column.",
                "Add data type casting or null handling for optional fields.",
            ],
            what_not_to_do=[
                "Do not silently drop rows that fail validation — log and alert instead.",
            ],
        ),
        "upstream_dependency": LLMExplanation(
            summary="The task failed because an upstream dependency did not complete successfully.",
            root_cause=(
                "This task depends on the output of an upstream task that failed "
                "or did not run. Airflow's trigger rules may have been set to "
                "'all_success' causing the cascade failure."
            ),
            confidence=classification.confidence,
            remediation_steps=[
                "Fix the upstream task failure first — this task's failure is likely a side effect.",
                "Consider using trigger_rule='all_done' if the task can run despite upstream failures.",
                "Add proper error handling for missing or incomplete upstream data.",
            ],
            what_not_to_do=[
                "Do not skip the upstream fix — this task will keep failing until the root cause is resolved.",
            ],
        ),
    }

    fallback = templates.get(
        ftype,
        LLMExplanation(
            summary="The task failed and the cause could not be automatically determined.",
            root_cause="Manual log review is required to identify the root cause.",
            confidence=0.2,
            remediation_steps=[
                "Review the full task log manually.",
                "Check Airflow scheduler logs for context.",
            ],
            what_not_to_do=[
                "Do not clear and retry the task without understanding the failure."
            ],
        ),
    )
    fallback.confidence = classification.confidence
    return fallback
