"""Anthropic provider — native Anthropic SDK."""

from __future__ import annotations

import logging
import time

from airflow_copilot.config import get_settings
from airflow_copilot.models import FailureClassification, LLMExplanation
from airflow_copilot.providers.base import BaseLLMProvider
from airflow_copilot.providers.utils import (
    build_prompt,
    fallback_explanation,
    parse_response,
)

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """You are an expert Airflow data engineering incident analyst.
Analyze the failed task details and produce a structured JSON explanation.
Respond ONLY with a JSON object matching this schema:
{"summary": "1-2 sentence summary", "root_cause": "1-3 sentences",
 "remediation_steps": ["step 1", "step 2"], "what_not_to_do": ["dangerous action"]}"""


class AnthropicProvider(BaseLLMProvider):
    def __init__(self, api_key: str | None = None, model: str | None = None):
        settings = get_settings()
        self.api_key = api_key or settings.openrouter_api_key
        self.model = model or "claude-3-5-sonnet-20241022"
        self._last_tokens = 0
        self._last_latency_ms = 0
        self._available = False
        try:
            import anthropic

            self.client = anthropic.Anthropic(api_key=self.api_key)
            self._available = True
        except ImportError:
            logger.warning("anthropic SDK not installed — Anthropic provider disabled")

    def provider_name(self) -> str:
        return "anthropic"

    def explain(
        self,
        dag_id: str,
        task_id: str,
        error_log: str,
        classification: FailureClassification,
    ) -> LLMExplanation:
        if not self._available:
            return fallback_explanation(classification)

        snippet = error_log[:3000]
        prompt = build_prompt(dag_id, task_id, classification, snippet)

        start = time.time()
        try:
            resp = self.client.messages.create(
                model=self.model,
                max_tokens=2000,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            self._last_latency_ms = int((time.time() - start) * 1000)
            self._last_tokens = (
                resp.usage.input_tokens + resp.usage.output_tokens if resp.usage else 0
            )
            text = (
                resp.content[0].text
                if resp.content and hasattr(resp.content[0], "text")
                else ""
            )
            return parse_response(text, classification.confidence)
        except Exception as e:
            logger.error("Anthropic call failed: %s", e)
            return fallback_explanation(classification)

    @property
    def last_token_count(self) -> int:
        return self._last_tokens

    @property
    def last_latency_ms(self) -> int:
        return self._last_latency_ms
