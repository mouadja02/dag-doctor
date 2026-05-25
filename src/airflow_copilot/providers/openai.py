"""OpenAI provider — native OpenAI SDK with structured output (JSON mode)."""

from __future__ import annotations

import logging
import time

from openai import OpenAI

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
Analyze the failed task details and produce a structured explanation.
Rules:
1. Be specific — mention column names, table names, file paths, error types.
2. Root cause should be 1-3 sentences of plain English.
3. Remediation steps must be safe.
4. "What not to do" lists dangerous actions to avoid.
5. Respond ONLY with a JSON object matching the requested schema."""


class OpenAIProvider(BaseLLMProvider):
    def __init__(self, api_key: str | None = None, model: str | None = None):
        settings = get_settings()
        self.api_key = api_key or settings.openrouter_api_key
        self.model = model or "gpt-4o"
        self.client = OpenAI(api_key=self.api_key)
        self._last_tokens = 0
        self._last_latency_ms = 0

    def provider_name(self) -> str:
        return "openai"

    def explain(
        self,
        dag_id: str,
        task_id: str,
        error_log: str,
        classification: FailureClassification,
    ) -> LLMExplanation:
        snippet = error_log[:3000]
        prompt = build_prompt(dag_id, task_id, classification, snippet)

        start = time.time()
        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.3,
                max_tokens=2000,
                response_format={"type": "json_object"},
            )
            self._last_latency_ms = int((time.time() - start) * 1000)
            self._last_tokens = resp.usage.total_tokens if resp.usage else 0
            text = resp.choices[0].message.content or ""
            return parse_response(text, classification.confidence)
        except Exception as e:
            logger.error("OpenAI call failed: %s", e)
            return fallback_explanation(classification)

    @property
    def last_token_count(self) -> int:
        return self._last_tokens

    @property
    def last_latency_ms(self) -> int:
        return self._last_latency_ms
