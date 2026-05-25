"""LLM provider registry — factory for selecting and configuring providers."""

from __future__ import annotations

import logging

from airflow_copilot.config import get_settings
from airflow_copilot.providers.base import BaseLLMProvider

logger = logging.getLogger(__name__)


def get_provider(name: str | None = None) -> BaseLLMProvider:
    """Factory: returns the configured LLM provider or fallback.

    Provider names: 'openai', 'anthropic', 'openrouter', 'fallback'
    Falls back to 'fallback' if no key is configured.
    """
    settings = get_settings()
    name = name or settings.llm_provider or "openrouter"

    if name == "openai" and settings.openrouter_api_key:
        from airflow_copilot.providers.openai import OpenAIProvider

        return OpenAIProvider(api_key=settings.openrouter_api_key)

    if name == "anthropic" and settings.openrouter_api_key:
        from airflow_copilot.providers.anthropic import AnthropicProvider

        return AnthropicProvider(api_key=settings.openrouter_api_key)

    if name == "openrouter" and settings.openrouter_api_key:
        from airflow_copilot.llm import OpenRouterProvider

        return OpenRouterProvider()

    logger.warning(
        "No LLM provider configured — using rule-based fallback. "
        "Set OPENROUTER_API_KEY in .env for AI-powered explanations."
    )
    from airflow_copilot.llm import FallbackProvider

    return FallbackProvider()
