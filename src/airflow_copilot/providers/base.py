"""Abstract LLM provider base class."""

from __future__ import annotations

from abc import ABC, abstractmethod

from airflow_copilot.models import FailureClassification, LLMExplanation


class BaseLLMProvider(ABC):
    """Abstract base class for LLM providers."""

    @abstractmethod
    def explain(
        self,
        dag_id: str,
        task_id: str,
        error_log: str,
        classification: FailureClassification,
    ) -> LLMExplanation: ...

    @abstractmethod
    def provider_name(self) -> str: ...
