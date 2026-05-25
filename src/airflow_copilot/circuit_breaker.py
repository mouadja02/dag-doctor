"""Circuit breaker pattern for external API calls — prevents cascading failures."""

from __future__ import annotations

import logging
import time
import threading

logger = logging.getLogger(__name__)

State = str  # "closed" | "open" | "half_open"


class CircuitBreaker:
    """Simple in-memory circuit breaker.

    Tracks failures. After threshold consecutive failures, opens the circuit.
    After recovery_timeout, allows one test request (half-open).
    On success, closes the circuit. On failure, re-opens.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state: State = "closed"
        self._failure_count: int = 0
        self._last_failure_time: float = 0.0
        self._lock = threading.Lock()

    @property
    def is_open(self) -> bool:
        return self._state == "open"

    def call(self, func, *args, **kwargs):
        """Execute func(*args, **kwargs) with circuit breaker protection.

        Returns the function's return value, or None if the circuit is open.
        Raises the original exception on failure in closed/half-open state.
        """
        with self._lock:
            if self._state == "open":
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = "half_open"
                    logger.info("Circuit %s: half-open, testing", self.name)
                else:
                    logger.warning("Circuit %s: open, request rejected", self.name)
                    return None
            elif self._state == "closed":
                pass

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure(e)
            raise

    def _on_success(self):
        with self._lock:
            if self._state == "half_open":
                logger.info("Circuit %s: closed after successful test", self.name)
            self._state = "closed"
            self._failure_count = 0

    def _on_failure(self, error: Exception):
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == "half_open":
                self._state = "open"
                logger.error(
                    "Circuit %s: re-opened after test failure: %s", self.name, error
                )
            elif self._failure_count >= self.failure_threshold:
                self._state = "open"
                logger.error(
                    "Circuit %s: opened after %d failures (last: %s)",
                    self.name,
                    self._failure_count,
                    error,
                )

    def reset(self):
        with self._lock:
            self._state = "closed"
            self._failure_count = 0


# Global circuit breakers
_llm_circuit = CircuitBreaker("llm", failure_threshold=3, recovery_timeout=120.0)
_airflow_circuit = CircuitBreaker("airflow", failure_threshold=5, recovery_timeout=60.0)


def get_llm_circuit() -> CircuitBreaker:
    return _llm_circuit


def get_airflow_circuit() -> CircuitBreaker:
    return _airflow_circuit
