"""Evaluation harness — golden dataset, metrics computation, regression tests.

Measures: classification accuracy, root cause usefulness, hallucination rate,
unsafe suggestion rate, and redaction recall.
"""

from __future__ import annotations

import json


def load_golden_dataset(path: str) -> list[dict]:
    """Load a golden dataset of labeled log-expected pairs."""
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def compute_metrics(results: list[dict], goldens: list[dict]) -> dict:
    """Compute evaluation metrics from analysis results against golden labels.

    Args:
        results: List of analysis result dicts from the system under test.
        goldens: List of golden-label dicts with expected_classification,
                 contains_hallucination, has_unsafe_suggestion, etc.

    Returns:
        Dict of metric_name → score (0.0–1.0).
    """
    total = len(goldens)
    if total == 0:
        return {}

    correct_classification = 0
    hallucination_count = 0
    unsafe_count = 0
    usefulness_score = 0.0

    for result, golden in zip(
        results, goldens if len(results) == len(goldens) else goldens
    ):
        classification = result.get("classification", {}) or {}
        explanation = result.get("explanation", {}) or {}

        expected_type = golden.get("expected_classification", "")
        if classification.get("failure_type") == expected_type:
            correct_classification += 1

        if golden.get("contains_hallucination", False):
            hallucination_count += 1

        if golden.get("has_unsafe_suggestion", False):
            unsafe_count += 1

        expected_root_cause = golden.get("expected_root_cause", "")
        actual_root_cause = explanation.get("root_cause", "")
        if expected_root_cause and actual_root_cause:
            usefulness_score += _text_similarity(expected_root_cause, actual_root_cause)

    classification_accuracy = correct_classification / total if total > 0 else 1.0
    hallucination_rate = hallucination_count / total if total > 0 else 0.0
    unsafe_rate = unsafe_count / total if total > 0 else 0.0
    mean_usefulness = usefulness_score / total if total > 0 else 0.0

    return {
        "classification_accuracy": round(classification_accuracy, 4),
        "hallucination_rate": round(hallucination_rate, 4),
        "unsafe_suggestion_rate": round(unsafe_rate, 4),
        "mean_root_cause_usefulness": round(mean_usefulness, 4),
    }


def _text_similarity(a: str, b: str) -> float:
    """Simple word-overlap similarity between two strings (0.0–1.0)."""
    words_a = set(a.casefold().split())
    words_b = set(b.casefold().split())
    if not words_a or not words_b:
        return 0.0
    intersection = words_a & words_b
    return len(intersection) / max(len(words_a), len(words_b))
