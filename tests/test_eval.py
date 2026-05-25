"""Evaluation harness tests — validate metrics computation against golden dataset."""

from __future__ import annotations

import os

from airflow_copilot.eval_harness import compute_metrics, load_golden_dataset


FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


class TestEvalHarness:
    def test_load_golden_dataset(self):
        path = os.path.join(FIXTURE_DIR, "golden_dataset.json")
        data = load_golden_dataset(path)
        assert len(data) == 5
        assert all("id" in entry for entry in data)
        assert all("expected_classification" in entry for entry in data)

    def test_compute_metrics_perfect_match(self):
        goldens = [
            {
                "expected_classification": "sql_error",
                "expected_root_cause": "column does not exist",
                "contains_hallucination": False,
                "has_unsafe_suggestion": False,
            }
        ]
        results = [
            {
                "classification": {"failure_type": "sql_error"},
                "explanation": {"root_cause": "column market_cap_usd does not exist"},
            }
        ]
        metrics = compute_metrics(results, goldens)
        assert metrics["classification_accuracy"] == 1.0
        assert metrics["hallucination_rate"] == 0.0
        assert metrics["unsafe_suggestion_rate"] == 0.0
        assert metrics["mean_root_cause_usefulness"] > 0.0

    def test_compute_metrics_mismatch(self):
        goldens = [
            {
                "expected_classification": "sql_error",
                "expected_root_cause": "sql issue",
                "contains_hallucination": False,
                "has_unsafe_suggestion": False,
            },
        ]
        results = [
            {
                "classification": {"failure_type": "timeout"},
                "explanation": {"root_cause": "timed out"},
            },
        ]
        metrics = compute_metrics(results, goldens)
        assert metrics["classification_accuracy"] == 0.0

    def test_compute_metrics_empty(self):
        assert compute_metrics([], []) == {}

    def test_golden_dataset_coverage(self):
        path = os.path.join(FIXTURE_DIR, "golden_dataset.json")
        goldens = load_golden_dataset(path)
        expected_types = {g["expected_classification"] for g in goldens}
        assert "sql_error" in expected_types
        assert "timeout" in expected_types
        assert "permissions_auth" in expected_types
        assert "missing_dependency" in expected_types
        assert "python_exception" in expected_types
