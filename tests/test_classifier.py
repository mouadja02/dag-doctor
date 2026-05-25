"""Tests for the failure classifier module."""

from airflow_copilot.classifier import classify
from airflow_copilot.log_parser import ParsedLog


class TestClassify:
    def test_classifies_sql_error(self):
        parsed = ParsedLog(
            sql_error="column 'market_cap_usd' does not exist",
            exception_type="ProgrammingError",
        )
        result = classify(parsed)

        assert result.failure_type == "sql_error"
        assert result.confidence > 0.8

    def test_classifies_timeout(self):
        parsed = ParsedLog(has_timeout=True, exception_type="AirflowTaskTimeout")
        result = classify(parsed)

        assert result.failure_type == "timeout"
        assert result.confidence > 0.8

    def test_classifies_permission_error(self):
        parsed = ParsedLog(has_permission_error=True)
        result = classify(parsed)

        assert result.failure_type == "permissions_auth"
        assert result.confidence > 0.8

    def test_classifies_import_error(self):
        parsed = ParsedLog(
            has_import_error=True,
            missing_module="web3",
            exception_type="ModuleNotFoundError",
        )
        result = classify(parsed)

        assert result.failure_type == "missing_dependency"
        assert result.confidence > 0.8

    def test_classifies_infrastructure_error(self):
        parsed = ParsedLog(has_connection_error=True)
        result = classify(parsed)

        assert result.failure_type == "infrastructure_resource"

    def test_classifies_oom(self):
        parsed = ParsedLog(has_oom=True)
        result = classify(parsed)

        assert result.failure_type == "infrastructure_resource"
        assert result.confidence > 0.8

    def test_classifies_schema_mismatch(self):
        parsed = ParsedLog(has_schema_mismatch=True)
        result = classify(parsed)

        assert result.failure_type == "schema_data_quality"
        assert result.confidence > 0.7

    def test_returns_unknown_for_empty_parsed_log(self):
        parsed = ParsedLog()
        result = classify(parsed)

        assert result.failure_type == "unknown"
        assert result.confidence < 0.5

    def test_confidence_is_capped_at_1(self):
        parsed = ParsedLog(
            sql_error="error",
            has_timeout=True,
            exception_type="ProgrammingError",
        )
        result = classify(parsed)

        assert result.confidence <= 1.0
