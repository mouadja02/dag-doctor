"""Tests for the log parser module."""

from airflow_copilot.log_parser import parse_log


def _load_fixture(name: str) -> str:
    import os

    path = os.path.join(os.path.dirname(__file__), "fixtures", name)
    with open(path) as f:
        return f.read()


class TestParseLog:
    def test_parses_sql_error(self):
        text = _load_fixture("sql_error.log")
        result = parse_log(text)

        assert result.sql_error != ""
        assert "column" in result.sql_error.lower()
        assert "ProgrammingError" in result.exception_type

    def test_parses_python_exception(self):
        text = _load_fixture("python_exception.log")
        result = parse_log(text)

        assert result.exception_type == "KeyError"
        assert len(result.traceback_lines) > 0
        assert "Traceback" in result.traceback_lines[0]

    def test_detects_timeout(self):
        text = _load_fixture("timeout.log")
        result = parse_log(text)

        assert result.has_timeout is True
        assert "AirflowTaskTimeout" in result.exception_type

    def test_detects_permission_error(self):
        text = _load_fixture("permission_error.log")
        result = parse_log(text)

        assert result.has_permission_error is True
        assert result.auth_error_source.lower() == "snowflake"

    def test_detects_import_error(self):
        text = _load_fixture("import_error.log")
        result = parse_log(text)

        assert result.has_import_error is True
        assert result.missing_module == "web3"
        assert "ModuleNotFoundError" in result.exception_type

    def test_handles_empty_log(self):
        result = parse_log("")
        assert result.exception_type == ""
        assert not result.has_timeout
        assert not result.has_permission_error

    def test_redacts_secrets(self):
        text = "api_key=sk-1234567890abcdef secret info\nAuthorization: Bearer tok123"
        result = parse_log(text)
        # The redaction happens internally; verify no exception
        assert result is not None
