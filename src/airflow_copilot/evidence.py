"""Advanced evidence extraction — provider-specific extractors with source offsets."""

from __future__ import annotations

from airflow_copilot.log_parser import ParsedLog
from airflow_copilot.models import EvidenceItem


def extract_evidence(
    parsed_log: ParsedLog, error_log: str, operator: str = ""
) -> list[EvidenceItem]:
    """Extract evidence items with source log offsets and provider-specific signals.

    Returns a list of EvidenceItem objects with source_line, context_lines, and signal_type.
    """
    items: list[EvidenceItem] = []

    if parsed_log.traceback_lines:
        clean_lines = [line for line in parsed_log.traceback_lines if line.strip()]
        items.append(
            EvidenceItem(
                source_line=clean_lines[-1] if clean_lines else "",
                context_lines=clean_lines[:4],
                signal_type="traceback",
            )
        )

    if parsed_log.sql_error:
        lines = error_log.split("\n")
        context = []
        for i, line in enumerate(lines):
            if parsed_log.sql_error.casefold() in line.casefold():
                start = max(0, i - 2)
                context = [
                    line.strip() for line in lines[start : i + 3] if line.strip()
                ]
                break
        items.append(
            EvidenceItem(
                source_line=parsed_log.sql_error,
                context_lines=context or [parsed_log.sql_error],
                signal_type="sql_error",
            )
        )

    if parsed_log.has_permission_error:
        for pattern in [
            "permission denied",
            "access denied",
            "authentication failed",
            "incorrect username or password",
        ]:
            if pattern in error_log.casefold():
                lines = error_log.split("\n")
                for line in lines:
                    if pattern in line.casefold():
                        items.append(
                            EvidenceItem(
                                source_line=line.strip(),
                                signal_type=parsed_log.auth_error_source
                                or "auth_error",
                            )
                        )
                        break
                break

    if parsed_log.has_import_error:
        items.append(
            EvidenceItem(
                source_line=f"No module named '{parsed_log.missing_module}'"
                if parsed_log.missing_module
                else "ImportError",
                signal_type="missing_dependency",
            )
        )

    if parsed_log.has_timeout:
        lines = error_log.split("\n")
        for line in lines:
            if "timeout" in line.casefold() or "sigterm" in line.casefold():
                items.append(
                    EvidenceItem(source_line=line.strip(), signal_type="timeout")
                )
                break

    if parsed_log.has_oom:
        lines = error_log.split("\n")
        for line in lines:
            if any(
                kw in line.casefold()
                for kw in (
                    "out of memory",
                    "oom",
                    "memoryerror",
                    "cannot allocate memory",
                )
            ):
                items.append(EvidenceItem(source_line=line.strip(), signal_type="oom"))
                break

    _extract_provider_specifics(parsed_log, error_log, operator, items)

    return items


def _extract_provider_specifics(
    parsed_log: ParsedLog,
    error_log: str,
    operator: str,
    items: list[EvidenceItem],
):
    """Add provider-specific evidence based on operator type."""
    content = error_log.casefold()

    if "snowflake" in content or operator.casefold() == "snowflakeoperator":
        for line in error_log.split("\n"):
            if any(
                kw in line.casefold()
                for kw in (
                    "snowflake",
                    "sql compilation error",
                    "authentication failed",
                )
            ):
                items.append(
                    EvidenceItem(source_line=line.strip(), signal_type="snowflake")
                )
                break

    if "bigquery" in content:
        for line in error_log.split("\n"):
            if any(
                kw in line.casefold()
                for kw in ("bigquery", "access denied", "not found")
            ):
                items.append(
                    EvidenceItem(source_line=line.strip(), signal_type="bigquery")
                )
                break

    if "dbt" in content or "dbt" in operator.casefold():
        for line in error_log.split("\n"):
            if any(
                kw in line.casefold()
                for kw in ("dbt", "compilation error", "model", "test")
            ):
                items.append(EvidenceItem(source_line=line.strip(), signal_type="dbt"))
                break

    if "spark" in content or operator.casefold() in (
        "sparkoperator",
        "sparksqloperator",
    ):
        for line in error_log.split("\n"):
            if any(
                kw in line.casefold() for kw in ("spark", "executor", "driver", "yarn")
            ):
                items.append(
                    EvidenceItem(source_line=line.strip(), signal_type="spark")
                )
                break

    if "kubernetes" in content or operator.casefold() in ("kubernetespodoperator",):
        for line in error_log.split("\n"):
            if any(
                kw in line.casefold()
                for kw in ("kubernetes", "pod", "container", "crashloopbackoff")
            ):
                items.append(
                    EvidenceItem(source_line=line.strip(), signal_type="kubernetes")
                )
                break
