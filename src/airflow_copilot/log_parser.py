"""Log parser — extracts structured failure information from raw Airflow task logs."""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class ParsedLog:
    exception_type: str = ""
    exception_message: str = ""
    sql_error: str = ""
    traceback_lines: list[str] = field(default_factory=list)
    has_timeout: bool = False
    has_permission_error: bool = False
    has_import_error: bool = False
    has_connection_error: bool = False
    has_oom: bool = False
    has_schema_mismatch: bool = False
    missing_module: str = ""
    auth_error_source: str = ""


_TRACEBACK_PATTERN = re.compile(
    r"^Traceback \(most recent call last\):",
    re.MULTILINE,
)
_EXCEPTION_LINE_PATTERN = re.compile(
    r"^(?=\S)(.*?)(Error|Exception|Warning|Failure|Fault|Timeout|Killed)(?::\s*(.+))?",
    re.MULTILINE,
)
_SQL_ERROR_PATTERNS = [
    re.compile(r"(?:column\s+\S+\s+does\s+not\s+exist)", re.IGNORECASE),
    re.compile(r"(?:relation\s+\S+\s+does\s+not\s+exist)", re.IGNORECASE),
    re.compile(r"(?:syntax\s+error\s+(?:at|near)\s)", re.IGNORECASE),
    re.compile(r"(?:invalid\s+input\s+syntax)", re.IGNORECASE),
    re.compile(r"(?:cannot\s+cast\s+type)", re.IGNORECASE),
    re.compile(
        r"(?:violates\s+(?:foreign\s+key|unique|not-null|check)\s+constraint)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:ProgrammingError|OperationalError|IntegrityError|DataError)", re.IGNORECASE
    ),
]
_TIMEOUT_PATTERNS = [
    re.compile(r"(?:timed?\s*out|timeout)", re.IGNORECASE),
    re.compile(r"(?:execution\s+timeout)", re.IGNORECASE),
    re.compile(r"(?:SIGTERM|SIGKILL)", re.IGNORECASE),
    re.compile(r"(?:task\s+(?:failed|killed).*?(?:time|duration))", re.IGNORECASE),
]
_PERMISSION_PATTERNS = [
    re.compile(r"(?:permission\s+denied)", re.IGNORECASE),
    re.compile(r"(?:access\s+denied)", re.IGNORECASE),
    re.compile(r"(?:authentication\s+failed)", re.IGNORECASE),
    re.compile(r"(?:incorrect\s+username\s+or\s+password)", re.IGNORECASE),
    re.compile(r"(?:401|403).*?(?:unauthorized|forbidden)", re.IGNORECASE),
    re.compile(r"(?:not\s+authorized)", re.IGNORECASE),
]
_IMPORT_PATTERNS = [
    re.compile(r"(?:No\s+module\s+named\s+['\"]?([^'\"]+)['\"]?)", re.IGNORECASE),
    re.compile(r"(?:ModuleNotFoundError|ImportError)", re.IGNORECASE),
]
_CONNECTION_PATTERNS = [
    re.compile(r"(?:connection\s+refused)", re.IGNORECASE),
    re.compile(r"(?:could\s+not\s+connect)", re.IGNORECASE),
    re.compile(r"(?:name\s+or\s+service\s+not\s+known)", re.IGNORECASE),
    re.compile(r"(?:no\s+route\s+to\s+host)", re.IGNORECASE),
]
_OOM_PATTERNS = [
    re.compile(r"(?:out\s+of\s+memory|OOM)", re.IGNORECASE),
    re.compile(r"(?:MemoryError)", re.IGNORECASE),
    re.compile(r"(?:cannot\s+allocate\s+memory)", re.IGNORECASE),
]
_SCHEMA_PATTERNS = [
    re.compile(r"(?:schema\s+(?:mismatch|changed?|error))", re.IGNORECASE),
    re.compile(r"(?:invalid\s+column\s+name)", re.IGNORECASE),
    re.compile(r"(?:column\s+\S+\s+not\s+found)", re.IGNORECASE),
]
_AUTH_SOURCE_PATTERN = re.compile(
    r"(?:Snowflake|snowflake|PostgreSQL|MySQL|BigQuery|Redshift)",
    re.IGNORECASE,
)


def parse_log(log_text: str) -> ParsedLog:
    """Parse a raw Airflow task log string into a structured ParsedLog.

    Extracts exception type, traceback, SQL errors, timeout signals,
    permission errors, import errors, connection errors, OOM, and schema issues.
    """
    result = ParsedLog()
    text = _redact_secrets(log_text)

    _extract_traceback(text, result)
    _extract_exception(text, result)
    _detect_signals(text, result)

    return result


def _extract_traceback(text: str, result: ParsedLog) -> None:
    tb_match = _TRACEBACK_PATTERN.search(text)
    if tb_match:
        start = tb_match.start()
        tb_block = text[start:]
        lines = tb_block.split("\n")
        result.traceback_lines = lines[:40]
        # Find and remove the traceback block to avoid double-detection
        end_marker = None
        for i, line in enumerate(lines):
            if _EXCEPTION_LINE_PATTERN.match(line) and i > 0:
                end_marker = i
                break
        if end_marker:
            result.traceback_lines = lines[: end_marker + 1]


def _extract_exception(text: str, result: ParsedLog) -> None:
    for match in _EXCEPTION_LINE_PATTERN.finditer(text):
        prefix = match.group(1) or ""
        suffix = match.group(2) or ""
        ex_type = prefix + suffix
        ex_msg = match.group(3) or ""

        # Skip artifacts from structured log markers
        if "---" in prefix or "---" in suffix:
            continue

        # Must contain a known exception keyword in the suffix
        if any(
            kw in suffix.lower()
            for kw in ("error", "exception", "failure", "fault", "timeout", "killed")
        ):
            result.exception_type = ex_type.strip()
            result.exception_message = ex_msg.strip()
            return


def _detect_signals(text: str, result: ParsedLog) -> None:
    for pattern in _SQL_ERROR_PATTERNS:
        m = pattern.search(text)
        if m:
            result.sql_error = m.group(0)
            break

    for pattern in _TIMEOUT_PATTERNS:
        if pattern.search(text):
            result.has_timeout = True
            break

    for pattern in _PERMISSION_PATTERNS:
        if pattern.search(text):
            result.has_permission_error = True
            auth_match = _AUTH_SOURCE_PATTERN.search(text)
            if auth_match:
                result.auth_error_source = auth_match.group(0)
            break

    for pattern in _IMPORT_PATTERNS:
        m = pattern.search(text)
        if m:
            result.has_import_error = True
            if m.lastindex and m.lastindex >= 1:
                result.missing_module = m.group(1)
            break

    for pattern in _CONNECTION_PATTERNS:
        if pattern.search(text):
            result.has_connection_error = True
            break

    for pattern in _OOM_PATTERNS:
        if pattern.search(text):
            result.has_oom = True
            break

    for pattern in _SCHEMA_PATTERNS:
        if pattern.search(text):
            result.has_schema_mismatch = True
            break


def _redact_secrets(text: str) -> str:
    """Redact common credential patterns from log text."""
    redactions = [
        (
            re.compile(
                r"(?:api[_-]?key|api[_-]?token|password|secret|token)\s*[=:]\s*\S+",
                re.IGNORECASE,
            ),
            "[REDACTED]",
        ),
        (re.compile(r"sk-[a-zA-Z0-9]{20,}", re.IGNORECASE), "[REDACTED_API_KEY]"),
        (
            re.compile(r"Authorization:\s*\S+", re.IGNORECASE),
            "Authorization: [REDACTED]",
        ),
    ]
    for pattern, replacement in redactions:
        text = pattern.sub(replacement, text)
    return text
