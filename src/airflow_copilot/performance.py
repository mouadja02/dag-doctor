"""Performance optimizations — concurrent fetching, caching, log chunking."""

from __future__ import annotations

import functools
import hashlib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable

import httpx

from airflow_copilot.models import DAGRun

CONCURRENT_EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix="perf-")

_cache: dict[str, tuple[float, Any]] = {}
_cache_ttl: float = 30.0


def cached(ttl: float = 30.0):
    """Simple in-memory TTL cache decorator."""

    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key_parts = (
                [func.__name__]
                + [str(a) for a in args]
                + [f"{k}={v}" for k, v in sorted(kwargs.items())]
            )
            key = hashlib.md5("|".join(key_parts).encode()).hexdigest()

            if key in _cache:
                cached_at, value = _cache[key]
                if time.time() - cached_at < ttl:
                    return value

            result = func(*args, **kwargs)
            _cache[key] = (time.time(), result)
            return result

        return wrapper

    return decorator


def clear_cache():
    _cache.clear()


def fetch_dag_runs_concurrent(
    dag_ids: list[str],
    client: httpx.Client,
    limit: int = 50,
) -> list[DAGRun]:
    """Fetch failed DAG runs from multiple DAGs in parallel."""

    def fetch_one(dag_id: str) -> list[dict]:
        try:
            resp = client.get(
                f"/api/v2/dags/{dag_id}/dagRuns",
                params={"limit": limit, "state": "failed"},
            )
            resp.raise_for_status()
            return resp.json().get("dag_runs", [])
        except Exception:
            return []

    all_runs: list[DAGRun] = []
    futures = {CONCURRENT_EXECUTOR.submit(fetch_one, did): did for did in dag_ids}

    for future in as_completed(futures):
        dag_runs = future.result()
        for r in dag_runs:
            all_runs.append(
                DAGRun(
                    dag_id=r.get("dag_id", ""),
                    dag_run_id=r.get("dag_run_id", ""),
                    logical_date=r.get("logical_date"),
                    start_date=r.get("start_date"),
                    end_date=r.get("end_date"),
                    state=r.get("state", ""),
                    run_type=r.get("run_type", ""),
                    conf=r.get("conf", {}) or {},
                )
            )

    all_runs.sort(key=lambda r: r.logical_date or "", reverse=True)
    return all_runs[:limit]


def summarize_large_log(log_text: str, max_chars: int = 8000) -> str:
    """Summarize a large log by extracting the most relevant sections.

    Keeps the first 10% (context), the full traceback (if any), and the last 20% (tail).
    """
    if len(log_text) <= max_chars:
        return log_text

    lines = log_text.split("\n")
    traceback_start = None
    traceback_end = None

    for i, line in enumerate(lines):
        if "Traceback (most recent call last):" in line:
            traceback_start = i
        if traceback_start is not None and (
            i == len(lines) - 1 or (line.strip() == "" and i > traceback_start + 2)
        ):
            if traceback_end is None:
                traceback_end = i
                break

    parts = []
    chars_used = 0
    head_size = max_chars // 10

    for line in lines[: max(1, len(lines) // 10)]:
        if chars_used + len(line) < head_size:
            parts.append(line)
            chars_used += len(line)

    if traceback_start is not None:
        tb_lines = lines[traceback_start : (traceback_end or (traceback_start + 20))]
        parts.append("\n--- TRACEBACK ---")
        for line in tb_lines:
            parts.append(line)
            chars_used += len(line)

    tail_size = max_chars - chars_used - 100
    if tail_size > 0:
        parts.append("\n--- TAIL ---")
        for line in lines[-max(1, len(lines) // 5) :]:
            if chars_used + len(line) < max_chars:
                parts.append(line)
                chars_used += len(line)

    return "\n".join(parts)
