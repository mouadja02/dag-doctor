"""Background analysis job queue — thread-pool-based async analysis.

Uses Python's ThreadPoolExecutor. Job states tracked in the database.
No external dependencies (no Redis/Celery required).
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone

from airflow_copilot.database import get_session
from airflow_copilot.orm import AnalysisJob

logger = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="analysis-")
_futures: dict[int, Future] = {}
_lock = threading.Lock()


def _update_job(job_id: int, **kwargs):
    try:
        with get_session() as session:
            job = session.get(AnalysisJob, job_id)
            if job:
                for key, value in kwargs.items():
                    setattr(job, key, value)
                job.updated_at = datetime.now(timezone.utc)
                session.commit()
    except Exception as e:
        logger.error("Failed to update job %s: %s", job_id, e)


def create_job(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int = 1,
    provider: str = "fallback",
) -> int:
    """Create a new analysis job and return its ID."""
    with get_session() as session:
        job = AnalysisJob(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            try_number=try_number,
            status="queued",
            progress="Waiting to start",
            provider=provider,
        )
        session.add(job)
        session.commit()
        return int(job.id) if job.id else 0


def get_job(job_id: int) -> dict | None:
    """Get job status by ID."""
    with get_session() as session:
        job = session.get(AnalysisJob, job_id)
        if not job:
            return None
        return {
            "id": job.id,
            "dag_id": job.dag_id,
            "task_id": job.task_id,
            "status": job.status,
            "progress": job.progress,
            "result_id": job.result_id,
            "error_message": job.error_message,
            "provider": job.provider,
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "updated_at": job.updated_at.isoformat() if job.updated_at else None,
        }


def cancel_job(job_id: int) -> bool:
    """Cancel a queued or running job."""
    with _lock:
        future = _futures.get(job_id)
        if future and not future.done():
            cancelled = future.cancel()
            if cancelled:
                _update_job(job_id, status="cancelled", progress="Cancelled by user")
            return cancelled
    _update_job(job_id, status="cancelled", progress="Cancelled by user")
    return True


def submit_job(job_id: int, analysis_fn, *args) -> bool:
    """Submit an analysis job to the thread pool. analysis_fn(job_id, *args) will be called."""
    _update_job(job_id, status="running", progress="Analysis started")

    def wrapper():
        try:
            analysis_fn(job_id, *args)
        except Exception as e:
            logger.error("Analysis job %s failed: %s", job_id, e)
            _update_job(
                job_id, status="failed", error_message=str(e), progress="Failed"
            )

    future = _executor.submit(wrapper)
    with _lock:
        _futures[job_id] = future
    return True


def complete_job(job_id: int, result_id: int):
    """Mark a job as successfully completed with a result."""
    _update_job(job_id, status="succeeded", result_id=result_id, progress="Complete")


def fail_job(job_id: int, error: str):
    """Mark a job as failed."""
    _update_job(job_id, status="failed", error_message=error, progress="Failed")


def shutdown():
    """Shutdown the thread pool. Call on app shutdown."""
    _executor.shutdown(wait=False)
