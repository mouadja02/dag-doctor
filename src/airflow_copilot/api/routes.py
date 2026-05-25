"""FastAPI routes for the dag-doctor API."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query

from airflow_copilot.airflow_client import AirflowClient
from airflow_copilot.classifier import classify
from airflow_copilot.llm import get_llm_provider
from airflow_copilot.log_parser import parse_log
from airflow_copilot.models import AnalysisResult, FailedTask
from airflow_copilot.report_generator import generate_report
from airflow_copilot.storage import Storage

logger = logging.getLogger(__name__)

router = APIRouter()
storage = Storage()
airflow = AirflowClient()
llm = get_llm_provider()


@router.get("/health")
async def health():
    af_ok = airflow.health_check()
    return {
        "status": "ok" if af_ok else "degraded",
        "airflow_connected": af_ok,
    }


@router.get("/airflow/failed-runs")
async def list_failed_runs(limit: int = Query(50, ge=1, le=200)):
    """List recent failed DAG runs from the connected Airflow instance."""
    try:
        runs = airflow.get_failed_dag_runs(limit=limit)
        return {"count": len(runs), "failed_runs": [r.model_dump() for r in runs]}
    except Exception as e:
        logger.error("Failed to fetch DAG runs: %s", e)
        raise HTTPException(status_code=502, detail=f"Airflow API error: {e}")


@router.get("/airflow/failed-runs/{dag_id}/{run_id}")
async def get_failed_run_detail(dag_id: str, run_id: str):
    """Get detailed info for a specific failed DAG run, including task instances."""
    try:
        dag_run = airflow.get_dag_run(dag_id, run_id)
        if not dag_run:
            raise HTTPException(status_code=404, detail="DAG run not found")
        task_instances = airflow.get_task_instances(dag_id, run_id)
        failed_tis = [ti for ti in task_instances if ti.state == "failed"]
        return {
            "dag_run": dag_run.model_dump(),
            "task_instances": [ti.model_dump() for ti in task_instances],
            "failed_task_count": len(failed_tis),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to fetch DAG run detail: %s", e)
        raise HTTPException(status_code=502, detail=f"Airflow API error: {e}")


@router.post("/analyze")
async def analyze_failure(dag_id: str, run_id: str, task_id: str, try_number: int = 1):
    """Run the full analysis pipeline on a failed task."""
    try:
        error_log = airflow.get_task_log(dag_id, run_id, task_id, try_number)
        if not error_log:
            raise HTTPException(status_code=404, detail="Task log not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to fetch task log: %s", e)
        raise HTTPException(status_code=502, detail=f"Airflow API error: {e}")

    parsed = parse_log(error_log)
    classification = classify(parsed)
    explanation = llm.explain(dag_id, task_id, error_log, classification)

    failed_task = FailedTask(
        dag_id=dag_id,
        dag_run_id=run_id,
        task_id=task_id,
        try_number=try_number,
        operator=parsed.exception_type or "unknown",
        error_log=error_log[:5000],
    )

    result = AnalysisResult(
        dag_id=failed_task.dag_id,
        dag_run_id=failed_task.dag_run_id,
        task_id=failed_task.task_id,
        logical_date=failed_task.logical_date,
        try_number=failed_task.try_number,
        classification=classification,
        explanation=explanation,
        report_markdown="",
    )
    result.report_markdown = generate_report(result)

    record_id = storage.save(result)
    result_dict = result.model_dump()
    result_dict["id"] = record_id

    return result_dict


@router.get("/reports")
async def list_reports(limit: int = Query(50, ge=1, le=200)):
    """List stored analysis reports."""
    results = storage.get_all(limit=limit)
    return {
        "count": len(results),
        "reports": [
            {
                "id": r.id,
                "dag_id": r.dag_id,
                "dag_run_id": r.dag_run_id,
                "task_id": r.task_id,
                "failure_type": r.classification.failure_type
                if r.classification
                else "unknown",
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in results
        ],
    }


@router.get("/reports/{report_id}")
async def get_report(report_id: int):
    """Get a specific analysis report by ID."""
    result = storage.get(report_id)
    if not result:
        raise HTTPException(status_code=404, detail="Report not found")
    return result.model_dump()
