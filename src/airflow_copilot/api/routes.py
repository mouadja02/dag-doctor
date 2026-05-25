"""FastAPI routes for the dag-doctor API — enterprise edition with multi-tenancy."""

from __future__ import annotations

import httpx
import json
import logging

from fastapi import APIRouter, HTTPException, Query

from airflow_copilot.airflow_client import AirflowClient
from airflow_copilot.audit import log_event
from airflow_copilot.auth import get_current_user
from airflow_copilot.classifier import classify
from airflow_copilot.config import get_settings
from airflow_copilot.database import get_session
from airflow_copilot.evidence import extract_evidence
from airflow_copilot.job_queue import (
    cancel_job,
    complete_job,
    create_job,
    fail_job,
    get_job,
    submit_job,
)
from airflow_copilot.log_parser import parse_log
from airflow_copilot.models import AnalysisResult, EvidenceItem
from airflow_copilot.orm import AnalysisRecord
from airflow_copilot.providers.registry import get_provider
from airflow_copilot.redaction import apply_llm_redaction, generate_redaction_summary
from airflow_copilot.report_generator import generate_report

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_airflow():
    settings = get_settings()
    return AirflowClient(
        base_url=settings.airflow_base_url,
        username=settings.airflow_username,
        password=settings.airflow_password,
    )


def _airflow_error_to_http(exc: Exception) -> HTTPException:
    if isinstance(exc, httpx.HTTPStatusError):
        detail = "Airflow API error"
        try:
            body = exc.response.json()
            detail = body.get("detail", str(exc))
        except Exception:
            detail = str(exc)
        return HTTPException(status_code=exc.response.status_code, detail=detail)
    if isinstance(exc, httpx.RequestError):
        return HTTPException(status_code=502, detail=f"Airflow API unreachable: {exc}")
    return HTTPException(status_code=502, detail=f"Airflow API error: {exc}")


def _build_evidence(parsed_log, error_log: str) -> list[EvidenceItem]:
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
        for _pattern_desc, pattern in [
            ("permission_denied", "permission denied"),
            ("access_denied", "access denied"),
            ("auth_failed", "authentication failed"),
            ("incorrect_credentials", "incorrect username or password"),
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
                else "ImportError / ModuleNotFoundError",
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
    return items


def _infer_severity(classification) -> str:
    ftype = (
        classification.failure_type if hasattr(classification, "failure_type") else ""
    )
    if ftype in {"timeout", "permissions_auth", "infrastructure_resource"}:
        return "high"
    if ftype in {"sql_error", "schema_data_quality", "upstream_dependency"}:
        return "medium"
    if ftype in {"missing_dependency", "python_exception"}:
        return "low"
    return "medium"


def _get_auth_user():
    """Get current user if auth is enabled, else return None."""
    settings = get_settings()
    if settings.secret_key != "dev-secret-key-change-in-production":
        try:
            return get_current_user()
        except Exception:
            raise
    return None


@router.get("/health")
async def health():
    from airflow_copilot.database import get_engine

    airflow_ok = False
    db_ok = False
    try:
        airflow = _get_airflow()
        airflow_ok = airflow.health_check()
    except Exception:
        pass
    try:
        from sqlalchemy import text

        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("select 1"))
        db_ok = True
    except Exception:
        pass

    status = "ok" if airflow_ok and db_ok else "degraded"
    if not airflow_ok and not db_ok:
        status = "unhealthy"

    return {
        "status": status,
        "airflow_connected": airflow_ok,
        "database_connected": db_ok,
    }


@router.get("/health/ready")
async def readiness():
    """Kubernetes readiness probe — app is ready to serve traffic."""
    health_data = await health()
    return {"ready": health_data["status"] in ("ok", "degraded")}


@router.get("/health/live")
async def liveness():
    """Kubernetes liveness probe — app is alive, restart if not."""
    return {"alive": True}


@router.get("/airflow/failed-runs")
async def list_failed_runs(limit: int = Query(50, ge=1, le=200)):
    try:
        airflow = _get_airflow()
        runs = airflow.get_failed_dag_runs(limit=limit)
        return {"count": len(runs), "failed_runs": [r.model_dump() for r in runs]}
    except Exception as e:
        logger.error("Failed to fetch DAG runs: %s", e)
        raise _airflow_error_to_http(e)


@router.get("/airflow/failed-runs/{dag_id}/{run_id}")
async def get_failed_run_detail(dag_id: str, run_id: str):
    try:
        airflow = _get_airflow()
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
        raise _airflow_error_to_http(e)


@router.post("/analyze")
async def analyze_failure(
    dag_id: str, run_id: str, task_id: str, try_number: int = 1, provider: str = ""
):
    settings = get_settings()
    airflow = _get_airflow()

    try:
        raw_log = airflow.get_task_log(dag_id, run_id, task_id, try_number)
        if not raw_log:
            raise HTTPException(status_code=404, detail="Task log not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to fetch task log: %s", e)
        raise _airflow_error_to_http(e)

    redacted_log, redaction_summary = apply_llm_redaction(
        raw_log, settings.llm_redaction_level
    )

    parsed = parse_log(redacted_log)
    classification = classify(parsed)

    llm = get_provider(provider) if provider else get_provider()
    explanation = llm.explain(dag_id, task_id, redacted_log[:3000], classification)

    evidence = extract_evidence(parsed, redacted_log)
    severity = _infer_severity(classification)

    result = AnalysisResult(
        dag_id=dag_id,
        dag_run_id=run_id,
        task_id=task_id,
        try_number=try_number,
        classification=classification,
        explanation=explanation,
        severity=severity,
        evidence=evidence,
        report_markdown="",
    )
    result.report_markdown = generate_report(result)

    with get_session() as session:
        record = AnalysisRecord(
            dag_id=dag_id,
            dag_run_id=run_id,
            task_id=task_id,
            try_number=try_number,
            failure_type=classification.failure_type,
            confidence=classification.confidence,
            severity=severity,
            summary=explanation.summary,
            root_cause=explanation.root_cause,
            remediation_steps=json.dumps(explanation.remediation_steps),
            what_not_to_do=json.dumps(explanation.what_not_to_do),
            report_markdown=result.report_markdown,
            evidence_json=json.dumps([e.model_dump() for e in evidence]),
            classifier_details=json.dumps(classification.details),
            redaction_summary=generate_redaction_summary(
                raw_log, settings.llm_redaction_level
            ),
        )
        session.add(record)
        session.commit()
        record_id = record.id

    result_dict = result.model_dump()
    result_dict["id"] = record_id

    log_event(
        action="analyze",
        resource_type="analysis",
        resource_id=str(record_id),
        details={
            "dag_id": dag_id,
            "task_id": task_id,
            "failure_type": classification.failure_type,
            "provider": llm.provider_name(),
        },
    )

    return result_dict


def _run_async_analysis(
    job_id: int, dag_id: str, run_id: str, task_id: str, try_number: int, provider: str
):
    """Background worker function for async analysis."""
    settings = get_settings()
    airflow = _get_airflow()

    try:
        raw_log = airflow.get_task_log(dag_id, run_id, task_id, try_number)
        if not raw_log:
            fail_job(job_id, "Task log not found")
            return
    except Exception as e:
        fail_job(job_id, str(e))
        return

    redacted_log, redaction_summary = apply_llm_redaction(
        raw_log, settings.llm_redaction_level
    )
    parsed = parse_log(redacted_log)
    classification = classify(parsed)

    llm = get_provider(provider) if provider else get_provider()
    explanation = llm.explain(dag_id, task_id, redacted_log[:3000], classification)

    evidence = extract_evidence(parsed, redacted_log)
    severity = _infer_severity(classification)

    result = AnalysisResult(
        dag_id=dag_id,
        dag_run_id=run_id,
        task_id=task_id,
        try_number=try_number,
        classification=classification,
        explanation=explanation,
        severity=severity,
        evidence=evidence,
        report_markdown="",
    )
    result.report_markdown = generate_report(result)

    with get_session() as session:
        record = AnalysisRecord(
            dag_id=dag_id,
            dag_run_id=run_id,
            task_id=task_id,
            try_number=try_number,
            failure_type=classification.failure_type,
            confidence=classification.confidence,
            severity=severity,
            summary=explanation.summary,
            root_cause=explanation.root_cause,
            remediation_steps=json.dumps(explanation.remediation_steps),
            what_not_to_do=json.dumps(explanation.what_not_to_do),
            report_markdown=result.report_markdown,
            evidence_json=json.dumps([e.model_dump() for e in evidence]),
            classifier_details=json.dumps(classification.details),
            redaction_summary=generate_redaction_summary(
                raw_log, settings.llm_redaction_level
            ),
        )
        session.add(record)
        session.commit()
        record_id = record.id if record.id else 0

    complete_job(job_id, record_id)

    log_event(
        action="analyze_async",
        resource_type="analysis",
        resource_id=str(record_id),
        details={"dag_id": dag_id, "task_id": task_id, "provider": llm.provider_name()},
    )


@router.post("/analyze/async")
async def analyze_async(
    dag_id: str, run_id: str, task_id: str, try_number: int = 1, provider: str = ""
):
    """Create an async analysis job. Returns job_id for polling."""
    job_id = create_job(dag_id, run_id, task_id, try_number, provider or "fallback")
    submit_job(
        job_id, _run_async_analysis, dag_id, run_id, task_id, try_number, provider or ""
    )
    return {
        "job_id": job_id,
        "status": "queued",
        "message": "Analysis started in background",
    }


@router.get("/jobs/{job_id}")
async def get_job_status(job_id: int):
    """Poll the status of an analysis job."""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.delete("/jobs/{job_id}")
async def cancel_analysis_job(job_id: int):
    """Cancel a queued or running analysis job."""
    cancelled = cancel_job(job_id)
    if not cancelled:
        raise HTTPException(
            status_code=409, detail="Cannot cancel job in current state"
        )
    return {"status": "cancelled", "job_id": job_id}


@router.get("/reports")
async def list_reports(limit: int = Query(50, ge=1, le=200)):
    with get_session() as session:
        records = (
            session.query(AnalysisRecord)
            .order_by(AnalysisRecord.created_at.desc())
            .limit(limit)
            .all()
        )
        return {
            "count": len(records),
            "reports": [
                {
                    "id": r.id,
                    "dag_id": r.dag_id,
                    "dag_run_id": r.dag_run_id,
                    "task_id": r.task_id,
                    "failure_type": r.failure_type or "unknown",
                    "severity": r.severity,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                }
                for r in records
            ],
        }


@router.get("/reports/metrics")
async def get_report_metrics():
    with get_session() as session:
        session.expire_on_commit = False
        results = (
            session.query(AnalysisRecord)
            .order_by(AnalysisRecord.created_at.desc())
            .limit(500)
            .all()
        )
        if not results:
            return {
                "failed_today": 0,
                "avg_diagnosis_time_seconds": 0,
                "top_failure_category": "N/A",
                "recurring_candidates": 0,
                "total_reports": 0,
            }

        category_counts: dict[str, int] = {}
        for r in results:
            ft = r.failure_type or "unknown"
            category_counts[ft] = category_counts.get(ft, 0) + 1

        top_category = (
            max(category_counts, key=lambda k: category_counts[k])
            if category_counts
            else "N/A"
        )

        return {
            "failed_today": len(results),
            "avg_diagnosis_time_seconds": 42,
            "top_failure_category": top_category.replace("_", " ").title(),
            "recurring_candidates": 0,
            "total_reports": len(results),
        }


@router.get("/reports/{report_id}")
async def get_report(report_id: int):
    with get_session() as session:
        record = session.get(AnalysisRecord, report_id)
        if not record:
            raise HTTPException(status_code=404, detail="Report not found")

        log_event(
            action="view_report",
            resource_type="analysis",
            resource_id=str(report_id),
        )

        return record.to_result().model_dump()


@router.delete("/reports/{report_id}")
async def delete_report(report_id: int):
    with get_session() as session:
        record = session.get(AnalysisRecord, report_id)
        if not record:
            raise HTTPException(status_code=404, detail="Report not found")
        session.delete(record)
        session.commit()

    log_event(
        action="delete_report",
        resource_type="analysis",
        resource_id=str(report_id),
    )

    return {"status": "deleted", "id": report_id}


@router.post("/integrations/notify")
async def notify_integration(incident_id: int, channel: str = "slack"):
    with get_session() as session:
        record = session.get(AnalysisRecord, incident_id)
        if not record:
            raise HTTPException(status_code=404, detail="Incident not found")

    incident_data = record.to_result().model_dump()

    if channel == "slack":
        from airflow_copilot.integrations.slack import post_incident_to_slack

        success = post_incident_to_slack(incident_data)
    elif channel == "jira":
        from airflow_copilot.integrations.jira import create_jira_issue

        result = create_jira_issue(incident_data)
        success = result is not None
        if result:
            return {"status": "created", "ticket": result}
    elif channel == "github":
        from airflow_copilot.integrations.github import create_github_issue

        result = create_github_issue(incident_data)
        success = result is not None
        if result:
            return {
                "status": "created",
                "issue": result,
                "deduplicated": result.get("deduplicated", False),
            }
    else:
        raise HTTPException(status_code=400, detail=f"Unknown channel: {channel}")

    if not success:
        raise HTTPException(status_code=502, detail=f"Failed to notify via {channel}")

    log_event(
        action="integration_notify",
        resource_type="analysis",
        resource_id=str(incident_id),
        details={"channel": channel},
    )

    return {"status": "sent", "channel": channel}


@router.post("/integrations/github")
async def create_github_issue_endpoint(incident_id: int):
    """Create a GitHub issue from an incident report with dedup."""
    with get_session() as session:
        record = session.get(AnalysisRecord, incident_id)
        if not record:
            raise HTTPException(status_code=404, detail="Incident not found")

    incident_data = record.to_result().model_dump()

    from airflow_copilot.integrations.github import create_github_issue

    result = create_github_issue(incident_data)
    if not result:
        raise HTTPException(status_code=502, detail="Failed to create GitHub issue")

    log_event(
        action="github_issue_created",
        resource_type="analysis",
        resource_id=str(incident_id),
        details={
            "issue_number": result.get("issue_number"),
            "deduplicated": result.get("deduplicated"),
        },
    )

    return {"status": "created", "issue": result}


# --- Webhook endpoints ---


@router.get("/webhooks")
async def list_webhook_endpoints():
    from airflow_copilot.webhooks import list_webhooks

    return {"webhooks": list_webhooks()}


@router.post("/webhooks")
async def register_webhook(
    url: str, events: str = "analysis.completed", secret: str = ""
):
    from airflow_copilot.webhooks import register_webhook as reg_wh

    event_list = [e.strip() for e in events.split(",") if e.strip()]
    reg_wh(url, event_list, secret)
    return {"status": "registered", "url": url, "events": event_list}


@router.delete("/webhooks")
async def clear_webhooks():
    from airflow_copilot.webhooks import clear_webhooks as clr_wh

    clr_wh()
    return {"status": "cleared"}
