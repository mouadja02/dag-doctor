"""Prometheus metrics for dag-doctor — requests, latency, analysis, integrations."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, generate_latest, REGISTRY
from fastapi import FastAPI, Request, Response

ANALYSIS_COUNT = Counter(
    "dagdoctor_analysis_total",
    "Total number of analyses performed",
    ["status", "provider"],
)

ANALYSIS_DURATION = Histogram(
    "dagdoctor_analysis_duration_seconds",
    "Analysis duration in seconds",
    ["provider"],
    buckets=[1, 5, 10, 30, 60, 120, 300],
)

API_REQUESTS = Counter(
    "dagdoctor_api_requests_total",
    "Total API requests",
    ["method", "endpoint", "status_code"],
)

API_REQUEST_DURATION = Histogram(
    "dagdoctor_api_request_duration_seconds",
    "API request duration in seconds",
    ["method", "endpoint"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1, 5, 10],
)

INTEGRATION_DISPATCHES = Counter(
    "dagdoctor_integration_dispatches_total",
    "Total integration notifications sent",
    ["channel", "status"],
)

AIRFLOW_API_ERRORS = Counter(
    "dagdoctor_airflow_api_errors_total",
    "Total Airflow API errors",
    ["status_code"],
)

LLM_ERRORS = Counter(
    "dagdoctor_llm_errors_total",
    "Total LLM provider errors",
    ["provider"],
)

ACTIVE_ANALYSES = Gauge(
    "dagdoctor_active_analyses",
    "Number of currently running analyses",
)

QUEUE_DEPTH = Gauge(
    "dagdoctor_analysis_queue_depth",
    "Number of queued analysis jobs",
)

REDACTION_EVENTS = Counter(
    "dagdoctor_redaction_events_total",
    "Total redaction events",
    ["category"],
)


def setup_metrics(app: FastAPI):
    """Register Prometheus metrics endpoint and request middleware on the FastAPI app."""

    @app.middleware("http")
    async def metrics_middleware(request: Request, call_next):
        import time

        start = time.time()
        response = await call_next(request)
        duration = time.time() - start

        route_name = "unknown"
        if request.scope.get("route"):
            route_name = request.scope["route"].path
        elif request.url.path.startswith("/metrics"):
            return response

        API_REQUESTS.labels(
            method=request.method,
            endpoint=route_name,
            status_code=str(response.status_code),
        ).inc()

        API_REQUEST_DURATION.labels(
            method=request.method,
            endpoint=route_name,
        ).observe(duration)

        return response

    @app.get("/metrics", include_in_schema=False)
    async def metrics():
        return Response(
            content=generate_latest(REGISTRY),
            media_type="text/plain",
        )
