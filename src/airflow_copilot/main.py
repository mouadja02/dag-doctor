"""dag-doctor FastAPI application — enterprise edition."""

from __future__ import annotations

import logging
import os

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from airflow_copilot.api.routes import router
from airflow_copilot.api.auth_routes import router as auth_router
from airflow_copilot.api.environment_routes import router as env_router
from airflow_copilot.api.admin_routes import router as admin_router
from airflow_copilot.api.intelligence_routes import router as intelligence_router
from airflow_copilot.config import get_settings
from airflow_copilot.database import init_db
from airflow_copilot.logging_config import configure_logging
from airflow_copilot.metrics import setup_metrics

settings = get_settings()
configure_logging(settings.log_level)

logger = logging.getLogger(__name__)

limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[f"{settings.rate_limit_requests}/minute"],
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("dag-doctor API starting", extra={"version": "0.1.0"})
    init_db()
    yield
    from airflow_copilot.job_queue import shutdown

    shutdown()
    logger.info("dag-doctor API shutting down")


app = FastAPI(
    title="dag-doctor",
    description="AI copilot for failed Airflow DAGs",
    version="0.1.0",
    lifespan=lifespan,
)

app.state.limiter = limiter

setup_metrics(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"detail": "Rate limit exceeded. Try again later."},
    )


app.include_router(router)
app.include_router(auth_router)
app.include_router(env_router)
app.include_router(admin_router)
app.include_router(intelligence_router)

demo_mode = os.getenv("DEMO_MODE", "").lower() in ("1", "true", "yes")
if demo_mode:
    from airflow_copilot.api.demo_routes import router as demo_router

    app.include_router(demo_router)
    logger.info("Demo mode enabled — serving fixture data")
