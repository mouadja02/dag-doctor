"""dag-doctor FastAPI application."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from airflow_copilot.api.routes import router

app = FastAPI(
    title="dag-doctor",
    description="An AI assistant that explains failed Airflow DAGs, identifies root causes, and suggests safe fixes.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
