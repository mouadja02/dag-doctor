"""Database engine and session management for dag-doctor.

Supports both SQLite (dev/demo) and PostgreSQL (production).
"""

from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from airflow_copilot.config import get_settings

_engine = None
_SessionLocal = None


def get_engine():
    global _engine, _SessionLocal
    if _engine is None:
        settings = get_settings()
        connect_args = {}
        if "sqlite" in settings.database_url:
            connect_args = {"check_same_thread": False}
        _engine = create_engine(
            settings.database_url,
            connect_args=connect_args,
            pool_pre_ping="postgresql" in settings.database_url,
        )
        _SessionLocal = sessionmaker(bind=_engine)
    return _engine


def get_session_factory():
    get_engine()
    return _SessionLocal


@contextmanager
def get_session() -> Session:
    """Context manager for database sessions."""
    factory = get_session_factory()
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db():
    """Create all tables. Called at startup. Alembic handles migrations in production."""
    from airflow_copilot.orm import Base

    engine = get_engine()
    Base.metadata.create_all(engine)
