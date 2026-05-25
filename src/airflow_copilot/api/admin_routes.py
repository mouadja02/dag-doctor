"""Admin dashboard API routes — governance, retention, audit view."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from airflow_copilot.auth import require_role
from airflow_copilot.database import get_session
from airflow_copilot.orm import AuditEvent, RetentionPolicy, User, Organization

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/audit")
def list_audit_events(
    limit: int = Query(100, ge=1, le=1000),
    action: str | None = None,
    session: Session = Depends(get_session),
    _user: User = Depends(require_role("admin")),
):
    query = session.query(AuditEvent).order_by(AuditEvent.created_at.desc())
    if action:
        query = query.filter(AuditEvent.action == action)

    events = query.limit(limit).all()
    return {
        "count": len(events),
        "events": [
            {
                "id": e.id,
                "org_id": e.org_id,
                "user_id": e.user_id,
                "action": e.action,
                "resource_type": e.resource_type,
                "resource_id": e.resource_id,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in events
        ],
    }


@router.get("/retention")
def get_retention(
    session: Session = Depends(get_session),
    _user: User = Depends(require_role("admin")),
):
    policy = session.query(RetentionPolicy).first()
    if not policy:
        return {"max_days": 90, "message": "Using default retention (90 days)"}
    return {"max_days": policy.max_days, "org_id": policy.org_id}


@router.post("/retention")
def set_retention(
    max_days: int = Query(7, ge=1, le=3650),
    session: Session = Depends(get_session),
    user: User = Depends(require_role("admin")),
):
    policy = session.query(RetentionPolicy).first()
    if policy:
        policy.max_days = max_days
    else:
        policy = RetentionPolicy(org_id=user.org_id, max_days=max_days)
        session.add(policy)
    session.commit()
    return {"max_days": max_days, "message": "Retention policy updated"}


@router.get("/stats")
def get_admin_stats(
    session: Session = Depends(get_session),
    _user: User = Depends(require_role("admin")),
):
    user_count = session.query(User).count()
    org_count = session.query(Organization).count()
    audit_count = session.query(AuditEvent).count()

    return {
        "users": user_count,
        "organizations": org_count,
        "audit_events": audit_count,
    }
