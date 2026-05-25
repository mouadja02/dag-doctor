"""Environment management API routes — CRUD for Airflow environments."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from airflow_copilot.auth import get_current_user, require_role
from airflow_copilot.database import get_session
from airflow_copilot.orm import AirflowEnvironment, User

router = APIRouter(prefix="/environments", tags=["environments"])


class EnvironmentCreate(BaseModel):
    name: str
    env_type: str = "dev"
    base_url: str
    username: str = ""
    password: str = ""


class EnvironmentUpdate(BaseModel):
    name: str | None = None
    env_type: str | None = None
    base_url: str | None = None
    username: str | None = None
    password: str | None = None
    is_active: bool | None = None


@router.get("/")
def list_environments(
    user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    envs = (
        session.query(AirflowEnvironment)
        .filter(
            AirflowEnvironment.org_id == user.org_id,
            AirflowEnvironment.is_active,
        )
        .all()
    )
    return {
        "count": len(envs),
        "environments": [
            {
                "id": e.id,
                "name": e.name,
                "env_type": e.env_type,
                "base_url": e.base_url,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in envs
        ],
    }


@router.post("/")
def create_environment(
    req: EnvironmentCreate,
    user: User = Depends(require_role("admin")),
    session: Session = Depends(get_session),
):
    env = AirflowEnvironment(
        org_id=user.org_id,
        name=req.name,
        env_type=req.env_type,
        base_url=req.base_url,
        username=req.username,
        password_encrypted=req.password,
    )
    session.add(env)
    session.commit()
    return {"id": env.id, "name": env.name, "env_type": env.env_type}


@router.patch("/{env_id}")
def update_environment(
    env_id: int,
    req: EnvironmentUpdate,
    user: User = Depends(require_role("admin")),
    session: Session = Depends(get_session),
):
    env = (
        session.query(AirflowEnvironment)
        .filter(
            AirflowEnvironment.id == env_id,
            AirflowEnvironment.org_id == user.org_id,
        )
        .first()
    )
    if not env:
        raise HTTPException(status_code=404, detail="Environment not found")

    if req.name is not None:
        env.name = req.name
    if req.env_type is not None:
        env.env_type = req.env_type
    if req.base_url is not None:
        env.base_url = req.base_url
    if req.username is not None:
        env.username = req.username
    if req.password is not None:
        env.password_encrypted = req.password
    if req.is_active is not None:
        env.is_active = req.is_active

    session.commit()
    return {"id": env.id, "name": env.name, "status": "updated"}


@router.delete("/{env_id}")
def delete_environment(
    env_id: int,
    user: User = Depends(require_role("admin")),
    session: Session = Depends(get_session),
):
    env = (
        session.query(AirflowEnvironment)
        .filter(
            AirflowEnvironment.id == env_id,
            AirflowEnvironment.org_id == user.org_id,
        )
        .first()
    )
    if not env:
        raise HTTPException(status_code=404, detail="Environment not found")
    env.is_active = False
    session.commit()
    return {"status": "deactivated"}
