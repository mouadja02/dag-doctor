"""Authentication API routes — registration, login, user profile."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from airflow_copilot.auth import (
    create_access_token,
    get_current_user,
    hash_password,
    verify_password,
)
from airflow_copilot.database import get_session
from airflow_copilot.orm import Organization, User

router = APIRouter(prefix="/auth", tags=["auth"])


class RegisterRequest(BaseModel):
    email: str
    password: str
    display_name: str = ""
    org_name: str | None = None


class LoginRequest(BaseModel):
    email: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user_id: int
    role: str
    org_id: int


class UserProfile(BaseModel):
    id: int
    email: str
    display_name: str
    role: str
    org_id: int
    org_name: str


@router.post("/register", response_model=TokenResponse)
def register(req: RegisterRequest, session: Session = Depends(get_session)):
    existing = session.query(User).filter(User.email == req.email).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email already registered",
        )

    org_name = req.org_name or f"{req.email.split('@')[0]}'s Org"
    slug = org_name.lower().replace(" ", "-").replace("'", "")
    org = Organization(name=org_name, slug=slug)
    session.add(org)
    session.commit()

    user = User(
        email=req.email,
        hashed_password=hash_password(req.password),
        display_name=req.display_name or req.email.split("@")[0],
        role="admin",
        org_id=org.id,
    )
    session.add(user)
    session.commit()

    token = create_access_token(user.id, user.role, org.id)
    return TokenResponse(
        access_token=token,
        user_id=user.id,
        role=user.role,
        org_id=org.id,
    )


@router.post("/login", response_model=TokenResponse)
def login(req: LoginRequest, session: Session = Depends(get_session)):
    user = session.query(User).filter(User.email == req.email).first()
    if not user or not verify_password(req.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is inactive",
        )

    token = create_access_token(user.id, user.role, user.org_id)
    return TokenResponse(
        access_token=token,
        user_id=user.id,
        role=user.role,
        org_id=int(user.org_id) if user.org_id else 0,
    )


@router.get("/me", response_model=UserProfile)
def get_me(user: User = Depends(get_current_user)):
    return UserProfile(
        id=int(user.id) if user.id else 0,
        email=user.email,
        display_name=user.display_name or "",
        role=user.role,
        org_id=int(user.org_id) if user.org_id else 0,
        org_name=user.organization.name if user.organization else "N/A",
    )
