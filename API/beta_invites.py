"""Single-use, email-bound beta codes backed by Clerk invitations."""
import hashlib
import hmac
import os
import re
import secrets
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import HTTPException
from pydantic import BaseModel, EmailStr
from sqlalchemy import text
from sqlalchemy.orm import Session

CODE_PATTERN = re.compile(r"^[A-Z2-9]{20}$")


class InviteClaim(BaseModel):
    code: str
    email: EmailStr


def code_hash(code: str) -> str:
    pepper = os.getenv("BETA_INVITE_PEPPER", "")
    if len(pepper) < 32:
        raise RuntimeError("BETA_INVITE_PEPPER must be configured (32+ characters)")
    return hmac.new(pepper.encode(), code.encode(), hashlib.sha256).hexdigest()


def issue_code(db: Session, email: str, days: int = 30) -> str:
    """Admin-only helper; print and deliver the returned code out of band."""
    code = "".join(secrets.choice("ABCDEFGHJKLMNPQRSTUVWXYZ23456789") for _ in range(20))
    db.execute(text("""INSERT INTO app.beta_invite_codes (code_hash, email, expires_at)
                       VALUES (:hash, :email, :expires)"""), {
        "hash": code_hash(code), "email": email.strip().lower(),
        "expires": datetime.now(timezone.utc) + timedelta(days=days),
    })
    db.commit()
    return code


def claim_code(db: Session, code: str, email: str) -> None:
    normalized_code = code.strip().upper().replace("-", "").replace(" ", "")
    normalized_email = email.strip().lower()
    if not CODE_PATTERN.fullmatch(normalized_code):
        raise HTTPException(400, "Invalid or expired invitation code")
    row = db.execute(text("""SELECT email, expires_at, claimed_at FROM app.beta_invite_codes
                            WHERE code_hash = :hash FOR UPDATE"""),
                     {"hash": code_hash(normalized_code)}).mappings().first()
    if (not row or row["email"].lower() != normalized_email or row["claimed_at"]
            or row["expires_at"] <= datetime.now(timezone.utc)):
        raise HTTPException(400, "Invalid or expired invitation code")
    secret = os.getenv("CLERK_SECRET_KEY", "")
    if not secret:
        raise HTTPException(503, "Invitations are unavailable")
    invitation_body = {"email_address": normalized_email, "notify": True,
                       "public_metadata": {"ballistics_beta": True}}
    # Without it the emailed link goes through Clerk's Account Portal to the
    # instance home URL, which is still v1 at the apex domain.
    redirect_url = os.getenv("BETA_INVITE_REDIRECT_URL", "").strip()
    if redirect_url:
        invitation_body["redirect_url"] = redirect_url
    try:
        response = httpx.post(
            os.getenv("CLERK_BACKEND_API_BASE", "https://api.clerk.com/v1").rstrip("/") + "/invitations",
            json=invitation_body,
            headers={"Authorization": f"Bearer {secret}", "Accept": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        invitation = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(503, "Could not send invitation; try again later") from exc
    invite_id = invitation.get("id")
    if not isinstance(invite_id, str) or not invite_id:
        raise HTTPException(503, "Could not confirm invitation")
    db.execute(text("""UPDATE app.beta_invite_codes
                       SET claimed_at = NOW(), clerk_invitation_id = :invite_id
                       WHERE code_hash = :hash"""),
               {"invite_id": invite_id, "hash": code_hash(normalized_code)})
    db.commit()
