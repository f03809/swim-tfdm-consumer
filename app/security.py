import re
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

import bcrypt
import jwt
from fastapi import HTTPException, Request, status
from fastapi.security import HTTPBasic

from app.config import settings

basic_auth = HTTPBasic(auto_error=False)


def _to_bytes(value: str) -> bytes:
    return value.encode("utf-8")


def hash_password(password: str) -> str:
    return bcrypt.hashpw(_to_bytes(password), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(_to_bytes(password), _to_bytes(hashed))


def generate_client_id() -> str:
    return f"client_{secrets.token_urlsafe(12)}"


def generate_client_secret() -> str:
    return secrets.token_urlsafe(32)


def create_token(client_id: str) -> str:
    now = datetime.now(UTC)
    payload = {
        "sub": client_id,
        "iat": now,
        "exp": now + timedelta(hours=settings.jwt_expiry_hours),
        "type": "access",
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def decode_token(token: str) -> dict[str, Any]:
    try:
        return jwt.decode(
            token, settings.jwt_secret, algorithms=[settings.jwt_algorithm]
        )
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired"
        ) from exc
    except jwt.InvalidTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        ) from exc


def get_bearer_token(request: Request) -> str | None:
    auth = request.headers.get("authorization")
    if not auth:
        return None
    match = re.match(r"^Bearer\s+(.+)$", auth, re.IGNORECASE)
    return match.group(1) if match else None
