import base64
from typing import Any

from fastapi import APIRouter, HTTPException, Request, status

from app.db import get_clients_collection
from app.security import create_token, decode_token, get_bearer_token, verify_password

router = APIRouter(prefix="/api/v1/auth")


@router.post("/token")
async def token(request: Request) -> dict[str, Any]:
    return await issue_token(request)


def _parse_basic_auth(request: Request) -> tuple[str, str] | None:
    auth = request.headers.get("authorization")
    if not auth:
        return None
    if not auth.lower().startswith("basic "):
        return None
    try:
        decoded = base64.b64decode(auth[6:]).decode("utf-8")
    except Exception:
        return None
    if ":" not in decoded:
        return None
    client_id, _, secret = decoded.partition(":")
    return client_id, secret


async def authenticate_client_credentials(request: Request) -> dict[str, Any]:
    parsed = _parse_basic_auth(request)
    if not parsed:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid Basic Authorization header",
            headers={"WWW-Authenticate": "Basic"},
        )
    client_id, secret = parsed
    coll = await get_clients_collection()
    client = await coll.find_one({"client_id": client_id, "status": "active"})
    if not client or not verify_password(secret, client.get("secret_hash", "")):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid client credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return client


async def issue_token(request: Request) -> dict[str, Any]:
    client = await authenticate_client_credentials(request)
    token = create_token(client["client_id"])
    return {
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": 24 * 60 * 60,
    }


async def get_current_client(request: Request) -> dict[str, Any]:
    token = get_bearer_token(request)
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization: Bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    payload = decode_token(token)
    client_id = payload.get("sub")
    if not client_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )
    coll = await get_clients_collection()
    client = await coll.find_one({"client_id": client_id, "status": "active"})
    if not client:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Client not found or inactive"
        )
    return client
