import json
import logging
from datetime import UTC, datetime
from typing import Any

from bson import ObjectId
from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.db import (
    get_admin_collection,
    get_clients_collection,
    get_subscriptions_collection,
)
from app.security import (
    generate_client_id,
    generate_client_secret,
    hash_password,
    verify_password,
)

logger = logging.getLogger(__name__)

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def _json_default(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def _prepare(doc: Any) -> Any:
    return json.loads(json.dumps(doc, default=_json_default))


def _subscription_admin_response(doc: dict[str, Any]) -> dict[str, Any]:
    resp = dict(doc)
    resp["subscription_id"] = str(resp.pop("_id", ""))
    return _prepare(resp)


async def _admin_exists() -> bool:
    coll = await get_admin_collection()
    return await coll.find_one({"username": "admin"}) is not None


async def _is_logged_in(request: Request) -> bool:
    if "admin" not in request.session:
        return False
    return await _admin_exists()


@router.get("/admin", response_class=HTMLResponse)
async def admin_root(request: Request) -> Any:
    if not await _admin_exists():
        return RedirectResponse(url="/admin/setup", status_code=302)
    if not await _is_logged_in(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    return RedirectResponse(url="/admin/dashboard", status_code=302)


@router.get("/admin/setup", response_class=HTMLResponse)
async def admin_setup_get(request: Request) -> Any:
    if await _admin_exists():
        if await _is_logged_in(request):
            return RedirectResponse(url="/admin/dashboard", status_code=302)
        return RedirectResponse(url="/admin/login", status_code=302)
    return templates.TemplateResponse(
        request,
        "admin_setup.html",
        {"error": None},
    )


@router.post("/admin/setup", response_class=HTMLResponse)
async def admin_setup_post(
    request: Request,
    password: str = Form(...),
    confirm_password: str = Form(...),
) -> Any:
    if await _admin_exists():
        if await _is_logged_in(request):
            return RedirectResponse(url="/admin/dashboard", status_code=302)
        return RedirectResponse(url="/admin/login", status_code=302)

    if password != confirm_password:
        return templates.TemplateResponse(
            request,
            "admin_setup.html",
            {"error": "Passwords do not match"},
            status_code=400,
        )

    if len(password) < 8:
        return templates.TemplateResponse(
            request,
            "admin_setup.html",
            {"error": "Password must be at least 8 characters"},
            status_code=400,
        )

    coll = await get_admin_collection()
    await coll.insert_one(
        {
            "username": "admin",
            "password_hash": hash_password(password),
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
    )
    request.session["admin"] = True
    return RedirectResponse(url="/admin/dashboard", status_code=302)


@router.get("/admin/login", response_class=HTMLResponse)
async def admin_login_get(request: Request) -> Any:
    if not await _admin_exists():
        return RedirectResponse(url="/admin/setup", status_code=302)
    if await _is_logged_in(request):
        return RedirectResponse(url="/admin/dashboard", status_code=302)
    return templates.TemplateResponse(
        request,
        "admin_login.html",
        {"error": None},
    )


@router.post("/admin/login", response_class=HTMLResponse)
async def admin_login_post(
    request: Request,
    password: str = Form(...),
) -> Any:
    if not await _admin_exists():
        return RedirectResponse(url="/admin/setup", status_code=302)

    coll = await get_admin_collection()
    admin = await coll.find_one({"username": "admin"})
    if not admin or not verify_password(password, admin.get("password_hash", "")):
        return templates.TemplateResponse(
            request,
            "admin_login.html",
            {"error": "Invalid password"},
            status_code=401,
        )

    request.session["admin"] = True
    return RedirectResponse(url="/admin/dashboard", status_code=302)


@router.get("/admin/logout")
async def admin_logout(request: Request) -> Any:
    request.session.pop("admin", None)
    return RedirectResponse(url="/admin/login", status_code=302)


@router.get("/admin/dashboard", response_class=HTMLResponse)
async def admin_dashboard(request: Request) -> Any:
    if not await _is_logged_in(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    clients_coll = await get_clients_collection()
    subs_coll = await get_subscriptions_collection()
    client_count = await clients_coll.count_documents({"status": "active"})
    subscription_count = await subs_coll.count_documents(
        {"status": {"$in": ["active", "failing"]}}
    )

    return templates.TemplateResponse(
        request,
        "admin_dashboard.html",
        {
            "client_count": client_count,
            "subscription_count": subscription_count,
        },
    )


@router.get("/admin/clients", response_class=HTMLResponse)
async def admin_clients(request: Request) -> Any:
    if not await _is_logged_in(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    clients_coll = await get_clients_collection()
    clients = await clients_coll.find().sort("created_at", -1).to_list(length=1000)

    return templates.TemplateResponse(
        request,
        "admin_clients.html",
        {
            "clients": [_prepare(c) for c in clients],
            "new_secret": None,
            "new_client_id": None,
        },
    )


@router.post("/admin/clients", response_class=HTMLResponse)
async def admin_create_client(
    request: Request,
    name: str = Form(...),
) -> Any:
    if not await _is_logged_in(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    client_id = generate_client_id()
    secret = generate_client_secret()
    now = datetime.now(UTC)

    coll = await get_clients_collection()
    await coll.insert_one(
        {
            "client_id": client_id,
            "name": name.strip(),
            "secret_hash": hash_password(secret),
            "status": "active",
            "created_at": now,
            "updated_at": now,
        }
    )

    clients = await coll.find().sort("created_at", -1).to_list(length=1000)
    return templates.TemplateResponse(
        request,
        "admin_clients.html",
        {
            "clients": [_prepare(c) for c in clients],
            "new_secret": secret,
            "new_client_id": client_id,
        },
    )


@router.post("/admin/clients/{client_id}/revoke")
async def admin_revoke_client(request: Request, client_id: str) -> Any:
    if not await _is_logged_in(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    coll = await get_clients_collection()
    await coll.update_one(
        {"client_id": client_id},
        {"$set": {"status": "revoked", "updated_at": datetime.now(UTC)}},
    )
    return RedirectResponse(url="/admin/clients", status_code=302)


@router.post("/admin/clients/{client_id}/reissue", response_class=HTMLResponse)
async def admin_reissue_client(request: Request, client_id: str) -> Any:
    if not await _is_logged_in(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    secret = generate_client_secret()
    coll = await get_clients_collection()
    result = await coll.update_one(
        {"client_id": client_id, "status": "active"},
        {
            "$set": {
                "secret_hash": hash_password(secret),
                "updated_at": datetime.now(UTC),
            }
        },
    )

    clients = await coll.find().sort("created_at", -1).to_list(length=1000)
    return templates.TemplateResponse(
        request,
        "admin_clients.html",
        {
            "clients": [_prepare(c) for c in clients],
            "new_secret": secret if result.modified_count else None,
            "new_client_id": client_id if result.modified_count else None,
        },
    )


@router.get("/admin/subscriptions", response_class=HTMLResponse)
async def admin_subscriptions(request: Request) -> Any:
    if not await _is_logged_in(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    subs_coll = await get_subscriptions_collection()
    subs = await (
        subs_coll.find({"status": {"$in": ["active", "failing"]}})
        .sort("created_at", -1)
        .to_list(length=1000)
    )

    return templates.TemplateResponse(
        request,
        "admin_subscriptions.html",
        {"subscriptions": [_subscription_admin_response(s) for s in subs]},
    )


@router.post("/admin/subscriptions/{subscription_id}/cancel")
async def admin_cancel_subscription(request: Request, subscription_id: str) -> Any:
    if not await _is_logged_in(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    coll = await get_subscriptions_collection()
    await coll.delete_one(
        {"_id": subscription_id, "status": {"$in": ["active", "failing"]}}
    )
    return RedirectResponse(url="/admin/subscriptions", status_code=302)
