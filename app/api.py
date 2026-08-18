import json
import logging
from datetime import datetime
from typing import Any

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.db import get_collection

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


@router.get("/flights/{flight_number}")
async def get_flight(flight_number: str) -> dict[str, Any]:
    coll = await get_collection()
    doc = await coll.find_one(
        {"flight_number": flight_number.upper(), "status": "active"},
        sort=[("created_at", -1)],
    )
    if not doc:
        raise HTTPException(status_code=404, detail="Flight not found")
    return _prepare(doc)


@router.get("/", response_class=HTMLResponse)
async def index(request: Request, q: str | None = None) -> Any:
    coll = await get_collection()
    query: dict[str, Any] = {"status": "active"}
    if q:
        query["flight_number"] = {"$regex": q.strip(), "$options": "i"}

    flights = (
        await coll.find(query)
        .sort("created_at", -1)
        .limit(50)
        .to_list(length=50)
    )

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "flights": _prepare(flights),
            "q": q or "",
        },
    )
