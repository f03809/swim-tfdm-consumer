import json
import logging
from datetime import datetime
from typing import Any

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.db import get_collection, get_route_collection, get_tfms_collection
from app.parser import _normalize_airport

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


def _normalize_flight_airports(flight: dict[str, Any]) -> dict[str, Any]:
    if flight.get("departure"):
        dpt = flight["departure"].get("departurePointText")
        if dpt:
            flight["departure"]["departurePointText"] = _normalize_airport(dpt)
    if flight.get("arrival"):
        arr = flight["arrival"].get("destinationPointText")
        if arr:
            flight["arrival"]["destinationPointText"] = _normalize_airport(arr)
    if isinstance(flight.get("tfdm_id_creator_airport"), dict):
        loc = flight["tfdm_id_creator_airport"].get("locationIndicator")
        if loc:
            flight["tfdm_id_creator_airport"]["locationIndicator"] = _normalize_airport(loc)
    return flight


def _normalize_tfms_airports(msg: dict[str, Any]) -> dict[str, Any]:
    if msg.get("departure_airport"):
        msg["departure_airport"] = _normalize_airport(msg["departure_airport"])
    if msg.get("arrival_airport"):
        msg["arrival_airport"] = _normalize_airport(msg["arrival_airport"])
    return msg


@router.get("/flights/{flight_number}")
async def get_flight(flight_number: str) -> dict[str, Any]:
    coll = await get_collection()
    doc = await coll.find_one(
        {"flight_number": flight_number.upper(), "status": "active"},
        sort=[("created_at", -1)],
    )
    if not doc:
        raise HTTPException(status_code=404, detail="Flight not found")
    _normalize_flight_airports(doc)
    return _prepare(doc)


@router.get("/flights/{flight_number}/route")
async def get_flight_route(flight_number: str) -> dict[str, Any]:
    route_coll = await get_route_collection()
    doc = await route_coll.find_one(
        {"flight_number": flight_number.upper(), "status": "active"},
        sort=[("updated_at", -1)],
    )
    if not doc:
        raise HTTPException(status_code=404, detail="Route not found for this flight")
    return _prepare(doc)


@router.get("/flights/{flight_number}/tfms", response_class=HTMLResponse)
async def flight_tfms(request: Request, flight_number: str) -> Any:
    tfms_coll = await get_tfms_collection()
    messages = (
        await tfms_coll.find({"flight_number": flight_number.upper(), "status": "active"})
        .sort("_id", -1)
        .limit(200)
        .to_list(length=200)
    )
    for msg in messages:
        _normalize_tfms_airports(msg)
    return templates.TemplateResponse(
        request,
        "tfms.html",
        {
            "flight_number": flight_number.upper(),
            "tfms_messages": _prepare(messages),
        },
    )


@router.get("/tfms/{tfms_id}", response_class=HTMLResponse)
async def tfms_detail(request: Request, tfms_id: str) -> Any:
    tfms_coll = await get_tfms_collection()
    try:
        message = await tfms_coll.find_one({"_id": ObjectId(tfms_id), "status": "active"})
    except Exception:
        raise HTTPException(status_code=404, detail="TFMS message not found")
    if not message:
        raise HTTPException(status_code=404, detail="TFMS message not found")
    _normalize_tfms_airports(message)
    return templates.TemplateResponse(
        request,
        "tfms_detail.html",
        {
            "flight_number": message.get("flight_number") or "",
            "message": _prepare(message),
        },
    )


@router.get("/", response_class=HTMLResponse)
async def index(request: Request, q: str | None = None) -> Any:
    coll = await get_collection()
    tfms_coll = await get_tfms_collection()
    query: dict[str, Any] = {"status": "active"}
    if q:
        query["flight_number"] = q.strip().upper()

    flights = (
        await coll.find(query)
        .sort("created_at", -1)
        .limit(50)
        .to_list(length=50)
    )
    for flight in flights:
        _normalize_flight_airports(flight)

    tfms_counts = {}
    for fn in {f.get("flight_number") for f in flights if f.get("flight_number")}:
        tfms_counts[fn] = await tfms_coll.count_documents({"flight_number": fn, "status": "active"})

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "flights": _prepare(flights),
            "tfms_counts": tfms_counts,
            "q": q or "",
        },
    )
