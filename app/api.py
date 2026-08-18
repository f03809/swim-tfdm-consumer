import json
import logging
from datetime import datetime
from typing import Any

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.db import get_collection, get_route_collection, get_tbfm_collection, get_tfms_collection
from app.parser import _content, _get, _normalize_airport

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


def _clean_departure(departure: Any) -> dict[str, Any]:
    if not isinstance(departure, dict):
        return {}
    clean: dict[str, Any] = {}
    airport = _normalize_airport(departure.get("departurePointText"))
    if airport:
        clean["airport"] = airport
    off_block = _content(
        _get(departure, "nas:offBlockTime", "nas:initial")
        or _get(departure, "offBlockTime", "initial")
    )
    if off_block:
        clean["offBlockTime"] = off_block
    runway_dep = _get(departure, "nas:runwayDepartureTime") or _get(departure, "runwayDepartureTime")
    if runway_dep:
        est = _content(
            _get(runway_dep, "nas:estimated", "nas:time") or _get(runway_dep, "estimated", "time")
        )
        if est:
            clean["estimatedRunwayDepartureTime"] = est
        earliest = _content(
            _get(runway_dep, "nas:earliest", "nas:time") or _get(runway_dep, "earliest", "time")
        )
        if earliest:
            clean["earliestRunwayDepartureTime"] = earliest
    rw_actual = _get(departure, "nas:runwayActual") or _get(departure, "runwayActual")
    rw_assigned = _get(departure, "nas:runwayAssigned") or _get(departure, "runwayAssigned")
    rw_predicted = _get(departure, "nas:runwayPredicted") or _get(departure, "runwayPredicted")
    runway = (
        _content(_get(rw_actual, "runwayDesignator"))
        or _content(_get(rw_assigned, "runwayDesignator"))
        or _content(_get(rw_predicted, "runwayDesignator"))
    )
    if runway:
        clean["runway"] = runway
    delay = _get(departure, "nas:departureDelay") or _get(departure, "departureDelay")
    if delay:
        predicted = _content(_get(delay, "nas:predictedDelay") or _get(delay, "predictedDelay"))
        current = _content(_get(delay, "nas:currentDelay") or _get(delay, "currentDelay"))
        if predicted and not isinstance(predicted, dict):
            clean["predictedDelay"] = predicted
        if current and not isinstance(current, dict):
            clean["currentDelay"] = current
    taxi = _get(departure, "nas:departureTaxiTime") or _get(departure, "departureTaxiTime")
    if taxi:
        total = _content(
            _get(taxi, "nas:totalEstimatedTaxiOutTime") or _get(taxi, "totalEstimatedTaxiOutTime")
        )
        if total:
            clean["estimatedTaxiOutTime"] = total
    spot = _get(departure, "nas:predictedDepartureSpot") or _get(departure, "predictedDepartureSpot")
    if spot:
        region = _content(_get(spot, "spotRegion"))
        if region:
            clean["predictedSpot"] = region
    fix = _get(departure, "nas:departureFix") or _get(departure, "departureFix")
    if fix:
        designated = _content(_get(fix, "nas:designatedPoint") or _get(fix, "designatedPoint"))
        if designated:
            clean["fix"] = designated
    return clean


def _clean_arrival(arrival: Any) -> dict[str, Any]:
    if not isinstance(arrival, dict):
        return {}
    clean: dict[str, Any] = {}
    airport = _normalize_airport(
        arrival.get("destinationPointText") or _get(arrival, "arrivalPointText")
    )
    if airport:
        clean["airport"] = airport
    fix = _get(arrival, "nas:arrivalFix") or _get(arrival, "arrivalFix")
    if fix:
        designated = _content(_get(fix, "nas:designatedPoint") or _get(fix, "designatedPoint"))
        if designated:
            clean["fix"] = designated
    rat = _get(arrival, "nas:runwayArrivalTime") or _get(arrival, "runwayArrivalTime")
    if rat:
        est = _get(rat, "nas:estimated") or _get(rat, "estimated")
        if est:
            t = _content(_get(est, "nas:time") or _get(est, "time"))
            if t:
                clean["estimatedArrivalTime"] = t
        actual = _get(rat, "nas:actual") or _get(rat, "actual")
        if actual:
            t = _content(_get(actual, "nas:time") or _get(actual, "time"))
            if t:
                clean["actualArrivalTime"] = t
    rw_actual = _get(arrival, "nas:runwayActual") or _get(arrival, "runwayActual")
    rw_assigned = _get(arrival, "nas:runwayAssigned") or _get(arrival, "runwayAssigned")
    rw_predicted = _get(arrival, "nas:runwayPredicted") or _get(arrival, "runwayPredicted")
    runway = (
        _content(_get(rw_actual, "runwayDesignator"))
        or _content(_get(rw_assigned, "runwayDesignator"))
        or _content(_get(rw_predicted, "runwayDesignator"))
    )
    if runway:
        clean["runway"] = runway
    taxi = _get(arrival, "nas:arrivalTaxiTime") or _get(arrival, "arrivalTaxiTime")
    if taxi:
        total = _content(
            _get(taxi, "nas:totalEstimatedTaxiInTime") or _get(taxi, "totalEstimatedTaxiInTime")
        )
        if total:
            clean["estimatedTaxiInTime"] = total
        elapsed = _content(
            _get(taxi, "nas:elapsedArrivalTaxiTime") or _get(taxi, "elapsedArrivalTaxiTime")
        )
        if elapsed:
            clean["elapsedTaxiInTime"] = elapsed
    spot = _get(arrival, "nas:predictedArrivalSpot") or _get(arrival, "predictedArrivalSpot")
    if spot:
        region = _content(_get(spot, "spotRegion"))
        if region:
            clean["predictedSpot"] = region
    actual_spot = _content(_get(arrival, "nas:actualArrivalSpot") or _get(arrival, "actualArrivalSpot"))
    if actual_spot:
        clean["actualSpot"] = actual_spot
    exit_time = _content(
        _get(arrival, "nas:movementAreaActualExitTime") or _get(arrival, "movementAreaActualExitTime")
    )
    if exit_time:
        clean["movementAreaActualExitTime"] = exit_time
    return clean


def _clean_flight_payload(doc: dict[str, Any]) -> dict[str, Any]:
    departure = doc.get("departure") or {}
    arrival = doc.get("arrival") or {}
    status = doc.get("flight_status") or {}
    state = _get(status, "nas:tfdmFlightState") or _get(status, "tfdmFlightState") or {}

    clean: dict[str, Any] = {
        "_id": doc.get("_id"),
        "tfdmId": doc.get("tfdm_id"),
        "tfmId": doc.get("tfm_id"),
        "flightPlanIdentifier": doc.get("flight_plan_identifier"),
        "flightNumber": doc.get("flight_number"),
        "airline": doc.get("major_carrier_identifier") or doc.get("tfms_airline"),
        "aircraftIdentification": doc.get("aircraft_identification"),
        "tfdmCreatorAirport": _normalize_airport(
            _content(_get(doc, "tfdm_id_creator_airport", "locationIndicator"))
        ),
        "departure": _clean_departure(departure),
        "arrival": _clean_arrival(arrival),
        "flightState": _content(state.get("value")),
        "flightStateSource": _content(state.get("source")),
        "flightStateReportedTime": _content(
            _get(state, "nas:reportedTimestamp") or _get(state, "reportedTimestamp")
        ),
        "tfdmFlightCreationTime": doc.get("tfdm_flight_creation_time"),
        "messageType": doc.get("message_type"),
        "createdAt": doc.get("created_at"),
        "updatedAt": doc.get("updated_at"),
        "status": doc.get("status"),
    }
    if doc.get("tfmsSummary"):
        clean["tfmsSummary"] = doc["tfmsSummary"]
    if doc.get("tbfmSummary"):
        clean["tbfmSummary"] = doc["tbfmSummary"]
    return {k: v for k, v in clean.items() if v is not None}


@router.get("/flights/{flight_number}")
async def get_flight(flight_number: str) -> dict[str, Any]:
    coll = await get_collection()
    flights = await (
        coll.find({"flight_number": flight_number.upper(), "status": "active"})
        .sort("created_at", -1)
        .limit(20)
        .to_list(length=20)
    )
    if not flights:
        raise HTTPException(status_code=404, detail="Flight not found")
    doc = max(flights, key=lambda f: (bool(f.get("tfmsSummary")), bool(f.get("tbfmSummary")), f.get("updated_at")))
    _normalize_flight_airports(doc)
    doc.pop("tfms_events", None)
    return _prepare(_clean_flight_payload(doc))


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


@router.get("/flights/{flight_number}/tbfm", response_class=HTMLResponse)
async def flight_tbfm(request: Request, flight_number: str) -> Any:
    tbfm_coll = await get_tbfm_collection()
    messages = (
        await tbfm_coll.find({"flight_number": flight_number.upper(), "status": "active"})
        .sort("_id", -1)
        .limit(200)
        .to_list(length=200)
    )
    for msg in messages:
        if msg.get("departure_airport"):
            msg["departure_airport"] = _normalize_airport(msg["departure_airport"])
        if msg.get("arrival_airport"):
            msg["arrival_airport"] = _normalize_airport(msg["arrival_airport"])
    return templates.TemplateResponse(
        request,
        "tbfm.html",
        {
            "flight_number": flight_number.upper(),
            "tbfm_messages": _prepare(messages),
        },
    )


@router.get("/tbfm/{tbfm_id}", response_class=HTMLResponse)
async def tbfm_detail(request: Request, tbfm_id: str) -> Any:
    tbfm_coll = await get_tbfm_collection()
    try:
        _id: Any = ObjectId(tbfm_id)
    except Exception:
        _id = tbfm_id
    message = await tbfm_coll.find_one({"_id": _id, "status": "active"})
    if not message:
        raise HTTPException(status_code=404, detail="TBFM message not found")
    if message.get("departure_airport"):
        message["departure_airport"] = _normalize_airport(message["departure_airport"])
    if message.get("arrival_airport"):
        message["arrival_airport"] = _normalize_airport(message["arrival_airport"])
    return templates.TemplateResponse(
        request,
        "tbfm_detail.html",
        {
            "flight_number": message.get("flight_number") or "",
            "message": _prepare(message),
        },
    )


@router.get("/", response_class=HTMLResponse)
async def index(request: Request, q: str | None = None) -> Any:
    coll = await get_collection()
    tfms_coll = await get_tfms_collection()
    tbfm_coll = await get_tbfm_collection()
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

    flight_numbers = {f.get("flight_number") for f in flights if f.get("flight_number")}
    tfms_counts = {}
    tbfm_counts = {}
    for fn in flight_numbers:
        tfms_counts[fn] = await tfms_coll.count_documents({"flight_number": fn, "status": "active"})
        tbfm_counts[fn] = await tbfm_coll.count_documents({"flight_number": fn, "status": "active"})

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "flights": _prepare(flights),
            "tfms_counts": tfms_counts,
            "tbfm_counts": tbfm_counts,
            "q": q or "",
        },
    )
