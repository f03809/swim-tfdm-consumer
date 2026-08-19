import json
from datetime import UTC, datetime
from typing import Any

from app.parser import _content, _get, _normalize_airport


def _denormalize_sfdps(obj: Any) -> Any:
    if isinstance(obj, dict):
        result: dict[str, Any] = {}
        for k, v in obj.items():
            if k.startswith("-"):
                result[k[1:]] = _denormalize_sfdps(v)
            result[k] = _denormalize_sfdps(v)
        return result
    if isinstance(obj, list):
        return [_denormalize_sfdps(item) for item in obj]
    return obj


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _split_pos(pos: str | None) -> tuple[float | None, float | None]:
    if not pos:
        return None, None
    parts = pos.strip().split()
    if len(parts) != 2:
        return None, None
    try:
        return float(parts[0]), float(parts[1])
    except Exception:
        return None, None


def parse_sfdps_message(raw: bytes) -> list[dict[str, Any]]:
    try:
        original_payload = json.loads(raw.decode("utf-8", errors="replace"))
        payload = _denormalize_sfdps(original_payload)
    except Exception:
        return []

    collection = payload.get("ns5:MessageCollection") or payload
    if not isinstance(collection, dict):
        return []

    messages = collection.get("message", [])
    if not messages:
        return []
    if isinstance(messages, dict):
        messages = [messages]

    docs: list[dict[str, Any]] = []
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        flight = msg.get("flight")
        if not isinstance(flight, dict):
            continue

        flight_ident = flight.get("flightIdentification") or {}
        departure = flight.get("departure") or {}
        arrival = flight.get("arrival") or {}
        flight_status = flight.get("flightStatus") or {}
        flight_plan = flight.get("flightPlan") or {}
        gufi_block = flight.get("gufi") or {}
        en_route = flight.get("enRoute") or {}
        position = (en_route.get("position") or {}) if en_route else {}
        controlling_unit = flight.get("controllingUnit") or {}

        lat, lon = _split_pos(_content(_get(position, "position", "location", "pos")))

        doc: dict[str, Any] = {
            "msg_type": "SFDPS",
            "gufi": _content(flight_plan.get("identifier")) or _content(gufi_block.get("#content")),
            "uuid_gufi": _content(gufi_block.get("#content")),
            "flight_number": _content(flight_ident.get("aircraftIdentification")),
            "departure_airport": _normalize_airport(_content(departure.get("departurePoint"))),
            "arrival_airport": _normalize_airport(_content(arrival.get("arrivalPoint"))),
            "fdps_flight_status": _content(flight_status.get("fdpsFlightStatus")),
            "source_time_stamp": _parse_iso(flight.get("timestamp")),
            "centre": _content(flight.get("centre")),
            "source": _content(flight.get("source")),
            "system": _content(flight.get("system")),
            "controlling_unit": _content(controlling_unit.get("unitIdentifier")),
            "sector": _content(controlling_unit.get("sectorIdentifier")),
            "position_lat": lat,
            "position_lon": lon,
            "position_time": _parse_iso(position.get("positionTime") or position.get("targetPositionTime")),
            "altitude": _content(_get(position, "altitude")),
            "speed": _content(_get(position, "actualSpeed", "surveillance")),
            "raw_sfdps_data": json.dumps(original_payload, indent=2),
        }

        dep_time = _get(departure, "runwayPositionAndTime", "runwayTime", "actual", "time")
        if dep_time:
            doc["actual_departure_time"] = _parse_iso(dep_time)

        arr_time = _get(arrival, "runwayPositionAndTime", "runwayTime", "estimated", "time")
        if arr_time:
            doc["estimated_arrival_time"] = _parse_iso(arr_time)

        docs.append(doc)

    return docs
