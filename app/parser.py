import logging
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


def _content(value: Any) -> Any:
    if isinstance(value, dict):
        return value.get("#content", value)
    return value


def _get(d: Any, *keys: str, default: Any = None) -> Any:
    for key in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(key, default)
        if d is None:
            return None
    return d


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _normalize_flight_number(value: Any) -> str | None:
    if not value:
        return None
    return str(value).strip().upper()


def _normalize_airport(value: Any) -> str | None:
    if not value:
        return None
    code = str(value).strip().upper()
    if len(code) == 4 and code.startswith("K") and code[1:].isalpha():
        return code[1:]
    return code


def is_delete_message(message_type: Any) -> bool:
    if not message_type:
        return False
    return "delete" in str(message_type).lower()


def parse_tfdm_message(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Convert a TFDM JSON message (already XML->JSON from the producer) into a flight dict."""
    root = raw.get("nas:NasMessage", raw)
    flight = _get(root, "nas:flight")
    if not isinstance(flight, dict):
        logger.warning("TFDM message missing nas:flight block")
        return None

    flight_id = _get(flight, "fx:flightIdentification", default={})
    if not isinstance(flight_id, dict):
        flight_id = {}

    tfdm_id_obj = flight_id.get("nas:tfdmId")
    tfdm_id = _content(tfdm_id_obj) if tfdm_id_obj else None
    tfm_id = _content(flight_id.get("nas:tfmId"))
    aircraft_identification = _content(flight_id.get("aircraftIdentification"))
    major_carrier_identifier = _content(flight_id.get("majorCarrierIdentifier"))
    tfms_airline = _content(flight_id.get("nas:tfmsAirline"))
    tfdm_id_creator_airport = _get(flight_id, "nas:tfdmIdCreatorAirport")
    if isinstance(tfdm_id_creator_airport, dict) and "locationIndicator" in tfdm_id_creator_airport:
        tfdm_id_creator_airport["locationIndicator"] = _normalize_airport(tfdm_id_creator_airport["locationIndicator"])

    flight_plan = _get(flight, "nas:flightPlan", default={})
    if not isinstance(flight_plan, dict):
        flight_plan = {}
    flight_plan_identifier = _content(flight_plan.get("identifier"))
    tfdm_id_of_fp = _content(flight_plan.get("nas:tfdmIdOfFlightPlanUsedForSurfaceManagement"))

    departure = _get(flight, "fx:departure", default={})
    if isinstance(departure, dict) and "departurePointText" in departure:
        departure["departurePointText"] = _normalize_airport(departure["departurePointText"])
    arrival = _get(flight, "fx:arrival", default={})
    if isinstance(arrival, dict) and "destinationPointText" in arrival:
        arrival["destinationPointText"] = _normalize_airport(arrival["destinationPointText"])
    aircraft = _get(flight, "fx:aircraft", default={})
    flight_status = _get(flight, "nas:flightStatus", default={})

    creation_time_obj = _get(flight, "nas:tfdmFlightCreationTime")
    tfdm_flight_creation_time = _parse_iso(_content(creation_time_obj))

    metadata = _get(root, "nas:metadata", default={})
    message_type = _content(metadata.get("messageType"))

    return {
        "tfdm_id": _content(tfdm_id),
        "tfm_id": _content(tfm_id),
        "flight_plan_identifier": _content(flight_plan_identifier),
        "tfdm_id_of_flight_plan_used_for_surface_management": _content(tfdm_id_of_fp),
        "flight_number": _normalize_flight_number(aircraft_identification),
        "major_carrier_identifier": _content(major_carrier_identifier),
        "aircraft_identification": _content(aircraft_identification),
        "tfms_airline": _content(tfms_airline),
        "tfdm_id_creator_airport": tfdm_id_creator_airport,
        "departure": departure if isinstance(departure, dict) else {},
        "arrival": arrival if isinstance(arrival, dict) else {},
        "aircraft": aircraft if isinstance(aircraft, dict) else {},
        "flight_identification": flight_id,
        "flight_plan": flight_plan,
        "flight_status": flight_status if isinstance(flight_status, dict) else {},
        "tfdm_flight_creation_time": tfdm_flight_creation_time,
        "message_type": _content(message_type),
    }
