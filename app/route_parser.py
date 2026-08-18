import logging
from typing import Any

from app.parser import _content, _get, _normalize_airport

logger = logging.getLogger(__name__)


def _collect_sequence(items: Any) -> list[dict[str, Any]]:
    if not items:
        return []
    if isinstance(items, dict):
        items = [items]
    return [
        {
            "sequence": int(item.get("sequenceNumber", 0)),
            "content": _content(item),
        }
        for item in items
        if isinstance(item, dict) and item.get("sequenceNumber")
    ]


def _merge_points(fixes: list[dict[str, Any]], waypoints: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_seq: dict[int, dict[str, Any]] = {}
    for fix in _collect_sequence(fixes):
        by_seq.setdefault(fix["sequence"], {"sequence": fix["sequence"]})["name"] = fix["content"]
    for wp in _collect_sequence(waypoints):
        pt = by_seq.setdefault(wp["sequence"], {"sequence": wp["sequence"]})
        # Waypoints use content as a placeholder; real lat/lon are separate keys.
        if isinstance(wp["content"], dict):
            pt["lat"] = wp["content"].get("latitudeDecimal")
            pt["lon"] = wp["content"].get("longitudeDecimal")
            pt["elapsed_time"] = wp["content"].get("elapsedTime")
    return sorted(by_seq.values(), key=lambda p: p["sequence"])


def _point_list(data: dict[str, Any]) -> list[dict[str, Any]]:
    traversal = data
    for key in ["nxcm:flightTraversalData2", "flightTraversalData2"]:
        if key in data:
            traversal = data[key]
            break
    fixes = _get(traversal, "nxce:fix") or _get(traversal, "fix")
    waypoints = _get(traversal, "nxce:waypoint") or _get(traversal, "waypoint")
    return _merge_points(fixes, waypoints)


def _parse_route_text(raw: dict[str, Any]) -> str | None:
    new_route = _get(raw, "fdm:flightPlanAmendmentInformation", "nxcm:amendmentData", "nxcm:newRouteOfFlight")
    if not new_route:
        return None
    if isinstance(new_route, dict):
        return _content(new_route.get("legacyFormat"))
    if isinstance(new_route, str):
        return new_route
    return None


def _parse_dp_star(data: dict[str, Any]) -> tuple[str | None, str | None, str | None, str | None]:
    dp = _get(data, "nxcm:dp") or _get(data, "dp")
    star = _get(data, "nxcm:star") or _get(data, "star")
    dp_name = _content(_get(dp, "routeName")) if dp else None
    dp_type = _content(_get(dp, "routeType")) if dp else None
    star_name = _content(_get(star, "routeName")) if star else None
    star_type = _content(_get(star, "routeType")) if star else None
    dp_transition = _content(_get(data, "nxcm:dpTransitionFix") or _get(data, "dpTransitionFix"))
    star_transition = _content(_get(data, "nxcm:starTransitionFix") or _get(data, "starTransitionFix"))
    return dp_name, dp_type, dp_transition, star_name, star_type, star_transition


def _format_dp_star(dp_name, dp_type, dp_transition, star_name, star_type, star_transition):
    dp = None
    star = None
    if dp_name:
        dp = dp_name
        if dp_type:
            dp = f"{dp} ({dp_type})"
        if dp_transition:
            dp = f"{dp} / {dp_transition}"
    if star_name:
        star = star_name
        if star_type:
            star = f"{star} ({star_type})"
        if star_transition:
            star = f"{star} / {star_transition}"
    return dp, star


def _parse_airways_centers_sectors(data: dict[str, Any]) -> tuple[list[str], list[str], list[str]]:
    traversal = data
    for key in ["nxcm:flightTraversalData2", "flightTraversalData2"]:
        if key in data:
            traversal = data[key]
            break
    airways = [i["content"] for i in _collect_sequence(_get(traversal, "nxce:airway") or _get(traversal, "airway"))]
    centers = [i["content"] for i in _collect_sequence(_get(traversal, "nxce:center") or _get(traversal, "center"))]
    sectors = [i["content"] for i in _collect_sequence(_get(traversal, "nxce:sector") or _get(traversal, "sector"))]
    return airways, centers, sectors


def extract_route(raw: dict[str, Any], msg_type: str) -> dict[str, Any] | None:
    """Pull the latest planned route out of a TFMS message if one is present."""
    if not raw or not msg_type:
        return None

    msg_type = str(msg_type)
    route: dict[str, Any] = {}

    if msg_type == "FlightRoute":
        data = _get(raw, "fdm:ncsmFlightRoute", "nxcm:ncsmRouteData") or _get(raw, "nxcm:ncsmRouteData")
        if not data:
            return None
        route["route_points"] = _point_list(data)
        airways, centers, sectors = _parse_airways_centers_sectors(data)
        route["airways"] = airways
        route["centers"] = centers
        route["sectors"] = sectors
        dp_name, dp_type, dp_transition, star_name, star_type, star_transition = _parse_dp_star(data)
        dp, star = _format_dp_star(dp_name, dp_type, dp_transition, star_name, star_type, star_transition)
        route["dp"] = dp
        route["star"] = star
        arr_fix = _get(data, "nxcm:arrivalFixAndTime") or _get(data, "arrivalFixAndTime")
        dep_fix = _get(data, "nxcm:departureFixAndTime") or _get(data, "departureFixAndTime")
        if arr_fix:
            route["arrival_fix"] = _content(_get(arr_fix, "fixName"))
        if dep_fix:
            route["departure_fix"] = _content(_get(dep_fix, "fixName"))
        return route

    if msg_type == "FlightSectors":
        data = _get(raw, "fdm:ncsmFlightSectors") or _get(raw, "ncsmFlightSectors")
        if not data:
            return None
        route["route_points"] = _point_list(data)
        airways, centers, sectors = _parse_airways_centers_sectors(data)
        route["airways"] = airways
        route["centers"] = centers
        route["sectors"] = sectors
        return route

    if msg_type == "flightPlanAmendmentInformation":
        text = _parse_route_text(raw)
        if text:
            route["route_text"] = text
        return route

    return None
