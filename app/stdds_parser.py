import json
from datetime import datetime
from typing import Any

from app.parser import _content, _get, _normalize_airport


def _parse_iso(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def parse_stdds_message(raw: bytes) -> list[dict[str, Any]]:
    try:
        payload = json.loads(raw.decode("utf-8", errors="replace"))
    except Exception:
        return []

    if "ns2:TAStatus" in payload:
        return []

    tap = payload.get("ns2:TATrackAndFlightPlan")
    if not isinstance(tap, dict):
        return []

    records = tap.get("record", [])
    if not records:
        return []
    if isinstance(records, dict):
        records = [records]

    src = tap.get("src")
    docs: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            continue

        track = record.get("track") or {}
        flight_plan = record.get("flightPlan") or {}
        enhanced = record.get("enhancedData") or {}

        doc: dict[str, Any] = {
            "msg_type": "STDDS",
            "src": _content(src),
            "rec_seq_num": _content(record.get("recSeqNum")),
            "rec_type": _content(record.get("recType")),
            "rec_stars_timestamp": _content(record.get("recSTARSTimestamp")),
            "rec_safa_receipt_time": _parse_iso(record.get("recSAFAReceiptTime")),
            "track_num": _content(track.get("trackNum")),
            "mrt_time": _parse_iso(track.get("mrtTime")),
            "track_status": _content(track.get("status")),
            "ac_address": _content(track.get("acAddress")),
            "lat": _to_float(track.get("lat")),
            "lon": _to_float(track.get("lon")),
            "x_pos": _to_int(track.get("xPos")),
            "y_pos": _to_int(track.get("yPos")),
            "v_vert": _to_int(track.get("vVert")),
            "vx": _to_int(track.get("vx")),
            "vy": _to_int(track.get("vy")),
            "v_vert_raw": _to_int(track.get("vVertRaw")),
            "vx_raw": _to_int(track.get("vxRaw")),
            "vy_raw": _to_int(track.get("vyRaw")),
            "frozen": _content(track.get("frozen")),
            "new": _content(track.get("new")),
            "pseudo": _content(track.get("pseudo")),
            "adsb": _content(track.get("adsb")),
            "reported_beacon_code": _content(track.get("reportedBeaconCode")),
            "reported_altitude": _to_int(track.get("reportedAltitude")),
            "flight_number": _content(flight_plan.get("acid")),
            "gufi": _content(enhanced.get("eramGufi")),
            "sfdps_gufi": _content(enhanced.get("sfdpsGufi")),
            "departure_airport": _normalize_airport(_content(enhanced.get("departureAirport"))),
            "arrival_airport": _normalize_airport(_content(enhanced.get("destinationAirport"))),
            "ac_type": _content(flight_plan.get("acType")),
            "runway": _content(flight_plan.get("runway")),
            "assigned_beacon_code": _content(flight_plan.get("assignedBeaconCode")),
            "entry_fix": _content(flight_plan.get("entryFix")),
            "exit_fix": _content(flight_plan.get("exitFix")),
            "sfpn": _content(flight_plan.get("sfpn")),
            "raw_stdds_data": json.dumps(record, indent=2),
        }

        docs.append(doc)

    return docs
