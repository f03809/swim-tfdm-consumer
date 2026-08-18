import xml.etree.ElementTree as ET
from typing import Any

from app.parser import _normalize_airport, _parse_iso


def _local(tag: str) -> str:
    return tag.split("}")[-1]


def _first_text(parent: ET.Element, local_tag: str) -> str | None:
    for e in parent.iter():
        if _local(e.tag) == local_tag:
            text = (e.text or "").strip()
            if text:
                return text
    return None


def _attr_text(elem: ET.Element | None, attr: str) -> str | None:
    if elem is None:
        return None
    return elem.get(attr)


def _find_ancestor(root: ET.Element, local_tag: str) -> ET.Element | None:
    for e in root.iter():
        if _local(e.tag) == local_tag:
            return e
    return None


def parse_tbfm_message(payload: bytes) -> dict[str, Any] | None:
    try:
        root = ET.fromstring(payload)
    except ET.ParseError:
        return None

    air = _find_ancestor(root, "air")
    if air is None:
        return None

    env = _find_ancestor(root, "env")
    tma = _find_ancestor(root, "tma")

    env_time = _attr_text(env, "envTime")
    env_srce = _attr_text(env, "envSrce")
    msg_time = _attr_text(tma, "msgTime")
    msg_id = _attr_text(tma, "msgId")

    aid = air.get("aid") or _first_text(air, "aid")
    apt = _normalize_airport(air.get("apt"))
    dap = _normalize_airport(air.get("dap"))

    return {
        "msg_type": "TBFM",
        "gufi": air.get("gufi"),
        "flight_number": aid,
        "departure_airport": dap,
        "arrival_airport": apt,
        "tma_id": air.get("tmaId"),
        "air_type": air.get("airType"),
        "cid": air.get("cid"),
        "env_time": _parse_iso(env_time),
        "env_srce": env_srce,
        "msg_time": _parse_iso(msg_time),
        "msg_id": msg_id,
        "raw_tbfm_data": payload.decode("utf-8", errors="replace"),
        "meter_fix": _first_text(air, "mfx"),
        "eta_mfx": _parse_iso(_first_text(air, "eta_mfx")),
        "eta_dfx": _parse_iso(_first_text(air, "eta_dfx")),
        "eta_sfx": _parse_iso(_first_text(air, "eta_sfx")),
        "eta_rwy": _parse_iso(_first_text(air, "eta_rwy")),
        "etd": _parse_iso(_first_text(air, "etd")),
        "rwy": _first_text(air, "rwy"),
        "mis_text": _first_text(air, "mis"),
        "mrp_type": _first_text(air, "mrp"),
        "tra_text": _first_text(air, "tra"),
        "spd_text": _first_text(air, "spd"),
        "std_text": _first_text(air, "std"),
        "sch_text": _first_text(air, "sch"),
    }
