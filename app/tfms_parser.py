import logging
from typing import Any

from app.parser import _content, _get, _normalize_flight_number, _parse_iso

logger = logging.getLogger(__name__)


def _extract_fi_flight_data(fd: Any) -> dict[str, Any] | None:
    if not isinstance(fd, dict):
        return None
    flight = _get(fd, "ns9:flight", default={})
    if not isinstance(flight, dict):
        return None

    aircraft_id = _content(flight.get("ns7:aircraftId"))
    gufi = _content(flight.get("ns7:gufi"))
    igtd = _parse_iso(_content(_get(flight, "ns7:igtd")))
    dep = _get(flight, "ns7:departurePoint", default={})
    arr = _get(flight, "ns7:arrivalPoint", default={})
    departure_airport = _content(dep.get("ns7:airport"))
    arrival_airport = _content(arr.get("ns7:airport"))
    flight_reference = _content(fd.get("ns9:flightReference"))
    status = _content(fd.get("ns9:status"))
    tmi_info_list = _get(fd, "ns9:tmiFlightInfoList", default={})
    tmi = _get(tmi_info_list, "ns9:tmi")
    fxa = _get(tmi_info_list, "ns9:fxaFlightData")

    return {
        "tfm_id": flight_reference,
        "gufi": gufi,
        "flight_number": _normalize_flight_number(aircraft_id),
        "igtd": igtd,
        "departure_airport": departure_airport,
        "arrival_airport": arrival_airport,
        "status": status,
        "tmi_info": tmi if isinstance(tmi, dict) else {},
        "fxa_flight_data": fxa if isinstance(fxa, dict) else {},
        "raw_flight_data": fd,
    }


def _extract_fltd_message(msg: Any) -> dict[str, Any] | None:
    if not isinstance(msg, dict):
        return None

    qid = _get(msg, "fdm:trackInformation", "nxcm:qualifiedAircraftId", default={})
    if not isinstance(qid, dict):
        qid = _get(msg, "trackInformation", "qualifiedAircraftId", default={})

    gufi = _content(qid.get("nxce:gufi") or qid.get("gufi"))
    aircraft_id = _content(qid.get("nxce:aircraftId") or qid.get("aircraftId"))
    igtd = _parse_iso(
        _content(qid.get("nxce:igtd") or qid.get("igtd"))
    )
    dep = _get(qid, "nxce:departurePoint", default={}) or _get(qid, "departurePoint", default={})
    arr = _get(qid, "nxce:arrivalPoint", default={}) or _get(qid, "arrivalPoint", default={})
    departure_airport = _content(dep.get("nxce:airport") or dep.get("airport"))
    arrival_airport = _content(arr.get("nxce:airport") or arr.get("airport"))

    return {
        "tfm_id": _content(msg.get("flightRef")),
        "gufi": gufi,
        "flight_number": _normalize_flight_number(aircraft_id or _content(msg.get("acid"))),
        "igtd": igtd,
        "departure_airport": departure_airport or _content(msg.get("depArpt")),
        "arrival_airport": arrival_airport or _content(msg.get("arrArpt")),
        "status": None,
        "tmi_info": {},
        "fxa_flight_data": _get(msg, "fdm:trackInformation") or _get(msg, "trackInformation") or {},
        "raw_flight_data": msg,
    }


def _parse_fi_message(fi_message: dict[str, Any]) -> list[dict[str, Any]]:
    msg_type = _content(fi_message.get("msgType"))
    source_time_stamp = _parse_iso(_content(fi_message.get("sourceTimeStamp")))
    tmi_list = _get(fi_message, "ns12:tmiFlightDataList", default={})

    if not tmi_list:
        logger.debug("FI message missing tmiFlightDataList")
        return []

    flight_data = tmi_list.get("ns12:flightData")
    if flight_data is None:
        logger.debug("FI message missing flightData")
        return []
    if isinstance(flight_data, dict):
        flight_data = [flight_data]

    results = []
    for fd in flight_data:
        doc = _extract_fi_flight_data(fd)
        if doc is None:
            continue
        doc["msg_type"] = msg_type
        doc["source_time_stamp"] = source_time_stamp
        results.append(doc)
    return results


def _parse_fltd_output(fltd_output: dict[str, Any]) -> list[dict[str, Any]]:
    fltd_messages = fltd_output.get("fdm:fltdMessage") or fltd_output.get("fltdMessage")
    if fltd_messages is None:
        logger.debug("fltdOutput missing fltdMessage")
        return []
    if isinstance(fltd_messages, dict):
        fltd_messages = [fltd_messages]

    results = []
    for msg in fltd_messages:
        if not isinstance(msg, dict):
            continue
        doc = _extract_fltd_message(msg)
        if doc is None:
            continue
        doc["msg_type"] = _content(msg.get("msgType"))
        doc["source_time_stamp"] = _parse_iso(_content(msg.get("sourceTimeStamp")))
        results.append(doc)
    return results


def parse_tfms_message(raw: dict[str, Any]) -> list[dict[str, Any]]:
    if "ns5:tfmDataService" in raw:
        root = raw["ns5:tfmDataService"]
        fi_output = _get(root, "ns5:fiOutput", default={})
        fi_message = _get(fi_output, "ns12:fiMessage", default={})
        return _parse_fi_message(fi_message)

    if "ds:tfmDataService" in raw:
        root = raw["ds:tfmDataService"]
        fltd_output = _get(root, "fltdOutput", default={})
        if not fltd_output:
            fltd_output = _get(root, "fdm:fltdOutput", default={})
        return _parse_fltd_output(fltd_output)

    logger.warning("Unknown TFMS message root: %s", list(raw.keys())[:3])
    return []
