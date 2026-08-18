import logging
from typing import Any

from app.parser import _content, _get, _normalize_flight_number, _parse_iso

logger = logging.getLogger(__name__)


def _extract_flight_data(fd: Any) -> dict[str, Any] | None:
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


def parse_tfms_message(raw: dict[str, Any]) -> list[dict[str, Any]]:
    root = raw.get("ns5:tfmDataService", raw)
    fi_output = _get(root, "ns5:fiOutput", default={})
    fi_message = _get(fi_output, "ns12:fiMessage", default={})
    msg_type = _content(fi_message.get("msgType"))
    source_time_stamp = _parse_iso(_content(fi_message.get("sourceTimeStamp")))
    tmi_list = _get(fi_message, "ns12:tmiFlightDataList", default={})

    if not tmi_list:
        logger.warning("TFMS message missing tmiFlightDataList")
        return []

    flight_data = tmi_list.get("ns12:flightData")
    if flight_data is None:
        logger.warning("TFMS message missing flightData")
        return []
    if isinstance(flight_data, dict):
        flight_data = [flight_data]

    results = []
    for fd in flight_data:
        doc = _extract_flight_data(fd)
        if doc is None:
            continue
        doc["msg_type"] = msg_type
        doc["source_time_stamp"] = source_time_stamp
        results.append(doc)
    return results
