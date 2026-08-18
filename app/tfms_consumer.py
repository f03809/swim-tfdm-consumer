import asyncio
import contextlib
import json
import logging
from datetime import UTC, datetime
from typing import Any

from aiokafka import AIOKafkaConsumer

from app.config import settings
from app.db import get_collection, get_route_collection, get_tfms_collection
from app.parser import _content, _get, _normalize_airport, _parse_iso
from app.route_parser import extract_route
from app.tfms_parser import parse_tfms_message

logger = logging.getLogger(__name__)


class TfmsConsumer:
    def __init__(self) -> None:
        self._stop = asyncio.Event()
        self._task: asyncio.Task[Any] | None = None
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            settings.kafka_tfms_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_tfms_group_id,
            auto_offset_reset=settings.kafka_auto_offset_reset,
            value_deserializer=lambda v: v,
        )
        self._task = asyncio.create_task(self._consume())

    async def _consume(self) -> None:
        if self._consumer is None:
            return
        try:
            await self._consumer.start()
            logger.info(
                "TFMS consumer started: topic=%s group=%s",
                settings.kafka_tfms_topic,
                settings.kafka_tfms_group_id,
            )
            async for msg in self._consumer:
                if self._stop.is_set():
                    break
                try:
                    payload = json.loads(
                        msg.value.decode("utf-8", errors="replace")
                    )
                    docs = parse_tfms_message(payload)
                    if not docs:
                        continue
                    for doc in docs:
                        await self._store(doc)
                except Exception:
                    logger.exception("Failed to process TFMS message")
        finally:
            if self._consumer:
                with contextlib.suppress(Exception):
                    await self._consumer.stop()

    async def _store(self, doc: dict[str, Any]) -> None:
        now = datetime.now(UTC)
        tfms_coll = await get_tfms_collection()
        flight_coll = await get_collection()
        doc["created_at"] = now
        doc["updated_at"] = now
        doc["status"] = "active"
        result = await tfms_coll.insert_one(doc)
        await self._link_to_flight(doc, flight_coll, tfms_coll, result.inserted_id, now)
        await self._update_route(doc, now)

    async def _update_route(self, doc: dict[str, Any], now: datetime) -> None:
        flight_number = doc.get("flight_number")
        if not flight_number:
            return
        route_data = extract_route(doc.get("raw_flight_data"), doc.get("msg_type"))
        if not route_data:
            return
        route_coll = await get_route_collection()
        dep = _normalize_airport(doc.get("departure_airport")) or "-"
        arr = _normalize_airport(doc.get("arrival_airport")) or "-"
        route_id = f"{flight_number}_{dep}_{arr}"
        update_doc = {
            "flight_number": flight_number,
            "departure_airport": dep,
            "arrival_airport": arr,
            "tfm_id": doc.get("tfm_id"),
            "gufi": doc.get("gufi"),
            "source_time_stamp": doc.get("source_time_stamp"),
            "msg_type": doc.get("msg_type"),
            "updated_at": now,
            "status": "active",
        }
        update_doc.update(route_data)
        await route_coll.update_one(
            {"_id": route_id},
            {"$set": update_doc},
            upsert=True,
        )

    async def _link_to_flight(
        self,
        doc: dict[str, Any],
        flight_coll: Any,
        tfms_coll: Any,
        tfms_id: Any,
        now: datetime,
    ) -> None:
        flight = None
        if doc.get("tfm_id"):
            flight = await flight_coll.find_one({"tfm_id": doc["tfm_id"], "status": "active"})
        if not flight and doc.get("gufi"):
            flight = await flight_coll.find_one({"flight_plan_identifier": doc["gufi"], "status": "active"})
        if not flight and doc.get("flight_number") and doc.get("departure_airport") and doc.get("arrival_airport"):
            flight = await flight_coll.find_one(
                {
                    "flight_number": doc["flight_number"],
                    "departure.departurePointText": doc["departure_airport"],
                    "arrival.destinationPointText": doc["arrival_airport"],
                    "status": "active",
                }
            )
        if not flight:
            return

        await tfms_coll.update_one(
            {"_id": tfms_id},
            {"$set": {"linked_flight_id": str(flight["_id"]), "updated_at": now}},
        )
        await self._update_flight_tfms(doc, flight, flight_coll, now)

    async def _update_flight_tfms(
        self,
        doc: dict[str, Any],
        flight: dict[str, Any],
        flight_coll: Any,
        now: datetime,
    ) -> None:
        summary = flight.get("tfmsSummary") or {}
        new = summary.copy()
        new["tfms_message_count"] = new.get("tfms_message_count", 0) + 1
        new["latest_source_time_stamp"] = doc.get("source_time_stamp")
        new["latest_msg_type"] = doc.get("msg_type")
        if doc.get("tfm_id"):
            new["tfm_id"] = doc["tfm_id"]
        if doc.get("gufi"):
            new["gufi"] = doc["gufi"]
        if doc.get("igtd"):
            if not new.get("first_igtd"):
                new["first_igtd"] = doc["igtd"]
            new["latest_igtd"] = doc["igtd"]
        if doc.get("departure_airport"):
            new["departure_airport"] = doc["departure_airport"]
        if doc.get("arrival_airport"):
            new["arrival_airport"] = doc["arrival_airport"]

        raw = doc.get("raw_flight_data") or {}
        msg_type = doc.get("msg_type")

        if msg_type == "FlightTimes":
            ft = _get(raw, "fdm:ncsmFlightTimes") or _get(raw, "ncsmFlightTimes")
            if ft:
                eta = _get(ft, "nxcm:eta") or _get(ft, "eta")
                if eta:
                    new["latest_eta"] = _parse_iso(_content(_get(eta, "timeValue")))
                etd = _get(ft, "nxcm:etd") or _get(ft, "etd")
                if etd:
                    new["latest_etd"] = _parse_iso(_content(_get(etd, "timeValue")))
                status_spec = _get(ft, "nxcm:flightStatusAndSpec") or _get(ft, "flightStatusAndSpec")
                if status_spec:
                    status = _content(
                        _get(status_spec, "nxcm:flightStatus") or _get(status_spec, "flightStatus")
                    )
                    if status:
                        new["latest_flight_status"] = status
                    model = _content(
                        _get(status_spec, "nxcm:aircraftModel") or _get(status_spec, "aircraftModel")
                    )
                    if model:
                        new["latest_aircraft_model"] = model

        if msg_type == "trackInformation":
            ti = _get(raw, "fdm:trackInformation") or _get(raw, "trackInformation")
            if ti:
                pos = _get(ti, "nxcm:currentPosition") or _get(ti, "currentPosition")
                if pos:
                    new["latest_position"] = {
                        "latitude": _content(
                            _get(pos, "nxce:latitudeDecimal") or _get(pos, "latitudeDecimal")
                        ),
                        "longitude": _content(
                            _get(pos, "nxce:longitudeDecimal") or _get(pos, "longitudeDecimal")
                        ),
                        "altitude": _content(_get(pos, "nxce:altitude") or _get(pos, "altitude")),
                        "speed": _content(_get(pos, "nxce:speed") or _get(pos, "speed")),
                    }

        if msg_type == "flightPlanAmendmentInformation":
            new_route = _get(
                raw, "fdm:flightPlanAmendmentInformation", "nxcm:amendmentData", "nxcm:newRouteOfFlight"
            ) or _get(raw, "nxcm:amendmentData", "nxcm:newRouteOfFlight")
            if new_route:
                new["latest_route_text"] = _content(_get(new_route, "legacyFormat"))

        new = {k: v for k, v in new.items() if v is not None}
        await flight_coll.update_one(
            {"_id": flight["_id"]},
            {"$set": {"tfmsSummary": new, "updated_at": now}},
        )

    async def stop(self) -> None:
        self._stop.set()
        if self._task and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        if self._consumer:
            with contextlib.suppress(Exception):
                await self._consumer.stop()
