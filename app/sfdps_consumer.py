import asyncio
import contextlib
import logging
from datetime import UTC, datetime
from typing import Any

from aiokafka import AIOKafkaConsumer

from app.config import settings
from app.db import get_collection, get_sfdps_collection
from app.parser import _normalize_airport
from app.sfdps_parser import parse_sfdps_message

logger = logging.getLogger(__name__)


class SfdpsConsumer:
    def __init__(self) -> None:
        self._stop = asyncio.Event()
        self._task: asyncio.Task[Any] | None = None
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            settings.kafka_sfdps_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_sfdps_group_id,
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
                "SFDPS consumer started: topic=%s group=%s",
                settings.kafka_sfdps_topic,
                settings.kafka_sfdps_group_id,
            )
            async for msg in self._consumer:
                if self._stop.is_set():
                    break
                try:
                    docs = parse_sfdps_message(msg.value)
                    for doc in docs:
                        await self._store(doc)
                except Exception:
                    logger.exception("Failed to process SFDPS message")
        finally:
            if self._consumer:
                with contextlib.suppress(Exception):
                    await self._consumer.stop()

    async def _store(self, doc: dict[str, Any]) -> None:
        now = datetime.now(UTC)
        sfdps_coll = await get_sfdps_collection()
        flight_coll = await get_collection()
        doc["created_at"] = now
        doc["updated_at"] = now
        doc["status"] = "active"
        gufi = doc.get("gufi") or "unknown"
        source_time = doc.get("source_time_stamp") or now.isoformat()
        sfdps_id = f"{gufi}:{source_time}"
        doc["_id"] = sfdps_id
        result = await sfdps_coll.update_one({"_id": sfdps_id}, {"$set": doc}, upsert=True)
        is_new = result.upserted_id is not None
        await self._link_to_flight(doc, flight_coll, sfdps_coll, sfdps_id, now, is_new)

    async def _link_to_flight(
        self,
        doc: dict[str, Any],
        flight_coll: Any,
        sfdps_coll: Any,
        sfdps_id: Any,
        now: datetime,
        is_new: bool,
    ) -> None:
        flight = None
        if doc.get("gufi"):
            flight = await flight_coll.find_one(
                {"flight_plan_identifier": doc["gufi"], "status": "active"}
            )
        if not flight and doc.get("flight_number") and doc.get("departure_airport") and doc.get("arrival_airport"):
            flight = await flight_coll.find_one(
                {
                    "flight_number": doc["flight_number"],
                    "departure.departurePointText": doc["departure_airport"],
                    "arrival.destinationPointText": doc["arrival_airport"],
                    "status": "active",
                }
            )
        if not flight and doc.get("flight_number"):
            flight = await flight_coll.find_one(
                {
                    "flight_number": doc["flight_number"],
                    "status": "active",
                },
                sort=[("updated_at", -1)],
            )
        if not flight:
            return

        await sfdps_coll.update_one(
            {"_id": sfdps_id},
            {"$set": {"linked_flight_id": str(flight["_id"]), "updated_at": now}},
        )
        await self._update_flight_sfdps(doc, flight, flight_coll, now, is_new)

    async def _update_flight_sfdps(
        self,
        doc: dict[str, Any],
        flight: dict[str, Any],
        flight_coll: Any,
        now: datetime,
        is_new: bool,
    ) -> None:
        summary = flight.get("sfdpsSummary") or {}
        new = summary.copy()
        if is_new:
            new["sfdps_message_count"] = new.get("sfdps_message_count", 0) + 1
        new["latest_source_time_stamp"] = doc.get("source_time_stamp")
        for key, field in [
            ("latest_fdps_flight_status", "fdps_flight_status"),
            ("latest_departure_airport", "departure_airport"),
            ("latest_arrival_airport", "arrival_airport"),
            ("latest_actual_departure_time", "actual_departure_time"),
            ("latest_estimated_arrival_time", "estimated_arrival_time"),
            ("latest_position_lat", "position_lat"),
            ("latest_position_lon", "position_lon"),
            ("latest_altitude", "altitude"),
            ("latest_speed", "speed"),
            ("latest_controlling_unit", "controlling_unit"),
            ("latest_sector", "sector"),
            ("latest_centre", "centre"),
            ("latest_source", "source"),
            ("latest_system", "system"),
        ]:
            if doc.get(field) is not None:
                new[key] = doc[field]
        if doc.get("gufi"):
            new["gufi"] = doc["gufi"]
        if doc.get("flight_number"):
            new["flight_number"] = doc["flight_number"]

        new = {k: v for k, v in new.items() if v is not None}
        await flight_coll.update_one(
            {"_id": flight["_id"]},
            {"$set": {"sfdpsSummary": new, "updated_at": now}},
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
