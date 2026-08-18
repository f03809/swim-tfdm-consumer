import asyncio
import contextlib
import logging
from datetime import UTC, datetime
from typing import Any

from aiokafka import AIOKafkaConsumer

from app.config import settings
from app.db import get_collection, get_tbfm_collection
from app.parser import _normalize_airport
from app.tbfm_parser import parse_tbfm_message

logger = logging.getLogger(__name__)


class TbfmConsumer:
    def __init__(self) -> None:
        self._stop = asyncio.Event()
        self._task: asyncio.Task[Any] | None = None
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            settings.kafka_tbfm_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_tbfm_group_id,
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
                "TBFM consumer started: topic=%s group=%s",
                settings.kafka_tbfm_topic,
                settings.kafka_tbfm_group_id,
            )
            async for msg in self._consumer:
                if self._stop.is_set():
                    break
                try:
                    doc = parse_tbfm_message(msg.value)
                    if doc is None:
                        continue
                    await self._store(doc)
                except Exception:
                    logger.exception("Failed to process TBFM message")
        finally:
            if self._consumer:
                with contextlib.suppress(Exception):
                    await self._consumer.stop()

    async def _store(self, doc: dict[str, Any]) -> None:
        now = datetime.now(UTC)
        tbfm_coll = await get_tbfm_collection()
        flight_coll = await get_collection()
        doc["created_at"] = now
        doc["updated_at"] = now
        doc["status"] = "active"
        result = await tbfm_coll.insert_one(doc)
        await self._link_to_flight(doc, flight_coll, tbfm_coll, result.inserted_id, now)

    async def _link_to_flight(
        self,
        doc: dict[str, Any],
        flight_coll: Any,
        tbfm_coll: Any,
        tbfm_id: Any,
        now: datetime,
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

        await tbfm_coll.update_one(
            {"_id": tbfm_id},
            {"$set": {"linked_flight_id": str(flight["_id"]), "updated_at": now}},
        )
        await self._update_flight_tbfm(doc, flight, flight_coll, now)

    async def _update_flight_tbfm(
        self,
        doc: dict[str, Any],
        flight: dict[str, Any],
        flight_coll: Any,
        now: datetime,
    ) -> None:
        summary = flight.get("tbfmSummary") or {}
        new = summary.copy()
        new["tbfm_message_count"] = new.get("tbfm_message_count", 0) + 1
        new["latest_msg_time"] = doc.get("msg_time") or doc.get("env_time")
        new["latest_env_srce"] = doc.get("env_srce")
        new["latest_tma_id"] = doc.get("tma_id")
        new["latest_air_type"] = doc.get("air_type")
        new["latest_meter_fix"] = doc.get("meter_fix")
        new["latest_meter_fix_eta"] = doc.get("eta_mfx")
        new["latest_runway_eta"] = doc.get("eta_rwy")
        new["latest_etd"] = doc.get("etd")
        new["latest_tbfm_runway"] = doc.get("rwy")
        new["latest_miles_in_trail"] = doc.get("mis_text")
        if doc.get("gufi"):
            new["gufi"] = doc["gufi"]
        if doc.get("flight_number"):
            new["flight_number"] = doc["flight_number"]
        if doc.get("departure_airport"):
            new["departure_airport"] = doc["departure_airport"]
        if doc.get("arrival_airport"):
            new["arrival_airport"] = doc["arrival_airport"]

        new = {k: v for k, v in new.items() if v is not None}
        await flight_coll.update_one(
            {"_id": flight["_id"]},
            {"$set": {"tbfmSummary": new, "updated_at": now}},
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
