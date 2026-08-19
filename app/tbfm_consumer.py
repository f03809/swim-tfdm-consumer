import asyncio
import contextlib
import logging
from datetime import UTC, datetime
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition

from app.config import settings
from app.db import get_collection, get_tbfm_collection, ping_mongodb
from app.parser import _normalize_airport
from app.tbfm_parser import parse_tbfm_message

logger = logging.getLogger(__name__)


class TbfmConsumer:
    def __init__(self) -> None:
        self._stop = asyncio.Event()
        self._task: asyncio.Task[Any] | None = None
        self._consumer: AIOKafkaConsumer | None = None

    async def _ensure_db_healthy(self) -> None:
        if self._consumer is None:
            return
        while not self._stop.is_set():
            if await ping_mongodb():
                break
            partitions = self._consumer.assignment()
            if partitions:
                self._consumer.pause(*partitions)
            logger.warning("MongoDB ping failed; pausing %s partitions", len(partitions))
            await asyncio.sleep(1)
        partitions = self._consumer.assignment()
        if partitions:
            self._consumer.resume(*partitions)

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            settings.kafka_tbfm_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_tbfm_group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
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
                await self._ensure_db_healthy()
                if self._stop.is_set():
                    break
                try:
                    doc = parse_tbfm_message(msg.value)
                    if doc is not None:
                        await self._store(doc)
                    await self._consumer.commit({
                        TopicPartition(msg.topic, msg.partition): msg.offset + 1
                    })
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
        tbfm_id = f"{doc.get('env_srce') or 'unknown'}:{doc.get('msg_id') or 'unknown'}"
        doc["_id"] = tbfm_id
        result = await tbfm_coll.update_one({"_id": tbfm_id}, {"$set": doc}, upsert=True)
        is_new = result.upserted_id is not None
        await self._link_to_flight(doc, flight_coll, tbfm_coll, tbfm_id, now, is_new)

    async def _link_to_flight(
        self,
        doc: dict[str, Any],
        flight_coll: Any,
        tbfm_coll: Any,
        tbfm_id: Any,
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

        await tbfm_coll.update_one(
            {"_id": tbfm_id},
            {"$set": {"linked_flight_id": str(flight["_id"]), "updated_at": now}},
        )
        await self._update_flight_tbfm(doc, flight, flight_coll, now, is_new)

    async def _update_flight_tbfm(
        self,
        doc: dict[str, Any],
        flight: dict[str, Any],
        flight_coll: Any,
        now: datetime,
        is_new: bool,
    ) -> None:
        summary = flight.get("tbfmSummary") or {}
        new = summary.copy()
        if is_new:
            new["tbfm_message_count"] = new.get("tbfm_message_count", 0) + 1
        new["latest_msg_time"] = doc.get("msg_time") or doc.get("env_time")
        for key, field in [
            ("latest_env_srce", "env_srce"),
            ("latest_tma_id", "tma_id"),
            ("latest_air_type", "air_type"),
            ("latest_meter_fix", "meter_fix"),
            ("latest_meter_fix_eta", "eta_mfx"),
            ("latest_runway_eta", "eta_rwy"),
            ("latest_etd", "etd"),
            ("latest_tbfm_runway", "rwy"),
            ("latest_miles_in_trail", "mis_text"),
            ("latest_mrp_type", "mrp_type"),
            ("latest_trajectory", "tra_text"),
            ("latest_speed", "spd_text"),
            ("latest_schedule", "sch_text"),
            ("latest_std", "std_text"),
        ]:
            if doc.get(field) is not None:
                new[key] = doc[field]
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
