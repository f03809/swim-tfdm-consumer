import asyncio
import contextlib
import json
import logging
from datetime import UTC, datetime
from typing import Any

from aiokafka import AIOKafkaConsumer

from app.config import settings
from app.db import get_collection, get_tfms_collection
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

        await flight_coll.update_one(
            {"_id": flight["_id"]},
            {
                "$push": {
                    "tfms_events": {
                        "tfms_id": str(tfms_id),
                        "msg_type": doc.get("msg_type"),
                        "source_time_stamp": doc.get("source_time_stamp"),
                        "status": doc.get("status"),
                        "gufi": doc.get("gufi"),
                        "tfm_id": doc.get("tfm_id"),
                    }
                },
                "$set": {"updated_at": now},
            },
        )
        await tfms_coll.update_one(
            {"_id": tfms_id},
            {"$set": {"linked_flight_id": str(flight["_id"]), "updated_at": now}},
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
