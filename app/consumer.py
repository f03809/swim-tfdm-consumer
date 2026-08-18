import asyncio
import contextlib
import json
import logging
from datetime import UTC, datetime
from typing import Any

from aiokafka import AIOKafkaConsumer

from app.config import settings
from app.db import get_collection
from app.parser import is_delete_message, parse_tfdm_message

logger = logging.getLogger(__name__)


class TfdmConsumer:
    def __init__(self) -> None:
        self._stop = asyncio.Event()
        self._task: asyncio.Task[Any] | None = None
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            settings.kafka_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_group_id,
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
                "TFDM consumer started: topic=%s group=%s",
                settings.kafka_topic,
                settings.kafka_group_id,
            )
            async for msg in self._consumer:
                if self._stop.is_set():
                    break
                try:
                    payload = json.loads(
                        msg.value.decode("utf-8", errors="replace")
                    )
                    doc = parse_tfdm_message(payload)
                    if doc is None:
                        continue
                    await self._upsert(doc)
                except Exception:
                    logger.exception("Failed to process TFDM message")
        finally:
            if self._consumer:
                with contextlib.suppress(Exception):
                    await self._consumer.stop()

    async def _upsert(self, doc: dict[str, Any]) -> None:
        now = datetime.now(UTC)
        coll = await get_collection()

        ids = {
            k: v
            for k, v in {
                "tfdm_id": doc.get("tfdm_id"),
                "tfm_id": doc.get("tfm_id"),
                "flight_plan_identifier": doc.get("flight_plan_identifier"),
            }.items()
            if v
        }
        if not ids:
            logger.warning("TFDM message has no usable identifiers; skipping")
            return

        existing = None
        for key in ("tfdm_id", "tfm_id", "flight_plan_identifier"):
            if key in ids:
                existing = await coll.find_one({key: ids[key], "status": "active"})
                if existing:
                    break
        if not existing and doc.get("flight_number"):
            dep_airport = doc.get("departure", {}).get("departurePointText")
            arr_airport = doc.get("arrival", {}).get("destinationPointText")
            if dep_airport and arr_airport:
                existing = await coll.find_one(
                    {
                        "flight_number": doc["flight_number"],
                        "departure.departurePointText": dep_airport,
                        "arrival.destinationPointText": arr_airport,
                        "status": "active",
                    }
                )

        if existing:
            if is_delete_message(doc.get("message_type")):
                await coll.update_one(
                    {"_id": existing["_id"]},
                    {"$set": {"status": "deleted", "updated_at": now}},
                )
            else:
                # Merge partial updates so missing fields in the message do not
                # erase fields already stored for this flight.
                updates = {k: v for k, v in doc.items() if v is not None and v != {}}
                updates["updated_at"] = now
                updates["status"] = "active"
                await coll.update_one({"_id": existing["_id"]}, {"$set": updates})
        else:
            if is_delete_message(doc.get("message_type")):
                logger.warning("Delete message for unknown flight; ignoring")
                return
            doc["created_at"] = now
            doc["updated_at"] = now
            doc["status"] = "active"
            await coll.insert_one(doc)

    async def stop(self) -> None:
        self._stop.set()
        if self._task and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        if self._consumer:
            with contextlib.suppress(Exception):
                await self._consumer.stop()
