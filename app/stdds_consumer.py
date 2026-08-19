import asyncio
import contextlib
import logging
from datetime import UTC, datetime
from typing import Any

from aiokafka import AIOKafkaConsumer

from app.config import settings
from app.db import get_collection, get_stdds_collection
from app.parser import _normalize_airport
from app.stdds_parser import parse_stdds_message

logger = logging.getLogger(__name__)


class StddsConsumer:
    def __init__(self) -> None:
        self._stop = asyncio.Event()
        self._task: asyncio.Task[Any] | None = None
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            settings.kafka_stdds_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_stdds_group_id,
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
                "STDDS consumer started: topic=%s group=%s",
                settings.kafka_stdds_topic,
                settings.kafka_stdds_group_id,
            )
            async for msg in self._consumer:
                if self._stop.is_set():
                    break
                try:
                    docs = parse_stdds_message(msg.value)
                    for doc in docs:
                        await self._store(doc)
                except Exception:
                    logger.exception("Failed to process STDDS message")
        finally:
            if self._consumer:
                with contextlib.suppress(Exception):
                    await self._consumer.stop()

    async def _store(self, doc: dict[str, Any]) -> None:
        now = datetime.now(UTC)
        stdds_coll = await get_stdds_collection()
        flight_coll = await get_collection()
        doc["created_at"] = now
        doc["updated_at"] = now
        doc["status"] = "active"
        src = doc.get("src") or "unknown"
        rec_ts = doc.get("rec_stars_timestamp") or "0"
        rec_seq = doc.get("rec_seq_num") or "0"
        stdds_id = f"{src}:{rec_ts}:{rec_seq}"
        doc["_id"] = stdds_id
        result = await stdds_coll.update_one({"_id": stdds_id}, {"$set": doc}, upsert=True)
        is_new = result.upserted_id is not None
        await self._link_to_flight(doc, flight_coll, stdds_coll, stdds_id, now, is_new)

    async def _link_to_flight(
        self,
        doc: dict[str, Any],
        flight_coll: Any,
        stdds_coll: Any,
        stdds_id: Any,
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

        await stdds_coll.update_one(
            {"_id": stdds_id},
            {"$set": {"linked_flight_id": str(flight["_id"]), "updated_at": now}},
        )
        await self._update_flight_stdds(doc, flight, flight_coll, now, is_new)

    async def _update_flight_stdds(
        self,
        doc: dict[str, Any],
        flight: dict[str, Any],
        flight_coll: Any,
        now: datetime,
        is_new: bool,
    ) -> None:
        summary = flight.get("stddsSummary") or {}
        new = summary.copy()
        if is_new:
            new["stdds_message_count"] = new.get("stdds_message_count", 0) + 1
        new["latest_mrt_time"] = doc.get("mrt_time")
        for key, field in [
            ("latest_src", "src"),
            ("latest_track_num", "track_num"),
            ("latest_track_status", "track_status"),
            ("latest_lat", "lat"),
            ("latest_lon", "lon"),
            ("latest_altitude", "reported_altitude"),
            ("latest_beacon_code", "reported_beacon_code"),
            ("latest_assigned_beacon_code", "assigned_beacon_code"),
            ("latest_ac_address", "ac_address"),
            ("latest_ac_type", "ac_type"),
            ("latest_runway", "runway"),
            ("latest_entry_fix", "entry_fix"),
            ("latest_exit_fix", "exit_fix"),
            ("latest_vx", "vx"),
            ("latest_vy", "vy"),
            ("latest_v_vert", "v_vert"),
            ("latest_adsb", "adsb"),
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
            {"$set": {"stddsSummary": new, "updated_at": now}},
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
