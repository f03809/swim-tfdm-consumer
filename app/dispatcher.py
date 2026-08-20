import asyncio
import contextlib
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from pymongo.errors import OperationFailure

from app.api import _clean_flight_payload, _normalize_flight_airports, _prepare
from app.config import settings
from app.db import get_collection, get_flight_webhooks_collection, get_subscriptions_collection

logger = logging.getLogger(__name__)

_http_client: httpx.AsyncClient | None = None
_stop: asyncio.Event = asyncio.Event()
_pending: set[str] = set()
_pending_event: asyncio.Event = asyncio.Event()
_coalescer_task: asyncio.Task[Any] | None = None
_inactivity_task: asyncio.Task[Any] | None = None
_watch_task: asyncio.Task[Any] | None = None

_METADATA_FIELDS = {"_id", "createdAt", "updatedAt", "status"}


def _strip_meta(payload: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in payload.items() if k not in _METADATA_FIELDS}


def _payload_changed(current: dict[str, Any], previous: dict[str, Any] | None) -> bool:
    if previous is None:
        return True
    return json.dumps(_strip_meta(current), sort_keys=True) != json.dumps(
        _strip_meta(previous), sort_keys=True
    )


def _build_payload(doc: dict[str, Any]) -> dict[str, Any] | None:
    if not doc:
        return None
    _normalize_flight_airports(doc)
    doc.pop("tfms_events", None)
    return _prepare(_clean_flight_payload(doc))


async def _get_latest_flight(flight_number: str) -> dict[str, Any] | None:
    coll = await get_collection()
    return await coll.find_one({"flight_number": flight_number}, sort=[("updated_at", -1)])


async def start() -> None:
    global _http_client, _stop, _pending, _pending_event
    global _coalescer_task, _inactivity_task, _watch_task

    _http_client = httpx.AsyncClient(timeout=settings.webhook_timeout_seconds)
    _stop.clear()
    _pending = set()
    _pending_event = asyncio.Event()

    _coalescer_task = asyncio.create_task(_coalescer())
    _inactivity_task = asyncio.create_task(_inactivity_scanner())
    _watch_task = asyncio.create_task(_watch_or_poll())

    logger.info("Webhook dispatcher started")


async def stop() -> None:
    _stop.set()
    _pending_event.set()
    for task in (_coalescer_task, _inactivity_task, _watch_task):
        if task and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
    if _http_client:
        await _http_client.aclose()
    logger.info("Webhook dispatcher stopped")


def _schedule(flight_number: str) -> None:
    if flight_number and flight_number not in _pending:
        _pending.add(flight_number)
        _pending_event.set()


async def _coalescer() -> None:
    while not _stop.is_set():
        if not _pending:
            try:
                await asyncio.wait_for(_pending_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
        if _stop.is_set():
            break

        # Coalesce all pending changes into one dispatch per flight
        await asyncio.sleep(settings.webhook_throttle_seconds)
        if _stop.is_set():
            break

        to_process = list(_pending)
        _pending.clear()
        _pending_event.clear()

        for fn in to_process:
            try:
                await _dispatch_flight(fn)
            except Exception:
                logger.exception("Failed to dispatch flight %s", fn)


async def _watch_or_poll() -> None:
    coll = await get_collection()
    try:
        stream = coll.watch(full_document="updateLookup")
        try:
            async for change in stream:
                if _stop.is_set():
                    break
                fn = _flight_number_from_change(change)
                if fn:
                    _schedule(fn)
        finally:
            await stream.close()
    except OperationFailure:
        logger.warning("MongoDB change stream not available; falling back to polling")
        await _poll_flights()
    except Exception:
        logger.exception("Flight watcher failed; falling back to polling")
        await _poll_flights()


def _flight_number_from_change(change: dict[str, Any]) -> str | None:
    op = change.get("operationType")
    if op in ("insert", "update", "replace"):
        return (change.get("fullDocument") or {}).get("flight_number")
    return None


async def _poll_flights() -> None:
    coll = await get_collection()
    last_poll = datetime.now(UTC) - timedelta(seconds=1)
    while not _stop.is_set():
        try:
            now = datetime.now(UTC)
            docs = await coll.find({"updated_at": {"$gt": last_poll}}).to_list(length=1000)
            for doc in docs:
                fn = doc.get("flight_number")
                if fn:
                    _schedule(fn)
            last_poll = now
        except Exception:
            logger.exception("Polling flights failed")

        try:
            await asyncio.wait_for(_stop.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            continue


async def _dispatch_flight(flight_number: str) -> None:
    doc = await _get_latest_flight(flight_number)
    if not doc:
        return

    payload = _build_payload(doc)
    if not payload:
        return

    webhooks_coll = await get_flight_webhooks_collection()
    existing = await webhooks_coll.find_one({"flight_number": flight_number})
    previous = existing.get("last_payload") if existing else None

    # Deleted flights always trigger a final webhook
    if doc.get("status") == "deleted":
        await _send_to_all_subscriptions(flight_number, payload, previous)
        subs_coll = await get_subscriptions_collection()
        await subs_coll.delete_many({"flight_number": flight_number, "status": {"$in": ["active", "failing"]}})
        await webhooks_coll.delete_one({"flight_number": flight_number})
        return

    if not _payload_changed(payload, previous):
        return

    now = datetime.now(UTC)
    cutoff = now - timedelta(seconds=settings.webhook_throttle_seconds)
    if existing and existing.get("last_sent_at") and existing["last_sent_at"] > cutoff:
        return

    await webhooks_coll.update_one(
        {"flight_number": flight_number},
        {"$set": {"last_payload": payload, "last_sent_at": now}},
        upsert=True,
    )

    await _send_to_all_subscriptions(flight_number, payload, previous)


async def _send_to_all_subscriptions(
    flight_number: str, payload: dict[str, Any], previous: dict[str, Any] | None
) -> None:
    subs_coll = await get_subscriptions_collection()
    subs = await subs_coll.find(
        {"flight_number": flight_number, "status": {"$in": ["active", "failing"]}}
    ).to_list(length=1000)
    if not subs:
        return
    for sub in subs:
        asyncio.create_task(_send_to_subscription_with_retries(sub, payload, previous))


async def _send_to_subscription_with_retries(
    sub: dict[str, Any], payload: dict[str, Any], previous: dict[str, Any] | None
) -> None:
    if _http_client is None:
        return

    body = {
        "subscription_id": sub["_id"],
        "event_type": "FLIGHT_UPDATED",
        "flight": payload,
        "previous_flight": previous,
    }

    last_status = 0
    last_error = ""

    for attempt in range(1 + settings.webhook_retries):
        if _stop.is_set():
            return
        try:
            response = await _http_client.post(
                sub["webhook_url"],
                json=body,
                timeout=settings.webhook_timeout_seconds,
            )
            if 200 <= response.status_code < 300:
                await _record_success(sub, response.status_code)
                return
            last_status = response.status_code
            last_error = f"HTTP {response.status_code}"
        except Exception as exc:
            last_status = 0
            last_error = str(exc)

        if attempt < settings.webhook_retries:
            delay = settings.webhook_retry_base_seconds * (5 ** attempt)
            try:
                await asyncio.wait_for(_stop.wait(), timeout=delay)
                return
            except asyncio.TimeoutError:
                continue

    await _record_failure(sub, last_status, last_error)


async def _record_success(sub: dict[str, Any], status_code: int) -> None:
    coll = await get_subscriptions_collection()
    await coll.update_one(
        {"_id": sub["_id"]},
        {
            "$set": {
                "status": "active",
                "failure_count": 0,
                "last_attempt_at": datetime.now(UTC),
                "last_attempt_status": status_code,
                "last_attempt_error": None,
                "updated_at": datetime.now(UTC),
            }
        },
    )


async def _record_failure(sub: dict[str, Any], status_code: int, error: str) -> None:
    coll = await get_subscriptions_collection()
    new_count = sub.get("failure_count", 0) + 1
    if new_count >= 10:
        await coll.delete_one({"_id": sub["_id"]})
        return
    await coll.update_one(
        {"_id": sub["_id"]},
        {
            "$set": {
                "status": "failing",
                "failure_count": new_count,
                "last_attempt_at": datetime.now(UTC),
                "last_attempt_status": status_code if status_code else None,
                "last_attempt_error": error,
                "updated_at": datetime.now(UTC),
            }
        },
    )


async def _inactivity_scanner() -> None:
    while not _stop.is_set():
        try:
            await _scan_inactivity()
        except Exception:
            logger.exception("Inactivity scan failed")

        try:
            await asyncio.wait_for(
                _stop.wait(),
                timeout=settings.inactivity_scan_interval_min * 60,
            )
        except asyncio.TimeoutError:
            continue


async def _scan_inactivity() -> None:
    subs_coll = await get_subscriptions_collection()
    flight_coll = await get_collection()
    now = datetime.now(UTC)
    active_cutoff = now - timedelta(minutes=settings.inactivity_timeout_min)
    preflight_cutoff = now - timedelta(minutes=settings.preflight_timeout_min)

    async for sub in subs_coll.find({"status": {"$in": ["active", "failing"]}}):
        fn = sub.get("flight_number")
        if not fn:
            continue
        flight = await flight_coll.find_one({"flight_number": fn}, sort=[("updated_at", -1)])
        if flight and flight.get("updated_at"):
            if flight["updated_at"] < active_cutoff:
                await subs_coll.delete_one({"_id": sub["_id"]})
        elif sub.get("created_at") and sub["created_at"] < preflight_cutoff:
            await subs_coll.delete_one({"_id": sub["_id"]})
