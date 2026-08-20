import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from pymongo.errors import DuplicateKeyError

from fastapi import APIRouter, Body, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field, field_validator

from app.auth import get_current_client
from app.config import settings
from app.db import (
    get_clients_collection,
    get_collection,
    get_flight_webhooks_collection,
    get_subscriptions_collection,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/subscriptions")


class SubscriptionCreate(BaseModel):
    flight_number: str = Field(..., min_length=1)
    webhook_url: str = Field(..., min_length=1)

    @field_validator("flight_number")
    @classmethod
    def _upper_flight(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("webhook_url")
    @classmethod
    def _check_url(cls, value: str) -> str:
        value = value.strip()
        if not value.startswith(("http://", "https://")):
            raise ValueError("webhook_url must start with http:// or https://")
        return value


def _prepare(doc: Any) -> Any:
    def _default(obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    return json.loads(json.dumps(doc, default=_default))


def _subscription_response(doc: dict[str, Any]) -> dict[str, Any]:
    resp = dict(doc)
    resp["subscription_id"] = str(resp.pop("_id", ""))
    return _prepare(resp)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_subscription(
    request: Request,
    payload: SubscriptionCreate = Body(...),
    client: dict[str, Any] = Depends(get_current_client),
) -> dict[str, Any]:
    now = datetime.now(UTC)
    sub_id = str(uuid.uuid4())

    # Ensure a flight document placeholder exists for webhook state
    flight_coll = await get_flight_webhooks_collection()
    try:
        await flight_coll.update_one(
            {"flight_number": payload.flight_number},
            {"$setOnInsert": {"last_payload": None, "last_sent_at": None, "last_message_at": None}},
            upsert=True,
        )
    except DuplicateKeyError:
        pass

    sub_coll = await get_subscriptions_collection()
    doc: dict[str, Any] = {
        "_id": sub_id,
        "client_id": client["client_id"],
        "flight_number": payload.flight_number,
        "webhook_url": payload.webhook_url,
        "status": "active",
        "failure_count": 0,
        "last_attempt_at": None,
        "last_attempt_status": None,
        "last_attempt_error": None,
        "created_at": now,
        "updated_at": now,
    }
    await sub_coll.insert_one(doc)
    logger.info("Created subscription %s for flight %s", sub_id, payload.flight_number)
    return _subscription_response(doc)


@router.get("")
async def list_subscriptions(
    request: Request,
    client: dict[str, Any] = Depends(get_current_client),
) -> list[dict[str, Any]]:
    sub_coll = await get_subscriptions_collection()
    docs = await (
        sub_coll.find({
            "client_id": client["client_id"],
            "status": {"$in": ["active", "failing"]},
        })
        .sort("created_at", -1)
        .to_list(length=1000)
    )
    return [_subscription_response(d) for d in docs]


@router.get("/{subscription_id}")
async def get_subscription(
    subscription_id: str,
    client: dict[str, Any] = Depends(get_current_client),
) -> dict[str, Any]:
    sub_coll = await get_subscriptions_collection()
    doc = await sub_coll.find_one(
        {"_id": subscription_id, "client_id": client["client_id"], "status": {"$in": ["active", "failing"]}}
    )
    if not doc:
        raise HTTPException(status_code=404, detail="Subscription not found")
    return _subscription_response(doc)


@router.delete("/{subscription_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_subscription(
    subscription_id: str,
    client: dict[str, Any] = Depends(get_current_client),
) -> None:
    sub_coll = await get_subscriptions_collection()
    result = await sub_coll.delete_one(
        {"_id": subscription_id, "client_id": client["client_id"], "status": {"$in": ["active", "failing"]}}
    )
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Subscription not found")
