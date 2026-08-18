from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase

from app.config import settings

_client: AsyncIOMotorClient | None = None
_db: AsyncIOMotorDatabase | None = None
_collection: AsyncIOMotorCollection | None = None
_tfms_collection: AsyncIOMotorCollection | None = None


async def get_collection() -> AsyncIOMotorCollection:
    global _client, _db, _collection
    if _client is None:
        _client = AsyncIOMotorClient(settings.mongodb_url)
        _db = _client[settings.mongodb_db]
        _collection = _db[settings.mongodb_collection]
        await _collection.create_index("tfdm_id")
        await _collection.create_index("tfm_id")
        await _collection.create_index("flight_plan_identifier")
        await _collection.create_index("flight_number")
        await _collection.create_index("status")
    return _collection


async def get_tfms_collection() -> AsyncIOMotorCollection:
    global _client, _db, _tfms_collection
    if _client is None:
        _client = AsyncIOMotorClient(settings.mongodb_url)
        _db = _client[settings.mongodb_db]
    if _tfms_collection is None:
        _tfms_collection = _db[settings.mongodb_tfms_collection]
        await _tfms_collection.create_index("tfm_id")
        await _tfms_collection.create_index("gufi")
        await _tfms_collection.create_index("flight_number")
        await _tfms_collection.create_index("linked_flight_id")
    return _tfms_collection


async def close_db() -> None:
    global _client
    if _client is not None:
        _client.close()
        _client = None
