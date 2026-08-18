from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase

from app.config import settings

_client: AsyncIOMotorClient | None = None
_db: AsyncIOMotorDatabase | None = None
_collection: AsyncIOMotorCollection | None = None


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


async def close_db() -> None:
    global _client
    if _client is not None:
        _client.close()
        _client = None
