from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase

from app.config import settings

_client: AsyncIOMotorClient | None = None
_db: AsyncIOMotorDatabase | None = None
_collection: AsyncIOMotorCollection | None = None
_tfms_collection: AsyncIOMotorCollection | None = None
_tbfm_collection: AsyncIOMotorCollection | None = None
_sfdps_collection: AsyncIOMotorCollection | None = None
_stdds_collection: AsyncIOMotorCollection | None = None
_route_collection: AsyncIOMotorCollection | None = None


async def get_collection() -> AsyncIOMotorCollection:
    global _client, _db, _collection
    if _client is None:
        _client = AsyncIOMotorClient(settings.mongodb_url)
        _db = _client[settings.mongodb_db]
    if _collection is None:
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


async def get_tbfm_collection() -> AsyncIOMotorCollection:
    global _client, _db, _tbfm_collection
    if _client is None:
        _client = AsyncIOMotorClient(settings.mongodb_url)
        _db = _client[settings.mongodb_db]
    if _tbfm_collection is None:
        _tbfm_collection = _db[settings.mongodb_tbfm_collection]
        await _tbfm_collection.create_index("gufi")
        await _tbfm_collection.create_index("flight_number")
        await _tbfm_collection.create_index("linked_flight_id")
    return _tbfm_collection


async def get_sfdps_collection() -> AsyncIOMotorCollection:
    global _client, _db, _sfdps_collection
    if _client is None:
        _client = AsyncIOMotorClient(settings.mongodb_url)
        _db = _client[settings.mongodb_db]
    if _sfdps_collection is None:
        _sfdps_collection = _db[settings.mongodb_sfdps_collection]
        await _sfdps_collection.create_index("gufi")
        await _sfdps_collection.create_index("flight_number")
        await _sfdps_collection.create_index("linked_flight_id")
    return _sfdps_collection


async def get_stdds_collection() -> AsyncIOMotorCollection:
    global _client, _db, _stdds_collection
    if _client is None:
        _client = AsyncIOMotorClient(settings.mongodb_url)
        _db = _client[settings.mongodb_db]
    if _stdds_collection is None:
        _stdds_collection = _db[settings.mongodb_stdds_collection]
        await _stdds_collection.create_index("gufi")
        await _stdds_collection.create_index("flight_number")
        await _stdds_collection.create_index("linked_flight_id")
    return _stdds_collection


async def get_route_collection() -> AsyncIOMotorCollection:
    global _client, _db, _route_collection
    if _client is None:
        _client = AsyncIOMotorClient(settings.mongodb_url)
        _db = _client[settings.mongodb_db]
    if _route_collection is None:
        _route_collection = _db["flight_routes"]
        await _route_collection.create_index("flight_number")
        await _route_collection.create_index("updated_at")
    return _route_collection


async def close_db() -> None:
    global _client
    if _client is not None:
        _client.close()
        _client = None
