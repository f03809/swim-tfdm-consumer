import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware

from app.admin import router as admin_router
from app.api import router as api_router
from app.auth import router as auth_router
from app.config import settings
from app.consumer import TfdmConsumer
from app.db import close_db
from app.dispatcher import start as start_dispatcher, stop as stop_dispatcher
from app.sfdps_consumer import SfdpsConsumer
from app.stdds_consumer import StddsConsumer
from app.subscriptions import router as subscriptions_router
from app.tbfm_consumer import TbfmConsumer
from app.tfms_consumer import TfmsConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

consumers = [
    TfdmConsumer(),
    TfmsConsumer(),
    TbfmConsumer(),
    SfdpsConsumer(),
    StddsConsumer(),
]


@asynccontextmanager
async def api_lifespan(app: FastAPI) -> Any:
    for consumer in consumers:
        await consumer.start()
    yield
    for consumer in consumers:
        await consumer.stop()
    await close_db()


@asynccontextmanager
async def dispatcher_lifespan(app: FastAPI) -> Any:
    await start_dispatcher()
    yield
    await stop_dispatcher()
    await close_db()


if settings.run_mode == "dispatcher":
    app = FastAPI(
        title="SWIM TFDM Consumer Dispatcher",
        description="Dispatches webhooks for SWIM flight subscriptions.",
        version="0.1.0",
        lifespan=dispatcher_lifespan,
    )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

else:
    app = FastAPI(
        title="SWIM TFDM Consumer",
        description="Consumes faa-tfdm-raw, faa-tfms-raw, faa-tbfm-raw, faa-sfdps-raw, and faa-stdds-raw, persists flight data, and serves an API + UI.",
        version="0.1.0",
        lifespan=api_lifespan,
    )

    app.add_middleware(
        SessionMiddleware,
        secret_key=settings.admin_session_secret,
        max_age=settings.admin_session_max_age,
        session_cookie="admin_session",
    )

    app.include_router(auth_router)
    app.include_router(subscriptions_router)
    app.include_router(admin_router)
    app.include_router(api_router)
