import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from app.api import router
from app.consumer import TfdmConsumer
from app.db import close_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

consumer = TfdmConsumer()


@asynccontextmanager
async def lifespan(app: FastAPI) -> Any:
    await consumer.start()
    yield
    await consumer.stop()
    await close_db()


app = FastAPI(
    title="SWIM TFDM Consumer",
    description="Consumes faa-tfdm-raw, persists flight data, and serves an API + UI.",
    version="0.1.0",
    lifespan=lifespan,
)
app.include_router(router)
