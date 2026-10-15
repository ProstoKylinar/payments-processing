import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import APIRouter, Depends, FastAPI
from faststream.rabbit import RabbitBroker

from app.api import router
from app.config import get_settings
from app.db import engine, session_factory
from app.messaging import declare_topology
from app.outbox import run_outbox_relay
from app.security import require_api_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    broker = RabbitBroker(settings.rabbitmq_url)
    await broker.start()
    await declare_topology(broker)

    stop = asyncio.Event()
    relay = asyncio.create_task(
        run_outbox_relay(
            broker,
            session_factory,
            app.state.outbox_wakeup,
            stop,
            poll_interval=settings.outbox_poll_interval,
            batch_size=settings.outbox_batch_size,
        ),
        name="outbox-relay",
    )
    try:
        yield
    finally:
        stop.set()
        await relay
        await broker.stop()
        await engine.dispose()


app = FastAPI(title="Payment Processing Service", version="1.0.0", lifespan=lifespan)
app.state.outbox_wakeup = asyncio.Event()
app.include_router(router)

# Healthcheck тоже закрыт API-ключом: «статический ключ для всех эндпоинтов».
health = APIRouter(dependencies=[Depends(require_api_key)])


@health.get("/health", include_in_schema=False)
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(health)
