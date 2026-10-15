"""Consumer платежей (FastStream + RabbitMQ).

Один обработчик делает всё: берёт сообщение из payments.new, эмулирует шлюз,
обновляет статус в БД и отправляет webhook. При любой ошибке:
  * попытка < max_attempts  -> в payments.retry.<attempt> (TTL = base * 2^(attempt-1)),
                               оттуда сообщение само возвращается в payments.new;
  * попытка == max_attempts -> в payments.dlq.
Номер попытки хранится в заголовке x-attempt.
"""

import asyncio
import logging
from typing import Any

import httpx
from faststream import FastStream
from faststream.middlewares import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitMessage

from app.config import get_settings
from app.db import engine, session_factory
from app.messaging import (
    EXCHANGE,
    NEW_QUEUE,
    RK_DLQ,
    declare_topology,
    retry_routing_key,
)
from app.processing import process_payment
from app.schemas import NewPaymentEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

settings = get_settings()
broker = RabbitBroker(settings.rabbitmq_url)
app = FastStream(broker)
http = httpx.AsyncClient(timeout=settings.webhook_timeout_seconds)


def current_attempt(headers: dict[str, Any] | None) -> int:
    try:
        return max(1, int((headers or {}).get("x-attempt", 1)))
    except (TypeError, ValueError):
        return 1


async def retry_or_dead_letter(
    event: NewPaymentEvent, attempt: int, error: Exception
) -> str:
    """Решает судьбу упавшего сообщения. Возвращает имя целевого routing key."""
    body = event.model_dump(mode="json")
    headers = {"x-attempt": attempt, "x-last-error": f"{type(error).__name__}: {error}"[:500]}
    if attempt >= settings.max_attempts:
        routing_key = RK_DLQ
    else:
        routing_key = retry_routing_key(attempt)
        headers["x-attempt"] = attempt + 1
    await broker.publish(
        body, exchange=EXCHANGE, routing_key=routing_key, persist=True, headers=headers
    )
    return routing_key


async def handle_new_payment(event: NewPaymentEvent, msg: RabbitMessage) -> None:
    attempt = current_attempt(msg.headers)
    try:
        await process_payment(event.payment_id, session_factory, settings, http)
    except Exception as exc:
        target = await retry_or_dead_letter(event, attempt, exc)
        log.warning(
            "Payment %s attempt %d/%d failed (%s: %s) -> %s",
            event.payment_id, attempt, settings.max_attempts,
            type(exc).__name__, exc, target,
        )


# REJECT_ON_ERROR: сообщение, которое не удалось даже разобрать, уходит
# reject'ом без requeue -> DLX основной очереди -> payments.dlq.
broker.subscriber(NEW_QUEUE, EXCHANGE, ack_policy=AckPolicy.REJECT_ON_ERROR)(handle_new_payment)


@app.after_startup
async def _declare() -> None:
    await declare_topology(broker)


@app.after_shutdown
async def _cleanup() -> None:
    await http.aclose()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(app.run())
