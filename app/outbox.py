"""Outbox relay: переносит события из таблицы outbox в RabbitMQ

Гарантии: at-least-once. Событие помечается опубликованным только после
подтверждения брокера (publisher confirms), в одной транзакции со строкой,
заблокированной FOR UPDATE SKIP LOCKED — поэтому несколько экземпляров relay
не дерутся за одни и те же строки. Дубли возможны только при падении между
confirm и commit; consumer к ним идемпотентен
"""

import asyncio
import logging

from faststream.rabbit import RabbitBroker
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.messaging import EXCHANGE, RK_NEW
from app.models import OutboxEvent

log = logging.getLogger(__name__)


async def publish_pending(
    broker: RabbitBroker, session_factory: async_sessionmaker, batch_size: int
) -> int:
    """Публикует одну пачку неопубликованных событий. Возвращает число отправленных."""
    sent = 0
    async with session_factory() as session, session.begin():
        rows = (
            await session.scalars(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.id)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
        ).all()
        try:
            for row in rows:
                await broker.publish(
                    row.payload,
                    exchange=EXCHANGE,
                    routing_key=RK_NEW,
                    message_id=str(row.id),
                    persist=True,
                    headers={"x-attempt": 1},
                )
                row.published_at = func.now()
                sent += 1
        except Exception:
            # Коммитим уже подтверждённые, остальные уйдут в следующем цикле.
            log.exception("Outbox publish failed after %d/%d events", sent, len(rows))
    return sent


async def run_outbox_relay(
    broker: RabbitBroker,
    session_factory: async_sessionmaker,
    wakeup: asyncio.Event,
    stop: asyncio.Event,
    *,
    poll_interval: float,
    batch_size: int,
) -> None:
    """Цикл relay: будится по `wakeup` (после создания платежа) или по таймеру."""
    while not stop.is_set():
        try:
            sent = await publish_pending(broker, session_factory, batch_size)
        except Exception:
            log.exception("Outbox relay iteration failed")
            sent = 0
        if sent >= batch_size:
            continue  # возможно, есть ещё — без паузы
        wakeup.clear()
        waiters = (asyncio.ensure_future(wakeup.wait()), asyncio.ensure_future(stop.wait()))
        await asyncio.wait(waiters, timeout=poll_interval, return_when=asyncio.FIRST_COMPLETED)
        for w in waiters:
            w.cancel()
