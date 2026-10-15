"""Бизнес-логика обработки платежа"""

import asyncio
import logging
import random
import uuid
from typing import Any

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config import Settings
from app.models import Payment, PaymentStatus

log = logging.getLogger(__name__)


class PaymentNotFound(Exception):
    pass


async def emulate_gateway(settings: Settings) -> PaymentStatus:
    """Эмуляция внешнего шлюза: 2–5 сек, 90% успех / 10% ошибка."""
    await asyncio.sleep(
        random.uniform(settings.processing_min_seconds, settings.processing_max_seconds)
    )
    return (
        PaymentStatus.SUCCEEDED
        if random.random() < settings.success_rate
        else PaymentStatus.FAILED
    )


def webhook_payload(p: Payment) -> dict[str, Any]:
    return {
        "event": "payment.processed",
        "payment_id": str(p.id),
        "status": p.status,
        "amount": str(p.amount),
        "currency": p.currency,
        "description": p.description,
        "metadata": p.meta,
        "created_at": p.created_at.isoformat(),
        "processed_at": p.processed_at.isoformat() if p.processed_at else None,
    }


async def process_payment(
    payment_id: uuid.UUID,
    session_factory: async_sessionmaker,
    settings: Settings,
    http: httpx.AsyncClient,
) -> None:
    """Обрабатывает платёж и шлёт webhook. Безопасна к повторному вызову.

    * Строка платежа блокируется FOR UPDATE: параллельные дубли сообщения не
      обработают платёж дважды.
    * Если платёж уже не pending (повтор после сбоя webhook / дубль сообщения),
      эмуляция пропускается и просто повторно отправляется webhook.
    * Webhook отправляется ПОСЛЕ коммита; ошибка отправки -> исключение -> retry.
    """
    async with session_factory() as session, session.begin():
        payment = await session.scalar(
            select(Payment).where(Payment.id == payment_id).with_for_update()
        )
        if payment is None:
            raise PaymentNotFound(str(payment_id))
        if payment.status == PaymentStatus.PENDING:
            payment.status = (await emulate_gateway(settings)).value
            # processed_at = время БД, чтобы не зависеть от часов приложения.
            # clock_timestamp(), а не now(): now() — начало транзакции (до эмуляции шлюза).
            payment.processed_at = func.clock_timestamp()
            await session.flush()
            await session.refresh(payment)
        snapshot = webhook_payload(payment)
        url = payment.webhook_url

    response = await http.post(url, json=snapshot)
    response.raise_for_status()
    log.info("Payment %s -> %s, webhook delivered", payment_id, snapshot["status"])
