import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.models import OutboxEvent, Payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentOut
from app.security import require_api_key

router = APIRouter(prefix="/api/v1", dependencies=[Depends(require_api_key)])

SessionDep = Annotated[AsyncSession, Depends(get_session)]


def _to_out(p: Payment) -> PaymentOut:
    return PaymentOut(
        payment_id=p.id,
        amount=p.amount,
        currency=p.currency,
        description=p.description,
        metadata=p.meta,
        status=p.status,
        idempotency_key=p.idempotency_key,
        webhook_url=p.webhook_url,
        created_at=p.created_at,
        processed_at=p.processed_at,
    )


def _accepted(p: Payment) -> PaymentAccepted:
    return PaymentAccepted(payment_id=p.id, status=p.status, created_at=p.created_at)


@router.post(
    "/payments",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    summary="Создать платёж (идемпотентно)",
)
async def create_payment(
    body: PaymentCreate,
    request: Request,
    session: SessionDep,
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=255)
    ],
) -> PaymentAccepted:
    values = {
        "amount": body.amount,
        "currency": body.currency.value,
        "description": body.description,
        "meta": body.metadata,
        "webhook_url": str(body.webhook_url),
    }

    # Платёж и событие outbox пишутся в ОДНОЙ транзакции (transactional outbox).
    # ON CONFLICT DO NOTHING атомарно защищает от дублей даже при гонке запросов.
    async with session.begin():
        payment = (
            await session.execute(
                pg_insert(Payment)
                .values(id=uuid.uuid4(), idempotency_key=idempotency_key, **values)
                .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
                .returning(Payment)
            )
        ).scalar_one_or_none()
        if payment is not None:
            session.add(
                OutboxEvent(
                    event_type="payment.created",
                    payload={"payment_id": str(payment.id)},
                )
            )

    if payment is not None:
        # Будим relay, чтобы не ждать poll-интервал.
        wakeup = getattr(request.app.state, "outbox_wakeup", None)
        if wakeup is not None:
            wakeup.set()
        return _accepted(payment)

    # Повторный запрос с тем же ключом: отдаём исходный платёж.
    existing = await session.scalar(
        select(Payment).where(Payment.idempotency_key == idempotency_key)
    )
    assert existing is not None
    if (
        existing.amount != values["amount"]
        or existing.currency != values["currency"]
        or existing.description != values["description"]
        or existing.meta != values["meta"]
        or existing.webhook_url != values["webhook_url"]
    ):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key was already used with a different request body",
        )
    return _accepted(existing)


@router.get(
    "/payments/{payment_id}",
    response_model=PaymentOut,
    summary="Получить информацию о платеже",
)
async def get_payment(payment_id: uuid.UUID, session: SessionDep) -> PaymentOut:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return _to_out(payment)
