import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from sqlalchemy import select

from app import consumer
from app.config import get_settings
from app.messaging import retry_delay_ms
from app.models import OutboxEvent, Payment, PaymentStatus
from app.outbox import publish_pending
from app.processing import PaymentNotFound, process_payment
from app.schemas import NewPaymentEvent

FAST = get_settings().model_copy(update={"processing_min_seconds": 0, "processing_max_seconds": 0})


async def make_payment(sf) -> uuid.UUID:
    async with sf() as s, s.begin():
        p = Payment(
            id=uuid.uuid4(), amount=10, currency="USD", description="d", meta={"a": 1},
            idempotency_key=str(uuid.uuid4()), webhook_url="https://hook.test/x",
        )
        s.add(p)
        s.add(OutboxEvent(event_type="payment.created", payload={"payment_id": str(p.id)}))
    return p.id


def http_client(status: int = 200, calls: list | None = None) -> httpx.AsyncClient:
    def handler(req: httpx.Request) -> httpx.Response:
        if calls is not None:
            calls.append(req)
        return httpx.Response(status)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def test_exponential_delays():
    assert [retry_delay_ms(n, 5000) for n in (1, 2, 3)] == [5000, 10000, 20000]


@pytest.mark.parametrize("rate,expected", [(1.0, "succeeded"), (0.0, "failed")])
async def test_process_updates_status_and_sends_webhook(sf, rate, expected):
    pid = await make_payment(sf)
    calls: list = []
    cfg = FAST.model_copy(update={"success_rate": rate})
    await process_payment(pid, sf, cfg, http_client(200, calls))
    async with sf() as s:
        p = await s.get(Payment, pid)
        assert p.status == expected and p.processed_at is not None
    assert len(calls) == 1
    import json
    body = json.loads(calls[0].content)
    assert body["payment_id"] == str(pid) and body["status"] == expected


async def test_webhook_failure_raises_and_retry_does_not_reprocess(sf):
    pid = await make_payment(sf)
    with pytest.raises(httpx.HTTPStatusError):
        await process_payment(pid, sf, FAST, http_client(500))
    async with sf() as s:
        first = (await s.get(Payment, pid)).status
    assert first != PaymentStatus.PENDING

    calls: list = []
    with patch("app.processing.emulate_gateway", new=AsyncMock()) as gw:
        await process_payment(pid, sf, FAST, http_client(200, calls))
        gw.assert_not_called()  # платёж уже финализирован — только webhook
    assert len(calls) == 1
    async with sf() as s:
        assert (await s.get(Payment, pid)).status == first


async def test_unknown_payment(sf):
    with pytest.raises(PaymentNotFound):
        await process_payment(uuid.uuid4(), sf, FAST, http_client())


@pytest.mark.parametrize(
    "attempt,key,next_attempt",
    [(1, "payments.retry.1", 2), (2, "payments.retry.2", 3), (3, "payments.dlq", 3)],
)
async def test_retry_then_dead_letter(attempt, key, next_attempt):
    ev = NewPaymentEvent(payment_id=uuid.uuid4())
    msg = SimpleNamespace(headers={"x-attempt": attempt})
    with (
        patch.object(consumer, "process_payment", AsyncMock(side_effect=RuntimeError("boom"))),
        patch.object(consumer.broker, "publish", AsyncMock()) as pub,
    ):
        await consumer.handle_new_payment(ev, msg)
    kw = pub.call_args.kwargs
    assert kw["routing_key"] == key and kw["headers"]["x-attempt"] == next_attempt
    assert "boom" in kw["headers"]["x-last-error"]
    assert pub.call_args.args[0] == {"payment_id": str(ev.payment_id)}


async def test_success_publishes_nothing():
    msg = SimpleNamespace(headers=None)
    with (
        patch.object(consumer, "process_payment", AsyncMock()),
        patch.object(consumer.broker, "publish", AsyncMock()) as pub,
    ):
        await consumer.handle_new_payment(NewPaymentEvent(payment_id=uuid.uuid4()), msg)
    pub.assert_not_called()


async def test_outbox_relay_marks_published(sf):
    pid = await make_payment(sf)
    broker = SimpleNamespace(publish=AsyncMock())
    assert await publish_pending(broker, sf, 100) == 1
    assert broker.publish.call_args.args[0] == {"payment_id": str(pid)}
    assert broker.publish.call_args.kwargs["routing_key"] == "payments.new"
    assert await publish_pending(broker, sf, 100) == 0  # повторно не публикуется


async def test_outbox_relay_keeps_unconfirmed_events(sf):
    for _ in range(3):
        await make_payment(sf)
    broker = SimpleNamespace(publish=AsyncMock(side_effect=[None, ConnectionError("down")]))
    assert await publish_pending(broker, sf, 100) == 1
    async with sf() as s:
        rows = (await s.scalars(select(OutboxEvent).order_by(OutboxEvent.id))).all()
    assert [r.published_at is not None for r in rows] == [True, False, False]
