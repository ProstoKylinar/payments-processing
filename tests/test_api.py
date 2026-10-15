import asyncio
import uuid

import httpx
from sqlalchemy import func, select

from app.db import session_factory
from app.main import app
from app.models import OutboxEvent, Payment

BODY = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #1",
    "metadata": {"order_id": 1},
    "webhook_url": "https://example.com/hook",
}


def hdr(key: str | None = None) -> dict[str, str]:
    return {"Idempotency-Key": key or str(uuid.uuid4())}


async def counts() -> tuple[int, int]:
    async with session_factory() as s:
        return (
            await s.scalar(select(func.count()).select_from(Payment)),
            await s.scalar(select(func.count()).select_from(OutboxEvent)),
        )


async def test_requires_api_key():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        r = await c.post("/api/v1/payments", json=BODY, headers=hdr())
        assert r.status_code == 401
        r = await c.get(f"/api/v1/payments/{uuid.uuid4()}", headers={"X-API-Key": "bad"})
        assert r.status_code == 401


async def test_create_returns_202_and_writes_outbox(client):
    r = await client.post("/api/v1/payments", json=BODY, headers=hdr())
    assert r.status_code == 202
    data = r.json()
    assert data["status"] == "pending" and data["payment_id"] and data["created_at"]
    assert await counts() == (1, 1)
    async with session_factory() as s:
        ev = await s.scalar(select(OutboxEvent))
        assert ev.payload == {"payment_id": data["payment_id"]} and ev.published_at is None


async def test_idempotent_repeat_returns_same_payment(client):
    h = hdr("key-1")
    r1 = await client.post("/api/v1/payments", json=BODY, headers=h)
    r2 = await client.post("/api/v1/payments", json=BODY, headers=h)
    assert r1.status_code == r2.status_code == 202
    assert r1.json()["payment_id"] == r2.json()["payment_id"]
    assert await counts() == (1, 1)


async def test_same_key_different_body_conflicts(client):
    h = hdr("key-2")
    await client.post("/api/v1/payments", json=BODY, headers=h)
    r = await client.post("/api/v1/payments", json={**BODY, "amount": "1.00"}, headers=h)
    assert r.status_code == 409
    assert await counts() == (1, 1)


async def test_concurrent_requests_create_single_payment(client):
    h = hdr("key-3")
    rs = await asyncio.gather(
        *[client.post("/api/v1/payments", json=BODY, headers=h) for _ in range(8)]
    )
    assert {r.status_code for r in rs} == {202}
    assert len({r.json()["payment_id"] for r in rs}) == 1
    assert await counts() == (1, 1)


async def test_validation(client):
    assert (await client.post("/api/v1/payments", json=BODY)).status_code == 422  # no key
    for bad in (
        {**BODY, "currency": "GBP"},
        {**BODY, "amount": "-5"},
        {**BODY, "amount": "1.234"},
        {**BODY, "webhook_url": "not-a-url"},
    ):
        r = await client.post("/api/v1/payments", json=bad, headers=hdr())
        assert r.status_code == 422, bad
    assert await counts() == (0, 0)


async def test_get_payment(client):
    pid = (await client.post("/api/v1/payments", json=BODY, headers=hdr("k"))).json()["payment_id"]
    r = await client.get(f"/api/v1/payments/{pid}")
    assert r.status_code == 200
    d = r.json()
    assert d["payment_id"] == pid and d["amount"] == "100.50" and d["currency"] == "RUB"
    assert d["metadata"] == {"order_id": 1} and d["idempotency_key"] == "k"
    assert d["status"] == "pending" and d["processed_at"] is None
    assert (await client.get(f"/api/v1/payments/{uuid.uuid4()}")).status_code == 404
