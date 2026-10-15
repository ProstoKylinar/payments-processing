"""Проверка проводки FastStream, а именно того, что сообщение из payments.new доходит до обработчика
с разобранным телом и заголовком x-attempt"""

import uuid
from unittest.mock import AsyncMock, patch

from faststream.rabbit import TestRabbitBroker

from app import consumer
from app.messaging import EXCHANGE, RK_NEW


async def test_message_reaches_handler_with_attempt_header():
    pid = uuid.uuid4()
    async with TestRabbitBroker(consumer.broker) as br:
        with (
            patch.object(
                consumer, "process_payment", AsyncMock(side_effect=RuntimeError("boom"))
            ) as proc,
            patch.object(consumer, "retry_or_dead_letter", AsyncMock()) as spy,
        ):
            await br.publish(
                {"payment_id": str(pid)},
                exchange=EXCHANGE,
                routing_key=RK_NEW,
                headers={"x-attempt": 3},
            )
    assert proc.call_args.args[0] == pid
    event, attempt, error = spy.call_args.args
    assert event.payment_id == pid and attempt == 3 and "boom" in str(error)
