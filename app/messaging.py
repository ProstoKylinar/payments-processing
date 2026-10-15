"""
                        ┌──────────────► payments.new  (основная очередь)
 exchange "payments" ───┼── payments.retry.N (TTL = base*2^(N-1)) ──► DLX → payments.new
      (direct)          └──────────────► payments.dlq  (после max_attempts)

Повторы реализованы через очереди с TTL без consumer'ов: по истечении TTL
сообщение «возвращается» в payments.new через dead-letter exchange.
Отдельная очередь на каждый уровень задержки исключает head-of-line blocking.
"""

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import get_settings

_settings = get_settings()

RK_NEW = "payments.new"
RK_DLQ = "payments.dlq"

EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)


def retry_routing_key(attempt: int) -> str:
    """Ключ очереди задержки после неудачной попытки №attempt."""
    return f"payments.retry.{attempt}"


def retry_delay_ms(attempt: int, base_ms: int | None = None) -> int:
    """Экспоненциальная задержка: base * 2^(attempt-1)."""
    base = _settings.retry_base_delay_ms if base_ms is None else base_ms
    return base * 2 ** (attempt - 1)


# Неразобранные/«ядовитые» сообщения (reject без requeue) тоже попадают в DLQ.
NEW_QUEUE = RabbitQueue(
    RK_NEW,
    durable=True,
    routing_key=RK_NEW,
    arguments={
        "x-dead-letter-exchange": EXCHANGE.name,
        "x-dead-letter-routing-key": RK_DLQ,
    },
)
DLQ = RabbitQueue(RK_DLQ, durable=True, routing_key=RK_DLQ)
RETRY_QUEUES = [
    RabbitQueue(
        retry_routing_key(n),
        durable=True,
        routing_key=retry_routing_key(n),
        arguments={
            "x-message-ttl": retry_delay_ms(n),
            "x-dead-letter-exchange": EXCHANGE.name,
            "x-dead-letter-routing-key": RK_NEW,
        },
    )
    for n in range(1, _settings.max_attempts)
]


async def declare_topology(broker: RabbitBroker) -> None:
    """Идемпотентно объявляет exchange, очереди и биндинги."""
    exchange = await broker.declare_exchange(EXCHANGE)
    for queue in (NEW_QUEUE, DLQ, *RETRY_QUEUES):
        declared = await broker.declare_queue(queue)
        await declared.bind(exchange, routing_key=queue.routing_key)
