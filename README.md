# Payment Processing Service

Асинхронный сервис процессинга платежей: FastAPI + Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL, RabbitMQ (FastStream), Alembic, Docker.

## Запуск

```bash
docker compose up --build
```

Порядок старта: `postgres` + `rabbitmq` (healthcheck) → `migrate` (alembic upgrade head) → `api` и `consumer`.

| Что | Адрес |
|---|---|
| API / Swagger | http://localhost:8000/docs |
| RabbitMQ UI | http://localhost:15672 (guest / guest) |

API-ключ по умолчанию: `secret-api-key` (переменная `API_KEY`).

## Примеры

В compose поднимается демо-приёмник webhook `webhook-stub` (`tools/webhook_stub.py`, не часть сервиса): `POST /hook` отвечает 200 и печатает тело в лог, `POST /fail` отвечает 500. Команды ниже (bash) можно выполнять как есть.

```bash
# 1. Создать платёж → 202 Accepted; payment_id сохраняем в переменную
PID=$(curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: secret-api-key" \
  -H "Idempotency-Key: order-1001" \
  -H "Content-Type: application/json" \
  -d '{"amount": "1500.50", "currency": "RUB", "description": "Заказ №1001",
       "metadata": {"order_id": 1001}, "webhook_url": "http://webhook-stub:9000/hook"}' \
  | python -c "import sys,json; print(json.load(sys.stdin)['payment_id'])")
echo "$PID"

# 2. Повтор с тем же Idempotency-Key → 202 и тот же payment_id (дубль не создаётся)
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: secret-api-key" -H "Idempotency-Key: order-1001" -H "Content-Type: application/json" \
  -d '{"amount": "1500.50", "currency": "RUB", "description": "Заказ №1001",
       "metadata": {"order_id": 1001}, "webhook_url": "http://webhook-stub:9000/hook"}'

# 3. Тот же ключ с другим телом → 409 Conflict
curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: secret-api-key" -H "Idempotency-Key: order-1001" -H "Content-Type: application/json" \
  -d '{"amount": "1.00", "currency": "RUB", "description": "Другое тело",
       "metadata": {}, "webhook_url": "http://webhook-stub:9000/hook"}'

# 4. Получить платёж (сразу — pending, через 2–5 сек — succeeded/failed)
curl -s http://localhost:8000/api/v1/payments/$PID -H "X-API-Key: secret-api-key"

# 5. Webhook, полученный заглушкой
docker compose logs webhook-stub
```

Retry → DLQ: платёж с `webhook_url` на `/fail` (приёмник всегда отвечает 500) после 3 попыток (через 5 с и 10 с) попадает в `payments.dlq`:

```bash
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: secret-api-key" -H "Idempotency-Key: dlq-demo-1" -H "Content-Type: application/json" \
  -d '{"amount": "10.00", "currency": "USD", "description": "DLQ demo",
       "metadata": {}, "webhook_url": "http://webhook-stub:9000/fail"}'

# через ~25 секунд
docker compose exec rabbitmq rabbitmqctl list_queues name messages
# payments.dlq должна содержать 1 сообщение (заголовок x-last-error — причина)
```

Webhook (`POST` на `webhook_url`, любой 2xx = доставлено):

```json
{"event": "payment.processed", "payment_id": "…", "status": "succeeded",
 "amount": "1500.50", "currency": "RUB", "description": "Заказ №1001",
 "metadata": {"order_id": 1001}, "created_at": "…", "processed_at": "…"}
```

Без/с неверным `X-API-Key` любой эндпоинт отвечает `401`.

## Архитектура

```
POST /payments ─► [ TX: INSERT payments + INSERT outbox ] ─► 202
                              │
                   outbox relay (в процессе api)
                   SELECT … FOR UPDATE SKIP LOCKED → publish (confirms) → published_at
                              ▼
        exchange "payments" (direct) ─► payments.new ─► consumer
                                            ▲              │ ошибка (шлюз/БД/webhook)
                  DLX после TTL             │              ▼
              payments.retry.1 (5s) ◄───────┴── attempt 1 → retry.1
              payments.retry.2 (10s) ◄─────────  attempt 2 → retry.2
                                                 attempt 3 → payments.dlq
```

- **Outbox.** Платёж и событие пишутся одной транзакцией, поэтому сообщение не теряется при падении между БД и брокером. Relay читает пачки с `FOR UPDATE SKIP LOCKED` (можно масштабировать), помечает событие опубликованным только после publisher confirm; частичный индекс `ix_outbox_unpublished` держит выборку дешёвой. Создание платежа «будит» relay, поэтому задержка публикации — миллисекунды, а таймер (`OUTBOX_POLL_INTERVAL`) — только страховка. Семантика доставки — at-least-once.
- **Идемпотентность.** Уникальный индекс на `idempotency_key` + `INSERT … ON CONFLICT DO NOTHING` — корректно и при гонке параллельных запросов. Повтор возвращает исходный платёж, другое тело при том же ключе → 409. Consumer тоже идемпотентен: строка платежа блокируется `FOR UPDATE`, уже финализированный платёж повторно не обрабатывается (повтор лишь заново шлёт webhook).
- **Consumer** (один обработчик): берёт сообщение → эмулирует шлюз (2–5 с, 90% успех) → обновляет статус → отправляет webhook. Webhook уходит после коммита; не-2xx/таймаут считается ошибкой.
- **Retry и DLQ.** 3 попытки, задержки экспоненциальные (`5s · 2^(n-1)`: 5 с, 10 с). Реализовано очередями с TTL и dead-letter-exchange — задержка не блокирует consumer, на каждый уровень своя очередь (нет head-of-line blocking). Номер попытки — в заголовке `x-attempt`, последняя ошибка — в `x-last-error`. После 3-й неудачи сообщение уходит в `payments.dlq`; нечитаемые сообщения (reject без requeue) попадают туда же через DLX основной очереди. Параметры: `MAX_ATTEMPTS`, `RETRY_BASE_DELAY_MS`.
- **Топология** объявляется идемпотентно и api, и consumer'ом (`app/messaging.py`). Если меняете `RETRY_BASE_DELAY_MS`/`MAX_ATTEMPTS`, удалите старые очереди (RabbitMQ не позволяет менять аргументы существующей очереди).

## Структура

```
app/
  main.py        FastAPI, lifespan (broker + outbox relay), /health
  api.py         POST/GET /api/v1/payments
  security.py    проверка X-API-Key (constant-time)
  models.py      Payment, OutboxEvent
  schemas.py     Pydantic v2 схемы
  outbox.py      relay
  messaging.py   exchange/очереди/retry/DLQ
  processing.py  эмуляция шлюза, статус, webhook
  consumer.py    FastStream-приложение
alembic/         миграции
tests/           pytest (нужна PostgreSQL)
```

## Тесты

Тестам нужна PostgreSQL на `localhost:5432` (БД `payments_test`, пользователь/пароль `postgres`/`postgres` — значения по умолчанию в `tests/conftest.py`):

```bash
docker run -d --name pg-test -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=payments_test -p 5432:5432 postgres:16-alpine
pip install -r requirements-dev.txt
pytest
```

Покрыто: авторизация, валидация, идемпотентность (включая 8 параллельных запросов), outbox, обработка платежа, повторная отправка webhook без повторной обработки, маршрутизация retry → DLQ, доставка сообщения из `payments.new` до обработчика (FastStream `TestRabbitBroker`).
