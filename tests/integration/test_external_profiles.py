"""Opt-in external profile checks; never contact services in the default suite."""

import asyncio
import os
from uuid import uuid4

import pytest


if os.getenv("PYSWITCH_RUN_INTEGRATION") != "1":
    pytest.skip("set PYSWITCH_RUN_INTEGRATION=1 to run external profile checks", allow_module_level=True)


TIMEOUT_SECONDS = 5


@pytest.mark.asyncio
async def test_postgres_repository_save_and_outbox_transaction():
    pytest.importorskip("sqlalchemy")
    pytest.importorskip("asyncpg")
    from sqlalchemy import text
    from sqlalchemy.exc import OperationalError, ProgrammingError
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from pyswitch.db.repository import SqlAlchemyPaymentRepository
    from pyswitch.domain import Payment, PaymentStatus
    from pyswitch.events import EventEnvelope, EventType

    database_url = os.getenv(
        "PYSWITCH_DATABASE_URL",
        "postgresql+asyncpg://pyswitch:pyswitch@localhost:5432/pyswitch",
    )
    engine = create_async_engine(
        database_url,
        connect_args={"timeout": TIMEOUT_SECONDS},
    )
    try:
        try:
            async with asyncio.timeout(TIMEOUT_SECONDS):
                async with engine.connect() as connection:
                    await connection.execute(text("SELECT 1"))
        except (OperationalError, OSError, TimeoutError) as exc:
            pytest.skip(f"PostgreSQL unavailable: {type(exc).__name__}")

        repository = SqlAlchemyPaymentRepository(async_sessionmaker(engine, expire_on_commit=False))
        payment = Payment(
            merchant_id=f"integration-{uuid4().hex[:12]}",
            amount=100,
            currency="INR",
            payment_method_type="card",
            idempotency_key=uuid4().hex,
            status=PaymentStatus.SUCCEEDED,
            request_fingerprint="f" * 64,
        )
        event = EventEnvelope(
            EventType.PAYMENT_SUCCEEDED,
            payment.merchant_id,
            payment.id,
            {"payment_id": str(payment.id), "status": payment.status.value},
        )
        try:
            await asyncio.wait_for(repository.save_with_events(payment, [event]), TIMEOUT_SECONDS)
        except ProgrammingError as exc:
            pytest.skip(f"PostgreSQL schema unavailable; run alembic upgrade head: {type(exc).__name__}")
        saved = await asyncio.wait_for(repository.get(payment.id), TIMEOUT_SECONDS)
        assert saved is not None
        assert saved.id == payment.id
        pending = await asyncio.wait_for(repository.unsent(), TIMEOUT_SECONDS)
        assert [item.id for item in pending] == [event.id]
        await asyncio.wait_for(repository.mark_published(event.id), TIMEOUT_SECONDS)
        assert await asyncio.wait_for(repository.unsent(), TIMEOUT_SECONDS) == []
    finally:
        await asyncio.wait_for(engine.dispose(), TIMEOUT_SECONDS)


@pytest.mark.asyncio
async def test_redis_idempotency_and_rate_limit_are_atomic():
    redis = pytest.importorskip("redis.asyncio")
    from redis.exceptions import RedisError

    from pyswitch.idempotency import IdempotencyStatus, RedisIdempotencyCoordinator
    from pyswitch.rate_limit import RedisTokenBucketLimiter

    client = redis.from_url(os.getenv("PYSWITCH_REDIS_URL", "redis://localhost:6379/0"))
    try:
        try:
            await asyncio.wait_for(client.ping(), TIMEOUT_SECONDS)
        except (RedisError, OSError, TimeoutError) as exc:
            pytest.skip(f"Redis unavailable: {type(exc).__name__}")

        merchant = f"integration-{uuid4().hex}"
        key = f"key-{uuid4().hex}"
        coordinator = RedisIdempotencyCoordinator(client, processing_ttl_seconds=30, poll_seconds=0.01)
        claimed = await asyncio.wait_for(coordinator.acquire(merchant, key, "fingerprint-a"), TIMEOUT_SECONDS)
        assert claimed.status is IdempotencyStatus.CLAIMED
        conflict = await asyncio.wait_for(coordinator.acquire(merchant, key, "fingerprint-b"), TIMEOUT_SECONDS)
        assert conflict.status is IdempotencyStatus.CONFLICT
        payment_id = uuid4()
        await asyncio.wait_for(coordinator.complete(merchant, key, "fingerprint-a", payment_id), TIMEOUT_SECONDS)
        completed = await asyncio.wait_for(coordinator.acquire(merchant, key, "fingerprint-a"), TIMEOUT_SECONDS)
        assert completed.status is IdempotencyStatus.COMPLETED
        assert completed.payment_id == payment_id

        limiter = RedisTokenBucketLimiter(client, capacity=1, refill_per_second=0.01)
        first = await asyncio.wait_for(limiter.consume(merchant), TIMEOUT_SECONDS)
        second = await asyncio.wait_for(limiter.consume(merchant), TIMEOUT_SECONDS)
        assert first.allowed is True
        assert second.allowed is False
        assert second.retry_after_seconds >= 1
    finally:
        await asyncio.wait_for(client.aclose(), TIMEOUT_SECONDS)


@pytest.mark.asyncio
async def test_kafka_publish_and_outbox_worker_mark_event_published():
    pytest.importorskip("aiokafka")
    from aiokafka.errors import KafkaError

    from pyswitch.broker import KafkaBroker
    from pyswitch.domain import Payment
    from pyswitch.events import EventEnvelope, EventType
    from pyswitch.outbox import OutboxDispatcher
    from pyswitch.store import InMemoryPaymentStore

    broker = KafkaBroker(
        os.getenv("PYSWITCH_KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        os.getenv("PYSWITCH_KAFKA_TOPIC", f"pyswitch.integration.{uuid4().hex}"),
    )
    started = False
    try:
        try:
            await asyncio.wait_for(broker.start(), TIMEOUT_SECONDS)
            started = True
        except (KafkaError, OSError, TimeoutError) as exc:
            try:
                await asyncio.wait_for(broker.stop(), TIMEOUT_SECONDS)
            except Exception:
                pass
            pytest.skip(f"Kafka/Redpanda unavailable: {type(exc).__name__}")

        store = InMemoryPaymentStore()
        payment = Payment("integration", 100, "INR", "card", uuid4().hex)
        event = EventEnvelope(EventType.PAYMENT_CREATED, payment.merchant_id, payment.id, {"payment_id": str(payment.id)})
        await store.append([event])
        dispatcher = OutboxDispatcher(
            store,
            broker,
            poll_interval_seconds=0.01,
            max_attempts=2,
            initial_backoff_seconds=0,
            max_backoff_seconds=0,
        )
        assert await asyncio.wait_for(dispatcher.dispatch_once(), TIMEOUT_SECONDS) == 1
        assert await store.unsent() == []
    finally:
        if started:
            await asyncio.wait_for(broker.stop(), TIMEOUT_SECONDS)
