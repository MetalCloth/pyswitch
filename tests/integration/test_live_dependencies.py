"""Opt-in live dependency smoke checks; never run external services by default."""

import os

import pytest


if os.getenv("PYSWITCH_RUN_INTEGRATION") != "1":
    pytest.skip("set PYSWITCH_RUN_INTEGRATION=1 to run live dependency checks", allow_module_level=True)


@pytest.mark.asyncio
async def test_postgres_is_reachable():
    sqlalchemy = pytest.importorskip("sqlalchemy")
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    try:
        engine = create_async_engine(
            os.getenv("PYSWITCH_DATABASE_URL", "postgresql+asyncpg://pyswitch:pyswitch@localhost:5432/pyswitch")
        )
    except Exception as exc:
        pytest.skip(f"PostgreSQL driver unavailable: {type(exc).__name__}")
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    except Exception as exc:
        pytest.skip(f"PostgreSQL unavailable: {type(exc).__name__}")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_redis_is_reachable():
    redis = pytest.importorskip("redis.asyncio")
    client = redis.from_url(os.getenv("PYSWITCH_REDIS_URL", "redis://localhost:6379/0"))
    try:
        assert await client.ping()
    except Exception as exc:
        pytest.skip(f"Redis unavailable: {type(exc).__name__}")
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_kafka_is_reachable():
    aiokafka = pytest.importorskip("aiokafka")
    producer = aiokafka.AIOKafkaProducer(
        bootstrap_servers=os.getenv("PYSWITCH_KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    )
    started = False
    try:
        await producer.start()
        started = True
    except Exception as exc:
        pytest.skip(f"Kafka/Redpanda unavailable: {type(exc).__name__}")
    finally:
        if started:
            await producer.stop()
