import asyncio

import pytest

from pyswitch.config import Settings
from pyswitch.events import EventEnvelope, EventType
from pyswitch.outbox import InMemoryBroker, OutboxDispatcher
from pyswitch.runtime import build_runtime
from pyswitch.store import InMemoryPaymentStore


def event() -> EventEnvelope:
    return EventEnvelope(EventType.PAYMENT_CREATED, "merchant", None, {"status": "PROCESSING"})


@pytest.mark.asyncio
async def test_worker_retries_outage_then_publishes_after_recovery_without_leaking_task():
    store = InMemoryPaymentStore()
    broker = InMemoryBroker()
    await store.append([event()])
    broker.unavailable = True
    worker = OutboxDispatcher(
        store,
        broker,
        poll_interval_seconds=0.01,
        max_attempts=2,
        initial_backoff_seconds=0,
        max_backoff_seconds=0,
    )
    await worker.start()
    await asyncio.sleep(0.03)
    assert broker.events == []
    assert len(await store.unsent()) == 1
    broker.unavailable = False
    for _ in range(20):
        if not await store.unsent():
            break
        await asyncio.sleep(0.01)
    await worker.stop()
    assert len(broker.events) == 1
    assert await store.unsent() == []
    assert worker._task is None


@pytest.mark.asyncio
async def test_runtime_stops_its_outbox_worker():
    runtime = build_runtime(Settings())
    await runtime.start()
    assert runtime.dispatcher._task is not None
    await runtime.close()
    assert runtime.dispatcher._task is None
