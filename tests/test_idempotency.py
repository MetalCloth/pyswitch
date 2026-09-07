import asyncio
from uuid import uuid4

import pytest

from pyswitch.idempotency import IdempotencyStatus, MemoryIdempotencyCoordinator
from pyswitch.providers.mock import MockStripeProvider
from pyswitch.service import PaymentInput, PaymentService
from pyswitch.store import InMemoryPaymentStore


@pytest.mark.asyncio
async def test_memory_coordinator_claims_once_waits_and_replays_result():
    coordinator = MemoryIdempotencyCoordinator()
    fingerprint = "fingerprint"
    first = await coordinator.acquire("merchant", "key", fingerprint)
    assert first.status is IdempotencyStatus.CLAIMED

    waiting = asyncio.create_task(coordinator.acquire("merchant", "key", fingerprint))
    await asyncio.sleep(0)
    assert not waiting.done()

    payment_id = uuid4()
    await coordinator.complete("merchant", "key", fingerprint, payment_id)
    second = await waiting
    assert second.status is IdempotencyStatus.COMPLETED
    assert second.payment_id == payment_id


@pytest.mark.asyncio
async def test_memory_coordinator_rejects_fingerprint_conflict():
    coordinator = MemoryIdempotencyCoordinator()
    await coordinator.acquire("merchant", "key", "first")
    conflict = await coordinator.acquire("merchant", "key", "different")
    assert conflict.status is IdempotencyStatus.CONFLICT


@pytest.mark.asyncio
async def test_service_concurrent_same_key_calls_provider_once():
    provider = MockStripeProvider()
    calls = 0
    original = provider.create_payment

    async def counted_create(**kwargs):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return await original(**kwargs)

    provider.create_payment = counted_create
    service = PaymentService([provider], InMemoryPaymentStore())
    data = PaymentInput("merchant", 100, "INR", "card", "test_card", "storm-key")
    payments = await asyncio.gather(*(service.create(data) for _ in range(25)))
    assert calls == 1
    assert {payment.id for payment in payments} == {payments[0].id}
