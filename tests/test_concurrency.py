import asyncio

import pytest

from pyswitch.config import Settings
from pyswitch.providers.mock import MockProvider
from pyswitch.providers.base import ProviderPaymentResult
from pyswitch.reliability import RetryPolicy
from pyswitch.service import PaymentInput, PaymentService
from pyswitch.store import InMemoryPaymentStore


class BlockingProvider(MockProvider):
    def __init__(self):
        super().__init__("blocking")
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.active = 0
        self.max_active = 0

    async def create_payment(self, *, payment_id: str, amount: int, currency: str, token: str):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.started.set()
        await self.release.wait()
        self.active -= 1
        return ProviderPaymentResult(reference=f"blocking-{payment_id}")


@pytest.mark.asyncio
async def test_provider_concurrency_limit_acquires_and_releases_semaphore():
    provider = BlockingProvider()
    service = PaymentService(
        [provider],
        InMemoryPaymentStore(),
        provider_concurrency_limit=1,
        retry_policy=RetryPolicy(max_attempts=1),
    )
    first = asyncio.create_task(service.create(PaymentInput("merchant", 100, "INR", "card", "test_card", "one")))
    await provider.started.wait()
    second = asyncio.create_task(service.create(PaymentInput("merchant", 100, "INR", "card", "test_card", "two")))
    await asyncio.sleep(0)
    assert provider.max_active == 1
    assert not second.done()
    provider.release.set()
    await asyncio.gather(first, second)
    assert provider.active == 0
    assert provider.max_active == 1


def test_provider_concurrency_limit_is_validated_from_environment(monkeypatch):
    monkeypatch.setenv("PYSWITCH_PROVIDER_CONCURRENCY_LIMIT", "-1")
    with pytest.raises(ValueError, match="PYSWITCH_PROVIDER_CONCURRENCY_LIMIT"):
        Settings.from_env()
