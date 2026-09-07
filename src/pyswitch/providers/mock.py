import asyncio
import random
from dataclasses import replace
from uuid import uuid4

from .base import PaymentProvider, ProviderConfig, ProviderError, ProviderPaymentResult


class MockProvider(PaymentProvider):
    def __init__(self, name: str, config: ProviderConfig | None = None, *, seed: int = 0) -> None:
        self.name = name
        self.config = config or ProviderConfig()
        self._random = random.Random(seed)

    def configure(self, **changes: object) -> None:
        allowed = set(ProviderConfig.__dataclass_fields__)
        unknown = set(changes) - allowed
        if unknown:
            raise ValueError(f"Unknown provider setting(s): {', '.join(sorted(unknown))}")
        self.config = replace(self.config, **changes)

    async def create_payment(self, *, payment_id: str, amount: int, currency: str, token: str) -> ProviderPaymentResult:
        config = self.config
        if config.min_latency_ms or config.max_latency_ms:
            delay = self._random.uniform(config.min_latency_ms, max(config.min_latency_ms, config.max_latency_ms)) / 1000
            await asyncio.sleep(delay)
        if config.forced_failure:
            raise ProviderError("PROVIDER_UNAVAILABLE", "Provider is forced to fail")
        roll = self._random.random()
        if roll < config.timeout_probability:
            raise ProviderError("PROVIDER_TIMEOUT", "Provider request timed out")
        if roll < config.timeout_probability + config.server_error_probability:
            raise ProviderError("PROVIDER_UNAVAILABLE", "Provider returned a server error")
        if roll < config.timeout_probability + config.server_error_probability + config.decline_probability:
            raise ProviderError("PAYMENT_DECLINED", "Payment was declined")
        if roll >= config.timeout_probability + config.server_error_probability + config.decline_probability + config.success_rate:
            raise ProviderError("PAYMENT_DECLINED", "Payment was declined")
        return ProviderPaymentResult(reference=f"{self.name}_{uuid4().hex}")

    async def refund_payment(self, *, payment_id: str, amount: int) -> ProviderPaymentResult:
        return ProviderPaymentResult(reference=f"refund_{self.name}_{uuid4().hex}")

    async def get_payment_status(self, *, provider_reference: str) -> str:
        return "SUCCEEDED"

    async def health_check(self) -> bool:
        return not self.config.forced_failure


class MockStripeProvider(MockProvider):
    def __init__(self, config: ProviderConfig | None = None) -> None:
        super().__init__("mockstripe", config, seed=1)


class MockAdyenProvider(MockProvider):
    def __init__(self, config: ProviderConfig | None = None) -> None:
        super().__init__("mockadyen", config, seed=2)


class MockRazorpayProvider(MockProvider):
    def __init__(self, config: ProviderConfig | None = None) -> None:
        super().__init__("mockrazorpay", config, seed=3)

