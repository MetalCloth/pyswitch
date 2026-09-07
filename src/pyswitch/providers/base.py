from dataclasses import dataclass
from typing import Protocol


class ProviderError(Exception):
    """An expected provider-side failure."""

    def __init__(self, code: str, message: str = "Provider request failed") -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class ProviderConfig:
    success_rate: float = 1.0
    min_latency_ms: int = 0
    max_latency_ms: int = 0
    timeout_probability: float = 0.0
    server_error_probability: float = 0.0
    decline_probability: float = 0.0
    forced_failure: bool = False


@dataclass(frozen=True, slots=True)
class ProviderPaymentResult:
    reference: str
    status: str = "SUCCEEDED"


class PaymentProvider(Protocol):
    name: str
    config: ProviderConfig

    async def create_payment(self, *, payment_id: str, amount: int, currency: str, token: str) -> ProviderPaymentResult: ...
    async def refund_payment(self, *, payment_id: str, amount: int) -> ProviderPaymentResult: ...
    async def get_payment_status(self, *, provider_reference: str) -> str: ...
    async def health_check(self) -> bool: ...

