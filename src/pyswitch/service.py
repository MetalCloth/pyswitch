import asyncio
import time
from dataclasses import dataclass

from .domain import Payment, PaymentAttempt, PaymentStatus
from .providers.base import PaymentProvider, ProviderError
from .routing import RoundRobinRouter
from .store import InMemoryPaymentStore


@dataclass(frozen=True, slots=True)
class PaymentInput:
    merchant_id: str
    amount: int
    currency: str
    payment_method_type: str
    token: str
    idempotency_key: str


class PaymentService:
    def __init__(self, providers: list[PaymentProvider], store: InMemoryPaymentStore) -> None:
        self.providers = providers
        self.store = store
        self.router = RoundRobinRouter()
        self._idempotency_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._lock = asyncio.Lock()

    async def create(self, data: PaymentInput) -> Payment:
        existing = await self.store.get_by_idempotency(data.merchant_id, data.idempotency_key)
        if existing:
            return existing
        # A per-key lock ensures duplicate requests cannot execute two provider calls.
        key = (data.merchant_id, data.idempotency_key)
        async with self._lock:
            lock = self._idempotency_locks.setdefault(key, asyncio.Lock())
        async with lock:
            existing = await self.store.get_by_idempotency(*key)
            if existing:
                return existing
            healthy = [p for p in self.providers if await p.health_check()]
            provider = await self.router.choose(healthy)
            payment = Payment(data.merchant_id, data.amount, data.currency, data.payment_method_type, data.idempotency_key)
            started = time.perf_counter()
            try:
                result = await provider.create_payment(payment_id=str(payment.id), amount=data.amount, currency=data.currency, token=data.token)
            except ProviderError as exc:
                payment.status = PaymentStatus.FAILED
                payment.attempts.append(PaymentAttempt(provider.name, 1, "FAILED", (time.perf_counter() - started) * 1000, error_code=exc.code))
                await self.store.save(payment)
                raise
            payment.status = PaymentStatus.SUCCEEDED
            payment.provider_reference = result.reference
            payment.attempts.append(PaymentAttempt(provider.name, 1, "SUCCEEDED", (time.perf_counter() - started) * 1000))
            await self.store.save(payment)
            return payment
