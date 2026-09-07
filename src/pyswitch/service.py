import asyncio
import time
from dataclasses import dataclass

from .domain import Payment, PaymentAttempt, PaymentStatus
from .providers.base import PaymentProvider, ProviderError
from .reliability import CircuitBreaker, RetryPolicy, is_retryable, run_with_retry
from .routing import RoundRobinRouter
from .repositories import PaymentRepository


@dataclass(frozen=True, slots=True)
class PaymentInput:
    merchant_id: str
    amount: int
    currency: str
    payment_method_type: str
    token: str
    idempotency_key: str


class PaymentService:
    def __init__(
        self,
        providers: list[PaymentProvider],
        store: PaymentRepository,
        *,
        retry_policy: RetryPolicy | None = None,
        circuit_failure_threshold: int = 3,
        circuit_cooldown_seconds: float = 30.0,
    ) -> None:
        self.providers = providers
        self.store = store
        self.router = RoundRobinRouter()
        self.retry_policy = retry_policy or RetryPolicy()
        self.circuits = {
            provider.name: CircuitBreaker(
                failure_threshold=circuit_failure_threshold,
                cooldown_seconds=circuit_cooldown_seconds,
            )
            for provider in providers
        }
        self._circuit_locks = {provider.name: asyncio.Lock() for provider in providers}
        self._idempotency_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._lock = asyncio.Lock()

    async def _allow_provider(self, provider: PaymentProvider) -> bool:
        async with self._circuit_locks[provider.name]:
            return self.circuits[provider.name].allow_request()

    async def _record_success(self, provider: PaymentProvider) -> None:
        async with self._circuit_locks[provider.name]:
            self.circuits[provider.name].record_success()

    async def _record_failure(self, provider: PaymentProvider) -> None:
        async with self._circuit_locks[provider.name]:
            self.circuits[provider.name].record_failure()

    async def _attempt_provider(self, payment: Payment, provider: PaymentProvider, data: PaymentInput) -> object:
        attempts: list[PaymentAttempt] = []

        async def operation():
            attempt_number = len(attempts) + 1
            if not await self._allow_provider(provider):
                raise ProviderError("CIRCUIT_OPEN", "Provider circuit is open")
            started = time.perf_counter()
            try:
                result = await provider.create_payment(
                    payment_id=str(payment.id), amount=data.amount, currency=data.currency, token=data.token
                )
            except ProviderError as error:
                await self._record_failure(provider)
                attempts.append(
                    PaymentAttempt(
                        provider.name,
                        attempt_number,
                        "RETRY" if is_retryable(error) else "FAILED",
                        (time.perf_counter() - started) * 1000,
                        error_code=error.code,
                    )
                )
                raise
            await self._record_success(provider)
            attempts.append(
                PaymentAttempt(provider.name, attempt_number, "SUCCEEDED", (time.perf_counter() - started) * 1000)
            )
            return result

        try:
            result, _ = await run_with_retry(operation, self.retry_policy)
        except ProviderError:
            if attempts:
                attempts[-1].result = "FAILED"
            payment.attempts.extend(attempts)
            raise
        payment.attempts.extend(attempts)
        return result

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
            payment = Payment(data.merchant_id, data.amount, data.currency, data.payment_method_type, data.idempotency_key)
            healthy = [p for p in self.providers if await p.health_check()]
            provider = await self.router.choose(healthy)
            ordered = [provider, *[p for p in healthy if p is not provider]]
            last_error: ProviderError | None = None
            for candidate in ordered:
                try:
                    result = await self._attempt_provider(payment, candidate, data)
                    payment.status = PaymentStatus.SUCCEEDED
                    payment.provider_reference = result.reference
                    await self.store.save(payment)
                    return payment
                except ProviderError as exc:
                    last_error = exc
                    if exc.code not in {"PROVIDER_UNAVAILABLE", "HTTP_502", "HTTP_503", "CIRCUIT_OPEN"}:
                        break
            payment.status = PaymentStatus.FAILED
            await self.store.save(payment)
            raise last_error or ProviderError("PROVIDER_UNAVAILABLE", "No provider completed the payment")
