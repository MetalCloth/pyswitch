import asyncio
import hashlib
import json
import time
from dataclasses import dataclass

from .domain import Payment, PaymentAttempt, PaymentStatus, Refund
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


class IdempotencyConflict(Exception):
    code = "DUPLICATE_REQUEST"


class RefundError(Exception):
    def __init__(self, message: str, code: str = "VALIDATION_ERROR") -> None:
        super().__init__(message)
        self.code = code


def request_fingerprint(data: PaymentInput) -> str:
    payload = {
        "merchant_id": data.merchant_id,
        "amount": data.amount,
        "currency": data.currency,
        "payment_method_type": data.payment_method_type,
        "token": data.token,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


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
        self._refund_locks: dict[str, asyncio.Lock] = {}
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
        fingerprint = request_fingerprint(data)
        existing = await self.store.get_by_idempotency(data.merchant_id, data.idempotency_key)
        if existing:
            if existing.request_fingerprint != fingerprint:
                raise IdempotencyConflict("Idempotency-Key was reused with different payment data")
            return existing
        # A per-key lock ensures duplicate requests cannot execute two provider calls.
        key = (data.merchant_id, data.idempotency_key)
        async with self._lock:
            lock = self._idempotency_locks.setdefault(key, asyncio.Lock())
        async with lock:
            existing = await self.store.get_by_idempotency(*key)
            if existing:
                if existing.request_fingerprint != fingerprint:
                    raise IdempotencyConflict("Idempotency-Key was reused with different payment data")
                return existing
            payment = Payment(
                data.merchant_id,
                data.amount,
                data.currency,
                data.payment_method_type,
                data.idempotency_key,
                request_fingerprint=fingerprint,
            )
            healthy = [p for p in self.providers if await p.health_check()]
            provider = await self.router.choose(healthy)
            ordered = [provider, *[p for p in healthy if p is not provider]]
            last_error: ProviderError | None = None
            for candidate in ordered:
                try:
                    result = await self._attempt_provider(payment, candidate, data)
                    payment.status = PaymentStatus.SUCCEEDED
                    payment.provider_reference = result.reference
                    payment.provider = candidate.name
                    await self.store.save(payment)
                    return payment
                except ProviderError as exc:
                    last_error = exc
                    if exc.code not in {"PROVIDER_UNAVAILABLE", "HTTP_502", "HTTP_503", "CIRCUIT_OPEN"}:
                        break
            payment.status = PaymentStatus.FAILED
            await self.store.save(payment)
            raise last_error or ProviderError("PROVIDER_UNAVAILABLE", "No provider completed the payment")

    async def refund(self, payment_id, amount: int | None = None) -> Refund:
        key = str(payment_id)
        async with self._lock:
            lock = self._refund_locks.setdefault(key, asyncio.Lock())
        async with lock:
            payment = await self.store.get(payment_id)
            if payment is None:
                raise RefundError("Payment not found", "VALIDATION_ERROR")
            if payment.status not in {PaymentStatus.SUCCEEDED, PaymentStatus.REFUNDED}:
                raise RefundError("Only a successful payment can be refunded")
            remaining = payment.amount - payment.refunded_amount
            refund_amount = remaining if amount is None else amount
            if refund_amount <= 0 or refund_amount > remaining:
                raise RefundError("Refund amount exceeds the remaining refundable amount")
            provider = next((p for p in self.providers if p.name == payment.provider), None)
            if provider is None:
                raise RefundError("Original payment provider is unavailable", "PROVIDER_UNAVAILABLE")
            result = await provider.refund_payment(payment_id=str(payment.id), amount=refund_amount)
            refund = Refund(payment.id, refund_amount, result.reference)
            payment.refunded_amount += refund_amount
            payment.refunds.append(refund)
            if payment.refunded_amount == payment.amount:
                payment.status = PaymentStatus.REFUNDED
            await self.store.save(payment)
            return refund
