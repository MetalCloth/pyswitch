import asyncio
import hashlib
import json
import time
from dataclasses import dataclass

from .domain import Payment, PaymentAttempt, PaymentStatus, Refund
from .events import EventEnvelope, EventType
from .idempotency import IdempotencyCoordinator, IdempotencyStatus, IdempotencyUnavailable, MemoryIdempotencyCoordinator
from .outbox import OutboxRepository
from .observability import Metrics
from .providers.base import PaymentProvider, ProviderError
from .reliability import CircuitBreaker, RetryPolicy, is_retryable, run_with_retry
from .routing import ProviderRouter, RoutingStrategy
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
        idempotency: IdempotencyCoordinator | None = None,
        outbox: OutboxRepository | None = None,
        metrics: Metrics | None = None,
        routing_strategy: RoutingStrategy = "round_robin",
    ) -> None:
        self.providers = providers
        self.store = store
        self.router = ProviderRouter(routing_strategy)
        self.retry_policy = retry_policy or RetryPolicy()
        self.idempotency = idempotency or MemoryIdempotencyCoordinator()
        self.outbox = outbox if outbox is not None else (store if hasattr(store, "append") else None)
        self.metrics = metrics
        self.circuits = {
            provider.name: CircuitBreaker(
                failure_threshold=circuit_failure_threshold,
                cooldown_seconds=circuit_cooldown_seconds,
            )
            for provider in providers
        }
        self._circuit_locks = {provider.name: asyncio.Lock() for provider in providers}
        self._refund_locks: dict[str, asyncio.Lock] = {}
        self._lock = asyncio.Lock()

    async def _allow_provider(self, provider: PaymentProvider) -> bool:
        async with self._circuit_locks[provider.name]:
            return self.circuits[provider.name].allow_request()

    async def _record_success(self, provider: PaymentProvider) -> None:
        async with self._circuit_locks[provider.name]:
            self.circuits[provider.name].record_success()
            self._set_circuit_metric(provider.name)

    async def _record_failure(self, provider: PaymentProvider) -> None:
        async with self._circuit_locks[provider.name]:
            self.circuits[provider.name].record_failure()
            self._set_circuit_metric(provider.name)

    def _set_circuit_metric(self, provider_name: str) -> None:
        if self.metrics is None:
            return
        state = self.circuits[provider_name].state.value
        for circuit_state in ("CLOSED", "OPEN", "HALF_OPEN"):
            self.metrics.provider_circuit_state.labels(provider=provider_name, state=circuit_state).set(circuit_state == state)

    def _event(self, payment: Payment, event_type: EventType, status: PaymentStatus | None = None) -> EventEnvelope:
        return EventEnvelope(
            event_type,
            payment.merchant_id,
            payment.id,
            {"payment_id": str(payment.id), "status": (status or payment.status).value},
            provider=payment.provider,
        )

    async def _save(self, payment: Payment, events: list[EventEnvelope] | None = None) -> None:
        events = events or []
        saver = getattr(self.store, "save_with_events", None)
        if events and self.outbox is self.store and saver is not None:
            await saver(payment, events)
            return
        await self.store.save(payment)
        if events and self.outbox is not None:
            await self.outbox.append(events)

    async def _attempt_provider(self, payment: Payment, provider: PaymentProvider, data: PaymentInput) -> object:
        attempts: list[PaymentAttempt] = []

        async def operation():
            attempt_number = len(attempts) + 1
            if not await self._allow_provider(provider):
                raise ProviderError("CIRCUIT_OPEN", "Provider circuit is open")
            started = time.perf_counter()
            self.router.start_request(provider)
            try:
                result = await provider.create_payment(
                    payment_id=str(payment.id), amount=data.amount, currency=data.currency, token=data.token
                )
            except ProviderError as error:
                latency_ms = (time.perf_counter() - started) * 1000
                self.router.finish_request(provider, success=False, latency_ms=latency_ms)
                await self._record_failure(provider)
                if self.metrics is not None:
                    error_code = error.code if error.code in {"PROVIDER_TIMEOUT", "PROVIDER_UNAVAILABLE", "PAYMENT_DECLINED", "HTTP_502", "HTTP_503"} else "other"
                    self.metrics.provider_requests_total.labels(provider=provider.name, result="error").inc()
                    self.metrics.provider_failures_total.labels(provider=provider.name, error_code=error_code).inc()
                    self.metrics.provider_latency_seconds.labels(provider=provider.name).observe(latency_ms / 1000)
                    if is_retryable(error):
                        self.metrics.retries_total.labels(provider=provider.name).inc()
                attempts.append(
                    PaymentAttempt(
                        provider.name,
                        attempt_number,
                        "RETRY" if is_retryable(error) else "FAILED",
                        latency_ms,
                        error_code=error.code,
                    )
                )
                raise
            except Exception:
                self.router.finish_request(
                    provider,
                    success=False,
                    latency_ms=(time.perf_counter() - started) * 1000,
                )
                raise
            await self._record_success(provider)
            latency_ms = (time.perf_counter() - started) * 1000
            self.router.finish_request(provider, success=True, latency_ms=latency_ms)
            if self.metrics is not None:
                self.metrics.provider_requests_total.labels(provider=provider.name, result="success").inc()
                self.metrics.provider_latency_seconds.labels(provider=provider.name).observe(latency_ms / 1000)
            attempts.append(
                PaymentAttempt(provider.name, attempt_number, "SUCCEEDED", latency_ms)
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
        decision = await self.idempotency.acquire(data.merchant_id, data.idempotency_key, fingerprint)
        if decision.status is IdempotencyStatus.CONFLICT:
            raise IdempotencyConflict("Idempotency-Key was reused with different payment data")
        if decision.status is IdempotencyStatus.COMPLETED:
            if self.metrics is not None:
                self.metrics.idempotency_hits_total.inc()
            existing = await self.store.get(decision.payment_id) if decision.payment_id else None
            if existing is None:
                raise IdempotencyUnavailable("Idempotency result is missing its authoritative payment")
            return existing
        claimed = True
        try:
            existing = await self.store.get_by_idempotency(data.merchant_id, data.idempotency_key)
            if existing:
                if existing.request_fingerprint != fingerprint:
                    raise IdempotencyConflict("Idempotency-Key was reused with different payment data")
                await self.idempotency.complete(data.merchant_id, data.idempotency_key, fingerprint, existing.id)
                claimed = False
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
                    await self._save(
                        payment,
                        [
                            self._event(payment, EventType.PAYMENT_CREATED, PaymentStatus.PROCESSING),
                            self._event(payment, EventType.PAYMENT_PROCESSING, PaymentStatus.PROCESSING),
                            self._event(payment, EventType.PAYMENT_SUCCEEDED),
                        ],
                    )
                    await self.idempotency.complete(data.merchant_id, data.idempotency_key, fingerprint, payment.id)
                    if self.metrics is not None:
                        self.metrics.payments_total.labels(status=payment.status.value).inc()
                    claimed = False
                    return payment
                except ProviderError as exc:
                    last_error = exc
                    if exc.code not in {"PROVIDER_UNAVAILABLE", "HTTP_502", "HTTP_503", "CIRCUIT_OPEN"}:
                        break
            payment.status = PaymentStatus.FAILED
            await self._save(
                payment,
                [
                    self._event(payment, EventType.PAYMENT_CREATED, PaymentStatus.PROCESSING),
                    self._event(payment, EventType.PAYMENT_PROCESSING, PaymentStatus.PROCESSING),
                    self._event(payment, EventType.PAYMENT_FAILED),
                ],
            )
            await self.idempotency.complete(data.merchant_id, data.idempotency_key, fingerprint, payment.id)
            if self.metrics is not None:
                self.metrics.payments_total.labels(status=payment.status.value).inc()
            claimed = False
            raise last_error or ProviderError("PROVIDER_UNAVAILABLE", "No provider completed the payment")
        except BaseException:
            if claimed:
                try:
                    await self.idempotency.abort(data.merchant_id, data.idempotency_key, fingerprint)
                except Exception:
                    pass
            raise

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
            await self._save(payment, [self._event(payment, EventType.PAYMENT_REFUNDED)])
            return refund
