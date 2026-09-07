import asyncio
import hashlib
import json
import time
from dataclasses import dataclass

from .domain import Payment, PaymentAttempt, PaymentStatus, Refund
from .events import EventEnvelope, EventType
from .idempotency import IdempotencyCoordinator, IdempotencyStatus, IdempotencyUnavailable, MemoryIdempotencyCoordinator
from .outbox import OutboxRepository
from .observability import Metrics, log_circuit_transition
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
        provider_concurrency_limit: int = 0,
    ) -> None:
        if provider_concurrency_limit < 0:
            raise ValueError("provider_concurrency_limit must be zero or positive")
        self.providers = providers
        self.store = store
        self.router = ProviderRouter(routing_strategy)
        self.retry_policy = retry_policy or RetryPolicy()
        self.idempotency = idempotency or MemoryIdempotencyCoordinator()
        self.outbox = outbox if outbox is not None else (store if hasattr(store, "append") else None)
        self.metrics = metrics
        self.provider_concurrency_limit = provider_concurrency_limit
        self._provider_semaphores: dict[str, asyncio.Semaphore | None] = {}
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
        for provider in providers:
            self._set_circuit_metric(provider.name)
            self.set_provider_concurrency(provider.name, provider.config.max_concurrency or provider_concurrency_limit)

    def set_provider_concurrency(self, provider_name: str, limit: int) -> None:
        if limit < 0:
            raise ValueError("provider concurrency limit must be zero or positive")
        self._provider_semaphores[provider_name] = asyncio.Semaphore(limit) if limit else None

    async def _allow_provider(self, provider: PaymentProvider) -> bool:
        async with self._circuit_locks[provider.name]:
            circuit = self.circuits[provider.name]
            previous = circuit.state
            allowed = circuit.allow_request()
            current = circuit.state
            self._set_circuit_metric(provider.name)
        if previous is not current:
            await self._record_circuit_transition(provider.name, previous, current)
        return allowed

    async def _record_success(self, provider: PaymentProvider) -> None:
        async with self._circuit_locks[provider.name]:
            circuit = self.circuits[provider.name]
            previous = circuit.state
            circuit.record_success()
            self._set_circuit_metric(provider.name)
            current = circuit.state
        if previous is not current:
            await self._record_circuit_transition(provider.name, previous, current)

    async def _record_failure(self, provider: PaymentProvider) -> None:
        async with self._circuit_locks[provider.name]:
            circuit = self.circuits[provider.name]
            previous = circuit.state
            circuit.record_failure()
            self._set_circuit_metric(provider.name)
            current = circuit.state
        if previous is not current:
            await self._record_circuit_transition(provider.name, previous, current)

    async def _record_circuit_transition(self, provider_name: str, previous, current) -> None:
        log_circuit_transition(provider=provider_name, from_state=previous.value, to_state=current.value)
        if self.metrics is not None:
            self.metrics.provider_circuit_transitions_total.labels(
                provider=provider_name,
                from_state=previous.value,
                to_state=current.value,
            ).inc()
        if self.outbox is not None and current.value in {"OPEN", "CLOSED"}:
            await self.outbox.append(
                [
                    EventEnvelope(
                        EventType.PROVIDER_CIRCUIT_OPENED
                        if current.value == "OPEN"
                        else EventType.PROVIDER_CIRCUIT_CLOSED,
                        "system",
                        None,
                        {"provider": provider_name, "from_state": previous.value, "to_state": current.value},
                        provider=provider_name,
                    )
                ]
            )

    def _set_circuit_metric(self, provider_name: str) -> None:
        if self.metrics is None:
            return
        state = self.circuits[provider_name].state.value
        for circuit_state in ("CLOSED", "OPEN", "HALF_OPEN"):
            self.metrics.provider_circuit_state.labels(provider=provider_name, state=circuit_state).set(circuit_state == state)

    def _set_inflight_metric(self, provider_name: str) -> None:
        if self.metrics is not None:
            stats = self.router.stats[provider_name]
            self.metrics.provider_inflight.labels(provider=provider_name).set(stats.inflight)
            self.metrics.provider_p95_latency_seconds.labels(provider=provider_name).set(stats.p95_latency_ms / 1000)
            self.metrics.provider_last_success_timestamp.labels(provider=provider_name).set(
                stats.last_success_at.timestamp() if stats.last_success_at else 0
            )
            self.metrics.provider_last_failure_timestamp.labels(provider=provider_name).set(
                stats.last_failure_at.timestamp() if stats.last_failure_at else 0
            )

    def _record_provider_failure_metrics(self, provider_name: str, error_code: str, latency_ms: float) -> None:
        if self.metrics is None:
            return
        bounded_code = error_code if error_code in {"PROVIDER_TIMEOUT", "PROVIDER_UNAVAILABLE", "PAYMENT_DECLINED", "HTTP_502", "HTTP_503"} else "other"
        self.metrics.provider_requests_total.labels(provider=provider_name, result="error").inc()
        self.metrics.provider_failures_total.labels(provider=provider_name, error_code=bounded_code).inc()
        self.metrics.provider_latency_seconds.labels(provider=provider_name).observe(latency_ms / 1000)
        if error_code == "PROVIDER_TIMEOUT":
            self.metrics.provider_timeouts_total.labels(provider=provider_name).inc()

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
            self._set_inflight_metric(provider.name)
            try:
                semaphore = self._provider_semaphores[provider.name]
                if semaphore is None:
                    result = await provider.create_payment(
                        payment_id=str(payment.id), amount=data.amount, currency=data.currency, token=data.token
                    )
                else:
                    async with semaphore:
                        result = await provider.create_payment(
                            payment_id=str(payment.id), amount=data.amount, currency=data.currency, token=data.token
                        )
            except ProviderError as error:
                latency_ms = (time.perf_counter() - started) * 1000
                self.router.finish_request(provider, success=False, latency_ms=latency_ms, error_code=error.code)
                self._set_inflight_metric(provider.name)
                await self._record_failure(provider)
                self._record_provider_failure_metrics(provider.name, error.code, latency_ms)
                if self.metrics is not None and is_retryable(error):
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
                latency_ms = (time.perf_counter() - started) * 1000
                self.router.finish_request(
                    provider,
                    success=False,
                    latency_ms=latency_ms,
                    error_code="INTERNAL_ERROR",
                )
                self._set_inflight_metric(provider.name)
                self._record_provider_failure_metrics(provider.name, "INTERNAL_ERROR", latency_ms)
                raise
            await self._record_success(provider)
            latency_ms = (time.perf_counter() - started) * 1000
            self.router.finish_request(provider, success=True, latency_ms=latency_ms)
            self._set_inflight_metric(provider.name)
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
