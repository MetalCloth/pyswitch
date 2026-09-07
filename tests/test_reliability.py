import pytest

from pyswitch.providers.base import ProviderError
from pyswitch.providers.mock import MockAdyenProvider, MockStripeProvider
from pyswitch.service import PaymentInput, PaymentService
from pyswitch.store import InMemoryPaymentStore
from pyswitch.reliability import CircuitBreaker, CircuitState, RetryPolicy, is_retryable, run_with_retry


def test_retry_classification_and_exponential_jitter_bounds():
    policy = RetryPolicy(initial_delay_seconds=0.1, max_delay_seconds=0.4, jitter_ratio=0.2)
    assert is_retryable(ProviderError("PROVIDER_TIMEOUT"))
    assert not is_retryable(ProviderError("PAYMENT_DECLINED"))
    assert policy.delay_for(0, random_value=0) == pytest.approx(0.08)
    assert policy.delay_for(1, random_value=1) == pytest.approx(0.24)
    assert policy.delay_for(4, random_value=0.5) == pytest.approx(0.4)


@pytest.mark.asyncio
async def test_retry_stops_on_non_retryable_error_and_records_delays():
    calls = 0
    delays: list[float] = []

    async def operation():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ProviderError("PROVIDER_TIMEOUT")
        raise ProviderError("PAYMENT_DECLINED")

    async def sleep(delay: float):
        delays.append(delay)

    with pytest.raises(ProviderError, match="Provider request failed"):
        await run_with_retry(operation, RetryPolicy(jitter_ratio=0), sleep=sleep)
    assert calls == 2
    assert delays == [0.1]


def test_circuit_breaker_opens_cools_down_and_recovers():
    now = 100.0
    breaker = CircuitBreaker(failure_threshold=2, cooldown_seconds=10, clock=lambda: now)
    breaker.record_failure()
    assert breaker.state is CircuitState.CLOSED
    breaker.record_failure()
    assert breaker.state is CircuitState.OPEN
    assert not breaker.allow_request()
    now += 10
    assert breaker.allow_request()
    assert not breaker.allow_request()
    breaker.record_success()
    assert breaker.state is CircuitState.CLOSED
    assert breaker.consecutive_failures == 0


@pytest.mark.asyncio
async def test_service_retries_transient_failure_then_fails_over_with_history():
    failing = MockStripeProvider()
    failing.configure(success_rate=0.0, server_error_probability=1.0)
    backup = MockAdyenProvider()
    service = PaymentService(
        [failing, backup],
        InMemoryPaymentStore(),
        retry_policy=RetryPolicy(max_attempts=3, initial_delay_seconds=0, max_delay_seconds=0, jitter_ratio=0),
        circuit_failure_threshold=2,
    )
    payment = await service.create(
        PaymentInput("merchant", 100, "INR", "card", "test_card", "failover-1")
    )
    assert payment.status == "SUCCEEDED"
    assert [attempt.provider for attempt in payment.attempts] == ["mockstripe", "mockstripe", "mockadyen"]
    assert payment.attempts[0].result == "RETRY"
    assert payment.attempts[1].error_code == "PROVIDER_UNAVAILABLE"
    assert service.circuits["mockstripe"].state is CircuitState.OPEN


@pytest.mark.asyncio
async def test_timeout_retries_without_blind_cross_provider_charge():
    timing_out = MockStripeProvider()
    timing_out.configure(success_rate=0.0, timeout_probability=1.0)
    backup = MockAdyenProvider()
    service = PaymentService(
        [timing_out, backup],
        InMemoryPaymentStore(),
        retry_policy=RetryPolicy(max_attempts=2, initial_delay_seconds=0, max_delay_seconds=0, jitter_ratio=0),
        circuit_failure_threshold=5,
    )
    with pytest.raises(ProviderError) as raised:
        await service.create(PaymentInput("merchant", 100, "INR", "card", "test_card", "timeout-1"))
    assert raised.value.code == "PROVIDER_TIMEOUT"
    assert service.circuits["mockadyen"].consecutive_failures == 0
