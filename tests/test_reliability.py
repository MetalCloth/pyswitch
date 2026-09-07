import pytest

from pyswitch.providers.base import ProviderError
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

