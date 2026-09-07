import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import TypeVar

from .providers.base import ProviderError

T = TypeVar("T")


RETRYABLE_ERRORS = frozenset({"PROVIDER_TIMEOUT", "PROVIDER_UNAVAILABLE", "HTTP_502", "HTTP_503"})


def is_retryable(error: ProviderError) -> bool:
    """Only transient provider failures may be retried."""
    return error.code in RETRYABLE_ERRORS


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    initial_delay_seconds: float = 0.1
    max_delay_seconds: float = 0.8
    jitter_ratio: float = 0.2

    def delay_for(self, retry_number: int, random_value: float | None = None) -> float:
        base = min(self.max_delay_seconds, self.initial_delay_seconds * (2 ** retry_number))
        if not self.jitter_ratio:
            return base
        value = random.random() if random_value is None else random_value
        return base * (1 + (value * 2 - 1) * self.jitter_ratio)


async def run_with_retry(
    operation: Callable[[], Awaitable[T]],
    policy: RetryPolicy,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> tuple[T, int]:
    """Run an operation and return its result plus the number of calls made."""
    last_error: ProviderError | None = None
    for attempt in range(policy.max_attempts):
        try:
            return await operation(), attempt + 1
        except ProviderError as error:
            last_error = error
            if not is_retryable(error) or attempt == policy.max_attempts - 1:
                raise
            await sleep(policy.delay_for(attempt))
    raise last_error or RuntimeError("retry policy made no attempt")


class CircuitState(StrEnum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    def __init__(
        self,
        *,
        failure_threshold: int = 3,
        cooldown_seconds: float = 30.0,
        half_open_probe_limit: int = 1,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold < 1 or cooldown_seconds < 0 or half_open_probe_limit < 1:
            raise ValueError("Circuit breaker thresholds must be positive")
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self.half_open_probe_limit = half_open_probe_limit
        self._clock = clock
        self.state = CircuitState.CLOSED
        self.consecutive_failures = 0
        self._opened_at = 0.0
        self._probes = 0

    def allow_request(self) -> bool:
        if self.state is CircuitState.CLOSED:
            return True
        if self.state is CircuitState.OPEN:
            if self._clock() - self._opened_at < self.cooldown_seconds:
                return False
            self.state = CircuitState.HALF_OPEN
            self._probes = 0
        if self._probes >= self.half_open_probe_limit:
            return False
        self._probes += 1
        return True

    def record_success(self) -> None:
        self.state = CircuitState.CLOSED
        self.consecutive_failures = 0
        self._probes = 0

    def record_failure(self) -> None:
        self.consecutive_failures += 1
        if self.state is CircuitState.HALF_OPEN or self.consecutive_failures >= self.failure_threshold:
            self.state = CircuitState.OPEN
            self._opened_at = self._clock()
            self._probes = 0


# ponytail: one shared lock per provider is added at service integration; the core stays lock-free for focused use.
