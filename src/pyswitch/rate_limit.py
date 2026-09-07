import asyncio
import hashlib
import math
import time
from dataclasses import dataclass
from typing import Any, Protocol


class RateLimitUnavailable(Exception):
    code = "INTERNAL_ERROR"


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    allowed: bool
    limit: int
    remaining: int
    retry_after_seconds: int = 0


class RateLimiter(Protocol):
    async def consume(self, merchant_id: str) -> RateLimitDecision: ...


class InMemoryTokenBucketLimiter:
    """Process-local fallback; it makes no cross-process rate-limit claim."""

    def __init__(self, *, capacity: int = 60, refill_per_second: float = 1.0, clock=time.monotonic) -> None:
        if capacity < 1 or refill_per_second <= 0:
            raise ValueError("Token bucket capacity and refill must be positive")
        self.capacity = capacity
        self.refill_per_second = refill_per_second
        self._clock = clock
        self._buckets: dict[str, tuple[float, float]] = {}
        self._lock = asyncio.Lock()

    async def consume(self, merchant_id: str) -> RateLimitDecision:
        async with self._lock:
            now = self._clock()
            tokens, updated = self._buckets.get(merchant_id, (float(self.capacity), now))
            tokens = min(float(self.capacity), tokens + max(0.0, now - updated) * self.refill_per_second)
            if tokens >= 1:
                tokens -= 1
                decision = RateLimitDecision(True, self.capacity, math.floor(tokens))
            else:
                retry_after = max(1, math.ceil((1 - tokens) / self.refill_per_second))
                decision = RateLimitDecision(False, self.capacity, 0, retry_after)
            self._buckets[merchant_id] = (tokens, now)
            return decision


class RedisTokenBucketLimiter:
    """Redis Lua token bucket; atomic across workers sharing the same Redis."""

    _SCRIPT = """
    local now = redis.call('TIME')
    local seconds = tonumber(now[1]) + tonumber(now[2]) / 1000000
    local tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens'))
    local updated = tonumber(redis.call('HGET', KEYS[1], 'updated'))
    local capacity = tonumber(ARGV[1])
    local refill = tonumber(ARGV[2])
    if not tokens then tokens = capacity end
    if not updated then updated = seconds end
    tokens = math.min(capacity, tokens + math.max(0, seconds - updated) * refill)
    local allowed = 0
    local retry = 0
    if tokens >= 1 then
      tokens = tokens - 1
      allowed = 1
    else
      retry = math.ceil((1 - tokens) / refill)
    end
    redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated', seconds)
    redis.call('EXPIRE', KEYS[1], math.ceil(capacity / refill * 2))
    return {allowed, math.floor(tokens), retry}
    """

    def __init__(self, redis: Any, *, capacity: int = 60, refill_per_second: float = 1.0) -> None:
        self.redis = redis
        self.capacity = capacity
        self.refill_per_second = refill_per_second

    @staticmethod
    def _redis_key(merchant_id: str) -> str:
        return f"pyswitch:rate:{hashlib.sha256(merchant_id.encode()).hexdigest()}"

    async def consume(self, merchant_id: str) -> RateLimitDecision:
        try:
            result = await self.redis.eval(
                self._SCRIPT,
                1,
                self._redis_key(merchant_id),
                self.capacity,
                self.refill_per_second,
            )
            values = [int(value) for value in result]
            return RateLimitDecision(bool(values[0]), self.capacity, values[1], values[2])
        except Exception as exc:
            raise RateLimitUnavailable("Redis rate limiting is unavailable") from exc

