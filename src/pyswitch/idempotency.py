import asyncio
import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol
from uuid import UUID


class IdempotencyStatus(StrEnum):
    CLAIMED = "CLAIMED"
    COMPLETED = "COMPLETED"
    CONFLICT = "CONFLICT"


class IdempotencyUnavailable(Exception):
    code = "INTERNAL_ERROR"


@dataclass(frozen=True, slots=True)
class IdempotencyDecision:
    status: IdempotencyStatus
    payment_id: UUID | None = None


class IdempotencyCoordinator(Protocol):
    async def acquire(self, merchant_id: str, key: str, fingerprint: str) -> IdempotencyDecision: ...
    async def complete(self, merchant_id: str, key: str, fingerprint: str, payment_id: UUID) -> None: ...
    async def abort(self, merchant_id: str, key: str, fingerprint: str) -> None: ...


class MemoryIdempotencyCoordinator:
    """Process-local coordinator used by default and in deterministic tests."""

    def __init__(self) -> None:
        self._records: dict[tuple[str, str], tuple[str, UUID | None, bool]] = {}
        self._locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._events: dict[tuple[str, str], asyncio.Event] = {}
        self._lock = asyncio.Lock()

    async def _key_lock(self, key: tuple[str, str]) -> asyncio.Lock:
        async with self._lock:
            return self._locks.setdefault(key, asyncio.Lock())

    async def acquire(self, merchant_id: str, key: str, fingerprint: str) -> IdempotencyDecision:
        record_key = (merchant_id, key)
        lock = await self._key_lock(record_key)
        while True:
            async with lock:
                record = self._records.get(record_key)
                if record is None:
                    self._records[record_key] = (fingerprint, None, False)
                    self._events[record_key] = asyncio.Event()
                    return IdempotencyDecision(IdempotencyStatus.CLAIMED)
                saved_fingerprint, payment_id, completed = record
                if saved_fingerprint != fingerprint:
                    return IdempotencyDecision(IdempotencyStatus.CONFLICT)
                if completed:
                    return IdempotencyDecision(IdempotencyStatus.COMPLETED, payment_id)
                event = self._events[record_key]
            await event.wait()

    async def complete(self, merchant_id: str, key: str, fingerprint: str, payment_id: UUID) -> None:
        record_key = (merchant_id, key)
        lock = await self._key_lock(record_key)
        async with lock:
            record = self._records.get(record_key)
            if record is None or record[0] != fingerprint:
                raise IdempotencyUnavailable("Idempotency claim is missing")
            self._records[record_key] = (fingerprint, payment_id, True)
            self._events[record_key].set()

    async def abort(self, merchant_id: str, key: str, fingerprint: str) -> None:
        record_key = (merchant_id, key)
        lock = await self._key_lock(record_key)
        async with lock:
            record = self._records.get(record_key)
            if record and record[0] == fingerprint and not record[2]:
                del self._records[record_key]
                self._events[record_key].set()


class RedisIdempotencyCoordinator:
    """Redis coordinator; requires redis-py asyncio and a reachable Redis server."""

    _CLAIM_SCRIPT = """
    local status = redis.call('HGET', KEYS[1], 'status')
    if not status then
      redis.call('HSET', KEYS[1], 'status', 'PROCESSING', 'fingerprint', ARGV[1])
      redis.call('EXPIRE', KEYS[1], ARGV[2])
      return {'CLAIMED', ''}
    end
    local fingerprint = redis.call('HGET', KEYS[1], 'fingerprint')
    if fingerprint ~= ARGV[1] then return {'CONFLICT', ''} end
    if status == 'COMPLETED' then return {'COMPLETED', redis.call('HGET', KEYS[1], 'payment_id')} end
    return {'PROCESSING', ''}
    """

    def __init__(self, redis: Any, *, processing_ttl_seconds: int = 90, poll_seconds: float = 0.05) -> None:
        self.redis = redis
        self.processing_ttl_seconds = processing_ttl_seconds
        self.poll_seconds = poll_seconds

    @staticmethod
    def _redis_key(merchant_id: str, key: str) -> str:
        digest = hashlib.sha256(f"{merchant_id}\0{key}".encode()).hexdigest()
        return f"pyswitch:idempotency:{digest}"

    async def acquire(self, merchant_id: str, key: str, fingerprint: str) -> IdempotencyDecision:
        redis_key = self._redis_key(merchant_id, key)
        try:
            while True:
                result = await self.redis.eval(self._CLAIM_SCRIPT, 1, redis_key, fingerprint, self.processing_ttl_seconds)
                status = result[0].decode() if isinstance(result[0], bytes) else result[0]
                if status == "CLAIMED":
                    return IdempotencyDecision(IdempotencyStatus.CLAIMED)
                if status == "CONFLICT":
                    return IdempotencyDecision(IdempotencyStatus.CONFLICT)
                if status == "COMPLETED":
                    value = result[1].decode() if isinstance(result[1], bytes) else result[1]
                    return IdempotencyDecision(IdempotencyStatus.COMPLETED, UUID(value))
                await asyncio.sleep(self.poll_seconds)
        except Exception as exc:
            raise IdempotencyUnavailable("Redis idempotency is unavailable") from exc

    async def complete(self, merchant_id: str, key: str, fingerprint: str, payment_id: UUID) -> None:
        redis_key = self._redis_key(merchant_id, key)
        try:
            await self.redis.hset(redis_key, mapping={"status": "COMPLETED", "fingerprint": fingerprint, "payment_id": str(payment_id)})
            await self.redis.expire(redis_key, self.processing_ttl_seconds)
        except Exception as exc:
            raise IdempotencyUnavailable("Redis idempotency is unavailable") from exc

    async def abort(self, merchant_id: str, key: str, fingerprint: str) -> None:
        redis_key = self._redis_key(merchant_id, key)
        try:
            current = await self.redis.hget(redis_key, "fingerprint")
            if current is not None and (current.decode() if isinstance(current, bytes) else current) == fingerprint:
                await self.redis.delete(redis_key)
        except Exception as exc:
            raise IdempotencyUnavailable("Redis idempotency is unavailable") from exc

