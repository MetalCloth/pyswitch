from uuid import uuid4

import pytest

from pyswitch.idempotency import IdempotencyStatus, IdempotencyUnavailable, RedisIdempotencyCoordinator
from pyswitch.rate_limit import RateLimitUnavailable, RedisTokenBucketLimiter


class FakeRedis:
    def __init__(self):
        self.records = {}

    async def eval(self, _script, _keys, key, fingerprint, _ttl):
        record = self.records.get(key)
        if record is None:
            self.records[key] = {"status": "PROCESSING", "fingerprint": fingerprint}
            return ["CLAIMED", ""]
        if record["fingerprint"] != fingerprint:
            return ["CONFLICT", ""]
        if record["status"] == "COMPLETED":
            return ["COMPLETED", record["payment_id"]]
        raise AssertionError("test fake does not wait on unfinished claims")

    async def hset(self, key, mapping):
        self.records[key] = mapping

    async def expire(self, _key, _ttl):
        return True

    async def hget(self, key, field):
        return self.records.get(key, {}).get(field)

    async def delete(self, key):
        self.records.pop(key, None)


@pytest.mark.asyncio
async def test_redis_coordinator_claim_complete_and_conflict_with_fake():
    redis = FakeRedis()
    coordinator = RedisIdempotencyCoordinator(redis)
    payment_id = uuid4()
    assert (await coordinator.acquire("merchant", "key", "fp")).status is IdempotencyStatus.CLAIMED
    await coordinator.complete("merchant", "key", "fp", payment_id)
    replay = await coordinator.acquire("merchant", "key", "fp")
    assert replay.status is IdempotencyStatus.COMPLETED
    assert replay.payment_id == payment_id
    conflict = await coordinator.acquire("merchant", "key", "different")
    assert conflict.status is IdempotencyStatus.CONFLICT


@pytest.mark.asyncio
async def test_redis_dependency_errors_fail_closed():
    class BrokenRedis:
        async def eval(self, *_args):
            raise ConnectionError("simulated Redis outage")

    with pytest.raises(IdempotencyUnavailable):
        await RedisIdempotencyCoordinator(BrokenRedis()).acquire("merchant", "key", "fp")
    with pytest.raises(RateLimitUnavailable):
        await RedisTokenBucketLimiter(BrokenRedis()).consume("merchant")

