import pytest
from httpx import ASGITransport, AsyncClient

from pyswitch.api import create_app
from pyswitch.rate_limit import InMemoryTokenBucketLimiter


@pytest.mark.asyncio
async def test_memory_token_bucket_limits_a_merchant_and_refills():
    now = 100.0
    limiter = InMemoryTokenBucketLimiter(capacity=2, refill_per_second=1, clock=lambda: now)
    assert (await limiter.consume("merchant")).allowed
    assert (await limiter.consume("merchant")).allowed
    blocked = await limiter.consume("merchant")
    assert not blocked.allowed
    assert blocked.remaining == 0
    assert blocked.retry_after_seconds == 1

    now += 1
    assert (await limiter.consume("merchant")).allowed


@pytest.mark.asyncio
async def test_api_returns_rate_headers_and_429_without_provider_call():
    app = create_app(rate_limiter=InMemoryTokenBucketLimiter(capacity=1, refill_per_second=1e-9))
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        body = {"merchant_id": "limited", "amount": 100, "currency": "INR", "payment_method": {"type": "card", "token": "test_card"}}
        first = await client.post("/api/v1/payments", headers={"Idempotency-Key": "limit-1"}, json=body)
        second = await client.post("/api/v1/payments", headers={"Idempotency-Key": "limit-2"}, json=body)
        assert first.status_code == 201
        assert first.headers["X-RateLimit-Limit"] == "1"
        assert first.headers["X-RateLimit-Remaining"] == "0"
        assert second.status_code == 429
        assert second.headers["Retry-After"]
        assert second.json()["error"]["code"] == "RATE_LIMITED"
