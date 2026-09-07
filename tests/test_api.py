import pytest
from httpx import ASGITransport, AsyncClient

from pyswitch.api import create_app


@pytest.fixture
def app():
    return create_app()


async def test_payment_create_fetch_and_round_robin(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        body = {"merchant_id": "merchant_123", "amount": 500000, "currency": "INR", "payment_method": {"type": "card", "token": "test_card"}}
        first = await client.post("/api/v1/payments", headers={"Idempotency-Key": "key-1"}, json=body)
        second = await client.post("/api/v1/payments", headers={"Idempotency-Key": "key-1"}, json=body)
        assert first.status_code == second.status_code == 201
        assert first.json()["id"] == second.json()["id"]
        fetched = await client.get(f"/api/v1/payments/{first.json()['id']}")
        assert fetched.json()["status"] == "SUCCEEDED"
        providers = await client.get("/api/v1/providers")
        assert [item["name"] for item in providers.json()] == ["mockstripe", "mockadyen", "mockrazorpay"]


async def test_invalid_request_is_rejected(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/v1/payments", headers={"Idempotency-Key": "bad"}, json={"merchant_id": "m", "amount": 0, "currency": "INR", "payment_method": {"type": "card", "token": "real_card"}})
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "VALIDATION_ERROR"


async def test_admin_can_force_failure_and_recover_provider(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        unauthorized = await client.post("/api/v1/admin/providers/mockstripe/fail")
        assert unauthorized.status_code == 401

        headers = {"X-Admin-Token": "local-dev-only"}
        failed = await client.post("/api/v1/admin/providers/mockstripe/fail", headers=headers)
        assert failed.status_code == 200
        detail = await client.get("/api/v1/providers/mockstripe")
        assert detail.json()["healthy"] is False

        body = {"merchant_id": "merchant_123", "amount": 500000, "currency": "INR", "payment_method": {"type": "card", "token": "test_card"}}
        payment = await client.post("/api/v1/payments", headers={"Idempotency-Key": "failover-demo"}, json=body)
        assert payment.status_code == 201

        recovered = await client.post("/api/v1/admin/providers/mockstripe/recover", headers=headers)
        assert recovered.status_code == 200
        assert (await client.get("/api/v1/providers/mockstripe")).json()["healthy"] is True


async def test_provider_config_updates_and_validates_latency_range(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"X-Admin-Token": "local-dev-only"}
        invalid = await client.put("/api/v1/admin/providers/mockadyen/config", headers=headers, json={"min_latency_ms": 100, "max_latency_ms": 10})
        assert invalid.status_code == 422
        updated = await client.put("/api/v1/admin/providers/mockadyen/config", headers=headers, json={"min_latency_ms": 10, "max_latency_ms": 20})
        assert updated.status_code == 200
        assert updated.json()["config"]["max_latency_ms"] == 20
