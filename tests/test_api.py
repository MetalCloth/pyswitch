import asyncio

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
        stats = providers.json()[0]["stats"]
        assert stats["total_requests"] == stats["successes"] == 1
        assert stats["failures"] == stats["timeouts"] == 0
        assert stats["p95_latency_ms"] >= 0
        assert stats["last_success_at"] is not None


async def test_payment_list_supports_merchant_status_and_time_filters(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        body = {"merchant_id": "filter-merchant", "amount": 100, "currency": "INR", "payment_method": {"type": "card", "token": "test_card"}}
        created = await client.post("/api/v1/payments", headers={"Idempotency-Key": "filter-1"}, json=body)
        assert created.status_code == 201
        payment = created.json()
        assert len((await client.get("/api/v1/payments", params={"merchant_id": "filter-merchant", "status": "SUCCEEDED"})).json()) == 1
        assert len((await client.get("/api/v1/payments", params={"merchant_id": "other-merchant"})).json()) == 0
        assert len((await client.get("/api/v1/payments", params={"created_after": payment["created_at"]})).json()) == 1
        assert len((await client.get("/api/v1/payments", params={"created_before": "2000-01-01T00:00:00Z"})).json()) == 0


async def test_invalid_request_is_rejected(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/v1/payments", headers={"Idempotency-Key": "bad"}, json={"merchant_id": "m", "amount": 0, "currency": "INR", "payment_method": {"type": "card", "token": "real_card"}})
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "VALIDATION_ERROR"


async def test_readiness_reports_provider_and_runtime_dependencies(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/ready")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert body["dependencies"] == {
        "storage": {"backend": "memory", "status": "ready"},
        "coordination": {"backend": "memory", "status": "ready"},
        "events": {"backend": "memory", "status": "ready"},
    }


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
        updated = await client.put("/api/v1/admin/providers/mockadyen/config", headers=headers, json={"min_latency_ms": 10, "max_latency_ms": 20, "max_concurrency": 2})
        assert updated.status_code == 200
        assert updated.json()["config"]["max_latency_ms"] == 20
        assert updated.json()["config"]["max_concurrency"] == 2


async def test_provider_stats_expose_timeout_evidence_after_api_failure(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"X-Admin-Token": "local-dev-only"}
        configured = await client.put(
            "/api/v1/admin/providers/mockstripe/config",
            headers=headers,
            json={"timeout_probability": 1.0},
        )
        assert configured.status_code == 200
        body = {
            "merchant_id": "timeout-merchant",
            "amount": 100,
            "currency": "INR",
            "payment_method": {"type": "card", "token": "test_card"},
        }
        payment = await client.post("/api/v1/payments", headers={"Idempotency-Key": "timeout-api"}, json=body)
        assert payment.status_code == 503
        stats = (await client.get("/api/v1/providers/mockstripe")).json()["stats"]
        assert stats["total_requests"] == stats["failures"] == stats["timeouts"] == 3
        assert stats["last_success_at"] is None
        assert stats["last_failure_at"] is not None


async def test_idempotency_conflict_is_rejected(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"Idempotency-Key": "same-key"}
        body = {"merchant_id": "merchant_123", "amount": 100, "currency": "INR", "payment_method": {"type": "card", "token": "test_card"}}
        assert (await client.post("/api/v1/payments", headers=headers, json=body)).status_code == 201
        changed = {**body, "amount": 101}
        response = await client.post("/api/v1/payments", headers=headers, json=changed)
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "DUPLICATE_REQUEST"


async def test_partial_full_and_concurrent_refunds_are_bounded(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        body = {"merchant_id": "merchant_123", "amount": 100, "currency": "INR", "payment_method": {"type": "card", "token": "test_card"}}
        created = await client.post("/api/v1/payments", headers={"Idempotency-Key": "refund-key"}, json=body)
        payment_id = created.json()["id"]
        partial = await client.post(f"/api/v1/payments/{payment_id}/refund", json={"amount": 40})
        assert partial.status_code == 200
        assert partial.json()["amount"] == 40

        results = await asyncio.gather(
            client.post(f"/api/v1/payments/{payment_id}/refund", json={"amount": 60}),
            client.post(f"/api/v1/payments/{payment_id}/refund", json={"amount": 60}),
        )
        assert sorted(response.status_code for response in results) == [200, 422]
        fetched = await client.get(f"/api/v1/payments/{payment_id}")
        assert fetched.json()["status"] == "REFUNDED"
        over_refund = await client.post(f"/api/v1/payments/{payment_id}/refund", json={"amount": 1})
        assert over_refund.status_code == 422
