import json
import logging

import pytest
from httpx import ASGITransport, AsyncClient

from pyswitch.api import create_app
from pyswitch.events import EventType
from pyswitch.observability import Metrics
from pyswitch.providers.mock import MockAdyenProvider, MockStripeProvider
from pyswitch.reliability import RetryPolicy
from pyswitch.service import PaymentInput, PaymentService
from pyswitch.store import InMemoryPaymentStore


@pytest.mark.asyncio
async def test_metrics_endpoint_emits_bounded_payment_and_rate_metrics():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        body = {"merchant_id": "metrics-merchant", "amount": 100, "currency": "INR", "payment_method": {"type": "card", "token": "test_card"}}
        await client.post("/api/v1/payments", headers={"Idempotency-Key": "metrics-1"}, json=body)
        metrics = await client.get("/metrics")
        assert metrics.status_code == 200
        assert 'pyswitch_payments_total{status="SUCCEEDED"} 1.0' in metrics.text
        assert "pyswitch_rate_limit_rejections_total" in metrics.text
        assert "metrics-merchant" not in metrics.text


@pytest.mark.asyncio
async def test_request_log_is_json_and_excludes_sensitive_fields(caplog):
    caplog.set_level(logging.INFO, logger="pyswitch.http")
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        body = {"merchant_id": "log-merchant", "amount": 100, "currency": "INR", "payment_method": {"type": "card", "token": "test_secret"}}
        response = await client.post("/api/v1/payments", headers={"Idempotency-Key": "secret-key"}, json=body)
        assert response.status_code == 201
    records = [json.loads(record.message) for record in caplog.records if record.name == "pyswitch.http"]
    request_record = next(record for record in records if record["event"] == "http_request")
    assert request_record["request_id"] == response.headers["X-Request-ID"]
    assert request_record["merchant_id"] == "log-merchant"
    assert "test_secret" not in caplog.text
    assert "secret-key" not in caplog.text


@pytest.mark.asyncio
async def test_circuit_transition_is_logged_metered_and_outboxed(caplog):
    metrics = Metrics()
    failing = MockStripeProvider()
    failing.configure(success_rate=0.0, server_error_probability=1.0)
    service = PaymentService(
        [failing, MockAdyenProvider()],
        InMemoryPaymentStore(),
        circuit_failure_threshold=1,
        retry_policy=RetryPolicy(max_attempts=1, initial_delay_seconds=0, max_delay_seconds=0, jitter_ratio=0),
        metrics=metrics,
    )
    with caplog.at_level(logging.INFO, logger="pyswitch.http"):
        payment = await service.create(PaymentInput("merchant", 100, "INR", "card", "test_card", "circuit-event"))
    assert payment.status.value == "SUCCEEDED"
    events = await service.store.unsent()
    transitions = [event for event in events if event.event_type is EventType.PROVIDER_CIRCUIT_OPENED]
    assert len(transitions) == 1
    assert transitions[0].payload["provider"] == "mockstripe"
    assert '"event":"provider_circuit_transition"' in caplog.text
    rendered = metrics.render().decode()
    assert 'pyswitch_provider_circuit_transitions_total{from_state="CLOSED",provider="mockstripe",to_state="OPEN"} 1.0' in rendered
    assert 'pyswitch_provider_inflight{provider="mockstripe"} 0.0' in rendered
