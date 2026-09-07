import json
import logging
import time
from collections.abc import Mapping

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest


class Metrics:
    """Prometheus metrics with bounded enum/provider labels only."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.payments_total = Counter("pyswitch_payments_total", "Payments by terminal status", ["status"], registry=self.registry)
        self.provider_requests_total = Counter("pyswitch_provider_requests_total", "Provider calls", ["provider", "result"], registry=self.registry)
        self.provider_failures_total = Counter("pyswitch_provider_failures_total", "Provider failures", ["provider", "error_code"], registry=self.registry)
        self.provider_latency_seconds = Histogram("pyswitch_provider_latency_seconds", "Provider latency", ["provider"], registry=self.registry)
        self.provider_inflight = Gauge("pyswitch_provider_inflight", "Provider requests in flight", ["provider"], registry=self.registry)
        self.provider_circuit_state = Gauge("pyswitch_provider_circuit_state", "Provider circuit state", ["provider", "state"], registry=self.registry)
        self.provider_circuit_transitions_total = Counter("pyswitch_provider_circuit_transitions_total", "Provider circuit transitions", ["provider", "from_state", "to_state"], registry=self.registry)
        self.retries_total = Counter("pyswitch_retries_total", "Provider retries", ["provider"], registry=self.registry)
        self.idempotency_hits_total = Counter("pyswitch_idempotency_hits_total", "Idempotency replays", registry=self.registry)
        self.rate_limit_rejections_total = Counter("pyswitch_rate_limit_rejections_total", "Rate limit rejections", registry=self.registry)

    def render(self) -> bytes:
        return generate_latest(self.registry)


request_logger = logging.getLogger("pyswitch.http")


def log_request(*, request_id: str, method: str, path: str, status_code: int, latency_ms: float, merchant_id: str | None = None, payment_id: str | None = None) -> None:
    """Emit a JSON line without request headers, bodies, tokens, or idempotency keys."""
    record = {
        "event": "http_request",
        "request_id": request_id,
        "method": method,
        "path": path,
        "status_code": status_code,
        "latency_ms": round(latency_ms, 3),
    }
    if merchant_id is not None:
        record["merchant_id"] = merchant_id
    if payment_id is not None:
        record["payment_id"] = payment_id
    request_logger.info(json.dumps(record, separators=(",", ":")))


def log_circuit_transition(*, provider: str, from_state: str, to_state: str) -> None:
    request_logger.info(
        json.dumps(
            {
                "event": "provider_circuit_transition",
                "provider": provider,
                "from_state": from_state,
                "to_state": to_state,
            },
            separators=(",", ":"),
        )
    )
