# Local operations runbook

The following is a simulated local operations scenario. It is not a production incident report and contains no latency, throughput, or recovery-time claim.

## Start and inspect

```bash
docker compose up --build
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/ready
curl http://127.0.0.1:8000/metrics
```

Open Prometheus at `http://127.0.0.1:9090` and Grafana at `http://127.0.0.1:3000` with the local `admin/admin` credentials from Compose. The dashboard is provisioned from repository JSON and will populate only for metrics emitted by the running app.

## Simulated provider event

1. Use the local admin API to set Stripe `server_error_probability` to `1`.
2. Submit a payment with a fresh idempotency key.
3. Inspect `/metrics` for provider requests, failures, retries, and circuit-state series.
4. Inspect application logs for a JSON request record. Confirm the request body, synthetic token, and idempotency key are absent.
5. Recover Stripe through the admin API and restore its simulation configuration.

This scenario exercises the local mock provider and default in-memory profile only. The separate Compose profile selects PostgreSQL, Redis, and Redpanda explicitly; use the external-profile integration tests and `scripts/observability_evidence.py` for evidence of those paths. Neither run proves long-term dashboard retention or multi-process production behavior.

## Simulated dependency outage

When running an explicit external profile, inject or stop an unavailable PostgreSQL, Redis, or Redpanda dependency and check readiness and worker behavior separately. `/ready` reports each selected runtime dependency and returns 503 when a dependency or provider is unavailable. Treat external container health as a prerequisite signal, not evidence that the app has used the dependency.
