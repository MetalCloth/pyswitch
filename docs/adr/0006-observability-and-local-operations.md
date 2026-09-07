# ADR 0006: Bounded observability and local operations

Status: accepted for the local simulation.

## Decision

Use a dedicated Prometheus registry per application instance. Counters, histograms, and gauges use bounded provider/status/error/circuit labels. Never label metrics with merchant IDs, payment IDs, idempotency keys, or tokens. Expose the registry at `/metrics`.

Emit one JSON request record after each handled request. Include request ID, method, path, status, and measured latency, plus safe merchant/payment identifiers when known. Do not inspect or serialize request bodies or sensitive headers.

Provide a local Compose stack with healthchecks for the app, PostgreSQL, Redis, Redpanda, Prometheus, and Grafana. Provision Prometheus scraping and a Grafana dashboard from repository files. Keep external service clients out of the default startup until their repository, coordinator, broker, and worker wiring is implemented.

## Consequences and deferred work

The default test suite remains external-service-free while metrics and logs are testable. The dashboard is reproducible configuration, not measured evidence. PostgreSQL/Redis/Redpanda connectivity, durable metrics/log shipping, alert rules, and load-test evidence remain deferred. Readiness currently reports simulated providers; Compose independently reports dependency container health.

