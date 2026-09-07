# ADR 0009: Operational health evidence

Status: accepted for local simulation.

## Decision

Provider circuit state changes are recorded as bounded transition metrics,
structured JSON logs, and `provider.circuit_opened` or
`provider.circuit_closed` outbox events. The in-process router also exports
request in-flight gauges. `/ready` reports each selected runtime dependency and
provider health, returning HTTP 503 when any required item is unavailable.

Payment listing accepts merchant, status, `created_after`, and `created_before`
filters and applies them in the repository seam so the same contract can be
implemented by the SQLAlchemy adapter.

## Consequences and limits

Circuit evidence is generated for transitions observed by one process. The
default outbox is in memory, and the external event worker, durable metrics
shipping, and shared provider-health history remain deferred. Provider request
totals, successes, failures, timeout counts, average latency, and p95 latency
are process-local measurements; p95 uses the router's bounded recent-request
window. Last success/failure timestamps are evidence from that same process,
and unexpected adapter exceptions are counted under the bounded `other`
failure category. Runtime readiness can check selected PostgreSQL and Redis
clients, but Compose still starts the default in-memory profile unless the
profile variables are explicitly set.
