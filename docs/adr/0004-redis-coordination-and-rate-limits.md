# ADR 0004: Redis coordination and merchant rate limits

Status: accepted for the local simulation.

## Decision

Use an atomic Redis Lua claim for idempotency and a Redis Lua token bucket for merchant rate limits. Idempotency records are keyed by a hashed merchant/key pair, contain a request fingerprint and status, and cache only the authoritative payment ID after the repository save succeeds. Processing claims expire so abandoned workers do not leave a permanent lock. Matching in-flight requests wait; mismatched fingerprints return `DUPLICATE_REQUEST`.

Use a merchant-scoped token bucket with configurable capacity and refill rate. Refill and consume happen atomically and use Redis server time. Allowed requests receive bounded rate headers; blocked requests return `429 RATE_LIMITED` and `Retry-After` without invoking a provider.

The application defaults to in-memory coordinator and limiter implementations for local startup and focused tests. Redis failures raise an explicit unavailable error and fail closed with HTTP 503. No automatic fallback from a live Redis coordinator to a process-local coordinator is performed, because that could permit duplicate provider execution or inconsistent limits across workers.

## Consequences

With a reachable shared Redis and a durable payment repository, coordination can span workers. The explicit runtime profile wires Redis clients during application composition, but this slice does not claim live cross-process verification. PostgreSQL remains the financial source of truth; Redis loss must never invent or overwrite payment records.
