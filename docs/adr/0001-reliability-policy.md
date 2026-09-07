# ADR 0001: Retry, circuit and failover policy

Status: accepted for the local simulation.

## Decision

PySwitch retries only transient provider errors: `PROVIDER_TIMEOUT`, `PROVIDER_UNAVAILABLE`, `HTTP_502`, and `HTTP_503`. The default policy makes three attempts with 100/200/400ms base delays and bounded 20% jitter. Declines and malformed or authentication failures are returned immediately.

Each provider owns a separate circuit breaker. The default circuit opens after three consecutive failures, rejects traffic during a 30-second cooldown, then permits one half-open probe. A successful probe closes the circuit; a failed probe opens it again.

After retry exhaustion, PySwitch may try another provider for `PROVIDER_UNAVAILABLE`, 502/503, or an already-open circuit. A timeout is treated as ambiguous: it is retried on the original provider, then returned to the caller without a blind cross-provider charge. Every provider call that was actually made is appended to the payment's attempt history.

## Consequences

The policy is safe for this synthetic demonstrator and makes outage behavior observable. The in-memory store is process-local and is not a financial source of truth; PostgreSQL persistence and durable idempotency are required before any production-like use.

