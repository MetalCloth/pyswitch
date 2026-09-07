# ADR 0007: Explicit runtime profiles

Status: accepted for local and integration composition.

## Decision

Construct storage, coordination, rate-limit, outbox, and broker dependencies in
`build_runtime(Settings)`. Select each dependency family with an environment
variable: `PYSWITCH_STORAGE_BACKEND`, `PYSWITCH_COORDINATION_BACKEND`, and
`PYSWITCH_EVENT_BACKEND`. The default for every family is the existing
process-local implementation.

The optional profiles construct the SQLAlchemy payment repository, Redis
idempotency/rate-limit adapters, and Kafka broker adapter only when explicitly
selected. `create_app` receives the resulting `Runtime` and owns its lifespan;
Kafka starts during lifespan startup, while database and Redis clients remain
lazy until their first operation.

The Compose image installs the optional runtime extras and its entrypoint runs
Alembic migrations before starting the API when the PostgreSQL profile is
selected. Compose passes explicit service URLs for all three external
backends.

## Consequences and limits

Dependency selection is visible, testable, and free of hidden module-level
clients. Invalid values and missing optional dependencies fail with actionable
messages. No external profile falls back to memory after selection, since that
could silently weaken durability or coordination guarantees.

The default app remains external-service-free. Opt-in integration checks cover
the repository plus outbox transaction, Redis atomic idempotency/rate limiting,
and Kafka publish plus outbox-worker marking when services and dependencies are
available. Those checks are bounded and skip unavailable environments; they do
not claim Kafka durability, cross-process failure recovery, or performance
results.
