# Runtime profiles

PySwitch composes its dependencies once from `Settings`. The default profile is
process-local and requires no external service. Each external profile is an
explicit opt-in; there is no automatic fallback after selecting it.

```mermaid
flowchart LR
    ENV[Environment] --> SETTINGS[Settings.from_env]
    SETTINGS --> FACTORY[build_runtime]
    FACTORY --> STORAGE[Memory or SQLAlchemy repository]
    FACTORY --> COORD[Memory or Redis idempotency and limits]
    FACTORY --> EVENTS[Memory or Kafka broker]
    STORAGE --> SERVICE[PaymentService]
    COORD --> SERVICE
    EVENTS --> SERVICE
```

The supported variables are:

| Variable | Values | Default |
| --- | --- | --- |
| `PYSWITCH_STORAGE_BACKEND` | `memory`, `postgres` | `memory` |
| `PYSWITCH_COORDINATION_BACKEND` | `memory`, `redis` | `memory` |
| `PYSWITCH_EVENT_BACKEND` | `memory`, `kafka` | `memory` |
| `PYSWITCH_DATABASE_URL` | SQLAlchemy async URL | `postgresql+asyncpg://pyswitch:pyswitch@postgres:5432/pyswitch` |
| `PYSWITCH_REDIS_URL` | Redis URL | `redis://redis:6379/0` |
| `PYSWITCH_KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap list | `redpanda:9092` |
| `PYSWITCH_KAFKA_TOPIC` | Kafka topic | `pyswitch.events` |

For example, this selects all optional adapters:

```bash
PYSWITCH_STORAGE_BACKEND=postgres \
PYSWITCH_COORDINATION_BACKEND=redis \
PYSWITCH_EVENT_BACKEND=kafka \
uvicorn pyswitch.main:app --host 0.0.0.0 --port 8000
```

The `db`, `redis`, and `events` optional extras are required for their
respective profiles. Missing extras, unsupported profile values, empty required
URLs, invalid SQLAlchemy URLs, and Kafka startup failures produce explicit
configuration errors. The factory does not connect to PostgreSQL or Redis while
building the app. Kafka is started during the app lifespan and is stopped on
shutdown.

The SQLAlchemy repository maps the existing payment, attempts, refunds, and
outbox model seam. The Compose entrypoint runs `alembic upgrade head` when
`PYSWITCH_STORAGE_BACKEND=postgres`, using `PYSWITCH_DATABASE_URL`; the image
includes the `db`, `redis`, and `events` extras required by the selected
profile. Compose sets all three external backend selectors and service DNS
URLs explicitly. The default local tests make no network calls.

The opt-in external profile checks run with:

```bash
PYSWITCH_RUN_INTEGRATION=1 pytest -q tests/integration/test_external_profiles.py
```

They exercise PostgreSQL payment plus outbox persistence, Redis atomic
idempotency and rate limiting, and Kafka publish plus outbox marking with
bounded timeouts. Missing packages or unavailable services skip with a reason;
the checks do not claim durability, cross-process recovery, delivery latency,
or production performance beyond the operations they complete.
