# PySwitch architecture

PySwitch keeps the public contract provider-neutral. The API validates a synthetic payment request, the service selects a provider, and the provider adapter is the only layer that knows how a simulated provider behaves.

## Components and boundaries

```mermaid
flowchart TB
    Client[Client with Idempotency-Key] --> Boundary[FastAPI boundary<br/>Pydantic validation + request ID]
    Boundary --> Service[PaymentService<br/>orchestration and provider-neutral rules]
    Service --> Runtime[Runtime profile<br/>explicit dependency composition]
    Runtime --> Store[(Memory or SQLAlchemy repository<br/>payment + attempt history)]
    Runtime --> Coordination[Memory or Redis<br/>idempotency + rate limits]
    Runtime --> Events[Memory or Kafka<br/>outbox broker]
    Service --> Router[ProviderRouter<br/>configurable strategy + local stats]
    Router --> Breakers[Independent CircuitBreaker per provider]
    Breakers --> Stripe[MockStripeProvider]
    Breakers --> Adyen[MockAdyenProvider]
    Breakers --> Razorpay[MockRazorpayProvider]
    Admin[Local admin API<br/>X-Admin-Token] --> Stripe
    Admin --> Adyen
    Admin --> Razorpay
```

The boundary owns HTTP concerns and maps expected domain errors to the common error envelope. `PaymentService` owns idempotency locking, provider ordering, retry policy, failover policy, and payment state. `ProviderRouter` uses only provider names, health-filtered candidates, and bounded request observations; it does not inspect provider-specific behavior. `CircuitBreaker` instances are keyed by provider name, so an outage in one adapter does not open another adapter's circuit. The three adapters implement the same asynchronous protocol and return provider-neutral results to the service.

Routing records a bounded recent window per provider: request success, request
latency, and current in-flight count. Weighted round robin uses
`max(0.05, recent_success_rate)` as its weight. Lowest latency minimizes
`average_latency_ms`, highest success rate maximizes the recent success rate,
and composite maximizes:

```text
success_rate^success_weight
  / (1 + average_latency_ms / 1000)^latency_weight
  / (1 + inflight)^load_weight
  / (1 + recent_failure_rate)^recent_failure_weight
```

The defaults are `(success_weight, latency_weight, load_weight,
recent_failure_weight) = (1, 1, 1, 0)`, which preserve the original score.
Weights are bounded to 0 through 5 and can be selected through environment
settings or the authenticated routing admin endpoint.

Empty candidate sets still raise `No healthy payment provider is available`.
These observations are process-local and come from the mock providers in the
default build; they are not production telemetry or a load-balancing result.

The default profile is intentionally process-local. `build_runtime(Settings)`
can select the SQLAlchemy repository, Redis coordinators, and Kafka broker
explicitly through environment variables; it never silently falls back to
memory after an external profile is selected. The optional adapters are
constructed without network calls, while migrations, row-lock behavior,
cross-process coordination, and live broker delivery still require an
integration environment.

## Payment data flow

```mermaid
sequenceDiagram
    participant C as Client
    participant A as FastAPI API
    participant S as PaymentService
    participant R as Router
    participant B as Provider circuit
    participant P as Mock provider
    participant D as Payment store
    C->>A: POST payment + Idempotency-Key
    A->>S: validated PaymentInput
    S->>D: look up merchant/key
    alt existing payment
        D-->>S: stored final payment
        S-->>A: same provider-neutral response
    else new operation
        S->>R: choose healthy provider
        R-->>S: provider order
        loop bounded retry budget
            S->>B: request circuit permit
            B-->>S: permit or open-circuit rejection
            S->>P: create_payment synthetic token
            P-->>S: success or typed ProviderError
            S->>D: append attempt
        end
        alt transient unavailable after retries
            S->>R: select alternate provider
            R-->>S: next healthy provider
            S->>P: create_payment on alternate
            P-->>S: success
        else ambiguous timeout
            S-->>A: PROVIDER_TIMEOUT after original-provider retries
        end
        S->>D: save final payment and attempt history
        S-->>A: provider-neutral response
    end
    A-->>C: payment id, status, request id
```

Provider names and tokens stay out of the client payment response. Attempt history is retained internally so a later durable repository can expose audit evidence without changing the public payment contract.

## Data model

The optional SQLAlchemy model and Alembic revision mirror this shape. The running default still uses the equivalent Python dataclasses in the in-memory repository.

```mermaid
erDiagram
    MERCHANT ||--o{ PAYMENT : owns
    PAYMENT ||--o{ PAYMENT_ATTEMPT : records
    PAYMENT ||--o{ REFUND : has
    PAYMENT ||--o{ OUTBOX_EVENT : emits
    MERCHANT {
        string id PK
        datetime created_at
    }
    PAYMENT {
        uuid id PK
        string merchant_id FK
        int amount
        string currency
        string status
        string idempotency_key
        string request_fingerprint
        string provider
        int refunded_amount
        datetime created_at
    }
    PAYMENT_ATTEMPT {
        uuid id PK
        uuid payment_id FK
        string provider
        int attempt_number
        string result
        string error_code
        datetime created_at
    }
    REFUND {
        uuid id PK
        uuid payment_id FK
        int amount
        string provider_reference
        datetime created_at
    }
    OUTBOX_EVENT {
        uuid id PK
        uuid aggregate_id
        string event_type
        json payload
        datetime published_at
    }
```

## Payment and refund transaction boundaries

```mermaid
sequenceDiagram
    participant S as PaymentService
    participant R as Repository
    participant P as Original provider
    S->>R: acquire merchant/key or payment lock
    S->>R: read existing payment
    alt new payment
        S->>P: create synthetic payment
        P-->>S: result
        S->>R: save payment + attempts
    else idempotency replay
        R-->>S: stored payment
    end
    S->>R: release payment lock
    S->>R: acquire refund lock
    S->>R: read refunded_amount
    S->>P: refund remaining or requested amount
    P-->>S: refund reference
    S->>R: save refund + updated payment total
    S->>R: release refund lock
```

The repository protocol is the seam for the memory and SQLAlchemy
implementations. The SQLAlchemy adapter writes payment and outbox rows in one
session transaction; live migration, database row-lock, and cross-process
behavior remain integration checks. Redis claims and Kafka publication are
explicit runtime profiles, while durable worker delivery is still deferred.

## Simulated outage timeline

This is a reproducible local simulation from `docs/runbook.md`, not a production incident:

| Point | Simulated action | Expected evidence |
| --- | --- | --- |
| T0 | Set Stripe `server_error_probability` to `1` through the local admin API. | Stripe remains health-checkable but every payment call returns `PROVIDER_UNAVAILABLE`. |
| T1 | Create a payment with a fresh idempotency key. | Stripe receives bounded retries; each attempted call is recorded. |
| T2 | Stripe reaches the configured consecutive-failure threshold. | Only Stripe's circuit changes to `OPEN`. |
| T3 | Retry budget is exhausted. | The service tries Adyen and can return a successful provider-neutral payment. |
| T4 | Read `/api/v1/providers`. | Stripe reports its circuit state; other providers remain independent. |
| T5 | Recover Stripe and restore its config. | The admin recovery resets the local circuit and future routing can include Stripe. |

## Actual-observed implementation notes

These notes describe behavior verified in the implementation and automated tests. They are not claims about a production outage, external latency, or benchmark performance.

- `tests/test_reliability.py::test_service_retries_transient_failure_then_fails_over_with_history` verifies transient failure retries, alternate-provider success, and recorded provider order.
- `tests/test_reliability.py::test_timeout_retries_without_blind_cross_provider_charge` verifies that an ambiguous timeout does not invoke the alternate provider.
- The current implementation uses an in-memory store and in-process locks, which is visible in `store.py` and `service.py`; process restart loses payment history.
- `pytest -q` and `compileall` are the release checks for this local slice; no production incident or load benchmark is represented here.
