# PySwitch architecture

PySwitch keeps the public contract provider-neutral. The API validates a synthetic payment request, the service selects a provider, and the provider adapter is the only layer that knows how a simulated provider behaves.

## Components and boundaries

```mermaid
flowchart TB
    Client[Client with Idempotency-Key] --> Boundary[FastAPI boundary<br/>Pydantic validation + request ID]
    Boundary --> Service[PaymentService<br/>orchestration and provider-neutral rules]
    Service --> Store[(InMemoryPaymentStore<br/>payment + attempt history)]
    Service --> Router[RoundRobinRouter<br/>healthy provider order]
    Router --> Breakers[Independent CircuitBreaker per provider]
    Breakers --> Stripe[MockStripeProvider]
    Breakers --> Adyen[MockAdyenProvider]
    Breakers --> Razorpay[MockRazorpayProvider]
    Admin[Local admin API<br/>X-Admin-Token] --> Stripe
    Admin --> Adyen
    Admin --> Razorpay
```

The boundary owns HTTP concerns and maps expected domain errors to the common error envelope. `PaymentService` owns idempotency locking, provider ordering, retry policy, failover policy, and payment state. `RoundRobinRouter` does not inspect provider-specific configuration. `CircuitBreaker` instances are keyed by provider name, so an outage in one adapter does not open another adapter's circuit. The three adapters implement the same asynchronous protocol and return provider-neutral results to the service.

The current store is intentionally process-local. It is a seam for the P1 build; PostgreSQL will become authoritative before the system is used outside a local simulation. The current idempotency lock prevents duplicate calls within one process. Redis and a database uniqueness constraint are required for the multi-process invariant in a later milestone.

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

The repository protocol is the seam for replacing these lock-and-save operations with PostgreSQL transactions. Redis claims, database row locks, and an outbox write in the same database transaction are deferred external-service work.

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
