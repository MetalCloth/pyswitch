# ADR 0002: Provider-neutral orchestration and routing

Status: accepted for the local simulation.

## Context

Clients need one stable payment contract while provider behavior, failures, and routing policy change independently. Provider-specific rules inside the API would make failover inconsistent and would expose implementation details to callers.

## Decision

The API converts validated input into `PaymentInput` and delegates provider selection and execution to `PaymentService`. The service depends on the asynchronous `PaymentProvider` protocol (`create_payment`, `refund_payment`, `get_payment_status`, and `health_check`). Stripe, Adyen, and Razorpay simulations implement that protocol behind separate adapters.

The router receives the currently health-checkable provider list and chooses round robin. The service keeps the selected provider first, then tries other eligible providers only for explicitly failover-safe errors: provider unavailable, HTTP 502/503, or an open circuit. A timeout remains tied to the original provider after its retry budget because the result can be ambiguous. The service records each provider attempt and returns a provider-neutral payment response.

Circuit state is held separately for each provider. Retry classification and circuit transitions live in reliability primitives rather than in provider adapters or HTTP handlers. This keeps the provider adapter responsible for simulation behavior and keeps orchestration rules testable with deterministic doubles.

## Alternatives considered

- Selecting a provider directly in each route was rejected because it duplicates business rules and makes sibling endpoints diverge.
- Sending every failure to the next provider was rejected because an ambiguous timeout could result in two provider-side charges.
- Embedding routing decisions in each mock adapter was rejected because adapters must remain swappable test doubles.

## Consequences

The client contract remains stable while routing and failure policy evolve. Attempt history provides evidence for a later audit/outbox implementation. The current round-robin policy is intentionally small; adaptive routing strategies and durable provider health snapshots belong to later milestones. The current in-memory store and process-local locks limit guarantees to one running process.

