# ADR 0003: Data layer, idempotency fingerprints, and refunds

Status: accepted for the local simulation.

## Decision

`PaymentRepository` defines the persistence contract used by `PaymentService`. `InMemoryPaymentStore` remains the default so the API starts without external services. Optional SQLAlchemy 2.x models and an Alembic `0001_initial` revision document the planned PostgreSQL shape: merchants, payments, payment attempts, refunds, and outbox events.

The service computes a canonical SHA-256 fingerprint from merchant, amount, currency, payment-method type, and synthetic token. A merchant plus idempotency key returns the stored payment only when its fingerprint matches. A different fingerprint returns `DUPLICATE_REQUEST` with HTTP 409. The current process-local lock prevents duplicate provider calls inside one process; the PostgreSQL uniqueness constraint and Redis `PROCESSING` claim are deferred.

Refunds are serialized per payment. A full refund uses the remaining refundable amount; a partial refund must be positive and no larger than that remainder. The original provider is preferred, and the payment becomes `REFUNDED` only when the cumulative refund equals the original amount. The current mock provider and in-memory store make this behavior testable without a network or database.

## Consequences and deferred work

The interim seam protects request reuse and over-refund arithmetic in one process while keeping the application runnable. It does not provide durability, cross-process idempotency, database row-lock semantics, transactional outbox delivery, or Redis expiry/recovery behavior. Install `.[db]` to inspect or run the SQLAlchemy/Alembic scaffold after PostgreSQL configuration is added; the default `.[dev]` test path does not claim those external services are active.

