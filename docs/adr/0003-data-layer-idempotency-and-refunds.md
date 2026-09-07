# ADR 0003: Data layer, idempotency fingerprints, and refunds

Status: accepted for the local simulation.

## Decision

`PaymentRepository` defines the persistence contract used by `PaymentService`. `InMemoryPaymentStore` remains the default so the API starts without external services. The explicit PostgreSQL profile uses the SQLAlchemy 2.x models and Alembic `0001_initial` revision for merchants, payments, payment attempts, refunds, and outbox events.

The service computes a canonical SHA-256 fingerprint from merchant, amount, currency, payment-method type, and synthetic token. A merchant plus idempotency key returns the stored payment only when its fingerprint matches. A different fingerprint returns `DUPLICATE_REQUEST` with HTTP 409. The current process-local lock prevents duplicate provider calls inside one process; the PostgreSQL uniqueness constraint and Redis `PROCESSING` claim are available through explicit profiles but are not live-verified here.

Refunds are serialized per payment. A full refund uses the remaining refundable amount; a partial refund must be positive and no larger than that remainder. The original provider is preferred, and the payment becomes `REFUNDED` only when the cumulative refund equals the original amount. The current mock provider and in-memory store make this behavior testable without a network or database.

## Consequences and deferred work

The default profile protects request reuse and over-refund arithmetic in one process while keeping the application runnable. The optional adapters provide the intended persistence and coordination seams, but this repository does not claim live durability, database row-lock semantics, Redis expiry/recovery, or cross-process behavior until those profiles run against their services. Install `.[db]` to apply the SQLAlchemy/Alembic shape; the default `.[dev]` test path remains external-service-free.
