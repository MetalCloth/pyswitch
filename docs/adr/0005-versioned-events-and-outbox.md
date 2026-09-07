# ADR 0005: Versioned events and transactional outbox

Status: accepted for the local simulation.

## Decision

Use a typed `EventEnvelope` with a version number, UUID, event type, merchant ID, optional aggregate/payment ID, optional provider, UTC timestamp, and JSON payload. Payment and refund state changes create the appropriate version-1 events. The repository seam exposes `save_with_events` so the local implementation stores authoritative state and event rows under one lock; a PostgreSQL adapter will map this operation to one database transaction.

Use an `OutboxDispatcher` that publishes unsent rows to an `EventBroker` and marks each row published only after broker acknowledgement. If publication fails, the row remains unsent for a later attempt. Consumers deduplicate by event UUID before applying their effect. Audit, analytics, and notification consumers therefore tolerate broker redelivery.

`InMemoryBroker` is the default test double. `KafkaBroker` is an optional aiokafka adapter for a Redpanda/Kafka-compatible broker and starts during the application lifespan when the explicit Kafka profile is selected. No external event effect is executed by the local consumer implementations.

## Consequences and deferred work

The event contract and failure behavior are testable without network services. The default store is process-local and does not provide durable outbox delivery. PostgreSQL transaction wiring, Redpanda deployment, retry/dead-letter policy, and live broker failure testing remain deferred in the default path. The opt-in `scripts/kafka_consumer_evidence.py` probe records one local consumer-group offset commit/restart result; long-lived consumer operations and external side effects remain deferred.
