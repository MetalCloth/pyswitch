from uuid import uuid4

import pytest

from pyswitch.domain import Payment
from pyswitch.events import EventEnvelope, EventType
from pyswitch.outbox import BrokerUnavailable, InMemoryBroker, OutboxDispatcher
from pyswitch.store import InMemoryPaymentStore


@pytest.mark.asyncio
async def test_event_envelope_is_versioned_and_serializable():
    payment_id = uuid4()
    event = EventEnvelope(EventType.PAYMENT_CREATED, "merchant", payment_id, {"status": "PROCESSING"})
    value = event.as_dict()
    assert value["event_type"] == "payment.created"
    assert value["version"] == 1
    assert value["aggregate_id"] == str(payment_id)


@pytest.mark.asyncio
async def test_outbox_keeps_events_unsent_when_broker_is_unavailable():
    store = InMemoryPaymentStore()
    payment = Payment("merchant", 100, "INR", "card", "key")
    event = EventEnvelope(EventType.PAYMENT_CREATED, payment.merchant_id, payment.id, {})
    await store.save_with_events(payment, [event])
    broker = InMemoryBroker()
    broker.unavailable = True
    dispatcher = OutboxDispatcher(store, broker)
    with pytest.raises(BrokerUnavailable):
        await dispatcher.dispatch_once()
    assert [saved.id for saved in await store.unsent()] == [event.id]


@pytest.mark.asyncio
async def test_replayed_event_is_harmless_for_idempotent_consumer():
    from pyswitch.consumers import AuditConsumer

    consumer = AuditConsumer()
    event = EventEnvelope(EventType.PAYMENT_SUCCEEDED, "merchant", uuid4(), {})
    assert await consumer.consume(event) is True
    assert await consumer.consume(event) is False
    assert len(consumer.events) == 1

