import pytest

from pyswitch.events import EventType
from pyswitch.service import PaymentInput, PaymentService
from pyswitch.store import InMemoryPaymentStore
from pyswitch.providers.mock import MockStripeProvider


@pytest.mark.asyncio
async def test_payment_and_refund_changes_are_saved_with_outbox_events():
    store = InMemoryPaymentStore()
    service = PaymentService([MockStripeProvider()], store)
    payment = await service.create(PaymentInput("merchant", 100, "INR", "card", "test_card", "events-1"))
    refund = await service.refund(payment.id, 25)
    assert refund.amount == 25
    events = await store.unsent()
    assert [event.event_type for event in events] == [
        EventType.PAYMENT_CREATED,
        EventType.PAYMENT_PROCESSING,
        EventType.PAYMENT_SUCCEEDED,
        EventType.PAYMENT_REFUNDED,
    ]
    assert (await store.get(payment.id)).refunded_amount == 25


@pytest.mark.asyncio
async def test_idempotency_replay_does_not_duplicate_outbox_events():
    store = InMemoryPaymentStore()
    service = PaymentService([MockStripeProvider()], store)
    data = PaymentInput("merchant", 100, "INR", "card", "test_card", "events-replay")
    first = await service.create(data)
    second = await service.create(data)
    assert first.id == second.id
    assert len(await store.unsent()) == 3

