import asyncio
from uuid import UUID

from .domain import Payment
from .events import EventEnvelope


class InMemoryPaymentStore:
    """P1 persistence seam; PostgreSQL/SQLAlchemy replaces this in P0/P4."""

    def __init__(self) -> None:
        self._payments: dict[UUID, Payment] = {}
        self._keys: dict[tuple[str, str], UUID] = {}
        self._events: dict[UUID, EventEnvelope] = {}
        self._published_events: set[UUID] = set()
        self._lock = asyncio.Lock()

    async def get_by_idempotency(self, merchant_id: str, key: str) -> Payment | None:
        async with self._lock:
            payment_id = self._keys.get((merchant_id, key))
            return self._payments.get(payment_id) if payment_id else None

    async def save(self, payment: Payment) -> None:
        async with self._lock:
            self._payments[payment.id] = payment
            self._keys[(payment.merchant_id, payment.idempotency_key)] = payment.id

    async def save_with_events(self, payment: Payment, events: list[EventEnvelope]) -> None:
        """Atomic payment + outbox seam; PostgreSQL will replace this lock with a transaction."""
        async with self._lock:
            self._payments[payment.id] = payment
            self._keys[(payment.merchant_id, payment.idempotency_key)] = payment.id
            for event in events:
                self._events[event.id] = event

    async def append(self, events: list[EventEnvelope]) -> None:
        async with self._lock:
            for event in events:
                self._events[event.id] = event

    async def unsent(self, limit: int = 100) -> list[EventEnvelope]:
        async with self._lock:
            return [event for event_id, event in self._events.items() if event_id not in self._published_events][:limit]

    async def mark_published(self, event_id: UUID) -> None:
        async with self._lock:
            if event_id in self._events:
                self._published_events.add(event_id)

    async def get(self, payment_id: UUID) -> Payment | None:
        async with self._lock:
            return self._payments.get(payment_id)

    async def list(self, merchant_id: str | None = None) -> list[Payment]:
        async with self._lock:
            values = list(self._payments.values())
        return [p for p in values if merchant_id is None or p.merchant_id == merchant_id]
