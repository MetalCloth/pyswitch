import asyncio
from uuid import UUID

from .domain import Payment


class InMemoryPaymentStore:
    """P1 persistence seam; PostgreSQL/SQLAlchemy replaces this in P0/P4."""

    def __init__(self) -> None:
        self._payments: dict[UUID, Payment] = {}
        self._keys: dict[tuple[str, str], UUID] = {}
        self._lock = asyncio.Lock()

    async def get_by_idempotency(self, merchant_id: str, key: str) -> Payment | None:
        async with self._lock:
            payment_id = self._keys.get((merchant_id, key))
            return self._payments.get(payment_id) if payment_id else None

    async def save(self, payment: Payment) -> None:
        async with self._lock:
            self._payments[payment.id] = payment
            self._keys[(payment.merchant_id, payment.idempotency_key)] = payment.id

    async def get(self, payment_id: UUID) -> Payment | None:
        async with self._lock:
            return self._payments.get(payment_id)

    async def list(self, merchant_id: str | None = None) -> list[Payment]:
        async with self._lock:
            values = list(self._payments.values())
        return [p for p in values if merchant_id is None or p.merchant_id == merchant_id]

