from datetime import datetime
from typing import Protocol
from uuid import UUID

from .domain import Payment


class PaymentRepository(Protocol):
    """Persistence contract implemented by the local seam and future SQLAlchemy adapter."""

    async def get_by_idempotency(self, merchant_id: str, key: str) -> Payment | None: ...
    async def save(self, payment: Payment) -> None: ...
    async def get(self, payment_id: UUID) -> Payment | None: ...
    async def list(
        self,
        merchant_id: str | None = None,
        status: str | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
    ) -> list[Payment]: ...
