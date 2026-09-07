from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4


class EventType(StrEnum):
    PAYMENT_CREATED = "payment.created"
    PAYMENT_PROCESSING = "payment.processing"
    PAYMENT_SUCCEEDED = "payment.succeeded"
    PAYMENT_FAILED = "payment.failed"
    PAYMENT_REFUNDED = "payment.refunded"
    PROVIDER_CIRCUIT_OPENED = "provider.circuit_opened"
    PROVIDER_CIRCUIT_CLOSED = "provider.circuit_closed"


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    event_type: EventType
    merchant_id: str
    aggregate_id: UUID | None
    payload: dict[str, Any]
    provider: str | None = None
    version: int = 1
    id: UUID = field(default_factory=uuid4)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["event_type"] = self.event_type.value
        value["id"] = str(self.id)
        value["aggregate_id"] = str(self.aggregate_id) if self.aggregate_id else None
        value["occurred_at"] = self.occurred_at.isoformat()
        return value

