from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from uuid import UUID, uuid4


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PaymentStatus(StrEnum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"


@dataclass(slots=True)
class PaymentAttempt:
    provider: str
    attempt_number: int
    result: str
    latency_ms: float
    created_at: datetime = field(default_factory=utcnow)
    error_code: str | None = None


@dataclass(slots=True)
class Refund:
    payment_id: UUID
    amount: int
    provider_reference: str | None = None
    id: UUID = field(default_factory=uuid4)
    created_at: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class Payment:
    merchant_id: str
    amount: int
    currency: str
    payment_method_type: str
    idempotency_key: str
    id: UUID = field(default_factory=uuid4)
    status: PaymentStatus = PaymentStatus.PROCESSING
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)
    attempts: list[PaymentAttempt] = field(default_factory=list)
    provider_reference: str | None = None
    provider: str | None = None
    request_fingerprint: str = ""
    refunded_amount: int = 0
    refunds: list[Refund] = field(default_factory=list)
