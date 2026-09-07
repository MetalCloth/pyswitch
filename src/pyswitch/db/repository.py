"""Optional SQLAlchemy repository used by the explicit PostgreSQL profile."""

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select, update
from sqlalchemy.orm import selectinload

from ..domain import Payment, PaymentAttempt, PaymentStatus, Refund
from ..events import EventEnvelope, EventType
from ..repositories import PaymentRepository
from .models import (
    MerchantRow,
    OutboxEventRow,
    PaymentAttemptRow,
    PaymentRow,
    RefundRow,
)


class SqlAlchemyPaymentRepository(PaymentRepository):
    """Small async adapter; connection and migration lifecycle stay with the host."""

    def __init__(self, session_factory: Any) -> None:
        self.session_factory = session_factory

    @staticmethod
    def _payment_options():
        return (
            selectinload(PaymentRow.attempts),
            selectinload(PaymentRow.refunds),
        )

    @staticmethod
    def _to_domain(row: PaymentRow) -> Payment:
        return Payment(
            merchant_id=row.merchant_id,
            amount=row.amount,
            currency=row.currency,
            payment_method_type=row.payment_method_type,
            idempotency_key=row.idempotency_key,
            id=row.id,
            status=PaymentStatus(row.status),
            created_at=row.created_at,
            updated_at=row.updated_at,
            attempts=[
                PaymentAttempt(
                    provider=item.provider,
                    attempt_number=item.attempt_number,
                    result=item.result,
                    latency_ms=float(item.latency_ms),
                    created_at=item.created_at,
                    error_code=item.error_code,
                )
                for item in row.attempts
            ],
            provider_reference=row.provider_reference,
            provider=row.provider,
            request_fingerprint=row.request_fingerprint,
            refunded_amount=row.refunded_amount,
            refunds=[
                Refund(
                    payment_id=item.payment_id,
                    amount=item.amount,
                    provider_reference=item.provider_reference,
                    id=item.id,
                    created_at=item.created_at,
                )
                for item in row.refunds
            ],
        )

    async def _find(self, session: Any, payment_id: UUID) -> PaymentRow | None:
        result = await session.execute(
            select(PaymentRow).options(*self._payment_options()).where(PaymentRow.id == payment_id)
        )
        return result.scalar_one_or_none()

    async def get_by_idempotency(self, merchant_id: str, key: str) -> Payment | None:
        async with self.session_factory() as session:
            result = await session.execute(
                select(PaymentRow)
                .options(*self._payment_options())
                .where(PaymentRow.merchant_id == merchant_id, PaymentRow.idempotency_key == key)
            )
            row = result.scalar_one_or_none()
            return self._to_domain(row) if row else None

    async def get(self, payment_id: UUID) -> Payment | None:
        async with self.session_factory() as session:
            row = await self._find(session, payment_id)
            return self._to_domain(row) if row else None

    async def list(self, merchant_id: str | None = None) -> list[Payment]:
        async with self.session_factory() as session:
            query = select(PaymentRow).options(*self._payment_options()).order_by(PaymentRow.created_at)
            if merchant_id is not None:
                query = query.where(PaymentRow.merchant_id == merchant_id)
            rows = (await session.execute(query)).scalars().all()
            return [self._to_domain(row) for row in rows]

    async def save(self, payment: Payment) -> None:
        await self.save_with_events(payment, [])

    async def save_with_events(self, payment: Payment, events: list[EventEnvelope]) -> None:
        async with self.session_factory() as session:
            async with session.begin():
                merchant = await session.get(MerchantRow, payment.merchant_id)
                if merchant is None:
                    session.add(MerchantRow(id=payment.merchant_id, created_at=payment.created_at))
                row = await self._find(session, payment.id)
                if row is None:
                    row = PaymentRow(
                        id=payment.id,
                        merchant_id=payment.merchant_id,
                        amount=payment.amount,
                        currency=payment.currency,
                        status=payment.status.value,
                        payment_method_type=payment.payment_method_type,
                        idempotency_key=payment.idempotency_key,
                        request_fingerprint=payment.request_fingerprint,
                        provider=payment.provider,
                        provider_reference=payment.provider_reference,
                        refunded_amount=payment.refunded_amount,
                        created_at=payment.created_at,
                        updated_at=payment.updated_at,
                    )
                    session.add(row)
                else:
                    row.status = payment.status.value
                    row.provider = payment.provider
                    row.provider_reference = payment.provider_reference
                    row.refunded_amount = payment.refunded_amount
                    row.updated_at = payment.updated_at
                existing_attempts = set(
                    (item.provider, item.attempt_number, item.created_at) for item in row.attempts
                )
                for item in payment.attempts:
                    marker = (item.provider, item.attempt_number, item.created_at)
                    if marker not in existing_attempts:
                        session.add(
                            PaymentAttemptRow(
                                id=uuid4(),
                                payment_id=payment.id,
                                provider=item.provider,
                                attempt_number=item.attempt_number,
                                result=item.result,
                                error_code=item.error_code,
                                latency_ms=round(item.latency_ms),
                                created_at=item.created_at,
                            )
                        )
                existing_refunds = {item.id for item in row.refunds}
                for item in payment.refunds:
                    if item.id not in existing_refunds:
                        session.add(
                            RefundRow(
                                id=item.id,
                                payment_id=item.payment_id,
                                amount=item.amount,
                                provider_reference=item.provider_reference,
                                created_at=item.created_at,
                            )
                        )
                for event in events:
                    session.add(
                        OutboxEventRow(
                            id=event.id,
                            event_type=event.event_type.value,
                            merchant_id=event.merchant_id,
                            aggregate_id=event.aggregate_id,
                            provider=event.provider,
                            version=event.version,
                            payload=event.payload,
                            created_at=event.occurred_at,
                        )
                    )

    async def append(self, events: list[EventEnvelope]) -> None:
        async with self.session_factory() as session:
            async with session.begin():
                for event in events:
                    session.add(
                        OutboxEventRow(
                            id=event.id,
                            event_type=event.event_type.value,
                            merchant_id=event.merchant_id,
                            aggregate_id=event.aggregate_id,
                            provider=event.provider,
                            version=event.version,
                            payload=event.payload,
                            created_at=event.occurred_at,
                        )
                    )

    async def unsent(self, limit: int = 100) -> list[EventEnvelope]:
        async with self.session_factory() as session:
            rows = (
                await session.execute(
                    select(OutboxEventRow)
                    .where(OutboxEventRow.published_at.is_(None))
                    .order_by(OutboxEventRow.created_at)
                    .limit(limit)
                )
            ).scalars().all()
            return [
                EventEnvelope(
                    event_type=EventType(row.event_type),
                    merchant_id=row.merchant_id,
                    aggregate_id=row.aggregate_id,
                    payload=row.payload,
                    provider=row.provider,
                    version=row.version,
                    id=row.id,
                    occurred_at=row.created_at,
                )
                for row in rows
            ]

    async def mark_published(self, event_id: UUID) -> None:
        async with self.session_factory() as session:
            async with session.begin():
                await session.execute(
                    update(OutboxEventRow)
                    .where(OutboxEventRow.id == event_id)
                    .values(published_at=datetime.now(timezone.utc))
                )
