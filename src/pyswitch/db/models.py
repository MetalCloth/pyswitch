from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class MerchantRow(Base):
    __tablename__ = "merchants"

    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payments: Mapped[list["PaymentRow"]] = relationship(back_populates="merchant")


class PaymentRow(Base):
    __tablename__ = "payments"
    __table_args__ = (
        UniqueConstraint("merchant_id", "idempotency_key", name="uq_payments_merchant_idempotency"),
        Index("ix_payments_merchant_created", "merchant_id", "created_at"),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    merchant_id: Mapped[str] = mapped_column(ForeignKey("merchants.id"), nullable=False)
    amount: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    payment_method_type: Mapped[str] = mapped_column(String(30), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    provider: Mapped[str | None] = mapped_column(String(50))
    provider_reference: Mapped[str | None] = mapped_column(String(255))
    refunded_amount: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    merchant: Mapped[MerchantRow] = relationship(back_populates="payments")
    attempts: Mapped[list["PaymentAttemptRow"]] = relationship(back_populates="payment")
    refunds: Mapped[list["RefundRow"]] = relationship(back_populates="payment")


class PaymentAttemptRow(Base):
    __tablename__ = "payment_attempts"
    __table_args__ = (Index("ix_payment_attempts_payment", "payment_id"),)

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id"), nullable=False)
    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    result: Mapped[str] = mapped_column(String(30), nullable=False)
    error_code: Mapped[str | None] = mapped_column(String(50))
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payment: Mapped[PaymentRow] = relationship(back_populates="attempts")


class RefundRow(Base):
    __tablename__ = "refunds"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id"), nullable=False)
    amount: Mapped[int] = mapped_column(Integer, nullable=False)
    provider_reference: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payment: Mapped[PaymentRow] = relationship(back_populates="refunds")


class OutboxEventRow(Base):
    __tablename__ = "outbox_events"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    aggregate_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
