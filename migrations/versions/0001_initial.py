"""Create the initial PySwitch payment tables."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table("merchants", sa.Column("id", sa.String(100), primary_key=True), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False))
    op.create_table(
        "payments",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("merchant_id", sa.String(100), sa.ForeignKey("merchants.id"), nullable=False),
        sa.Column("amount", sa.Integer, nullable=False), sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("status", sa.String(20), nullable=False), sa.Column("payment_method_type", sa.String(30), nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False), sa.Column("provider", sa.String(50)),
        sa.Column("provider_reference", sa.String(255)), sa.Column("refunded_amount", sa.Integer, nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False), sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("merchant_id", "idempotency_key", name="uq_payments_merchant_idempotency"),
    )
    op.create_index("ix_payments_merchant_created", "payments", ["merchant_id", "created_at"])
    op.create_table(
        "payment_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True), sa.Column("payment_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("payments.id"), nullable=False),
        sa.Column("provider", sa.String(50), nullable=False), sa.Column("attempt_number", sa.Integer, nullable=False), sa.Column("result", sa.String(30), nullable=False),
        sa.Column("error_code", sa.String(50)), sa.Column("latency_ms", sa.Integer, nullable=False), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_payment_attempts_payment", "payment_attempts", ["payment_id"])
    op.create_table(
        "refunds",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True), sa.Column("payment_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("payments.id"), nullable=False),
        sa.Column("amount", sa.Integer, nullable=False), sa.Column("provider_reference", sa.String(255)), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True), sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("aggregate_id", postgresql.UUID(as_uuid=True)), sa.Column("payload", postgresql.JSONB, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False), sa.Column("published_at", sa.DateTime(timezone=True)),
    )


def downgrade() -> None:
    op.drop_table("outbox_events")
    op.drop_table("refunds")
    op.drop_index("ix_payment_attempts_payment", table_name="payment_attempts")
    op.drop_table("payment_attempts")
    op.drop_index("ix_payments_merchant_created", table_name="payments")
    op.drop_table("payments")
    op.drop_table("merchants")

