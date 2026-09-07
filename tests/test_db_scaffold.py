import pytest


sqlalchemy = pytest.importorskip("sqlalchemy")

from pyswitch.db.models import Base, PaymentRow  # noqa: E402


def test_optional_models_define_payment_constraints():
    table = PaymentRow.__table__
    assert {"payments", "payment_attempts", "refunds", "outbox_events", "merchants"} <= set(Base.metadata.tables)
    assert any(
        constraint.name == "uq_payments_merchant_idempotency"
        for constraint in table.constraints
    )
    assert "request_fingerprint" in table.c

