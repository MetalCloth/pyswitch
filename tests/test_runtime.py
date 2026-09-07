import pytest

from pyswitch.config import Settings
from pyswitch.idempotency import MemoryIdempotencyCoordinator, RedisIdempotencyCoordinator
from pyswitch.outbox import InMemoryBroker
from pyswitch.rate_limit import InMemoryTokenBucketLimiter, RedisTokenBucketLimiter
from pyswitch.runtime import RuntimeConfigurationError, build_runtime
from pyswitch.store import InMemoryPaymentStore


def test_default_runtime_is_process_local():
    runtime = build_runtime(Settings())
    assert isinstance(runtime.store, InMemoryPaymentStore)
    assert isinstance(runtime.idempotency, MemoryIdempotencyCoordinator)
    assert isinstance(runtime.rate_limiter, InMemoryTokenBucketLimiter)
    assert isinstance(runtime.broker, InMemoryBroker)


def test_environment_selects_explicit_profiles(monkeypatch):
    monkeypatch.setenv("PYSWITCH_STORAGE_BACKEND", "postgres")
    monkeypatch.setenv("PYSWITCH_COORDINATION_BACKEND", "redis")
    monkeypatch.setenv("PYSWITCH_EVENT_BACKEND", "kafka")
    settings = Settings.from_env()
    assert (settings.storage_backend, settings.coordination_backend, settings.event_backend) == (
        "postgres",
        "redis",
        "kafka",
    )
    pytest.importorskip("redis")
    pytest.importorskip("aiokafka")
    runtime = build_runtime(settings)
    assert type(runtime.store).__name__ == "SqlAlchemyPaymentRepository"
    assert isinstance(runtime.idempotency, RedisIdempotencyCoordinator)
    assert isinstance(runtime.rate_limiter, RedisTokenBucketLimiter)
    assert type(runtime.broker).__name__ == "KafkaBroker"


def test_unknown_profile_has_actionable_configuration_error(monkeypatch):
    monkeypatch.setenv("PYSWITCH_COORDINATION_BACKEND", "memcached")
    with pytest.raises(ValueError, match="PYSWITCH_COORDINATION_BACKEND.*memory, redis"):
        Settings.from_env()


def test_missing_external_url_has_actionable_configuration_error(monkeypatch):
    monkeypatch.setenv("PYSWITCH_EVENT_BACKEND", "kafka")
    monkeypatch.setenv("PYSWITCH_KAFKA_BOOTSTRAP_SERVERS", "")
    with pytest.raises(ValueError, match="PYSWITCH_KAFKA_BOOTSTRAP_SERVERS is required"):
        Settings.from_env()


def test_invalid_postgres_url_reports_profile_and_url(monkeypatch):
    settings = Settings(storage_backend="postgres", database_url="not-a-database-url")
    with pytest.raises(RuntimeConfigurationError, match="PostgreSQL storage could not be configured"):
        build_runtime(settings)
