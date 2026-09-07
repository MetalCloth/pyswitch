"""Explicit dependency composition for local and optional external profiles."""

from dataclasses import dataclass, field
from typing import Any

from .broker import KafkaBroker
from .config import Settings
from .idempotency import IdempotencyCoordinator, MemoryIdempotencyCoordinator, RedisIdempotencyCoordinator
from .outbox import EventBroker, InMemoryBroker, OutboxRepository
from .rate_limit import InMemoryTokenBucketLimiter, RateLimiter, RedisTokenBucketLimiter
from .repositories import PaymentRepository
from .store import InMemoryPaymentStore


class RuntimeConfigurationError(RuntimeError):
    """Raised when an explicitly selected profile cannot be constructed."""


@dataclass(slots=True)
class Runtime:
    store: PaymentRepository
    idempotency: IdempotencyCoordinator
    rate_limiter: RateLimiter
    outbox: OutboxRepository
    broker: EventBroker
    _resources: list[Any] = field(default_factory=list, repr=False)
    _started: bool = field(default=False, repr=False)

    async def start(self) -> None:
        if isinstance(self.broker, KafkaBroker):
            try:
                await self.broker.start()
            except Exception as exc:
                raise RuntimeConfigurationError("Kafka runtime startup failed; check PYSWITCH_KAFKA_BOOTSTRAP_SERVERS") from exc
        self._started = True

    async def close(self) -> None:
        if isinstance(self.broker, KafkaBroker) and self._started:
            await self.broker.stop()
        for resource in self._resources:
            close = getattr(resource, "aclose", None)
            if close is not None:
                await close()
            else:
                dispose = getattr(resource, "dispose", None)
                if dispose is not None:
                    await dispose()
        self._started = False


def _build_store(settings: Settings, resources: list[Any]) -> PaymentRepository:
    if settings.storage_backend == "memory":
        return InMemoryPaymentStore()
    try:
        from .db.repository import SqlAlchemyPaymentRepository
        from .db.session import session_factory

        maker = session_factory(settings.database_url)
        resources.append(maker.kw["bind"])
        return SqlAlchemyPaymentRepository(maker)
    except ImportError as exc:
        raise RuntimeConfigurationError(
            "PostgreSQL storage requires the optional db dependencies; install pyswitch[db]"
        ) from exc
    except Exception as exc:
        raise RuntimeConfigurationError(
            "PostgreSQL storage could not be configured from PYSWITCH_DATABASE_URL"
        ) from exc


def _build_coordination(settings: Settings, resources: list[Any]) -> tuple[IdempotencyCoordinator, RateLimiter]:
    if settings.coordination_backend == "memory":
        return MemoryIdempotencyCoordinator(), InMemoryTokenBucketLimiter(
            capacity=settings.rate_limit_capacity,
            refill_per_second=settings.rate_limit_refill_per_second,
        )
    try:
        from redis import asyncio as redis

        client = redis.from_url(settings.redis_url)
    except ImportError as exc:
        raise RuntimeConfigurationError(
            "Redis coordination requires the optional redis dependency; install pyswitch[redis]"
        ) from exc
    except Exception as exc:
        raise RuntimeConfigurationError("Redis coordination could not be configured from PYSWITCH_REDIS_URL") from exc
    resources.append(client)
    return (
        RedisIdempotencyCoordinator(client),
        RedisTokenBucketLimiter(
            client,
            capacity=settings.rate_limit_capacity,
            refill_per_second=settings.rate_limit_refill_per_second,
        ),
    )


def _build_events(settings: Settings) -> EventBroker:
    if settings.event_backend == "memory":
        return InMemoryBroker()
    try:
        return KafkaBroker(settings.kafka_bootstrap_servers, settings.kafka_topic)
    except ImportError as exc:
        raise RuntimeConfigurationError(
            "Kafka events require the optional events dependency; install pyswitch[events]"
        ) from exc
    except Exception as exc:
        raise RuntimeConfigurationError("Kafka events could not be configured from PYSWITCH_KAFKA_BOOTSTRAP_SERVERS") from exc


def build_runtime(settings: Settings) -> Runtime:
    """Build all app dependencies from explicit settings; no network calls occur here."""
    resources: list[Any] = []
    store = _build_store(settings, resources)
    idempotency, rate_limiter = _build_coordination(settings, resources)
    broker = _build_events(settings)
    if not hasattr(store, "append"):
        raise RuntimeConfigurationError("Selected storage profile does not provide an outbox repository")
    return Runtime(store, idempotency, rate_limiter, store, broker, resources)
