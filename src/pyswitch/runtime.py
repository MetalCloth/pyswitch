"""Explicit dependency composition for local and optional external profiles."""

from dataclasses import dataclass, field
from typing import Any

from .broker import KafkaBroker
from .config import Settings
from .idempotency import IdempotencyCoordinator, MemoryIdempotencyCoordinator, RedisIdempotencyCoordinator
from .outbox import EventBroker, InMemoryBroker, OutboxDispatcher, OutboxRepository
from .rate_limit import InMemoryTokenBucketLimiter, RateLimiter, RedisTokenBucketLimiter
from .repositories import PaymentRepository
from .store import InMemoryPaymentStore


class RuntimeConfigurationError(RuntimeError):
    """Raised when an explicitly selected profile cannot be constructed."""


@dataclass(slots=True)
class Runtime:
    settings: Settings
    store: PaymentRepository
    idempotency: IdempotencyCoordinator
    rate_limiter: RateLimiter
    outbox: OutboxRepository
    broker: EventBroker
    dispatcher: OutboxDispatcher
    _resources: list[Any] = field(default_factory=list, repr=False)
    _started: bool = field(default=False, repr=False)
    _storage_resource: Any | None = field(default=None, repr=False)
    _coordination_resource: Any | None = field(default=None, repr=False)

    async def start(self) -> None:
        if isinstance(self.broker, KafkaBroker):
            try:
                await self.broker.start()
            except Exception as exc:
                raise RuntimeConfigurationError("Kafka runtime startup failed; check PYSWITCH_KAFKA_BOOTSTRAP_SERVERS") from exc
        await self.dispatcher.start()
        self._started = True

    async def close(self) -> None:
        await self.dispatcher.stop()
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

    async def readiness(self) -> dict[str, dict[str, str]]:
        dependencies: dict[str, dict[str, str]] = {}
        if self.settings.storage_backend == "memory":
            dependencies["storage"] = {"backend": "memory", "status": "ready"}
        else:
            try:
                async with self._storage_resource.connect() as connection:
                    from sqlalchemy import text

                    await connection.execute(text("SELECT 1"))
                dependencies["storage"] = {"backend": "postgres", "status": "ready"}
            except Exception as exc:
                dependencies["storage"] = {"backend": "postgres", "status": "unavailable", "error": type(exc).__name__}
        if self.settings.coordination_backend == "memory":
            dependencies["coordination"] = {"backend": "memory", "status": "ready"}
        else:
            try:
                await self._coordination_resource.ping()
                dependencies["coordination"] = {"backend": "redis", "status": "ready"}
            except Exception as exc:
                dependencies["coordination"] = {"backend": "redis", "status": "unavailable", "error": type(exc).__name__}
        if self.settings.event_backend == "memory":
            dependencies["events"] = {"backend": "memory", "status": "ready"}
        else:
            dependencies["events"] = {
                "backend": "kafka",
                "status": "ready" if self._started else "starting",
            }
        return dependencies


def _build_store(settings: Settings) -> tuple[PaymentRepository, Any | None]:
    if settings.storage_backend == "memory":
        return InMemoryPaymentStore(), None
    try:
        from .db.repository import SqlAlchemyPaymentRepository
        from .db.session import session_factory

        maker = session_factory(settings.database_url)
        engine = maker.kw["bind"]
        return SqlAlchemyPaymentRepository(maker), engine
    except ImportError as exc:
        raise RuntimeConfigurationError(
            "PostgreSQL storage requires the optional db dependencies; install pyswitch[db]"
        ) from exc
    except Exception as exc:
        raise RuntimeConfigurationError(
            "PostgreSQL storage could not be configured from PYSWITCH_DATABASE_URL"
        ) from exc


def _build_coordination(settings: Settings) -> tuple[IdempotencyCoordinator, RateLimiter, Any | None]:
    if settings.coordination_backend == "memory":
        return MemoryIdempotencyCoordinator(), InMemoryTokenBucketLimiter(
            capacity=settings.rate_limit_capacity,
            refill_per_second=settings.rate_limit_refill_per_second,
        ), None
    try:
        from redis import asyncio as redis

        client = redis.from_url(settings.redis_url)
    except ImportError as exc:
        raise RuntimeConfigurationError(
            "Redis coordination requires the optional redis dependency; install pyswitch[redis]"
        ) from exc
    except Exception as exc:
        raise RuntimeConfigurationError("Redis coordination could not be configured from PYSWITCH_REDIS_URL") from exc
    return (
        RedisIdempotencyCoordinator(client),
        RedisTokenBucketLimiter(
            client,
            capacity=settings.rate_limit_capacity,
            refill_per_second=settings.rate_limit_refill_per_second,
        ),
        client,
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
    store, storage_resource = _build_store(settings)
    idempotency, rate_limiter, coordination_resource = _build_coordination(settings)
    broker = _build_events(settings)
    if not hasattr(store, "append"):
        raise RuntimeConfigurationError("Selected storage profile does not provide an outbox repository")
    resources = [resource for resource in (storage_resource, coordination_resource) if resource is not None]
    dispatcher = OutboxDispatcher(store, broker)
    return Runtime(settings, store, idempotency, rate_limiter, store, broker, dispatcher, resources, False, storage_resource, coordination_resource)
