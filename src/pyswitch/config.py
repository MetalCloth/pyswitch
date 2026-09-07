from dataclasses import dataclass
import os
from typing import Literal, cast

from .routing import ROUTING_STRATEGIES, RoutingStrategy


StorageBackend = Literal["memory", "postgres"]
CoordinationBackend = Literal["memory", "redis"]
EventBackend = Literal["memory", "kafka"]

DEFAULT_DATABASE_URL = "postgresql+asyncpg://pyswitch:pyswitch@postgres:5432/pyswitch"
DEFAULT_REDIS_URL = "redis://redis:6379/0"
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "redpanda:9092"
DEFAULT_KAFKA_TOPIC = "pyswitch.events"


def _backend(name: str, value: str, allowed: tuple[str, ...]) -> str:
    if value not in allowed:
        choices = ", ".join(allowed)
        raise ValueError(f"{name} must be one of: {choices}; got {value!r}")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    storage_backend: StorageBackend = "memory"
    coordination_backend: CoordinationBackend = "memory"
    event_backend: EventBackend = "memory"
    database_url: str = DEFAULT_DATABASE_URL
    redis_url: str = DEFAULT_REDIS_URL
    kafka_bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP_SERVERS
    kafka_topic: str = DEFAULT_KAFKA_TOPIC
    routing_strategy: RoutingStrategy = "round_robin"
    supported_currencies: frozenset[str] = frozenset({"INR", "USD", "EUR"})
    request_timeout_seconds: float = 5.0
    admin_token: str = "local-dev-only"
    rate_limit_capacity: int = 60
    rate_limit_refill_per_second: float = 1.0
    provider_concurrency_limit: int = 0

    @classmethod
    def from_env(cls) -> "Settings":
        currencies = os.getenv("PYSWITCH_SUPPORTED_CURRENCIES", "INR,USD,EUR")
        storage_backend = _backend(
            "PYSWITCH_STORAGE_BACKEND",
            os.getenv("PYSWITCH_STORAGE_BACKEND", "memory"),
            ("memory", "postgres"),
        )
        coordination_backend = _backend(
            "PYSWITCH_COORDINATION_BACKEND",
            os.getenv("PYSWITCH_COORDINATION_BACKEND", "memory"),
            ("memory", "redis"),
        )
        event_backend = _backend(
            "PYSWITCH_EVENT_BACKEND",
            os.getenv("PYSWITCH_EVENT_BACKEND", "memory"),
            ("memory", "kafka"),
        )
        database_url = os.getenv("PYSWITCH_DATABASE_URL", DEFAULT_DATABASE_URL)
        redis_url = os.getenv("PYSWITCH_REDIS_URL", DEFAULT_REDIS_URL)
        kafka_bootstrap_servers = os.getenv("PYSWITCH_KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS)
        kafka_topic = os.getenv("PYSWITCH_KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC)
        if storage_backend == "postgres" and not database_url:
            raise ValueError("PYSWITCH_DATABASE_URL is required when PYSWITCH_STORAGE_BACKEND=postgres")
        if coordination_backend == "redis" and not redis_url:
            raise ValueError("PYSWITCH_REDIS_URL is required when PYSWITCH_COORDINATION_BACKEND=redis")
        if event_backend == "kafka" and not kafka_bootstrap_servers:
            raise ValueError("PYSWITCH_KAFKA_BOOTSTRAP_SERVERS is required when PYSWITCH_EVENT_BACKEND=kafka")
        provider_concurrency_limit = int(os.getenv("PYSWITCH_PROVIDER_CONCURRENCY_LIMIT", "0"))
        if provider_concurrency_limit < 0:
            raise ValueError("PYSWITCH_PROVIDER_CONCURRENCY_LIMIT must be zero or positive")
        return cls(
            storage_backend=cast(StorageBackend, storage_backend),
            coordination_backend=cast(CoordinationBackend, coordination_backend),
            event_backend=cast(EventBackend, event_backend),
            database_url=database_url,
            redis_url=redis_url,
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            kafka_topic=kafka_topic,
            routing_strategy=cast(
                RoutingStrategy,
                _backend(
                    "PYSWITCH_ROUTING_STRATEGY",
                    os.getenv("PYSWITCH_ROUTING_STRATEGY", "round_robin"),
                    ROUTING_STRATEGIES,
                ),
            ),
            supported_currencies=frozenset(c.strip().upper() for c in currencies.split(",") if c.strip()),
            request_timeout_seconds=float(os.getenv("PYSWITCH_REQUEST_TIMEOUT_SECONDS", "5")),
            admin_token=os.getenv("PYSWITCH_ADMIN_TOKEN", "local-dev-only"),
            rate_limit_capacity=int(os.getenv("PYSWITCH_RATE_LIMIT_CAPACITY", "60")),
            rate_limit_refill_per_second=float(os.getenv("PYSWITCH_RATE_LIMIT_REFILL_PER_SECOND", "1")),
            provider_concurrency_limit=provider_concurrency_limit,
        )
