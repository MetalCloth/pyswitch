import json
from typing import Any

from .events import EventEnvelope
from .outbox import EventBroker


class KafkaBroker(EventBroker):
    """Optional aiokafka adapter for Redpanda/Kafka-compatible brokers."""

    def __init__(self, bootstrap_servers: str, topic: str = "pyswitch.events") -> None:
        try:
            from aiokafka import AIOKafkaProducer
        except ImportError as exc:
            raise RuntimeError("Install pyswitch[events] to use KafkaBroker") from exc
        # aiokafka binds its producer to a running event loop. Runtime assembly
        # is intentionally synchronous, so defer construction until startup.
        self._producer_factory = AIOKafkaProducer
        self._bootstrap_servers = bootstrap_servers
        self._producer: Any | None = None
        self.topic = topic

    async def start(self) -> None:
        if self._producer is None:
            self._producer = self._producer_factory(bootstrap_servers=self._bootstrap_servers)
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def publish(self, event: EventEnvelope) -> None:
        if self._producer is None:
            await self.start()
        await self._producer.send_and_wait(self.topic, json.dumps(event.as_dict()).encode())
