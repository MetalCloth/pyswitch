import json

from .events import EventEnvelope
from .outbox import EventBroker


class KafkaBroker(EventBroker):
    """Optional aiokafka adapter for Redpanda/Kafka-compatible brokers."""

    def __init__(self, bootstrap_servers: str, topic: str = "pyswitch.events") -> None:
        try:
            from aiokafka import AIOKafkaProducer
        except ImportError as exc:
            raise RuntimeError("Install pyswitch[events] to use KafkaBroker") from exc
        self._producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
        self.topic = topic

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish(self, event: EventEnvelope) -> None:
        await self._producer.send_and_wait(self.topic, json.dumps(event.as_dict()).encode())
