from typing import Protocol
from uuid import UUID

from .events import EventEnvelope


class OutboxRepository(Protocol):
    async def append(self, events: list[EventEnvelope]) -> None: ...
    async def unsent(self, limit: int = 100) -> list[EventEnvelope]: ...
    async def mark_published(self, event_id: UUID) -> None: ...



class EventBroker(Protocol):
    async def publish(self, event: EventEnvelope) -> None: ...


class BrokerUnavailable(Exception):
    pass


class InMemoryBroker:
    """Deterministic broker fake; set unavailable to simulate a broker outage."""

    def __init__(self) -> None:
        self.events: list[EventEnvelope] = []
        self.unavailable = False

    async def publish(self, event: EventEnvelope) -> None:
        if self.unavailable:
            raise BrokerUnavailable("Simulated broker outage")
        self.events.append(event)


class OutboxDispatcher:
    def __init__(self, outbox: OutboxRepository, broker: EventBroker) -> None:
        self.outbox = outbox
        self.broker = broker

    async def dispatch_once(self, limit: int = 100) -> int:
        published = 0
        for event in await self.outbox.unsent(limit):
            await self.broker.publish(event)
            await self.outbox.mark_published(event.id)
            published += 1
        return published
