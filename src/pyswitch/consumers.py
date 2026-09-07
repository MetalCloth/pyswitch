import asyncio
from collections.abc import Callable

from .events import EventEnvelope


class EventConsumer:
    async def consume(self, event: EventEnvelope) -> bool:
        raise NotImplementedError


class IdempotentRecordingConsumer(EventConsumer):
    """Small consumer contract with UUID deduplication before side effects."""

    def __init__(self, effect: Callable[[EventEnvelope], None] | None = None) -> None:
        self.seen: set = set()
        self.events: list[EventEnvelope] = []
        self._lock = asyncio.Lock()
        self._effect = effect

    async def consume(self, event: EventEnvelope) -> bool:
        async with self._lock:
            if event.id in self.seen:
                return False
            self.seen.add(event.id)
            self.events.append(event)
            if self._effect:
                self._effect(event)
            return True


class AuditConsumer(IdempotentRecordingConsumer):
    pass


class AnalyticsConsumer(IdempotentRecordingConsumer):
    pass


class NotificationConsumer(IdempotentRecordingConsumer):
    pass

