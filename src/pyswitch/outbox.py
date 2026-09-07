import asyncio
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
    def __init__(
        self,
        outbox: OutboxRepository,
        broker: EventBroker,
        *,
        poll_interval_seconds: float = 0.25,
        max_attempts: int = 3,
        initial_backoff_seconds: float = 0.1,
        max_backoff_seconds: float = 1.0,
    ) -> None:
        if poll_interval_seconds <= 0 or max_attempts < 1 or initial_backoff_seconds < 0 or max_backoff_seconds < 0:
            raise ValueError("Outbox worker timing and attempts must be non-negative and bounded")
        self.outbox = outbox
        self.broker = broker
        self.poll_interval_seconds = poll_interval_seconds
        self.max_attempts = max_attempts
        self.initial_backoff_seconds = initial_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def dispatch_once(self, limit: int = 100) -> int:
        published = 0
        for event in await self.outbox.unsent(limit):
            last_error: Exception | None = None
            for attempt in range(self.max_attempts):
                try:
                    await self.broker.publish(event)
                    await self.outbox.mark_published(event.id)
                    published += 1
                    last_error = None
                    break
                except Exception as exc:
                    last_error = exc
                    if attempt == self.max_attempts - 1:
                        break
                    delay = min(self.max_backoff_seconds, self.initial_backoff_seconds * (2**attempt))
                    await asyncio.sleep(delay)
            if last_error is not None:
                raise last_error
        return published

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.dispatch_once()
            except Exception:
                pass
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.poll_interval_seconds)
            except asyncio.TimeoutError:
                pass

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self.run(), name="pyswitch-outbox-worker")

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        self._stop.set()
        if not task.done():
            task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None
