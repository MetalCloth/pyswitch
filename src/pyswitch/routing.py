from collections.abc import Sequence

from .providers.base import PaymentProvider


class RoundRobinRouter:
    def __init__(self) -> None:
        self._next = 0

    async def choose(self, providers: Sequence[PaymentProvider]) -> PaymentProvider:
        if not providers:
            raise LookupError("No healthy payment provider is available")
        provider = providers[self._next % len(providers)]
        self._next += 1
        return provider

