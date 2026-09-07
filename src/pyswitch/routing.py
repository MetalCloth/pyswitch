from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import ceil, isfinite
from typing import Literal

from .providers.base import PaymentProvider


RoutingStrategy = Literal[
    "round_robin",
    "weighted_round_robin",
    "lowest_latency",
    "highest_success_rate",
    "composite",
]
ROUTING_STRATEGIES: tuple[RoutingStrategy, ...] = (
    "round_robin",
    "weighted_round_robin",
    "lowest_latency",
    "highest_success_rate",
    "composite",
)
MAX_COMPOSITE_WEIGHT = 5.0


@dataclass(frozen=True, slots=True)
class CompositeWeights:
    """Bounded exponents for the provider-neutral composite score."""

    success: float = 1.0
    latency: float = 1.0
    load: float = 1.0
    recent_failure: float = 0.0

    def __post_init__(self) -> None:
        for name, value in (
            ("success", self.success),
            ("latency", self.latency),
            ("load", self.load),
            ("recent_failure", self.recent_failure),
        ):
            if not isfinite(value) or not 0 <= value <= MAX_COMPOSITE_WEIGHT:
                raise ValueError(f"composite {name} weight must be between 0 and {MAX_COMPOSITE_WEIGHT}")

    def as_dict(self) -> dict[str, float]:
        return {
            "success": self.success,
            "latency": self.latency,
            "load": self.load,
            "recent_failure": self.recent_failure,
        }


@dataclass(slots=True)
class ProviderStats:
    """Bounded local observations used by provider-neutral routing strategies."""

    window_size: int = 20
    recent: deque[tuple[bool, float]] = field(default_factory=deque)
    inflight: int = 0
    total_requests: int = 0
    successes: int = 0
    failures: int = 0
    timeouts: int = 0
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None

    def start(self) -> None:
        self.inflight += 1

    def finish(self, *, success: bool, latency_ms: float, error_code: str | None = None) -> None:
        self.inflight = max(0, self.inflight - 1)
        self.total_requests += 1
        if success:
            self.successes += 1
            self.last_success_at = datetime.now(timezone.utc)
        else:
            self.failures += 1
            self.last_failure_at = datetime.now(timezone.utc)
            if error_code == "PROVIDER_TIMEOUT":
                self.timeouts += 1
        self.recent.append((success, max(0.0, latency_ms)))
        while len(self.recent) > self.window_size:
            self.recent.popleft()

    @property
    def success_rate(self) -> float:
        return sum(success for success, _ in self.recent) / len(self.recent) if self.recent else 1.0

    @property
    def recent_failure_rate(self) -> float:
        return 1.0 - self.success_rate

    @property
    def average_latency_ms(self) -> float:
        return sum(latency for _, latency in self.recent) / len(self.recent) if self.recent else 0.0

    @property
    def p95_latency_ms(self) -> float:
        if not self.recent:
            return 0.0
        latencies = sorted(latency for _, latency in self.recent)
        return latencies[max(0, ceil(len(latencies) * 0.95) - 1)]


class ProviderRouter:
    def __init__(
        self,
        strategy: RoutingStrategy = "round_robin",
        *,
        window_size: int = 20,
        composite_weights: CompositeWeights | None = None,
    ) -> None:
        if strategy not in ROUTING_STRATEGIES:
            choices = ", ".join(ROUTING_STRATEGIES)
            raise ValueError(f"routing strategy must be one of: {choices}; got {strategy!r}")
        if window_size < 1:
            raise ValueError("routing stats window_size must be positive")
        self.strategy = strategy
        self._next = 0
        self._weighted_current: dict[str, float] = {}
        self.stats: dict[str, ProviderStats] = {}
        self.window_size = window_size
        self.composite_weights = composite_weights or CompositeWeights()

    def set_strategy(self, strategy: RoutingStrategy) -> None:
        if strategy not in ROUTING_STRATEGIES:
            choices = ", ".join(ROUTING_STRATEGIES)
            raise ValueError(f"routing strategy must be one of: {choices}; got {strategy!r}")
        self.strategy = strategy

    def set_composite_weights(self, weights: CompositeWeights) -> None:
        self.composite_weights = weights

    def _stats_for(self, provider: PaymentProvider) -> ProviderStats:
        return self.stats.setdefault(provider.name, ProviderStats(self.window_size))

    def start_request(self, provider: PaymentProvider) -> None:
        self._stats_for(provider).start()

    def finish_request(
        self,
        provider: PaymentProvider,
        *,
        success: bool,
        latency_ms: float,
        error_code: str | None = None,
    ) -> None:
        self._stats_for(provider).finish(success=success, latency_ms=latency_ms, error_code=error_code)

    async def choose(self, providers: Sequence[PaymentProvider]) -> PaymentProvider:
        if not providers:
            raise LookupError("No healthy payment provider is available")
        if self.strategy == "round_robin":
            return self._round_robin(providers)
        if self.strategy == "weighted_round_robin":
            return self._weighted_round_robin(providers)
        if self.strategy == "lowest_latency":
            return min(providers, key=lambda provider: self._latency_key(provider, providers))
        if self.strategy == "highest_success_rate":
            return max(providers, key=lambda provider: self._success_key(provider, providers))
        return max(providers, key=lambda provider: self._composite_score(provider))

    def _round_robin(self, providers: Sequence[PaymentProvider]) -> PaymentProvider:
        provider = providers[self._next % len(providers)]
        self._next += 1
        return provider

    def _weight(self, provider: PaymentProvider) -> float:
        return max(0.05, self._stats_for(provider).success_rate)

    def _weighted_round_robin(self, providers: Sequence[PaymentProvider]) -> PaymentProvider:
        total = 0.0
        selected = providers[0]
        selected_score = float("-inf")
        for provider in providers:
            weight = self._weight(provider)
            total += weight
            score = self._weighted_current.get(provider.name, 0.0) + weight
            self._weighted_current[provider.name] = score
            if score > selected_score:
                selected, selected_score = provider, score
        self._weighted_current[selected.name] -= total
        return selected

    def _latency_key(self, provider: PaymentProvider, providers: Sequence[PaymentProvider]) -> tuple[float, float, int]:
        stats = self._stats_for(provider)
        return (stats.average_latency_ms if stats.recent else float("inf"), -stats.success_rate, providers.index(provider))

    def _success_key(self, provider: PaymentProvider, providers: Sequence[PaymentProvider]) -> tuple[float, float, int, int]:
        stats = self._stats_for(provider)
        return (stats.success_rate, -stats.average_latency_ms, -stats.inflight, -providers.index(provider))

    def _composite_score(self, provider: PaymentProvider) -> float:
        stats = self._stats_for(provider)
        weights = self.composite_weights
        return (
            stats.success_rate**weights.success
            / (1.0 + stats.average_latency_ms / 1000.0) ** weights.latency
            / (1.0 + stats.inflight) ** weights.load
            / (1.0 + stats.recent_failure_rate) ** weights.recent_failure
        )


class RoundRobinRouter(ProviderRouter):
    """Compatibility name for callers that explicitly want default routing."""

    def __init__(self) -> None:
        super().__init__("round_robin")
