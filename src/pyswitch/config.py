from dataclasses import dataclass
import os


@dataclass(frozen=True, slots=True)
class Settings:
    routing_strategy: str = "round_robin"
    supported_currencies: frozenset[str] = frozenset({"INR", "USD", "EUR"})
    request_timeout_seconds: float = 5.0

    @classmethod
    def from_env(cls) -> "Settings":
        currencies = os.getenv("PYSWITCH_SUPPORTED_CURRENCIES", "INR,USD,EUR")
        return cls(
            routing_strategy=os.getenv("PYSWITCH_ROUTING_STRATEGY", "round_robin"),
            supported_currencies=frozenset(c.strip().upper() for c in currencies.split(",") if c.strip()),
            request_timeout_seconds=float(os.getenv("PYSWITCH_REQUEST_TIMEOUT_SECONDS", "5")),
        )

