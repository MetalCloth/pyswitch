import pytest
from httpx import ASGITransport, AsyncClient

from pyswitch.api import create_app
from pyswitch.config import Settings
from pyswitch.providers.mock import MockProvider
from pyswitch.routing import CompositeWeights, ProviderRouter, ProviderStats
from pyswitch.service import PaymentInput, PaymentService
from pyswitch.store import InMemoryPaymentStore


def providers():
    return [MockProvider("a"), MockProvider("b"), MockProvider("c")]


async def choose(router, items, count=1):
    return [(await router.choose(items)).name for _ in range(count)]


@pytest.mark.asyncio
async def test_round_robin_preserves_default_order():
    assert await choose(ProviderRouter("round_robin"), providers(), 4) == ["a", "b", "c", "a"]


@pytest.mark.asyncio
async def test_weighted_round_robin_uses_recent_success_rate():
    items = providers()[:2]
    router = ProviderRouter("weighted_round_robin")
    for _ in range(4):
        router.finish_request(items[0], success=True, latency_ms=10)
    router.finish_request(items[1], success=False, latency_ms=10)
    selected = await choose(router, items, 100)
    assert selected.count("a") == 95
    assert selected.count("b") == 5


@pytest.mark.asyncio
async def test_lowest_latency_and_highest_success_rate_are_deterministic():
    items = providers()[:2]
    router = ProviderRouter("lowest_latency")
    for _ in range(3):
        router.finish_request(items[0], success=True, latency_ms=100)
        router.finish_request(items[1], success=True, latency_ms=20)
    assert await choose(router, items) == ["b"]

    router.set_strategy("highest_success_rate")
    for _ in range(3):
        router.finish_request(items[0], success=True, latency_ms=100)
        router.finish_request(items[1], success=False, latency_ms=20)
    assert await choose(router, items) == ["a"]


@pytest.mark.asyncio
async def test_composite_balances_success_latency_and_inflight():
    items = providers()[:2]
    router = ProviderRouter("composite")
    for _ in range(4):
        router.finish_request(items[0], success=True, latency_ms=100)
        router.finish_request(items[1], success=True, latency_ms=500)
    router.start_request(items[0])
    assert await choose(router, items) == ["b"]
    router.finish_request(items[0], success=True, latency_ms=100)
    assert await choose(router, items) == ["a"]


@pytest.mark.asyncio
async def test_composite_weights_can_emphasize_recent_failures():
    items = providers()[:2]
    router = ProviderRouter(
        "composite",
        composite_weights=CompositeWeights(success=0, latency=0, load=0, recent_failure=5),
    )
    router.finish_request(items[0], success=True, latency_ms=10)
    router.finish_request(items[1], success=False, latency_ms=10)
    assert await choose(router, items) == ["a"]


@pytest.mark.asyncio
async def test_empty_and_all_unhealthy_provider_sets_fail_cleanly():
    with pytest.raises(LookupError, match="No healthy"):
        await ProviderRouter().choose([])
    failing = providers()
    for provider in failing:
        provider.configure(forced_failure=True)
    service = PaymentService(failing, InMemoryPaymentStore())
    with pytest.raises(LookupError, match="No healthy"):
        await service.create(PaymentInput("merchant", 100, "INR", "card", "test_card", "routing-empty"))


def test_routing_strategy_is_validated_from_environment(monkeypatch):
    monkeypatch.setenv("PYSWITCH_ROUTING_STRATEGY", "random")
    with pytest.raises(ValueError, match="PYSWITCH_ROUTING_STRATEGY.*composite"):
        Settings.from_env()


def test_composite_weights_are_bounded_in_settings(monkeypatch):
    monkeypatch.setenv("PYSWITCH_ROUTING_COMPOSITE_SUCCESS_WEIGHT", "2.5")
    monkeypatch.setenv("PYSWITCH_ROUTING_COMPOSITE_RECENT_FAILURE_WEIGHT", "5")
    settings = Settings.from_env()
    assert settings.composite_weights.success == 2.5
    assert settings.composite_weights.recent_failure == 5

    monkeypatch.setenv("PYSWITCH_ROUTING_COMPOSITE_LOAD_WEIGHT", "5.1")
    with pytest.raises(ValueError, match="PYSWITCH_ROUTING_COMPOSITE_LOAD_WEIGHT"):
        Settings.from_env()


def test_settings_composite_weights_are_wired_into_service_router():
    weights = CompositeWeights(success=2, latency=3, load=1, recent_failure=4)
    app = create_app(Settings(composite_weights=weights))
    assert app.state.service.router.composite_weights == weights


def test_provider_stats_keep_bounded_health_evidence_and_p95():
    stats = ProviderStats(window_size=3)
    stats.finish(success=True, latency_ms=10)
    stats.finish(success=False, latency_ms=100, error_code="PROVIDER_TIMEOUT")
    stats.finish(success=True, latency_ms=20)
    stats.finish(success=True, latency_ms=40)
    assert (stats.total_requests, stats.successes, stats.failures, stats.timeouts) == (4, 3, 1, 1)
    assert len(stats.recent) == 3
    assert stats.p95_latency_ms == 100
    assert stats.last_success_at is not None
    assert stats.last_failure_at is not None


@pytest.mark.asyncio
async def test_admin_can_change_routing_strategy_using_goal_path():
    app = create_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put(
            "/api/v1/admin/routing-strategy",
            headers={"X-Admin-Token": "local-dev-only"},
            json={"strategy": "lowest_latency"},
        )
    assert response.status_code == 200
    assert response.json() == {"strategy": "lowest_latency"}


@pytest.mark.asyncio
async def test_admin_can_configure_bounded_composite_weights():
    app = create_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put(
            "/api/v1/admin/routing-strategy",
            headers={"X-Admin-Token": "local-dev-only"},
            json={
                "strategy": "composite",
                "composite_latency_weight": 2.5,
                "composite_recent_failure_weight": 4,
            },
        )
        invalid = await client.put(
            "/api/v1/admin/routing-strategy",
            headers={"X-Admin-Token": "local-dev-only"},
            json={"strategy": "composite", "composite_load_weight": 5.1},
        )
    assert response.status_code == 200
    assert response.json() == {
        "strategy": "composite",
        "composite_weights": {"success": 1.0, "latency": 2.5, "load": 1.0, "recent_failure": 4.0},
    }
    assert invalid.status_code == 422
