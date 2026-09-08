import json
from pathlib import Path

import pytest

from pyswitch.config import normalize_database_url


ROOT = Path(__file__).parents[1]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("postgres://user:pass@host/db", "postgresql+asyncpg://user:pass@host/db"),
        ("postgresql://user:pass@host/db", "postgresql+asyncpg://user:pass@host/db"),
        ("postgresql+asyncpg://user:pass@host/db", "postgresql+asyncpg://user:pass@host/db"),
    ],
)
def test_normalize_database_url_for_render_and_sqlalchemy(value, expected):
    assert normalize_database_url(value) == expected


def test_dashboard_json_is_valid_and_has_bounded_panels():
    dashboard = json.loads((ROOT / "grafana/dashboards/pyswitch.json").read_text())
    assert dashboard["title"] == "PySwitch Operations"
    assert len(dashboard["panels"]) == 4


def test_compose_declares_required_services():
    yaml = pytest.importorskip("yaml")
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    assert {"app", "postgres", "redis", "redpanda", "prometheus", "grafana"} <= set(compose["services"])
    assert compose["services"]["app"]["healthcheck"]["test"][0] == "CMD"
    assert compose["services"]["prometheus"]["depends_on"]["app"]["condition"] == "service_healthy"


def test_render_blueprint_declares_public_demo_profile():
    yaml = pytest.importorskip("yaml")
    blueprint = yaml.safe_load((ROOT / "render.yaml").read_text())
    services = {service["name"]: service for service in blueprint["services"]}
    assert {"pyswitch-api", "pyswitch-cache"} <= services.keys()
    assert blueprint["databases"][0]["name"] == "pyswitch-db"
    assert services["pyswitch-api"]["healthCheckPath"] == "/ready"
    assert services["pyswitch-api"]["runtime"] == "docker"
    assert services["pyswitch-cache"]["type"] == "keyvalue"
