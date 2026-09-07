import json
from pathlib import Path

import pytest


ROOT = Path(__file__).parents[1]


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
