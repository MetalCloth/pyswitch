#!/usr/bin/env python3
"""Record sanitized API, Prometheus, and Grafana Compose evidence."""

from __future__ import annotations

import argparse
import base64
import json
import platform
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


def fetch(url: str, *, method: str = "GET", body: bytes | None = None,
          headers: dict[str, str] | None = None, timeout: float = 5.0):
    request = Request(url, method=method, data=body, headers=headers or {})
    with urlopen(request, timeout=timeout) as response:
        raw = response.read()
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            value = raw.decode(errors="replace")
        return response.status, value


def wait_for(url: str, timeout: float):
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return fetch(url, timeout=3)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            last = exc
            time.sleep(1)
    raise RuntimeError(f"timed out waiting for {url}: {last}")


def git_commit() -> str | None:
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], check=True,
                              capture_output=True, text=True, timeout=2).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-url", default="http://127.0.0.1:8000")
    parser.add_argument("--prometheus-url", default="http://127.0.0.1:9090")
    parser.add_argument("--grafana-url", default="http://127.0.0.1:3000")
    parser.add_argument("--admin-user", default="admin")
    parser.add_argument("--admin-password", default="admin")
    parser.add_argument("--scrape-wait", type=float, default=12.0)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--output", type=Path, default=Path("benchmark/results/observability-compose.json"))
    args = parser.parse_args()
    if args.scrape_wait < 0 or args.timeout <= 0:
        parser.error("scrape-wait must be non-negative and timeout must be positive")

    checks: dict[str, object] = {}
    status, health = wait_for(f"{args.app_url}/health", args.timeout * 8)
    checks["app_health"] = {"status_code": status, "status": health.get("status") if isinstance(health, dict) else None}
    status, ready = wait_for(f"{args.app_url}/ready", args.timeout * 8)
    checks["app_ready"] = {"status_code": status, "status": ready.get("status") if isinstance(ready, dict) else None}

    payload = json.dumps({
        "merchant_id": "observability-evidence",
        "amount": 125,
        "currency": "INR",
        "payment_method": {"type": "card", "token": "test_card"},
    }).encode()
    status, _ = fetch(
        f"{args.app_url}/api/v1/payments", method="POST", body=payload,
        headers={"Content-Type": "application/json", "Idempotency-Key": "observability-evidence-key"},
        timeout=args.timeout,
    )
    checks["synthetic_payment"] = {"status_code": status, "accepted": status == 201}

    time.sleep(args.scrape_wait)
    status, prom_ready = wait_for(f"{args.prometheus_url}/-/ready", args.timeout * 8)
    checks["prometheus_ready"] = {"status_code": status, "body": prom_ready if isinstance(prom_ready, str) else None}
    status, prom_query = fetch(
        f"{args.prometheus_url}/api/v1/query?query={quote('pyswitch_payments_total', safe='')}",
        timeout=args.timeout,
    )
    series = prom_query.get("data", {}).get("result", []) if isinstance(prom_query, dict) else []
    checks["prometheus_payment_series"] = {"status_code": status, "result_count": len(series)}

    status, grafana_health = wait_for(f"{args.grafana_url}/api/health", args.timeout * 8)
    checks["grafana_health"] = {
        "status_code": status,
        "database": grafana_health.get("database") if isinstance(grafana_health, dict) else None,
        "version": grafana_health.get("version") if isinstance(grafana_health, dict) else None,
    }
    encoded_auth = base64.b64encode(f"{args.admin_user}:{args.admin_password}".encode()).decode()
    auth = {"Authorization": f"Basic {encoded_auth}"}
    status, dashboards = fetch(f"{args.grafana_url}/api/search?query=PySwitch", headers=auth, timeout=args.timeout)
    dashboards = dashboards if isinstance(dashboards, list) else []
    checks["grafana_dashboards"] = {
        "status_code": status,
        "count": len(dashboards),
        "titles": [item.get("title") for item in dashboards if isinstance(item, dict)],
    }
    status, datasource = fetch(f"{args.grafana_url}/api/datasources/uid/prometheus/health", headers=auth, timeout=args.timeout)
    checks["grafana_prometheus_datasource"] = {
        "status_code": status,
        "status": datasource.get("status") if isinstance(datasource, dict) else None,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({
        "schema_version": 1,
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "git_commit": git_commit(),
        "runtime": {"python": platform.python_version(), "platform": platform.platform()},
        "configuration": {
            "app_url": args.app_url,
            "prometheus_url": args.prometheus_url,
            "grafana_url": args.grafana_url,
            "scrape_wait_seconds": args.scrape_wait,
            "synthetic_only": True,
        },
        "checks": checks,
        "privacy": "Synthetic credentials, tokens, idempotency keys, and payment identifiers are omitted; only statuses, counts, and dashboard metadata are recorded.",
    }, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(checks, indent=2))
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
