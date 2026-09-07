#!/usr/bin/env python3
"""Run bounded, synthetic HTTP load scenarios and write measured evidence.

This is the dependency-free fallback for environments where the optional
Locust package cannot be installed.  Start one PySwitch process first, then
run a scenario against it.  The script records request-level samples without
persisting idempotency keys or payment identifiers.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import subprocess
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx


PAYMENT_PATH = "/api/v1/payments"
PAYMENT_BODY = {
    "merchant_id": "load-evidence-merchant",
    "amount": 100,
    "currency": "INR",
    "payment_method": {"type": "card", "token": "test_card"},
}


@dataclass(slots=True)
class Sample:
    phase: str
    operation: str
    status_code: int | None
    latency_ms: float
    result_status: str | None = None
    error_code: str | None = None
    network_error: str | None = None


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int((len(ordered) - 1) * fraction)))
    return round(ordered[index], 3)


def _git_commit() -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


def _default_output(scenario: str) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("benchmark/results") / f"load-{scenario}-{stamp}.json"


def _payment_request(key: str) -> tuple[dict[str, Any], dict[str, str]]:
    return PAYMENT_BODY, {"Idempotency-Key": key}


async def request_payment(
    client: httpx.AsyncClient,
    *,
    phase: str,
    key: str,
    operation: str = "POST /api/v1/payments",
) -> tuple[Sample, str | None]:
    started = time.perf_counter()
    try:
        body, headers = _payment_request(key)
        response = await client.post(PAYMENT_PATH, headers=headers, json=body)
        elapsed = (time.perf_counter() - started) * 1000
        data = response.json()
        error = data.get("error") if isinstance(data, dict) else None
        result_status = data.get("status") if isinstance(data, dict) else None
        error_code = error.get("code") if isinstance(error, dict) else None
        payment_id = data.get("id") if response.status_code == 201 and isinstance(data, dict) else None
        return (
            Sample(phase, operation, response.status_code, round(elapsed, 3), result_status, error_code),
            payment_id,
        )
    except Exception as exc:  # bounded load evidence should retain transport failures
        elapsed = (time.perf_counter() - started) * 1000
        return Sample(phase, operation, None, round(elapsed, 3), network_error=type(exc).__name__), None


async def admin_request(
    client: httpx.AsyncClient,
    *,
    provider: str,
    action: str,
    admin_token: str,
) -> Sample:
    started = time.perf_counter()
    try:
        response = await client.post(
            f"/api/v1/admin/providers/{provider}/{action}",
            headers={"X-Admin-Token": admin_token},
        )
        elapsed = (time.perf_counter() - started) * 1000
        data = response.json()
        error = data.get("error") if isinstance(data, dict) else None
        error_code = error.get("code") if isinstance(error, dict) else None
        return Sample("control", f"POST admin/{action}", response.status_code, round(elapsed, 3), error_code=error_code)
    except Exception as exc:
        elapsed = (time.perf_counter() - started) * 1000
        return Sample("control", f"POST admin/{action}", None, round(elapsed, 3), network_error=type(exc).__name__)


async def provider_snapshot(client: httpx.AsyncClient) -> dict[str, Any]:
    """Capture bounded provider health/counter evidence without configuration secrets."""
    try:
        response = await client.get("/api/v1/providers")
        data = response.json()
        if response.status_code != 200 or not isinstance(data, list):
            return {"status_code": response.status_code}
        providers = []
        for provider in data:
            stats = provider.get("stats") or {}
            providers.append(
                {
                    "name": provider.get("name"),
                    "healthy": provider.get("healthy"),
                    "circuit_state": provider.get("circuit_state"),
                    "total_requests": stats.get("total_requests", 0),
                    "successes": stats.get("successes", 0),
                    "failures": stats.get("failures", 0),
                    "timeouts": stats.get("timeouts", 0),
                }
            )
        return {"status_code": response.status_code, "providers": providers}
    except Exception as exc:
        return {"status_code": None, "network_error": type(exc).__name__}


async def run_concurrent(
    client: httpx.AsyncClient,
    *,
    count: int,
    concurrency: int,
    phase: str,
    same_key: bool = False,
) -> tuple[list[Sample], set[str]]:
    """Launch a bounded batch, limiting in-flight requests to ``concurrency``."""
    semaphore = asyncio.Semaphore(max(1, concurrency))
    shared_key = f"load-{uuid4().hex}" if same_key else None
    payment_ids: set[str] = set()

    async def one(index: int) -> tuple[Sample, str | None]:
        async with semaphore:
            key = shared_key or f"load-{phase}-{index}-{uuid4().hex}"
            return await request_payment(client, phase=phase, key=key)

    samples_and_ids = await asyncio.gather(*(one(index) for index in range(count)))
    samples = []
    for sample, payment_id in samples_and_ids:
        samples.append(sample)
        if payment_id:
            payment_ids.add(payment_id)
    return samples, payment_ids


async def run_rate(
    client: httpx.AsyncClient,
    *,
    count: int,
    rate: float,
    users: int,
    phase: str,
) -> tuple[list[Sample], set[str]]:
    """Schedule requests at a fixed rate while bounding in-flight users."""
    if rate <= 0:
        raise ValueError("rate must be positive")
    semaphore = asyncio.Semaphore(max(1, users))
    results: list[tuple[Sample, str | None]] = []

    async def one(index: int) -> None:
        async with semaphore:
            results.append(
                await request_payment(
                    client,
                    phase=phase,
                    key=f"load-{phase}-{index}-{uuid4().hex}",
                )
            )

    started = time.perf_counter()
    pending: set[asyncio.Task[None]] = set()
    for index in range(count):
        due = started + index / rate
        delay = due - time.perf_counter()
        if delay > 0:
            await asyncio.sleep(delay)
        pending.add(asyncio.create_task(one(index)))
        if len(pending) >= max(1, users):
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                await task
    if pending:
        await asyncio.gather(*pending)
    samples = [sample for sample, _ in results]
    payment_ids = {payment_id for _, payment_id in results if payment_id}
    return samples, payment_ids


async def run_outage(
    client: httpx.AsyncClient,
    *,
    provider: str,
    admin_token: str,
    users: int,
    rate: float,
    duration: float,
) -> tuple[list[Sample], set[str], dict[str, Any]]:
    controls: list[Sample] = []
    snapshots: dict[str, dict[str, Any]] = {"before_fail": await provider_snapshot(client)}
    controls.append(await admin_request(client, provider=provider, action="fail", admin_token=admin_token))
    snapshots["after_fail"] = await provider_snapshot(client)
    outage_count = max(1, int(round(rate * duration)))
    outage_samples, outage_ids = await run_rate(
        client,
        count=outage_count,
        rate=rate,
        users=users,
        phase="outage",
    )
    snapshots["after_outage"] = await provider_snapshot(client)
    controls.append(await admin_request(client, provider=provider, action="recover", admin_token=admin_token))
    snapshots["after_recover"] = await provider_snapshot(client)
    recovery_started = time.perf_counter()
    recovery_samples, recovery_ids = await run_rate(
        client,
        count=max(1, min(outage_count, users)),
        rate=rate,
        users=users,
        phase="recovery",
    )
    recovery_elapsed_ms = round((time.perf_counter() - recovery_started) * 1000, 3)
    snapshots["after_recovery"] = await provider_snapshot(client)
    controls.append(await admin_request(client, provider=provider, action="recover", admin_token=admin_token))
    before_by_name = {
        item.get("name"): item for item in snapshots.get("before_fail", {}).get("providers", [])
    }
    after_by_name = {
        item.get("name"): item for item in snapshots.get("after_outage", {}).get("providers", [])
    }
    request_deltas = {
        name: after_by_name[name].get("total_requests", 0) - before_by_name[name].get("total_requests", 0)
        for name in before_by_name.keys() & after_by_name.keys()
    }
    return (
        controls + outage_samples + recovery_samples,
        outage_ids | recovery_ids,
        {
            "provider": provider,
            "recovery_elapsed_ms": recovery_elapsed_ms,
            "provider_snapshots": snapshots,
            "outage_request_deltas": request_deltas,
            "failover_observed": any(
                delta > 0 for name, delta in request_deltas.items() if name != provider
            ),
        },
    )


def summarize(samples: list[Sample], started: float, payment_ids: set[str]) -> dict[str, Any]:
    elapsed = max(0.0, time.perf_counter() - started)
    measured = [sample.latency_ms for sample in samples if sample.status_code is not None]
    status_counts = Counter(str(sample.status_code) for sample in samples if sample.status_code is not None)
    network_errors = Counter(sample.network_error for sample in samples if sample.network_error)
    by_phase: dict[str, Any] = {}
    for phase in sorted({sample.phase for sample in samples}):
        phase_samples = [sample for sample in samples if sample.phase == phase]
        phase_statuses = Counter(
            str(sample.status_code) for sample in phase_samples if sample.status_code is not None
        )
        phase_latencies = [sample.latency_ms for sample in phase_samples if sample.status_code is not None]
        by_phase[phase] = {
            "requests": len(phase_samples),
            "status_counts": dict(sorted(phase_statuses.items())),
            "p50_latency_ms": percentile(phase_latencies, 0.50),
            "p95_latency_ms": percentile(phase_latencies, 0.95),
            "p99_latency_ms": percentile(phase_latencies, 0.99),
        }
    return {
        "requests": len(samples),
        "status_counts": dict(sorted(status_counts.items())),
        "network_errors": dict(sorted(network_errors.items())),
        "successful_payment_responses": status_counts.get("201", 0),
        "p50_latency_ms": percentile(measured, 0.50),
        "p95_latency_ms": percentile(measured, 0.95),
        "p99_latency_ms": percentile(measured, 0.99),
        "observed_requests_per_second": round(len(samples) / elapsed, 3) if elapsed else None,
        "elapsed_seconds": round(elapsed, 3),
        "distinct_payment_ids": len(payment_ids),
        "by_phase": by_phase,
    }


async def run(args: argparse.Namespace) -> dict[str, Any]:
    limits = httpx.Limits(
        max_connections=max(1, args.users),
        max_keepalive_connections=max(1, min(args.users, 100)),
    )
    timeout = httpx.Timeout(args.timeout)
    started = time.perf_counter()
    all_samples: list[Sample] = []
    payment_ids: set[str] = set()
    scenario_details: dict[str, Any] = {}
    async with httpx.AsyncClient(base_url=args.base_url.rstrip("/"), limits=limits, timeout=timeout) as client:
        if args.scenario == "normal":
            count = args.requests if args.requests is not None else max(1, round(args.rate * args.duration))
            samples, ids = await run_rate(
                client,
                count=count,
                rate=args.rate,
                users=args.users,
                phase="normal",
            )
        elif args.scenario == "concurrent":
            count = args.requests if args.requests is not None else 1000
            samples, ids = await run_concurrent(
                client,
                count=count,
                concurrency=args.users,
                phase="concurrent",
            )
        elif args.scenario == "idempotency":
            count = args.requests if args.requests is not None else 500
            samples, ids = await run_concurrent(
                client,
                count=count,
                concurrency=args.users,
                phase="idempotency",
                same_key=True,
            )
            scenario_details["equivalent_payment_responses"] = (
                len(ids) == 1 and all(sample.status_code == 201 for sample in samples)
            )
        else:
            samples, ids, scenario_details = await run_outage(
                client,
                provider=args.provider,
                admin_token=args.admin_token,
                users=args.users,
                rate=args.rate,
                duration=args.duration,
            )
        all_samples.extend(samples)
        payment_ids.update(ids)
    return {
        "schema_version": 1,
        "scenario": args.scenario,
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "git_commit": _git_commit(),
        "command": " ".join(sys.argv),
        "base_url": args.base_url,
        "runtime": {"python": platform.python_version(), "platform": platform.platform()},
        "configuration": {
            "users": args.users,
            "rate_per_second": args.rate,
            "duration_seconds": args.duration,
            "requests": args.requests,
            "timeout_seconds": args.timeout,
            "provider": args.provider if args.scenario == "outage" else None,
        },
        "summary": summarize(all_samples, started, payment_ids),
        "details": scenario_details,
        "samples": [asdict(sample) for sample in all_samples],
        "privacy": "Synthetic test tokens only; request keys and payment identifiers are omitted from samples.",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=os.getenv("PYSWITCH_LOAD_BASE_URL", "http://127.0.0.1:8000"))
    parser.add_argument(
        "--scenario",
        choices=("normal", "concurrent", "outage", "idempotency"),
        default="normal",
    )
    parser.add_argument("--users", type=int, default=100, help="Maximum in-flight requests (virtual users).")
    parser.add_argument("--rate", type=float, default=10.0, help="Scheduled request rate for normal/outage scenarios.")
    parser.add_argument("--duration", type=float, default=10.0, help="Duration used to derive normal/outage request count.")
    parser.add_argument("--requests", type=int, help="Exact request count; otherwise derived by scenario defaults.")
    parser.add_argument("--provider", default="mockstripe", help="Provider to fail/recover in the outage scenario.")
    parser.add_argument("--admin-token", default=os.getenv("PYSWITCH_ADMIN_TOKEN", "local-dev-only"))
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--output", type=Path, help="JSON output path (default: benchmark/results/load-<scenario>-<timestamp>.json).")
    args = parser.parse_args()
    if args.users < 1 or args.rate <= 0 or args.duration <= 0 or args.timeout <= 0:
        parser.error("users, rate, duration and timeout must be positive")
    if args.requests is not None and args.requests < 1:
        parser.error("requests must be positive")
    args.output = args.output or _default_output(args.scenario)
    return args


def main() -> None:
    args = parse_args()
    evidence = asyncio.run(run(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2) + "\n")
    summary = evidence["summary"]
    print(
        f"{evidence['scenario']}: {summary['requests']} requests, "
        f"statuses={summary['status_counts']}, p95_ms={summary['p95_latency_ms']}, "
        f"output={args.output}"
    )


if __name__ == "__main__":
    main()
