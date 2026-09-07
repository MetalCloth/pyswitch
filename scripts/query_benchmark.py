#!/usr/bin/env python3
"""Measure the payment list query before and after its composite index.

The benchmark uses a temporary PostgreSQL table and therefore cannot modify
application data.  It records PostgreSQL's JSON EXPLAIN ANALYZE plans and
timings for a merchant/date ordered list query with the same
``(merchant_id, created_at)`` index as the application migration.
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


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


def _default_output() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("benchmark/results") / f"query-{stamp}.json"


def _safe_database_url(database_url: str) -> str:
    """Keep host/database context while excluding credentials from evidence."""
    parsed = urlsplit(database_url)
    if not parsed.hostname:
        return "<redacted>"
    host = parsed.hostname
    if parsed.port:
        host = f"{host}:{parsed.port}"
    return urlunsplit((parsed.scheme, host, parsed.path, "", ""))


async def explain(connection: Any, query: str, repeats: int) -> dict[str, Any]:
    plans: list[dict[str, Any]] = []
    for _ in range(repeats):
        result = await connection.execute(text(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"))
        payload = result.scalar_one()
        plans.append(payload[0] if isinstance(payload, list) else payload)
    timings = [float(plan["Execution Time"]) for plan in plans]
    planning = [float(plan["Planning Time"]) for plan in plans]
    return {
        "repetitions": repeats,
        "execution_time_ms": {
            "min": round(min(timings), 3),
            "median": round(sorted(timings)[len(timings) // 2], 3),
            "max": round(max(timings), 3),
        },
        "planning_time_ms": {
            "min": round(min(planning), 3),
            "median": round(sorted(planning)[len(planning) // 2], 3),
            "max": round(max(planning), 3),
        },
        "plans": plans,
    }


async def run(args: argparse.Namespace) -> dict[str, Any]:
    engine = create_async_engine(args.database_url, connect_args={"timeout": args.timeout})
    merchant = "merchant-0042"
    query = (
        "SELECT id, merchant_id, amount, currency, status, created_at "
        f"FROM pyswitch_query_benchmark "
        f"WHERE merchant_id = '{merchant}' ORDER BY created_at"
    )
    started = time.perf_counter()
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "CREATE TEMP TABLE pyswitch_query_benchmark AS "
                    "SELECT g::bigint AS id, "
                    "'merchant-' || lpad((g % :merchant_count)::text, 4, '0') AS merchant_id, "
                    "(100 + (g % 10000))::integer AS amount, "
                    "CASE WHEN g % 3 = 0 THEN 'SUCCEEDED' WHEN g % 3 = 1 THEN 'FAILED' ELSE 'PROCESSING' END AS status, "
                    "CASE WHEN g % 2 = 0 THEN 'INR' ELSE 'USD' END AS currency, "
                    "now() - (g * interval '1 second') AS created_at "
                    "FROM generate_series(1, :row_count) AS g"
                ),
                {"merchant_count": args.merchant_count, "row_count": args.rows},
            )
            await connection.execute(text("ANALYZE pyswitch_query_benchmark"))
            before = await explain(connection, query, args.repetitions)
            await connection.execute(
                text(
                    "CREATE INDEX pyswitch_query_benchmark_merchant_created "
                    "ON pyswitch_query_benchmark (merchant_id, created_at)"
                )
            )
            await connection.execute(text("ANALYZE pyswitch_query_benchmark"))
            after = await explain(connection, query, args.repetitions)
    finally:
        await engine.dispose()
    return {
        "schema_version": 1,
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "git_commit": _git_commit(),
        "command": " ".join(sys.argv),
        "database_url": _safe_database_url(args.database_url),
        "runtime": {"python": platform.python_version(), "platform": platform.platform()},
        "configuration": {
            "rows": args.rows,
            "merchant_count": args.merchant_count,
            "target_merchant": merchant,
            "repetitions": args.repetitions,
            "query": query,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        },
        "before_index": before,
        "after_index": after,
        "interpretation": "Measured temporary-table EXPLAIN ANALYZE evidence; timings are local-environment observations, not production SLOs.",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=os.getenv(
            "PYSWITCH_DATABASE_URL",
            "postgresql+asyncpg://pyswitch:pyswitch@localhost:5432/pyswitch",
        ),
    )
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--merchant-count", type=int, default=100)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    if args.rows < 1 or args.merchant_count < 2 or args.repetitions < 1 or args.timeout <= 0:
        parser.error("rows and repetitions must be positive; merchant-count must be at least 2; timeout must be positive")
    args.output = args.output or _default_output()
    return args


def main() -> None:
    args = parse_args()
    evidence = asyncio.run(run(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2) + "\n")
    print(
        "query benchmark: "
        f"before median={evidence['before_index']['execution_time_ms']['median']} ms, "
        f"after median={evidence['after_index']['execution_time_ms']['median']} ms, "
        f"output={args.output}"
    )


if __name__ == "__main__":
    main()
