#!/usr/bin/env python3
"""Verify Kafka group offsets and UUID-deduplicated consumer effects.

The probe creates a unique topic and consumer group, publishes three unique
events plus one duplicate event, commits offsets, and starts the same group a
second time.  It records only counts and offsets, never event payloads.
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
from uuid import UUID, uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from pyswitch.consumers import AuditConsumer
from pyswitch.events import EventEnvelope, EventType


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
    return Path("benchmark/results") / f"kafka-consumer-{stamp}.json"


def _safe_bootstrap(bootstrap_servers: str) -> str:
    """Drop credentials if a URL-style bootstrap value is supplied."""
    if "://" not in bootstrap_servers:
        return bootstrap_servers
    parsed = urlsplit(bootstrap_servers)
    host = parsed.hostname or "<redacted>"
    if parsed.port:
        host = f"{host}:{parsed.port}"
    return urlunsplit((parsed.scheme, host, parsed.path, "", ""))


def _decode_event(raw: bytes) -> EventEnvelope:
    value = json.loads(raw)
    aggregate_id = value.get("aggregate_id")
    return EventEnvelope(
        event_type=EventType(value["event_type"]),
        merchant_id=value["merchant_id"],
        aggregate_id=UUID(aggregate_id) if aggregate_id else None,
        payload=value["payload"],
        provider=value.get("provider"),
        version=value.get("version", 1),
        id=UUID(value["id"]),
        occurred_at=datetime.fromisoformat(value["occurred_at"]),
    )


async def _collect(consumer: Any, expected: int, timeout: float) -> list[Any]:
    deadline = asyncio.get_running_loop().time() + timeout
    records: list[Any] = []
    while len(records) < expected and asyncio.get_running_loop().time() < deadline:
        batches = await consumer.getmany(timeout_ms=250)
        for batch in batches.values():
            records.extend(batch)
    return records


async def _collect_until_quiet(consumer: Any, quiet_seconds: float) -> list[Any]:
    deadline = asyncio.get_running_loop().time() + quiet_seconds
    records: list[Any] = []
    while asyncio.get_running_loop().time() < deadline:
        batches = await consumer.getmany(timeout_ms=250)
        for batch in batches.values():
            records.extend(batch)
    return records


async def run(args: argparse.Namespace) -> dict[str, Any]:
    topic = f"{args.topic_prefix}.{uuid4().hex}"
    group_id = f"pyswitch-evidence-{uuid4().hex}"
    merchant = f"kafka-evidence-{uuid4().hex[:8]}"
    shared_event = EventEnvelope(
        EventType.PAYMENT_SUCCEEDED,
        merchant,
        uuid4(),
        {"status": "SUCCEEDED", "synthetic": True},
    )
    unique_events = [
        shared_event,
        EventEnvelope(EventType.PAYMENT_SUCCEEDED, merchant, uuid4(), {"status": "SUCCEEDED", "synthetic": True}),
        EventEnvelope(EventType.PAYMENT_FAILED, merchant, uuid4(), {"status": "FAILED", "synthetic": True}),
    ]
    events = [*unique_events, shared_event]
    producer = AIOKafkaProducer(bootstrap_servers=args.bootstrap_servers)
    started = time.perf_counter()
    await producer.start()
    try:
        for event in events:
            await producer.send_and_wait(topic, json.dumps(event.as_dict()).encode())
    finally:
        await producer.stop()

    audit = AuditConsumer()
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        records = await _collect(consumer, len(events), args.timeout)
        applied = 0
        for record in records:
            if await audit.consume(_decode_event(record.value)):
                applied += 1
        if len(records) != len(events):
            raise RuntimeError(f"consumer received {len(records)} records; expected {len(events)}")
        await consumer.commit()
        committed_offsets: dict[str, int] = {}
        for record in records:
            committed_offsets[str(record.partition)] = max(
                committed_offsets.get(str(record.partition), 0), record.offset + 1
            )
    finally:
        await consumer.stop()

    restarted = AIOKafkaConsumer(
        topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await restarted.start()
    try:
        redelivered = await _collect_until_quiet(restarted, args.quiet_seconds)
    finally:
        await restarted.stop()
    return {
        "schema_version": 1,
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "git_commit": _git_commit(),
        "command": " ".join(sys.argv),
        "bootstrap_servers": _safe_bootstrap(args.bootstrap_servers),
        "runtime": {"python": platform.python_version(), "platform": platform.platform()},
        "configuration": {
            "produced_records": len(events),
            "unique_event_ids": len(unique_events),
            "duplicate_records": len(events) - len(unique_events),
            "consumer_timeout_seconds": args.timeout,
            "quiet_window_seconds": args.quiet_seconds,
        },
        "evidence": {
            "consumed_records": len(records),
            "applied_effects": len(audit.events),
            "deduplicated_effects": applied == len(unique_events),
            "committed_offsets": committed_offsets,
            "redelivered_after_restart": len(redelivered),
            "offsets_persisted": len(redelivered) == 0,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        },
        "privacy": "Synthetic event payloads only; topic, group, event IDs and payloads are omitted from evidence.",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("PYSWITCH_KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
    )
    parser.add_argument("--topic-prefix", default="pyswitch.consumer.evidence")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--quiet-seconds", type=float, default=2.0)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    if args.timeout <= 0 or args.quiet_seconds <= 0:
        parser.error("timeout and quiet-seconds must be positive")
    args.output = args.output or _default_output()
    return args


def main() -> None:
    args = parse_args()
    evidence = asyncio.run(run(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2) + "\n")
    print(
        "Kafka consumer evidence: "
        f"consumed={evidence['evidence']['consumed_records']}, "
        f"applied={evidence['evidence']['applied_effects']}, "
        f"redelivered_after_restart={evidence['evidence']['redelivered_after_restart']}, "
        f"output={args.output}"
    )


if __name__ == "__main__":
    main()
