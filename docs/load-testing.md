# Load, query, and live event evidence

PySwitch keeps the optional Locust scenario in `locustfile.py` for teams that
already have the `load` extra available:

```bash
pip install -e '.[load]'
locust -f locustfile.py --host http://127.0.0.1:8000
```

When Locust cannot be installed, `scripts/load_evidence.py` provides the
reproducible bounded fallback using the existing `dev` extra and HTTPX. Start
one standalone process with a high synthetic-only rate limit so the rate
limiter does not turn the benchmark into a rejection test:

```bash
PYSWITCH_RATE_LIMIT_CAPACITY=5000 \
PYSWITCH_RATE_LIMIT_REFILL_PER_SECOND=5000 \
uvicorn pyswitch.main:app --host 127.0.0.1 --port 8000
```

Run the requested scenarios from a second terminal:

```bash
python scripts/load_evidence.py --scenario normal --users 100 --rate 10 --duration 10 \
  --output benchmark/results/load-normal.json
python scripts/load_evidence.py --scenario concurrent --users 1000 --requests 1000 \
  --output benchmark/results/load-concurrent.json
python scripts/load_evidence.py --scenario outage --users 100 --rate 10 --duration 10 \
  --provider mockstripe --admin-token local-dev-only \
  --output benchmark/results/load-outage.json
python scripts/load_evidence.py --scenario idempotency --users 500 --requests 500 \
  --output benchmark/results/load-idempotency.json
```

The runner records status counts, p50/p95/p99 request latency, observed rate,
transport errors, per-phase summaries, provider counter deltas for the outage
scenario, and whether all same-key responses referred to one payment. Raw
samples contain no idempotency keys or payment identifiers. The controls are
synthetic and local; they do not represent a production SLO or real payment
traffic.

The repository contains these measured bounded runs from a single local
environment:

| Artifact | Configuration | Observed result |
| --- | --- | --- |
| `benchmark/results/load-normal-100users-10rps-10s.json` | 100 users, 10 requests/s, 10 s | 100/100 HTTP 201; p95 5.760 ms |
| `benchmark/results/load-concurrent-1000users.json` | 1,000 concurrent requests | 1,000/1,000 HTTP 201; p95 2,629.452 ms |
| `benchmark/results/load-outage-mockstripe-100users-10rps-10s.json` | forced `mockstripe` outage and recovery | 200/200 payment requests HTTP 201; failover and provider snapshots recorded |
| `benchmark/results/load-idempotency-500same-key.json` | 500 concurrent requests, one key | 500/500 HTTP 201; one distinct payment ID |

Each JSON file includes the exact command, UTC timestamp, Git commit, Python
version, platform, and request-level measurements. These values are local
observations, and should be regenerated after changing the runtime or host.

## Indexed query comparison

`scripts/query_benchmark.py` uses a temporary PostgreSQL table, so it cannot
modify application rows. It executes the payment list shape before and after
creating the migration's `(merchant_id, created_at)` index, capturing JSON
`EXPLAIN (ANALYZE, BUFFERS)` plans and repeated timings:

```bash
pip install -e '.[db]'
python scripts/query_benchmark.py \
  --database-url postgresql+asyncpg://pyswitch:pyswitch@localhost:5432/pyswitch \
  --rows 50000 --merchant-count 100 --repetitions 5 \
  --output benchmark/results/query-index-comparison.json
```

The committed local run uses 50,000 temporary rows and five repetitions:
`benchmark/results/query-index-comparison-50000rows.json`. Its median
execution time was 3.876 ms before the index and 0.578 ms after it; the plans
changed from a sequential scan to a bitmap index scan. This is an indexed
query demonstration on a temporary table, not a production performance
claim.

## Kafka consumer group offsets

`scripts/kafka_consumer_evidence.py` is an opt-in Redpanda probe. It publishes
three unique versioned events and one duplicate event to a unique topic,
consumes them with an explicit group and manual offset commit, then restarts
the same group. The audit consumer applies three effects, deduplicates the
duplicate UUID, and the restarted group must receive zero records:

```bash
pip install -e '.[events]'
python scripts/kafka_consumer_evidence.py \
  --bootstrap-servers localhost:19092 \
  --output benchmark/results/kafka-consumer-offsets.json
```

The committed probe recorded 4 consumed records, 3 applied effects, one
committed partition offset, and zero redeliveries after restart. Topic and
group names, event IDs, and payloads are intentionally omitted from the
artifact. This verifies local Kafka group-offset behavior and the existing
UUID-deduplicating consumer contract; external side effects and a long-lived
production consumer deployment remain outside this repository.

Live dependency smoke checks remain opt-in:

```bash
PYSWITCH_RUN_INTEGRATION=1 pytest -q tests/integration
```

They probe PostgreSQL, Redis, and Kafka/Redpanda URLs from the runtime
environment. Missing optional packages or unavailable services are reported
as pytest skips. They do not claim cross-process performance, sustained
delivery rates, or production recovery timing.
