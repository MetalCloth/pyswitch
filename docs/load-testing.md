# Optional load and live dependency checks

The repository includes an opt-in Locust scenario in `locustfile.py`. Install
the optional extra and run it against a locally started app:

```bash
pip install -e '.[load]'
locust -f locustfile.py --host http://127.0.0.1:8000
```

The scenario creates synthetic card-token payments and merchant-scoped list
requests. It does not publish benchmark results. Record the Locust command,
environment, user/rate settings, raw output, and charts before drawing any
performance conclusion.

Live dependency smoke checks are also opt-in:

```bash
PYSWITCH_RUN_INTEGRATION=1 pytest -q tests/integration
```

The checks probe PostgreSQL, Redis, and Kafka/Redpanda URLs from the runtime
environment. Missing optional packages or unavailable services are reported as
pytest skips. They verify reachability only; they do not apply migrations,
exercise payment transactions, prove cross-process idempotency, or measure
delivery and latency.
