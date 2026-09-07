# Simulated Redis outage runbook

This is a local, simulated dependency-failure procedure. It does not contact a real payment provider and does not report production outage timing or benchmark results.

## Local fallback behavior

The default `create_app()` uses `MemoryIdempotencyCoordinator` and `InMemoryTokenBucketLimiter`. Stop or omit Redis and start the app as usual:

```bash
source .venv/bin/activate
uvicorn pyswitch.main:app --reload
```

Payments and rate limits continue to work in this mode, but their guarantees are process-local and disappear on restart. Do not describe this mode as cross-process protection.

## Redis-backed failure simulation

The Redis classes are dependency-injected, so a test double can raise `ConnectionError` from `eval`. In this simulated failure, `RedisIdempotencyCoordinator` raises `IdempotencyUnavailable` and `RedisTokenBucketLimiter` raises `RateLimitUnavailable`. The API maps either to HTTP 503 and does not call a payment provider.

The fake behavior is covered by `tests/test_redis_controls.py::test_redis_dependency_errors_fail_closed`. A live Redis outage drill and cross-process concurrency test are deferred until Redis is wired into the application factory and local Compose.

## Recovery check

After restoring Redis, issue a new request and inspect the coordinator and rate-limit results. A pre-existing payment must still be read from the authoritative repository; a cached Redis payment ID alone is never treated as a complete financial record.

