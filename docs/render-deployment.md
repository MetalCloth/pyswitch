# Render deployment

The repository includes [`render.yaml`](../render.yaml), a small public demo
profile for Render. It creates one Docker web service, one managed PostgreSQL
database, and one Redis-compatible Key Value instance in Singapore. The web
service uses `/ready` as its health check, runs the existing migration entrypoint,
and generates the admin token inside Render.

This profile sets `PYSWITCH_EVENT_BACKEND=memory` because it does not provision
a Kafka-compatible broker. Payments and idempotency use PostgreSQL and Redis;
the in-process event broker is suitable for the demonstrator but does not give
durable event delivery across restarts. The local Compose profile remains the
full Redpanda, Prometheus, and Grafana environment.

The Blueprint uses free plans as a starting point. Review Render's current
[Postgres plan limits](https://render.com/docs/postgresql-creating-connecting) and
[Key Value persistence rules](https://render.com/docs/key-value) before treating
the service as a durable public system; free resources are intended for a demo.

## Create the services

1. Open Render and choose **New → Blueprint**.
2. Connect `https://github.com/MetalCloth/pyswitch`, select `main`, and choose
   `render.yaml` from the repository root.
3. Review the three resources and deploy the Blueprint. Do not replace the
   generated `PYSWITCH_ADMIN_TOKEN` with a value committed to Git.
4. Wait for the web service deploy and migration step to finish. Render's
   public service URL will look like `https://pyswitch-api.onrender.com`.

The service is a synthetic payment demo. It uses the deterministic mock
Stripe, Adyen, and Razorpay adapters and never accepts raw card data or calls
a real payment provider.

## Verify the public service

Set the URL locally, without committing it:

```bash
export PYSWITCH_RENDER_URL='https://pyswitch-api.onrender.com'
curl -fsS "$PYSWITCH_RENDER_URL/health"
curl -fsS "$PYSWITCH_RENDER_URL/ready"
curl -fsS -X POST "$PYSWITCH_RENDER_URL/api/v1/payments" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: render-smoke-1' \
  -d '{"merchant_id":"render-demo","amount":100,"currency":"INR","payment_method":{"type":"card","token":"test_card"}}'
```

The expected health responses are HTTP 200. The payment smoke request should
return HTTP 201 with `status` set to `SUCCEEDED`. The token and idempotency key
above are synthetic and should not be reused for real traffic.

## Run a small public benchmark

Run the bounded scenarios from a machine with the repository's development
extra installed. Start gently because a free single-instance service is a
demo target, not a capacity claim:

```bash
PYSWITCH_LOAD_BASE_URL="$PYSWITCH_RENDER_URL" \
  .venv/bin/python scripts/load_evidence.py \
  --scenario normal --users 10 --rate 2 --duration 60 \
  --output /tmp/pyswitch-render-normal.json

PYSWITCH_LOAD_BASE_URL="$PYSWITCH_RENDER_URL" \
  .venv/bin/python scripts/load_evidence.py \
  --scenario concurrent --users 25 --requests 100 \
  --output /tmp/pyswitch-render-concurrent.json
```

Record the URL, plan, region, commit, request counts, status counts, p95/p99,
and any restarts with the results. Repeat the 100-user and 1,000-request local
stress scenarios only after observing the service's CPU, memory, database
connections, and error rate. Local results in [`load-testing.md`](load-testing.md)
are comparison evidence, not a prediction of Render capacity.

## Remove the demo

Delete or suspend the Blueprint from the Render Dashboard when the public URL
is no longer needed. Keep the local Compose stack for the full observability
and Kafka-compatible event demonstration.
