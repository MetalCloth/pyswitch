# Local reliability runbook

These steps exercise simulated provider failures. They do not contact a real payment service and do not process real money.

Start the API from the repository root:

```bash
source .venv/bin/activate
uvicorn pyswitch.main:app --reload
```

Use the local admin token from `.env.example`:

```bash
export ADMIN_TOKEN=local-dev-only
```

To make Stripe return a transient server error while remaining health-checkable, configure it with a 100% server-error probability:

```bash
curl -X PUT http://127.0.0.1:8000/api/v1/admin/providers/mockstripe/config \
  -H "X-Admin-Token: $ADMIN_TOKEN" -H 'Content-Type: application/json' \
  -d '{"success_rate":0,"server_error_probability":1}'
```

Create a payment with a new idempotency key. The service retries Stripe, opens only Stripe's circuit after the configured threshold, and then tries Adyen. The client response remains provider-neutral. `GET /api/v1/providers` shows each circuit state; provider-attempt history is retained in the service store for the next persistence milestone.

To force health-based removal immediately:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/admin/providers/mockstripe/fail \
  -H "X-Admin-Token: $ADMIN_TOKEN"
```

Recover the provider and reset its circuit:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/admin/providers/mockstripe/recover \
  -H "X-Admin-Token: $ADMIN_TOKEN"
```

Restore the default simulation before another run:

```bash
curl -X PUT http://127.0.0.1:8000/api/v1/admin/providers/mockstripe/config \
  -H "X-Admin-Token: $ADMIN_TOKEN" -H 'Content-Type: application/json' \
  -d '{"success_rate":1,"server_error_probability":0,"timeout_probability":0,"decline_probability":0}'
```

For an ambiguous timeout simulation, set `timeout_probability` to `1`. The service retries the original provider and returns `PROVIDER_TIMEOUT` after the retry budget; it does not charge the alternate provider.

