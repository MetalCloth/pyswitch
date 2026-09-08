# ADR 0008: Provider-neutral adaptive routing strategies

Status: accepted for local simulation.

## Decision

Keep provider selection behind `ProviderRouter` and select its strategy with
`PYSWITCH_ROUTING_STRATEGY` or the authenticated
`PUT /api/v1/admin/routing-strategy` endpoint. Supported values are
`round_robin`, `weighted_round_robin`, `lowest_latency`,
`highest_success_rate`, and `composite`. The default remains `round_robin`.

The router records a bounded recent window per provider containing success,
latency, failure, and in-flight observations. Weighted round robin uses
`max(0.05, success_rate)` as the provider weight. Composite scoring is:

```text
success_rate^success_weight
  / (1 + average_latency_ms / 1000)^latency_weight
  / (1 + inflight)^load_weight
  / (1 + recent_failure_rate)^recent_failure_weight
```

Weights are bounded from 0 through 5 and are configurable with
`PYSWITCH_ROUTING_COMPOSITE_SUCCESS_WEIGHT`,
`PYSWITCH_ROUTING_COMPOSITE_LATENCY_WEIGHT`,
`PYSWITCH_ROUTING_COMPOSITE_LOAD_WEIGHT`, and
`PYSWITCH_ROUTING_COMPOSITE_RECENT_FAILURE_WEIGHT`, or the corresponding
fields on the authenticated routing admin endpoint. Defaults `(1, 1, 1, 0)`
preserve the previous composite score while leaving recent-failure weighting
opt-in.

Provider adapters remain behind the common protocol. Routing does not contain
Stripe, Adyen, Razorpay, or other provider-specific branches.

The composite decision is split into a small functional core: mutable
`ProviderStats` values are converted to frozen `ProviderObservation` snapshots,
then `select_composite()` evaluates a tuple of frozen `RoutingCandidate` values.
The function returns a candidate and does not mutate observations, counters, or
provider adapters. The router remains responsible for collecting observations
and applying the selected position to the live provider list.

## Consequences and limits

Each strategy is deterministic for a fixed observation window, and an empty or
all-unhealthy candidate set has the existing explicit lookup failure. Provider
responses update the stats after each attempt, including retries, and the API
exposes only bounded aggregate stats.

The observations are process-local and are based on mock-provider behavior in
the default application. They do not provide shared-worker coordination,
durable history, production telemetry, or evidence that one strategy improves
real traffic. A shared stats store and production calibration require an
integration and measurement phase.
