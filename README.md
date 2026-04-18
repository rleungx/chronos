# Chronos

Chronos is a gRPC-based timestamp service for applications that need monotonic, timeline-scoped
timestamp allocation without exposing routing, ownership, or failover mechanics to application
code.

From an application developer's perspective, the happy path is intentionally small:

1. Create one client bound to one timeline
2. Call one allocation API

Everything else — route discovery, stale-route recovery, owner changes, and retry behavior — is
handled by the Chronos client/runtime layer.

## What Chronos provides

- A single allocation surface for normal application usage
- Timeline-scoped routing so one logical workload can keep using the same client contract
- Internal handling for route ensure, route refresh, and stale-route retry
- Production-oriented operational assets in this repo: validation gates, alerts, dashboards,
  runbooks, and release/rollback guidance

## Start here

- If you want to integrate Chronos into an application, start with `clients/README.md`.
- If you want to operate or evaluate Chronos in a production setting, start with
  `docs/production.md`.

## Notes

- Your application should not call internal routing RPCs directly.
- `timeline_key` is chosen once when the client is created.
- Allocation is the only operation the application should need during normal use.
- Route ensure, route refresh, and stale-route retry are internal client behavior.
- Route refresh reconnects allocation traffic to the current owner endpoint.
- Direct protobuf route-management RPC use is considered internal or advanced usage.

## Clients

- Client index: `clients/README.md`

## Production operations

- Start with `docs/production.md` for the operator path.
- Use `docs/release.md` for release validation and `docs/rollback.md` for rollback handling.
- Use `observability/README.md` for Prometheus, Grafana, alerts, and runbook links.
- Dependency policy lives in `deny.toml` and is enforced by CI.
