# Minimal workspace roadmap

This workspace is expected to evolve in these phases.

## Phase 1 — semantic completeness
- Fill out kernel workflow semantics.
- Add more runtime pumps (timers, activities, sweeps).
- Strengthen tests and state-machine coverage.

## Phase 2 — DSQL implementation
- Replace the in-memory store with a real DSQL store.
- Add fenced bundle leases.
- Add connection director integration.

## Phase 3 — transport and control plane
- Add edge service.
- Add proto/gRPC shell.
- Add autoscaler, controller, and placement APIs.

## Phase 4 — archival and production hardening
- Add S3 archival.
- Add SQL-native visibility.
- Add self-healing and admission-control policy loops.
