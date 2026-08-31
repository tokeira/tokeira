# tokeira-observability

Shared process-level observability installation for Tokeira binaries and
embedded hosts.

## Where it sits

This cross-cutting crate sits above domain crates and below process bootstrap.
Domain crates own their metric helpers and manifests; this crate validates and
installs one coherent metrics, logging, tracing, readiness, redaction, and local
telemetry surface.

## Surface map

| Area | Representative contracts |
|---|---|
| Configuration | `ProcessObservabilityConfig`, `ServiceName`, log and OTLP settings |
| Metrics governance | `MetricManifest`, descriptors, units, bounded labels, manifest validation |
| Metrics runtime | Prometheus recorder installation, process/build metadata, embedded lifecycle metrics |
| Tracing and logs | Subscriber installation, reload handle, channel trace context, error-biased sampling |
| Readiness | `ReadinessRegistry`, checks, results, status, and handles |
| Local HTTP | Metrics, readiness, and redacted configuration state |
| Safety | Redaction helpers, coordinated `ObservabilityShutdown`, test support |

The primary entry point is `install_observability`, which validates
configuration and all supplied domain manifests, installs process-global
recorders and subscribers, and starts the local telemetry endpoint.

## Contracts

- Metric and tracing installation is process-global and may succeed only once.
- Validation runs before the global installation slot is reserved.
- If installation fails after reservation, the reservation is released so an
  embedding process or isolated test is not permanently poisoned.
- Metric labels use bounded vocabularies; forbidden or unbounded dimensions are
  rejected by manifest validation.
- Domain crates can record their own signals without depending on process
  bootstrap.
- Configuration exposed through the local endpoint is redacted.

## It does not own

The crate does not decide domain metrics, engine readiness policy, deployment
dashboards, or process lifecycle. Callers supply manifests and readiness checks;
the engine or binary coordinates shutdown.

## Pointers

- [Crate root](../../crates/tokeira-observability/src/lib.rs)
- [Configuration](config.md)
- [Engine facade](engine.md)
- [Architecture decisions](../architecture/005-decisions-and-boundaries.md)
