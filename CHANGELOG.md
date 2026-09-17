# Changelog

All notable user-facing changes to this project are documented here.

## 0.4.0 on 2026-09-17

### Added

* CHASM tasks carry stable names and task-owned codecs; libraries register typed pure and side-effect handlers and search-attribute definitions.
* Standalone activities persist completion callbacks and a deployment-version target and drive callback delivery through registered tasks, gated off.
* Standalone activities deliver completion callbacks over Nexus or into a staging component; versioned standalone tasks go only to scoped workers of the matching release.
* Embedders build the engine with extension CHASM libraries, executors and a clock behind the chasm-extensions feature; a config key gates standalone-activity callbacks.
* Embedded CHASM applications can list and count their component executions with scoped pagination and typed visibility attributes.

### Changed

* CHASM current-run pointers are keyed by archetype in a new table; legacy activity pointers are backfilled at engine start and the old table stays read-only.
* CHASM runs registered pure tasks, fences external outcomes, and rebuilds pending timers and effects from committed state on startup and periodic scans.
* Crate and readiness documentation describes the CHASM plane as an extension surface: registered libraries, typed handlers, the executors and rebuild scan, the archetype pointer.

### Fixed

* Standalone activity starts now commit atomically, preventing stranded runs and reporting state_transition_count as 1 immediately after start, matching Temporal v1.31.0.
* Concurrent starts fence the current-run pointer, so duplicate request IDs return one execution, and losing creates leave no orphaned nodes.
* The CHASM rebuild pass isolates an execution it cannot serve instead of stalling every other one; start refuses storage it cannot serve and names why.
* A scoped worker can claim a versioned standalone activity when the CHASM clock is a simulation clock; task-token provenance now expires on real time.
* Advance the DSQL schema contract to V071 so default-gate embedded startup installs and accepts the CHASM pointer and backfill tables.

### Security

* rustls 0.23.45 closes RUSTSEC-2026-0285 (TLS 1.3 handshake messages accepted across encryption levels); aws-lc-rs and rustls-webpki move with it.







## 0.3.1 on 2026-09-07

### Fixed

* Existing embedded DSQL now honors the configured database endpoint through wake refresh and reports the pool locator, enabling PrivateLink without new configuration fields.

## 0.3.0 on 2026-09-06

### Added

* Visibility queries resolve predicates against registered fields and validate literal types before execution; an absent or blank predicate matches every execution in scope.

### Changed

* Public storage signatures now use SQLx 0.9 types; the Aurora DSQL connector moves to 0.2.2 in the 0.3.0 release train.
* The gRPC edge, in-process endpoint, listeners, and generated bindings move to tonic 0.14, prost 0.14, and hyper 1; public message and status types change accordingly.

### Removed

* Regenerating the protobuf bindings no longer needs a system `protoc`; `proto-sync` compiles the vendored protos in pure Rust.

### Security

* Remove the legacy AWS HTTPS client and its four rustls-webpki and h2 advisory exceptions from the workspace.

## 0.2.0 on 2026-09-06

### Added

* Workflow-task started events and poll responses carry Temporal's continue-as-new advice (`suggest_continue_as_new`, its reasons, and the persisted history size).
* Embedded engines can attach a TCP listener after startup (`Engine::listen`) that serves the same Temporal gRPC services as the in-process endpoint.

### Changed

* The embedded engine's client seam moves to Temporal Rust SDK 1.0.0; embedding hosts should upgrade their SDK pins to match.
* AWS SDK clients in the published crates no longer enable the legacy rustls connector, and Postcard drops its unused heapless default; formats and behaviour are unchanged.
* Scoped worker credentials may describe their own namespace by exact name, so standard Temporal SDK Workers (Core-based Python, TypeScript, .NET, and Rust) start on the scoped credential alone; describing a namespace by ID, listing namespaces, and every namespace mutation stay denied.
* Describe reports the history size accounted at commit; state and history blobs gain versioned envelopes, so stores written by 0.1.2 must be recreated before upgrading.

## 0.1.2 on 2026-09-03

### Added

* Add a deterministic, resumable release engineering pipeline.

### Changed

* The embedded engine's client seam moves to Temporal Rust SDK 0.8.0; embedding hosts should upgrade their SDK pins to match.

### Fixed

* Bound Aurora DSQL connection-class pressure warnings: sustained permit contention warns at most once per minute per class instead of flooding the log.
* Validate a rendered configuration root standalone: artifact validators now live in the production validation module, and tkr validates an explicit rendered root without deployment admission.

