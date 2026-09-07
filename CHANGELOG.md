# Changelog

All notable user-facing changes to this project are documented here.

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

