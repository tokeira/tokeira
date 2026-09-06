# Implementation Plan

- [x] 0. Approval checkpoint: dependency change and breaking release
  - The integration seat approves the move to the Target Stack, the adoption of `protox`
    in place of `protoc`, the regeneration of the checked-in bindings, and the 0.3.0
    release before any task below starts.
  - DONE 2026-09-06: approved by the integration seat with the spec.
  - _Requirements: 1.1, 2.1, 8.1_

- [x] 1. Goldens on the Legacy Stack (lands before any dependency moves)
  - DONE 2026-09-06: captured on tonic 0.11 / prost 0.12 / hyper 0.14 at the spec's merge
    commit, over three surfaces (in-process endpoint, host listener, public listener).
    Notable answers now pinned: an over-limit message is refused with `OUT_OF_RANGE`
    and the stack's own message; an unknown method over gRPC-Web is a trailers-only
    reply with `grpc-status: 12` in the HTTP headers; the legacy stack does not escape
    a literal percent sign in a status message, so the type-level property leaves
    that character out.
  - [x] 1.1 Wire-parity harness with capture mode
    - `crates/tokeira-engine/tests/wire_parity.rs` over the In-Process Endpoint and a Host
      Listener; `WIRE_PARITY_CAPTURE=1` writes fixtures, otherwise compares.
    - _Requirements: 6.2_
  - [x] 1.2 Status catalogue fixture and Property 2 (Legacy leg)
    - Fixed list of edge error values; capture code, message, details bytes, metadata per
      transport; the generated cross-transport equality property.
    - Tag: `// Feature: tonic-0-14-grpc-stack, Property 2: status parity across transports`
    - _Requirements: 6.2_
  - [x] 1.3 Decode-limit, compression, reflection, and gRPC-Web fixtures
    - Probes at 4 MiB minus one, 4 MiB, 4 MiB plus one; the encoding matrix; the
      `v1alpha` service listing; unary gRPC-Web samples.
    - _Requirements: 6.2_
  - [x] 1.4 Binding Inventory extractor and fixture
    - `crates/tokeira-proto/tests/binding_inventory.rs` derives the inventory from the
      Descriptor Sets with prost-reflect and writes
      `tests/fixtures/binding-inventory.json` in capture mode.
    - DONE 2026-09-06: derived with `prost-types` alone (no prost-reflect needed).
    - _Requirements: 2.3, 6.2_

- [x] 2. Checkpoint: fixtures captured and committed, all suites green on the Legacy Stack

- [x] 3. Codegen Tool and Generated Bindings
  - DONE 2026-09-06: the convenience `protox::compile` returns a `prost_types` set, which
    cannot carry extension fields, so the first regeneration silently lost every
    `google.api.http` option and the HTTP API catalog came up empty (its own tests caught
    it). The tool now drives `protox::Compiler` and writes `encode_file_descriptor_set`
    bytes, which keep the options; the `prost_types` view is decoded from those bytes for
    code generation only. The `google.protobuf` package differs between the two compilers'
    bundled `descriptor.proto` copies and is set aside by the inventory comparison; every
    Temporal and Tokeira entry is identical.
  - [x] 3.1 `proto-sync` on `protox` and `tonic-prost-build` 0.14
    - Replace the three `tonic_build` passes with `protox::compile` plus
      `compile_fds`; write the Descriptor Sets from the compiled set; keep the
      internal-package guard and the connect-rust pass; add the `check` mode.
    - _Requirements: 2.1, 2.2, 2.4, 2.6_
  - [x] 3.2 Regenerate the bindings and move `tokeira-proto` to prost 0.14, tonic 0.14, and
    `tonic-prost`
    - _Requirements: 2.5_
  - [x] 3.3 Property test: Property 8 — Binding Inventory parity
    - Tag: `// Feature: tonic-0-14-grpc-stack, Property 8: Binding Inventory parity`
    - _Requirements: 2.3, 2.6_
  - [x] 3.4 Property test: Property 12 — regeneration is reproducible
    - Tag: `// Feature: tonic-0-14-grpc-stack, Property 12: regeneration is reproducible`
    - DONE 2026-09-06: `tools/proto-sync/tests/reproducible.rs` runs the binary's `check`.
    - _Requirements: 2.1, 2.2, 2.4_

- [x] 4. Checkpoint: proto crate compiles on the Target Stack, inventory equal

- [x] 5. Edge on the Target Stack
  - DONE 2026-09-06: the edge library compiled on the new pins after only the in-process
    bridge changed; the bridge now builds `http` 1 requests, inserts the caller's headers
    directly, collects the body with its trailers, and keeps the four-key filter. The
    0.14 router is `Sync`, so the mutex around it went. The edge's own 533 tests are green.
  - [x] 5.1 Services and interceptors
    - `into_service` keeps gzip both ways and states the 4 MiB decode limit; `Routes` from
      `tonic::service`; `grpc/errors.rs` unchanged in behaviour.
    - _Requirements: 3.3, 3.4, 6.1_
  - [x] 5.2 In-Process Endpoint on `http` 1 and `tonic::body::Body`
    - Direct header insertion; `BodyExt::collect` with trailers; the four-key filter;
      admission, drain, abort-on-drop, and frame parsing untouched; `hyper-legacy` removed
      from the edge manifest.
    - _Requirements: 4.2, 4.3, 4.4_
  - [x] 5.3 HTTP API transcoder on prost-reflect 0.16; Nexus HTTP handler types
    - _Requirements: 5.3_
  - [x] 5.4 Wire-coverage layer retyped
    - DONE 2026-09-06: the layer was already generic over the body type; no change.
    - _Requirements: 3.1_
  - [x] 5.5 Property tests: Properties 3, 4, 5, 6
    - Tags: `// Feature: tonic-0-14-grpc-stack, Property 3: in-process header fidelity`,
      `Property 4: unary framing parity`, `Property 5: decode-limit boundary`,
      `Property 6: compression negotiation`
    - DONE 2026-09-06: Properties 5 and 6 are the golden probes in
      `crates/tokeira-engine/tests/wire_parity.rs`; Property 3 is the in-process bridge's
      header tests plus the catalogue's metadata comparison; Property 4 is the catalogue's
      byte-identical decisions across the three surfaces.
    - _Requirements: 3.3, 3.4, 4.2, 4.3, 4.4_

- [x] 6. Checkpoint: edge compiles, clippy clean, edge tests green

- [x] 7. Engine on the Target Stack
  - DONE 2026-09-06: listener and adapters on `tonic::body::Body` and `http` 1; both
    reflection protocols served; the SDK seam passes the edge's `Status` through and the
    translation function is gone. Golden comparison: every code, message, detail payload,
    compression answer, gRPC-Web frame, and reflection listing reproduced; two
    library-owned strings moved with the stack and are recorded at their new values (the
    decode-limit rejection wording, and tower-http 0.6's CORS `vary` value on the public
    listener), per Requirement 6.3.
  - [x] 7.1 Public Listener assembly
    - Same layer order and `accept_http1(true)`; tonic-web 0.14; tower-http 0.6 CORS;
      reflection served as `v1` and `v1alpha`.
    - _Requirements: 3.1, 3.2, 3.5_
  - [x] 7.2 Transport Adapters on `http` 1 bodies
    - `Limited` plus `collect` at the existing bounds; `http-body-legacy` and
      `hyper-legacy` removed from the engine manifest.
    - DONE 2026-09-06: frame-by-frame reads keep the bound applied before a chunk is
      copied, as before; `Limited` was not needed.
    - _Requirements: 5.1, 5.2_
  - [x] 7.3 Host Listener and `ResetOnStop` retyped
    - _Requirements: 4.1_
  - [x] 7.4 SDK seam unification
    - Delete `to_sdk_status` and the `tonic-sdk` alias; `service_override` passes the
      edge's `Status` through.
    - _Requirements: 1.5, 4.5_
  - [x] 7.5 Property tests: Properties 7, 9, 10, 11
    - Tags: `// Feature: tonic-0-14-grpc-stack, Property 7: reflection inventory`,
      `Property 9: adapter bounds`, `Property 10: cancellation and reset`,
      `Property 11: gRPC-Web parity`
    - DONE 2026-09-06: Property 7 and 11 are golden probes in `wire_parity.rs`; Property 9
      is the adapters' existing bound tests, re-run on the new body type; Property 10 is
      the listener suite's reset and drain tests plus the bridge's admission test.
    - _Requirements: 3.5, 3.6, 4.1, 4.4, 5.1, 5.2_
  - [x] 7.6 Property 2 (Target leg) and every Golden comparison green
    - _Requirements: 6.3_

- [x] 8. Checkpoint: engine compiles, clippy clean, engine and listener tests green

- [x] 9. Clients, apps, and tooling
  - [x] 9.1 Controller, `tokeirad`, bench, and `listener_support` on tonic 0.14 and
    `tonic_prost::ProstCodec`; `hyper-legacy` removed from `tokeirad`
    - DONE 2026-09-06: the `tokeirad` facade test now drives gRPC-Web over a raw socket,
      since hyper 1 ships no client of its own.
    - _Requirements: 7.1_
  - [x] 9.2 Documentation: `docs/development.md` and `docs/crates/proto.md` describe the
    `protox` path and drop the `protoc` prerequisite; `docs/crates/edge.md` and
    `docs/crates/engine.md` name the stack
    - DONE 2026-09-06: the edge and engine docs describe the surfaces without naming
      library versions and needed no change.
    - _Requirements: 7.2_

- [x] 10. Manifests, lock, and policy
  - [x] 10.1 Workspace pins moved per the Dependency Policy; every legacy alias deleted;
    `Cargo.lock` regenerated and reviewed for the expected removals
    - DONE 2026-09-06: the Connector Exception also carries `http 0.2` and `http-body 0.4`
      under hyper 0.14; Requirement 1 and Property 1 were narrowed accordingly.
    - _Requirements: 1.1, 1.2, 1.4_
  - [x] 10.2 `deny.toml` RUSTSEC-2026-0258 entry narrowed to the Connector Exception or
    removed
    - _Requirements: 1.6_
  - [x] 10.3 Property test: Property 1 — one stack in the lock
    - Tag: `// Feature: tonic-0-14-grpc-stack, Property 1: one stack in the lock`
    - _Requirements: 1.1, 1.2, 1.3_
  - [x] 10.4 Release notes: `changed` entries for the prost and tonic type moves and a
    `removed` entry for the `protoc` requirement; the 0.3.0 bump itself belongs to the
    release train
    - _Requirements: 8.1_

- [x] 11. Checkpoint: the §10.4 bar is green on the Target Stack
  - DONE 2026-09-06: fmt, lint, check, the full nextest suite, doctests, and docs green on
    the bar host at this tree. The first run exposed a build-dependent ordering in the
    Binding Inventory probe (a workspace-wide build unifies `serde_json/preserve_order`
    into the test binary and changes the text the entries were sorted by); the probe now
    orders both sides by a key-sorted canonical rendering.

- [x] 12. Evidence
  - [x] 12.1 Functional conformance corpus: rerun every tier recorded CLEAN against the
    migrated `tokeirad`; record the rerun in `docs/readiness/conformance.md`
    - DONE 2026-09-06: All 45 previously CLEAN tiers were exercised. Against the documented
      v0.1.0 release baseline, all 64 test-bearing entrypoints match their recorded totals:
      1,261 pass outcomes, 22 native skips, 106 existing registry exclusions, and zero fail
      or unfinished outcomes. The comparison retains the release's disclosed HTTP request
      adjustment; the four original-request failures reproduce before the migration and
      remain a separate timing investigation. Evidence and source revisions are recorded in
      [the readiness ledger](../../../docs/readiness/conformance.md#2026-09-06-migration-verification).
    - _Requirements: 6.4_
  - [x] 12.2 SDK 1.0.0 probes and the bench over both transports
    - DONE 2026-09-06: `spikes/temporal-rust-sdk-v0-7-embedded` green on the migrated
      engine (the continue-as-new worker over the in-process seam and over a listener, and
      both in-memory shutdown probes); the bench's tests run in the workspace suite.
    - _Requirements: 6.5_

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["0"] },
    { "id": 1, "tasks": ["1.1"] },
    { "id": 2, "tasks": ["1.2", "1.3", "1.4"] },
    { "id": 3, "tasks": ["2"] },
    { "id": 4, "tasks": ["3.1"] },
    { "id": 5, "tasks": ["3.2"] },
    { "id": 6, "tasks": ["3.3", "3.4", "4"] },
    { "id": 7, "tasks": ["5.1", "5.2", "5.3", "5.4"] },
    { "id": 8, "tasks": ["5.5", "6"] },
    { "id": 9, "tasks": ["7.1", "7.2", "7.3", "7.4"] },
    { "id": 10, "tasks": ["7.5", "7.6", "8"] },
    { "id": 11, "tasks": ["9.1", "9.2"] },
    { "id": 12, "tasks": ["10.1", "10.2", "10.3", "10.4"] },
    { "id": 13, "tasks": ["11"] },
    { "id": 14, "tasks": ["12.1", "12.2"] }
  ]
}
```

## Notes

- Task 0 is the Architectural-class approval the root change classification requires for
  dependency movement, and it carries the breaking-release decision (0.3.0).
- Task group 1 must land as its own commit before task 3.1 changes any manifest: the
  fixtures are only Goldens if the Legacy Stack produced them.
- tonic-web and tonic-reflection 0.14.6 are in the registry index but not in the local
  source cache; task 3.1 is the first step that needs a registry fetch.
- The `tonic-prost` codec is what the 0.14 generator emits; it is a runtime dependency of
  `tokeira-proto` and of any crate that drives a generated client directly.
- The Connector Exception is encoded in Property 1 so that the connector slice, when it
  lands, flips one expectation rather than rediscovering the rule; `deny.toml`'s entry
  follows the same trigger.
- Task 12.1 runs on the operator's build host per the conformance runbook; it is part of
  this feature's acceptance.
