# Implementation Plan

- [ ] 0. Approval checkpoint: dependency change and breaking release
  - The integration seat approves the move to the Target Stack, the adoption of `protox`
    in place of `protoc`, the regeneration of the checked-in bindings, and the 0.3.0
    release before any task below starts.
  - _Requirements: 1.1, 2.1, 8.1_

- [ ] 1. Goldens on the Legacy Stack (lands before any dependency moves)
  - [ ] 1.1 Wire-parity harness with capture mode
    - `crates/tokeira-engine/tests/wire_parity.rs` over the In-Process Endpoint and a Host
      Listener; `WIRE_PARITY_CAPTURE=1` writes fixtures, otherwise compares.
    - _Requirements: 6.2_
  - [ ] 1.2 Status catalogue fixture and Property 2 (Legacy leg)
    - Fixed list of edge error values; capture code, message, details bytes, metadata per
      transport; the generated cross-transport equality property.
    - Tag: `// Feature: tonic-0-14-grpc-stack, Property 2: status parity across transports`
    - _Requirements: 6.2_
  - [ ] 1.3 Decode-limit, compression, reflection, and gRPC-Web fixtures
    - Probes at 4 MiB minus one, 4 MiB, 4 MiB plus one; the encoding matrix; the
      `v1alpha` service listing; unary gRPC-Web samples.
    - _Requirements: 6.2_
  - [ ] 1.4 Binding Inventory extractor and fixture
    - `crates/tokeira-proto/tests/binding_inventory.rs` derives the inventory from the
      Descriptor Sets with prost-reflect and writes
      `tests/fixtures/binding-inventory.json` in capture mode.
    - _Requirements: 2.3, 6.2_

- [ ] 2. Checkpoint: fixtures captured and committed, all suites green on the Legacy Stack

- [ ] 3. Codegen Tool and Generated Bindings
  - [ ] 3.1 `proto-sync` on `protox` and `tonic-prost-build` 0.14
    - Replace the three `tonic_build` passes with `protox::compile` plus
      `compile_fds`; write the Descriptor Sets from the compiled set; keep the
      internal-package guard and the connect-rust pass; add the `check` mode.
    - _Requirements: 2.1, 2.2, 2.4, 2.6_
  - [ ] 3.2 Regenerate the bindings and move `tokeira-proto` to prost 0.14, tonic 0.14, and
    `tonic-prost`
    - _Requirements: 2.5_
  - [ ] 3.3 Property test: Property 8 — Binding Inventory parity
    - Tag: `// Feature: tonic-0-14-grpc-stack, Property 8: Binding Inventory parity`
    - _Requirements: 2.3, 2.6_
  - [ ] 3.4 Property test: Property 12 — regeneration is reproducible
    - Tag: `// Feature: tonic-0-14-grpc-stack, Property 12: regeneration is reproducible`
    - _Requirements: 2.1, 2.2, 2.4_

- [ ] 4. Checkpoint: proto crate compiles on the Target Stack, inventory equal

- [ ] 5. Edge on the Target Stack
  - [ ] 5.1 Services and interceptors
    - `into_service` keeps gzip both ways and states the 4 MiB decode limit; `Routes` from
      `tonic::service`; `grpc/errors.rs` unchanged in behaviour.
    - _Requirements: 3.3, 3.4, 6.1_
  - [ ] 5.2 In-Process Endpoint on `http` 1 and `tonic::body::Body`
    - Direct header insertion; `BodyExt::collect` with trailers; the four-key filter;
      admission, drain, abort-on-drop, and frame parsing untouched; `hyper-legacy` removed
      from the edge manifest.
    - _Requirements: 4.2, 4.3, 4.4_
  - [ ] 5.3 HTTP API transcoder on prost-reflect 0.16; Nexus HTTP handler types
    - _Requirements: 5.3_
  - [ ] 5.4 Wire-coverage layer retyped
    - _Requirements: 3.1_
  - [ ] 5.5 Property tests: Properties 3, 4, 5, 6
    - Tags: `// Feature: tonic-0-14-grpc-stack, Property 3: in-process header fidelity`,
      `Property 4: unary framing parity`, `Property 5: decode-limit boundary`,
      `Property 6: compression negotiation`
    - _Requirements: 3.3, 3.4, 4.2, 4.3, 4.4_

- [ ] 6. Checkpoint: edge compiles, clippy clean, edge tests green

- [ ] 7. Engine on the Target Stack
  - [ ] 7.1 Public Listener assembly
    - Same layer order and `accept_http1(true)`; tonic-web 0.14; tower-http 0.6 CORS;
      reflection served as `v1` and `v1alpha`.
    - _Requirements: 3.1, 3.2, 3.5_
  - [ ] 7.2 Transport Adapters on `http` 1 bodies
    - `Limited` plus `collect` at the existing bounds; `http-body-legacy` and
      `hyper-legacy` removed from the engine manifest.
    - _Requirements: 5.1, 5.2_
  - [ ] 7.3 Host Listener and `ResetOnStop` retyped
    - _Requirements: 4.1_
  - [ ] 7.4 SDK seam unification
    - Delete `to_sdk_status` and the `tonic-sdk` alias; `service_override` passes the
      edge's `Status` through.
    - _Requirements: 1.5, 4.5_
  - [ ] 7.5 Property tests: Properties 7, 9, 10, 11
    - Tags: `// Feature: tonic-0-14-grpc-stack, Property 7: reflection inventory`,
      `Property 9: adapter bounds`, `Property 10: cancellation and reset`,
      `Property 11: gRPC-Web parity`
    - _Requirements: 3.5, 3.6, 4.1, 4.4, 5.1, 5.2_
  - [ ] 7.6 Property 2 (Target leg) and every Golden comparison green
    - _Requirements: 6.3_

- [ ] 8. Checkpoint: engine compiles, clippy clean, engine and listener tests green

- [ ] 9. Clients, apps, and tooling
  - [ ] 9.1 Controller, `tokeirad`, bench, and `listener_support` on tonic 0.14 and
    `tonic_prost::ProstCodec`; `hyper-legacy` removed from `tokeirad`
    - _Requirements: 7.1_
  - [ ] 9.2 Documentation: `docs/development.md` and `docs/crates/proto.md` describe the
    `protox` path and drop the `protoc` prerequisite; `docs/crates/edge.md` and
    `docs/crates/engine.md` name the stack
    - _Requirements: 7.2_

- [ ] 10. Manifests, lock, and policy
  - [ ] 10.1 Workspace pins moved per the Dependency Policy; every legacy alias deleted;
    `Cargo.lock` regenerated and reviewed for the expected removals
    - _Requirements: 1.1, 1.2, 1.4_
  - [ ] 10.2 `deny.toml` RUSTSEC-2026-0258 entry narrowed to the Connector Exception or
    removed
    - _Requirements: 1.6_
  - [ ] 10.3 Property test: Property 1 — one stack in the lock
    - Tag: `// Feature: tonic-0-14-grpc-stack, Property 1: one stack in the lock`
    - _Requirements: 1.1, 1.2, 1.3_
  - [ ] 10.4 Release notes: `changed` entries for the prost and tonic type moves and a
    `removed` entry for the `protoc` requirement; the 0.3.0 bump itself belongs to the
    release train
    - _Requirements: 8.1_

- [ ] 11. Checkpoint: the §10.4 bar is green on the Target Stack

- [ ] 12. Evidence
  - [ ] 12.1 Functional conformance corpus: rerun every tier recorded CLEAN against the
    migrated `tokeirad`; record the rerun in `docs/readiness/conformance.md`
    - _Requirements: 6.4_
  - [ ] 12.2 SDK 1.0.0 probes and the bench over both transports
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
