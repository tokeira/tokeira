# Implementation Plan

- [ ] 1. Expose the assembled routes from the edge
  - [ ] 1.1 Add `InProcessGrpcService::routes(&self) -> Routes`
    - Clone the mutex-guarded `Routes` in `crates/tokeira-edge/src/in_process.rs`; document
      that the clone dispatches into the same services, interceptors, and handler runtime
      as `call`, and that listener calls bypass `InProcessAdmission` by design.
    - Unit test: a clone serves the same routes as the original for one unary call.
    - _Requirements: 2.1, 2.5_

- [ ] 2. Listener API and registry in `tokeira-engine`
  - [ ] 2.1 Add `EngineListenError`, `EngineListenerShutdownError`, and
    `EmbeddedShutdownFailure::ListenerDrain`
    - `thiserror` enums with the messages from the design's error table; extend the
      `Display` of `EmbeddedEngineShutdownError` for the new failure kind.
    - _Requirements: 4.1, 4.3, 4.4, 5.5, 6.1_
  - [ ] 2.2 Add `ListenerSlot`, `ListenerRegistry`, and the `Engine::listeners` field
    - Child cancellation token per listener derived from `background_cancel`; shared
      join-handle slot; registry behind a `std::sync::Mutex` so `Drop` can read it.
    - _Requirements: 1.5, 5.6, 5.9_
  - [ ] 2.3 Implement `Engine::listen(addr)`
    - Refuse after shutdown began; bind; resolve the bound address; build
      `Server::builder().add_routes(service.routes())` plus the pinned reflection service;
      spawn `serve_with_incoming_shutdown` on the engine-host runtime; register the slot.
    - Never read `infrastructure.network.grpc_addr`.
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.6, 1.7, 3.1, 3.2, 3.3, 4.1, 4.2, 4.3_
  - [ ] 2.4 Implement `EngineListener` (`bound_addr`, `shutdown`, `Drop`)
    - `shutdown`: cancel, await the task under the 30 s listener deadline, deregister,
      map errors. `Drop`: cancel only.
    - _Requirements: 5.1, 5.2, 5.7, 5.9, 6.2_
  - [ ] 2.5 Integrate listener stop into `Engine::shutdown`
    - Cancel every slot after `coordinator.begin_shutdown()` signals the runtime, await
      all tasks within the engine deadline before `coordinator.shutdown`, push
      `ListenerDrain` on timeout or task error, then continue the existing sequence.
      `Drop for Engine` needs no change beyond the child-token derivation.
    - _Requirements: 5.3, 5.4, 5.5, 5.6, 5.8_

- [ ] 3. Checkpoint: engine and edge compile, clippy clean, existing embedded tests green
  - `cargo clippy -p tokeira-edge -p tokeira-engine --all-targets --locked`;
    `cargo nextest run -p tokeira-engine --locked`; the zero-listener effect-model
    property still passes unchanged.
  - _Requirements: 6.4_

- [ ] 4. Example-based tests (`crates/tokeira-engine/tests/embedded_listener.rs`)
  - [ ] 4.1 Ephemeral bind reports a concrete port; reflection lists the Workflow service
    - _Requirements: 1.2, 1.3, 1.6_
  - [ ] 4.2 Occupied port returns `Bind`; engine still serves in-process afterwards
    - _Requirements: 4.1, 4.2_
  - [ ] 4.3 `listen` after `shutdown` began returns `ShutDown`
    - _Requirements: 4.3_
  - [ ] 4.4 Listener drop without `shutdown` releases the port; engine still serves
    - _Requirements: 5.7, 5.2_
  - [ ] 4.5 Metadata and status-details round trip over the listener match in-process
    - Send request metadata, assert response metadata and a rich error's
      `grpc-status-details-bin` are identical on both transports.
    - _Requirements: 2.6_

- [ ] 5. Property test: Property 3 — bind failure is a no-op
  - `proptest` over occupied and unbindable addresses; assert registry, startup report,
    and in-process behaviour are unchanged and no task was spawned.
  - Tag: `// Feature: embedded-engine-listener, Property 3: bind failure is a no-op`
  - _Requirements: 4.1, 4.2, 3.2_

- [ ] 6. Property test: Property 4 — listener lifecycle state machine
  - Reference-model `proptest` over generated interleavings of listen, listener shutdown,
    listener drop, engine shutdown, and engine drop across multiple listeners; assert
    accept windows, socket release, no leftover tasks, and `ShutDown` after engine
    shutdown.
  - Tag: `// Feature: embedded-engine-listener, Property 4: listener lifecycle state machine`
  - _Requirements: 1.5, 4.3, 5.1, 5.2, 5.6, 5.7, 5.9_

- [ ] 7. Property test: Property 6 — startup stays zero-listener
  - Extend the embedded startup effect model in `crates/tokeira-engine/src/lib.rs` so
    every start path across storage modes still records `listener_attempts == 0`, and
    only `listen` increments it.
  - Tag: `// Feature: embedded-engine-listener, Property 6: startup stays zero-listener`
  - _Requirements: 1.7, 6.4_

- [ ] 8. Integration properties over the in-memory engine (`embedded_listener.rs`)
  - [ ] 8.1 Property 1 — one engine behind two transports
    - Generated RPC sequences split between transports: start via callback, poll and
      complete via a raw-proto worker over TCP, query, update, update-with-start,
      activity heartbeat and cancellation, describe and history on both sides; compare
      against the all-in-process outcome. Exercise namespace `tokeira-cloud` alongside
      `default`.
    - Tag: `// Feature: embedded-engine-listener, Property 1: one engine behind two transports`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.6_
  - [ ] 8.2 Property 2 — authorization parity across transports
    - Start with a JWT authorization policy from `TokeiraConfig`; for generated
      identity/metadata cases assert equal statuses on both transports, including
      `PERMISSION_DENIED` and `UNAUTHENTICATED` mappings.
    - Tag: `// Feature: embedded-engine-listener, Property 2: authorization parity across transports`
    - _Requirements: 2.5_
  - [ ] 8.3 Property 5 — engine shutdown drains listeners first and within the deadline
    - Hold open long polls and a cancelled long poll over the listener during
      `Engine::shutdown`; assert ordering (listener stopped before in-process drain),
      completion inside the deadline, and `ListenerDrain` only when a handler is pinned
      past the deadline.
    - Tag: `// Feature: embedded-engine-listener, Property 5: engine shutdown drains listeners first`
    - _Requirements: 2.7, 5.3, 5.4, 5.5, 5.8_

- [ ] 9. Live DSQL evidence (`crates/tokeira-engine/tests/live_managed_dsql.rs`)
  - [ ] 9.1 Property 7 — listener attachment is storage-inert
    - In the existing managed lifecycle test: attach a listener, run a network worker
      through one execution, stop the listener, shut down, restart against the same
      history, and assert a competing start still fails at the ownership phase while a
      listener is attached.
    - Tag: `// Feature: embedded-engine-listener, Property 7: listener attachment is storage-inert`
    - _Requirements: 3.1, 3.3, 7.1, 7.2_

- [ ] 10. Documentation and sibling-spec alignment
  - [ ] 10.1 README, crate README, and `docs/crates/engine.md`
    - Complete start, listen, connect, stop, shutdown example; contracts list gains the
      optional listener.
    - _Requirements: 6.3_
  - [x] 10.2 Narrow the zero-listener statements in `managed-embedded-dsql`
    - Glossary "Embedded Engine", Requirement 1.7, and design Property 14 read "binds no
      listener at startup; a host may attach one through `Engine::listen`".
    - DONE with the spec: the glossary entry, the transport row of the target-state
      table, Requirement 1.7, and Property 14 were amended when this spec was authored.
    - _Requirements: 1.7, 6.4_
  - [ ] 10.3 Changie entry (`Added`) for the 0.1.3 train
    - _Requirements: 6.1_

- [ ] 11. Container evidence (joint with Tokeira Cloud, not in the default suite)
  - Run the pinned Rust SDK worker from the `spikes/` SDK crate and from a container
    against a host listener using only the published API; record the outcome in the PR.
  - _Requirements: 7.3_

- [ ] 12. Checkpoint: the §10.4 bar is green
  - Formatting, workspace lint, check, nextest, doctests, and docs with `--locked`.
  - _Requirements: 6.4_

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["2.1", "2.2"] },
    { "id": 2, "tasks": ["2.3", "2.4"] },
    { "id": 3, "tasks": ["2.5"] },
    { "id": 4, "tasks": ["3"] },
    { "id": 5, "tasks": ["4.1", "4.2", "4.3", "4.4", "4.5", "5", "6", "7"] },
    { "id": 6, "tasks": ["8.1", "8.2", "8.3"] },
    { "id": 7, "tasks": ["9.1", "10.1", "10.2", "10.3"] },
    { "id": 8, "tasks": ["11", "12"] }
  ]
}
```

## Notes

- The listener serves gRPC and reflection only. The daemon's HTTP/JSON, gRPC-Web, and
  Nexus HTTP layers are already constructed for both transports and dropped on the
  embedded branch, so a later `ListenOptions` is additive and needs no restructuring.
- The raw-proto worker loop in the integration tests keeps `temporalio-sdk` out of the
  engine's dev-dependencies (a dependency change is a separate reviewed decision). Real
  SDK-worker evidence comes from task 11.
- Task 9 needs the live DSQL host and the `dsql-integration` feature, per the existing
  live test's own gating.
- This feature ships in the same 0.1.3 train as
  [continue-as-new-advice](../continue-as-new-advice/tasks.md); that plan's
  transport-independence task depends on task 2 here.
