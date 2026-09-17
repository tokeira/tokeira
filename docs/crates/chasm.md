# tokeira-chasm

Pure CHASM component state-machine substrate. It defines reusable components,
node trees, transitions, tasks, addressing, and visibility contributions for
durable execution beyond the workflow state machine.

## Where it sits

CHASM is a peer of `tokeira-kernel` in the authoritative runtime and storage
plane. It computes what changes and what work is staged; `tokeira-runtime`
chooses when to run a transition and `tokeira-storage` persists it.

## Surface map

| Area | Representative contracts |
|---|---|
| Components | `Component`, `Lifecycle`, `RootComponent`, `EngineComponent`, `LifecycleState` |
| Fields | `Field`, `Map`, `ParentPtr`, `FieldRegistry`, `NodeHandle` |
| Tree | `ExecutionKey`, `ChasmNode`, `NodeTree`, `TransitionResult` |
| Clock | `VersionedTransition`, `Staleness` |
| Addressing | `ComponentRef`, `PathEncoder`, `PathSegment` |
| Registry | `Library`, `RegistryBuilder`, `Registry`, archetype identifiers |
| Tasks | `Task`, `TaskOutbox`, `TaskKind`, validators and scheduled tasks |
| Handlers | `PureTaskHandler`, `SideEffectTaskHandler` |
| Task identity | `TaskId`, `task_type_id_for_fqn`, `RESERVED_TASK_ID_LIMIT` |
| Outbox policy | `OutboxValidator`, `RegistryOutboxValidator`, `RetainAllValidator` |
| Staged work | `StartActivityTask`, `DeploymentVersionTarget` |
| Visibility | `VisibilityContributor`, `VisibilitySnapshot`, search-attribute provider |

## Typed task behaviour

A library registers behaviour for a task type through one of two traits, each bound
to a single materializable root component:

- `PureTaskHandler` validates, then mutates the component inside the fenced
  transition. It mirrors `chasm/task.go` and `chasm/registrable_task.go @ v1.31.0`.
- `SideEffectTaskHandler` validates, then applies an external result through
  `on_outcome`. It deliberately omits upstream's side-effect `Execute`/`Discard`:
  the I/O belongs to a runtime executor, so the substrate stays pure.

Registration erases both into monomorphized byte-codec closures. There is no
reflection, and child materialization is not supported.

## Task identity and the sealed built-in rule

Every task type resolves to a stable `u32`. Ids below `RESERVED_TASK_ID_LIMIT`
(1024) belong to built-in libraries, which keeps existing activity outboxes —
ids 1 to 5 — readable across releases. Every other id is
`task_type_id_for_fqn(FQN)`, an FNV-1a/32 hash of the fully-qualified name.

`RegistryBuilder::seal_built_ins` freezes the library names registered so far, and
repeated calls are idempotent. After the seal an explicit reserved id is refused,
and a derived id landing inside the reserved range is reported as a collision
rather than silently overlapping a built-in. An extension therefore cannot be
promoted into the built-in range, by claiming an id or by hashing into one.

## Invariants

- The crate is deterministic and side-effect-free: no I/O, async, storage,
  networking, or metrics.
- Fallible framework operations return `ChasmError`; panics are not a
  framework-control boundary.
- Field discovery is static. `tokeira-chasm-derive` generates the registry at
  compile time, and this crate performs no runtime reflection.
- Closing a transition stamps dirty nodes with one monotonic execution clock and
  returns a complete staged-task outbox. An `OutboxValidator` runs at close, so a
  task made obsolete by the same transition is dropped rather than staged;
  `RegistryOutboxValidator` delegates to the registered handler's `validate`.
- A task type's id is stable for the life of the persisted outbox, and the
  built-in id range is sealed against extensions.
- Component references carry enough version information to detect stale access.
- Business-ID reuse and conflict policies are pure inputs to the runtime start
  path.

## It does not own

The crate does not load or store node rows, retry CAS conflicts, execute tasks,
evaluate wall-clock timers, or serve Activity Execution RPCs. Those belong to
storage, runtime, and edge respectively.

## Pointers

- [Crate root](../../crates/tokeira-chasm/src/lib.rs)
- [Derive macro](chasm-derive.md)
- [Standalone activity](chasm-activity.md)
- [Runtime CHASM facade](../../crates/tokeira-runtime/src/chasm/mod.rs)
- [CHASM storage contract](../../crates/tokeira-storage/src/chasm.rs)
