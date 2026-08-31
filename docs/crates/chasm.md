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
| Visibility | `VisibilityContributor`, `VisibilitySnapshot`, search-attribute provider |

## Invariants

- The crate is deterministic and side-effect-free: no I/O, async, storage,
  networking, or metrics.
- Fallible framework operations return `ChasmError`; panics are not a
  framework-control boundary.
- Field discovery is static. `tokeira-chasm-derive` generates the registry at
  compile time, and this crate performs no runtime reflection.
- Closing a transition stamps dirty nodes with one monotonic execution clock and
  returns a complete staged-task outbox.
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
