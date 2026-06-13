# Tokeira Scenarios

End-to-end **scenario samples** that exercise Tokeira's distinctive machinery against a
running `tokeirad`. Each scenario drives a complete, observable flow — start to
finish — through a capability that is hard to validate with a unit test or a
single-call SDK example.

## What these are not

These are **not** "how to use the SDK" examples. Basic usage — connecting a client,
defining a workflow, scheduling an activity, polling for a result — is covered by the
client SDK examples (the Temporal Rust SDK ships its own `hello_world`,
`message_passing`, `continue_as_new`, `child_workflows`, and more). Duplicating those
here adds maintenance cost without exercising anything Tokeira-specific.

## What these are

Scenarios target the **server-side machinery** that is unique to or load-bearing in
Tokeira: worker versioning and deployment routing, history replay determinism, the
authoritative per-run transition log, drainage lifecycles, projection/visibility
behaviour, and similar. A good scenario:

- drives a multi-step flow that only makes sense end-to-end (e.g. ramp traffic to a new
  version, observe routing change, drain the old version);
- asserts an **observable** outcome a user or operator could see (describe output,
  routing decisions, completion results), not an internal data structure;
- is reproducible against a local `tokeirad` and documents the exact steps to run it;
- doubles as living documentation for the capability it exercises.

## Layout

Each scenario is a **standalone Cargo project** (its own `Cargo.toml` and `target/`),
listed in the workspace `exclude` set — not a workspace member. It builds against the
published client SDK crates the same way a downstream consumer would, so the scenario
also acts as a consumer-perspective canary. Build one with:

```bash
cargo build --manifest-path scenarios/<name>/Cargo.toml
```

| Scenario | Exercises | Status |
|----------|-----------|--------|
| `worker-versioning/` | Worker Deployment routing: current/ramping versions, pinned vs auto-upgrade behaviour, version transition, drainage | Design / scaffold — gated on the `worker-deployments` spec landing |
| `standalone-activities/` | Standalone-activity lifecycle (CHASM component #1): start → worker poll/respond → describe, across completed / failed / terminated outcomes | Runnable against a `tokeirad` with `enable_standalone_activities = true` |

## Adding a scenario

1. Create `scenarios/<name>/` with its own `Cargo.toml` (path or published-crate deps).
2. Add `scenarios/<name>` to the `exclude` list in the workspace root `Cargo.toml`.
3. Write a `README.md` that states what machinery it exercises, the observable outcome
   it asserts, and the exact run steps.
4. Keep the scenario reproducible against a local `tokeirad`; document any required
   server configuration.
