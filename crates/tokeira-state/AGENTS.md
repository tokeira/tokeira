# AGENTS — tokeira-state

Crate-local rules. The root `AGENTS.md` still applies; this refines it for deployment
state persistence. On conflict, the stricter rule wins.

## The one boundary: state writes are compare-and-swap, and snapshots are immutable

This crate persists deployment state for the IaC/runtime engines. State corruption is
expensive and hard to reverse, so the concurrency and durability contracts are not
optional:

- **CAS, never force-overwrite.** A save carries the caller's expected version. If it is
  stale, the backend MUST return `StateError::Conflict` — the caller re-reads, re-plans,
  and retries. Do not add a "force" path, do not swallow a conflict, do not last-writer-wins.
  (`CasStore` writes the full document through the manifest API; `S3StateStore` uses an
  ETag/`If-Match` CAS plus a lease lock. Both honor the same contract.)
- **Snapshots are immutable.** In `S3StateStore`, `snapshots/<ts>-<uuid>.json` objects are
  write-once; only the `manifest.json` pointer is mutable (and only via CAS). Never rewrite
  or delete an existing snapshot in place.
- **Tolerate a missing store on load.** `load()` MUST return the document's `Default` when
  the backing store does not yet exist, so the remote-state module can bootstrap the store
  on the first apply. A missing store is a normal cold-start, not an error.
- **Validate on the boundary.** Documents implement `Validate`; the store validates after
  load and before save (root: "typos caught at parse time" extends to structural state).
  Do not bypass validation to persist a "temporarily" invalid document.

## Evolving a state document type

State documents are serde values shared across deploy cycles, so an old store must still
deserialize under new code:

- Add fields with `#[serde(default)]`; do not repurpose or silently change the meaning of an
  existing field.
- Removing or renaming a field is a breaking change to on-disk/in-S3 state — treat it as
  Destructive (root "Change Classification"): spec update or explicit approval, and a
  migration story for existing state, before doing it.

## Where things belong instead

- *What* infrastructure/runtime state means and how it converges → the IaC engine
  (`tokeira-iac`) and the platform modules. This crate stores and CAS-guards bytes; it does
  not interpret deployment semantics.
- Domain validation beyond structural integrity → the owning engine's `Validate` impl, not
  the store.
