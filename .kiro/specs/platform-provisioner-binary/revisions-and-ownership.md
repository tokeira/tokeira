# `tkp` Revisions & Ownership — advance, restore, recover

A full, code-accurate reference for the four `tkp` lifecycle verbs that move a deployment between
**configuration revisions** and **engine identities**: `describe`, `revert`, `upgrade`, `rollback`.
(`resume` was dropped — see [§7](#7-recovery-there-is-no-resume-verb).)

- **Design:** [design.md](./design.md) §Command behaviour and outputs, §Upgrade and rollback
- **Requirements:** [requirements.md](./requirements.md) Req 4 (upgrade), Req 9 (rollback), Req 13 (config revision)
- **Governing proposal:** [002 — definition-driven rollback](./proposals/002-definition-driven-rollback.md)
- **Completion tracking:** [tasks.md](./tasks.md) task 19 (wave 11); status tasks 8.2 / 8.4 / 8.5 / 14.3

> **Status in one line.** All four verbs are wired and their **envelope state-machines are implemented and
> unit-tested**, but every mutating path is exercised only against the `local` platform's *empty* apply.
> The real cross-engine and multi-binary work (upgrade re-provisioning, rollback's two-binary orchestration,
> `describe`'s two views) is unbuilt — [§6](#6-what-is-missing).

---

## 1. The four verbs at a glance

| Verb | Moves | Engine identity | Config revision | Binary count | 002 mapping |
|------|-------|-----------------|-----------------|--------------|-------------|
| `describe` | nothing (read-only) | — | — | 1 | — |
| `revert --to <rev>` | config → a prior revision | **unchanged** | advances (monotonic-forward) | 1 (same engine) | **Configuration rollback** (§2) |
| `upgrade` | engine A → B | **changes** (A→B) | unchanged | 1 (candidate B) | opens the checkpoint rollback depends on |
| `rollback` | engine B → A | **changes** (B→A) | unchanged (restores A's ref) | 2 (B undo, A reconcile) | **Engine-upgrade rollback** (§2) |

The load-bearing distinction: **a config change is data (a revision); an engine change is code (an identity).**
`revert` is the config axis; `upgrade`/`rollback` are the identity axis. This is why `revert` is one binary
and needs no checkpoint, while `rollback` is two binaries over an `upgrade`'s checkpoint.

---

## 2. Relationship to Proposal 002 (definition-driven rollback)

[Proposal 002](./proposals/002-definition-driven-rollback.md) is **Accepted** and is the mechanism these
verbs implement. Its thesis: **rollback is not the inverse of what an apply did** — it is *restore the
retained prior configuration revision and let the forward engine reconcile toward it.* No before-images, no
per-kind inverse capability. The prior revision **is** the before-image, and it is deterministic and
hermetic (`platform-config-dsl` Proposal 003), so re-applying it provably reproduces the prior desired state.

002 defines **two rollback classes**, and they map directly onto two different `tkp` verbs:

### 2a. Configuration rollback (same engine) → `revert`
> *002 §"The two rollback classes": "restore the prior revision and `apply`. **One operation, one binary,
> no delta, no re-pin.** This is the everyday rollback."*

This is exactly `tkp revert --to <rev>` (Req 13.3). The engine identity does not move; the retained config
**source** for the target revision is restored into the live config file and the ordinary gated `apply`
reconciles toward it. This is 002's **Phase 1**, and it is **complete** ([§4b](#4b-revert)).

### 2b. Engine-upgrade rollback (cross engine A→B) → `rollback`
> *002 §Design "Rollback algorithm (upgrade case)": B deletes what it created → atomic re-pin to A →
> A `refresh_state` + forward-apply the retained revision R_a.*

This is `tkp rollback`, and it depends on an `upgrade` having captured a `[A final]` checkpoint. 002's
**Authorship invariant** — *no binary computes a delta over a representation authored by another engine* —
is preserved by splitting the work:

- **B's role collapses to delete-only** (`keys(S_B) − keys(S_A)`). Deletion is already state-driven, so B
  needs no new capability, and B (not A) must delete B-introduced kinds A cannot name (Req 10, fail-closed).
- **A reconciles over *live*, not over B's state.** After re-pin, A observes provider truth (`refresh_state`)
  and forward-applies its own retained revision R_a — never reinterpreting B's recorded state.

The checkpoint's **`from_config_ref`** is the "load-bearing rollback baseline" 002 calls for — A re-applies
its own retained revision. This is 002's **Phase 3**, and it is the **primary gap** ([§6](#6-what-is-missing)).

**Not before-images.** Per 002 Decision 4, the only thing that survives Proposal 001 is an optional
**ids-only** `ChangeLog` (`id + op`) for audit — never a before-image store. The `Operation` marker carries
exactly this (`audit_log: Option<ChangeLog>`), currently always `None`.

---

## 3. The shared substrate — what these verbs read and write

All four verbs operate over one authoritative document and a small set of sibling files under the
deployment directory. Understanding these is prerequisite to reading the per-verb mutation lists.

```
<deployment-dir>/
├── definition.tkd            # live config SOURCE (compose-syn)   ─┐ exactly one of these is
├── deployment.toml           # live config SOURCE (local)         ─┘ the "live config file"
└── state/
    ├── envelope/             # CAS store of the DeploymentStateEnvelope (THE authoritative doc)
    ├── config-revisions/
    │   └── <n>/<basename>    # retained config SOURCE for revision n (revert's raw material)
    ├── infra/                # infra state docs (written by platform::infra_apply; infra_head → here)
    └── deploy/               # runtime/service state docs (runtime_head → here)
```

### The `DeploymentStateEnvelope` — the authoritative document
`crates/tokeira-provisioner/src/lib.rs:214`. Stored via a `tokeira_state` CAS store
(`CasStore` over `LocalBackend` at `state/envelope`, constructed in `envelope_store`,
`apps/tkp/src/main.rs:150`). Every mutating verb ends in a `store.save(&envelope, &version)` — a
**compare-and-swap** commit against the loaded `version` (optimistic concurrency).

| Field | Meaning | Advanced by |
|-------|---------|-------------|
| `binding: Option<ProvenanceStamp>` | the engine identity authorized to operate (`A`/`B`); `None` = Unknown | `upgrade` (→B), `rollback` (→A) |
| `config_revision: u64` | monotonic config counter | `apply`, `revert` |
| `effective_config_ref: Option<String>` | `sha256:` of the live config source | `apply`, `revert`; restored by `rollback` |
| `integrity: Option<IntegrityManifest>` | manifest of the bound engine's bytes | `upgrade` (→B's), `rollback` (→A's) |
| `checkpoint: Option<RollbackCheckpoint>` | `[A final]` — captured by upgrade, consumed by rollback | `upgrade` (set), `rollback` (clear) |
| `operation: Option<Operation>` | in-flight marker (`UpgradeInFlight`/`RollbackInFlight` + resumable `phase`) | `upgrade`/`rollback` (open/close) |
| `lock: Option<OperationLock>` | remote mutual-exclusion lease | the lock wrapper (`with_operation_lock`) |
| `infra_head` / `runtime_head: Option<SnapshotRef>` | current infra/runtime state pointers | `platform::infra_apply`; restored by `rollback` |

The `RollbackCheckpoint` (`lib.rs:155`) holds `from_provenance` (A's stamp), `from_integrity` (A's manifest),
`from_infra_head` / `from_runtime_head` (A's state heads, spanning both engines), and **`from_config_ref`**
(A's prior config ref — the 002 baseline).

### Two distinct locks
- **Operator lock** (`lock.toml`, `tkr`-side) — confines mutation to one selected deployment (Req 8).
- **Operation lock** (`OperationLock` in the envelope + the process-level `with_operation_lock` wrapper,
  `apps/tkp/src/lock.rs`) — remote mutual exclusion so two mutations can't run at once. Every mutating verb
  in `main.rs` is wrapped: `lock::with_operation_lock(&dir, "<verb>", || …)` (`apps/tkp/src/main.rs:118‑140`).

---

## 4. Per-command mechanics

Every mutating verb follows the same shell contract (design §Command behaviour): **resolve → gate → lock →
plan → apply → record → report**. The gate (`apps/tkp/src/gate.rs`, `evaluate_gate`) refuses a non-`Match`
binding before any mutation, except the `DevIterate` dev path.

### 4a. `describe`

- **Class:** read-only. **Gate:** evaluated for *report* only — never blocks. **Lock:** none.
- **Dispatch:** `apps/tkp/src/main.rs:112` → `describe()` (`main.rs:157`).
- **Mechanics:** loads the envelope, builds a `DescribeReport` (`main.rs:207`, `::build` at `:222`) pairing
  the *running* `ProvenanceStamp::current` against the *recorded* binding, and prints either a human view
  (`print_human`, `main.rs:250`) or the full record as `--json`.
- **Mutates in the deployment dir:** **nothing.** Pure read of `state/envelope`.
- **Reports:** running stamp (version/git_sha/`source_tree_hash`/build_mode); envelope (id, `schema_version`,
  `config_revision`, `effective_config_ref`); binding verdict (+ proceeds/authoritative); integrity summary;
  infra/runtime head presence; the operation marker; the lock holder.
- **Status:** **complete for today's envelope**, as a *single consolidated view* + `--json`. The design's
  **two views** (short operator vs `--verbose` verification/debug with the full per-artifact manifest,
  `EngineIdentity`, snapshot ref, retained-revision list) is **not built** — task 19.1.

### 4b. `revert`

- **Class:** mutating (config axis). **Gate:** blocks on non-`Match`. **Lock:** `"revert"`.
- **Dispatch:** `apps/tkp/src/main.rs:129` → `revert::revert(&dir, to)` (`apps/tkp/src/revert.rs:28`).
- **Mechanics** (002 configuration-rollback, Req 13.3):
  1. Load envelope; **gate** (`revert.rs:37`).
  2. Refuse a non-prior target (`to_revision >= config_revision`) or an **unretained** one
     (`config_history::is_retained`, `revert.rs:56‑68`).
  3. **Restore** the target revision's retained config source into the live config file
     (`config_history::restore`, `apps/tkp/src/config_history.rs:79`).
  4. **Reconcile:** `platform::infra_apply` drives the live footprint toward the restored config.
  5. **Re-stamp:** `binding = running`; `config_revision += 1`; `effective_config_ref = config_ref(dir)`;
     snapshot the *new* revision; CAS-save (`revert.rs:86‑94`).
- **Mutates in the deployment dir:**
  - the **live config file** (`definition.tkd` / `deployment.toml`) ← restored from the target snapshot;
  - `state/infra/*` (+ `infra_head`) ← via the reconcile (real for compose-syn; **empty for local**);
  - `state/config-revisions/<new_n>/<basename>` ← the new forward revision's snapshot;
  - `state/envelope` ← binding, `config_revision` (+1), `effective_config_ref`.
- **Monotonic-forward:** reverting to `N` produces a *new* revision whose content equals `N`'s — the counter
  is never rewound, so history stays append-only and a revert is itself revertable.
- **Status:** **complete** as a single-binary flow (task 14.3). Its only dependency for real effect is that
  `platform::infra_apply` becomes a real platform apply (compose-syn), covered by task 15.

### 4c. `upgrade`

- **Class:** mutating (identity axis). **Gate:** permits the `Candidate` (running B ≠ recorded A). **Lock:** `"upgrade"`.
- **Dispatch:** `apps/tkp/src/main.rs:134` → `upgrade::upgrade(&dir)` (`apps/tkp/src/upgrade.rs:25`).
- **Mechanics** (task 5.3 / 8.4; the only verb that authoritatively advances the engine identity):
  1. Load envelope; require a recorded `A` (**refuse unstamped**, `upgrade.rs:34`).
  2. `evaluate_upgrade(A, B)` → `Refuse` | `VersionedAdvance` | `Promotion`
     (`crates/tokeira-provisioner/src/upgrade.rs:26`).
  3. **State-schema migration boundary** (before any mutation): `MigrationRegistry::check_path` refuses an
     unbridged transition; `needs_migration` runs the forward migration and advances `schema_version`
     (`upgrade.rs:57‑67`). *The registry is currently empty, so this is a same-schema no-op.*
  4. **Atomic ownership transfer** (`transfer_ownership`, `upgrade.rs:101`): `envelope.begin_upgrade(B, …)`
     (`lib.rs:279`) captures `[A final]` as the `RollbackCheckpoint` (incl. A's integrity, state heads, and
     `from_config_ref`), flips `binding → B`, opens the `UpgradeInFlight` marker (`phase = "ownership-
     transferred"`); then **re-records `integrity` for B** (`running_integrity_manifest`). Persisted in
     **one CAS commit before any provider mutation** (`upgrade.rs:70‑74`).
  5. **Apply B's plan:** `platform::infra_apply` (`upgrade.rs:78`).
  6. **Close the marker:** `envelope.close_operation()` (`lib.rs:351`); CAS-save.
- **Mutates in the deployment dir:**
  - `state/envelope` **twice** — (i) the atomic transfer (checkpoint set, `binding → B`, marker open,
    `integrity → B`), then (ii) the marker close. The checkpoint is **retained** past close (rollback needs it).
  - `state/infra/*` (+ `infra_head`) ← via B's apply (real for compose-syn; **empty for local**).
  - It does **not** touch `config_revision` or `config-revisions/` — an upgrade is an engine change, not a
    config change.
- **Crash safety:** because the ownership transfer is one CAS commit *before* any provider mutation, a crash
  at any point recovers as **B with an open marker**, never an ambiguous "pending" binding (Property 15).
- **Status:** **ownership-transfer machinery done + tested** (`upgrade_refuses_an_unstamped_deployment`,
  `upgrade_refuses_versioned_to_dev_restamp`, `ownership_transfer_rerecords_integrity_for_the_new_engine`).
  "Apply B's plan" is only the local **empty** apply; multi-platform re-provisioning, a populated migration
  registry, the audit change log, and the advisory baseline drift gate are missing — task 19.2.

### 4d. `rollback`

- **Class:** mutating (identity axis, two-binary). **Gate:** permits the `Rollback` class. **Lock:** `"rollback"`,
  held **continuously** across the whole sequence (Req 12.2, via the `main.rs:138` wrapper).
- **Dispatch:** `apps/tkp/src/main.rs:138` → `rollback::rollback(&dir)` (`apps/tkp/src/rollback.rs:27`).
- **Mechanics** (002 upgrade-case algorithm):
  1. Load envelope; **binding gate** — the running binary must be the current engine `B` (`rollback.rs:43`).
  2. **Precondition:** a `[A final]` checkpoint must exist, else fail-closed (`rollback.rs:62`).
  3. **B delete-only pass** over `keys(S_B) − keys(S_A)` (`rollback.rs:73‑76`). *Empty for local; the real
     `Engine::destroy_selected` wires here for real platforms.*
  4. **Atomic re-pin to A:** `envelope.begin_rollback(…)` (`lib.rs:320`) restores `binding → A`,
     `integrity → A`, `infra_head`/`runtime_head → A`, and `effective_config_ref → A`, and opens the
     `RollbackInFlight` marker (`phase = "re-pinned-to-A"`). CAS-save (`rollback.rs:82‑88`).
  5. **A forward-reconciles** toward its retained prior revision via `platform::infra_apply` (`rollback.rs:95`).
  6. **Complete:** `envelope.complete_rollback()` (`lib.rs:345`) clears the marker **and consumes the
     checkpoint**; CAS-save.
- **Mutates in the deployment dir:**
  - live resources — B's delete-only pass (real platforms; **no-op for local**);
  - `state/envelope` **twice** — the re-pin (binding/integrity/heads/config-ref → A, marker open), then
    complete (marker + checkpoint cleared);
  - `state/infra/*` (+ `infra_head`) ← via A's reconcile (real for compose-syn; **empty for local**).
- **Status:** the **envelope state-machine is done + tested** (`rollback_refuses_a_mismatched_running_binary`,
  `rollback_refuses_without_a_checkpoint`, `rollback_repins_to_the_checkpoint_engine`). The **two-binary
  orchestration** is missing — task 19.3.

---

## 5. Cross-cutting: crate and function map

| Concern | Crate | Symbol (file:line) |
|---------|-------|--------------------|
| CLI dispatch, `describe` | `tkp` (`apps/tkp`) | `main.rs:110` (match), `describe` `main.rs:157`, `DescribeReport` `main.rs:207` |
| Verb bodies | `tkp` | `apply.rs:26`, `revert.rs:28`, `upgrade.rs:25` (`transfer_ownership:101`), `rollback.rs:27` |
| Config-revision retention | `tkp` | `config_history.rs`: `snapshot:54`, `restore:79`, `is_retained:72`, `config_file:39` |
| Config content ref | `tkp` | `apply.rs:105` (`config_ref` → `sha256:…`) |
| Binding gate | `tkp` | `gate.rs` (`evaluate_gate`) |
| Operation lock wrapper | `tkp` | `lock.rs` (`with_operation_lock`) |
| Platform dispatch | `tkp` | `platform.rs` (`detect`, `infra_apply`, `Platform`) |
| **Envelope + state-machine** | `tokeira-provisioner` | `lib.rs`: `DeploymentStateEnvelope:214`, `RollbackCheckpoint:155`, `Operation:181`, `begin_upgrade:279`, `begin_rollback:320`, `complete_rollback:345`, `close_operation:351`, `ProvenanceStamp::current:91` |
| Upgrade decision | `tokeira-provisioner` | `upgrade.rs:16` (`UpgradeDecision`), `:26` (`evaluate_upgrade`) |
| State-schema migration | `tokeira-provisioner` | `MigrationRegistry` (`check_path`, `needs_migration`) |
| CAS state store | `tokeira-state` | `CasStore`, `LocalBackend`, `DeploymentStore`, `SnapshotRef` |

---

## 6. What is missing

The recurring theme: **envelope state-machines are real; real provider work is not.** Every
`platform::infra_apply` above is exercised only against the `local` platform, whose apply is an empty no-op.
The concrete gaps, mapped to [tasks.md](./tasks.md) task 19:

| # | Verb | Missing | Task |
|---|------|---------|------|
| 1 | `describe` | The **two views** — split the single view into short *operator* (default) and *verification/debug* (`--verbose`) with the full per-artifact manifest, `EngineIdentity` (task 16), snapshot ref (task 17), retained-revision list. | 19.1 |
| 2 | `upgrade` | **Real cross-engine re-provisioning** through the platform seam (vs local's empty apply); a **populated `MigrationRegistry`**; the ids-only **audit change log**; the **advisory baseline drift gate** (Req 4.7). | 19.2 |
| 3 | `rollback` | The **two-binary orchestration** — `tkr` relaunches A for the reconcile after B's re-pin (today B does it all in-process); the real **`destroy_selected`** delete-only over live resources; **both-binary checksum** verification; lock held across the relaunch boundary. | 19.3 |
| 4 | `rollback` | **Restore of R_a's config *source*.** `begin_rollback` restores the `effective_config_ref` *pointer* from the checkpoint, but the live config **source file** is not restored before A reconciles — so A's forward-apply toward R_a is not yet driven by R_a's source (moot for local's empty apply; required for a real platform). Part of the two-binary work. | 19.3 |
| 5 | all | **Real platform apply.** `revert`/`upgrade`/`rollback` all drive `platform::infra_apply`, tested only on local. The compose-syn Bollard path is unexercised by these verbs — the live-drive exercise closes this. | 15 |

---

## 7. Recovery: there is no `resume` verb

`resume` was **dropped**. An interrupted `upgrade`/`rollback` is recovered by **re-running that same verb**:
its steps are idempotent and read the `Operation` marker's `phase` to skip completed work. While a marker is
open, only the in-flight verb (re-run resumes it), `rollback` (to abort an interrupted upgrade forward to A),
and `describe` are permitted — every other mutating verb refuses. The `Operation.phase` field
(`lib.rs:185`) is the durable resumable marker; wiring re-run to *read* that phase and skip completed steps
is task 19.4 (today re-running restarts the idempotent steps from the top).
