# Tokeira Kairo — Cockpit Direction (standalone GPUI app)

> Status: design direction (resolves NORTH-STAR §9 open-decision #4, "cockpit form factor").
> Not yet a buildable spec. The walking skeleton (`.kiro/specs/agentic-walking-skeleton/`) stays
> TUI-first and is unaffected by this decision; this doc describes the cockpit-proper that follows
> the skeleton.
>
> **Product name: Tokeira Kairo** — the operator cockpit for tokeira-orchestrated agentic
> workflows. (The orchestration spine remains `tokeira`/`tokeirad`; Kairo is the surface on top.)

## Decision

Tokeira Kairo — the operator-facing surface for originating, managing, and end-goaling multiple
agent workflows (NORTH-STAR §3, §7) — is a **standalone GPUI application** that integrates with a
supervised `tokeirad`.

Three options were weighed:

| Option | Verdict |
|---|---|
| **Fork the Zed editor** | Rejected for now. Multi-month, perpetual rebase against fast-moving upstream, and GPUI is not a stable public API contract. Reconsider only if Kairo later needs editor internals unobtainable as a GPUI consumer. |
| **ACP integration (agents inside Zed)** | Rejected. Tried in practice and underwhelming; also inverts ownership — Zed becomes the host rather than tokeira being the spine and Kairo being ours. |
| **Standalone GPUI app** | **Chosen.** Build on the Apache-2.0 `gpui` crate to get a GPU-accelerated, Rust-native UI without owning an editor fork. The whole cockpit plane stays Rust, consistent with the Rust-driven spine (NORTH-STAR §6). |

## tokeira integration: integrated, not in-process

`tokeirad` is a durable-execution server and MUST keep its own crash domain and lifecycle. Kairo
does not link it in-process.

- Kairo **supervises** `tokeirad` as a managed child process (start / stop / health), so a Kairo
  restart never bounces the spine, and a spine crash surfaces in Kairo rather than taking it down.
- Kairo is a **client** of the spine over the standard contract: read workflow advancement via
  **queries + visibility**, write interventions via **signals + updates**. These are the exact
  primitives the reference TUI uses, so Kairo is built against a contract the walking skeleton has
  already validated. The UI can be swapped; the spine contract does not move.

### Operator verbs → spine contract

The four operator verbs (NORTH-STAR §3) map onto the same primitives the skeleton proves:

| Verb | Mechanism |
|---|---|
| **approve** | signal / update to release a workflow-level gate (and tool-level approval relay) |
| **guide** | signal carrying steering input into the running workflow |
| **terminate** | `RequestCancel` / `Terminate` (operator path, gap-ledger #13) |
| **revert** | update/signal driving the workflow's compensation/rollback path |

## Workspace / worktrees: git now, DeltaDB behind a seam

Code-worktree state is a **different plane** from workflow-execution state. tokeira's history stays
authoritative for *workflow execution* (AGENTS §3); the worktree layer is authoritative for *the
code and the conversation that produced it*. They compose; they do not compete.

- **Today:** git worktree per task (the existing "worktree per task" Manifest profile, NORTH-STAR
  §"Workspace = Manifest").
- **Likely inevitable:** Zed's **DeltaDB** — version control built on fine-grained addressable
  deltas, CRDT-replicated worktrees many agents edit at once, and bidirectional conversation↔code
  anchoring that survives code movement (beta a few weeks out as of 2026-06-11,
  [zed.dev/blog/introducing-deltadb](https://zed.dev/blog/introducing-deltadb); content paraphrased
  for licensing). It is a strong candidate to *be* the Workspace + Project-Memory + review planes at
  once, collapsing the PR/review ceremony into the agent conversation.
- **Rule:** DeltaDB is **not essential** and MUST sit behind a workspace/memory interface that git
  worktrees satisfy today. A few-weeks-old closed beta from another vendor must never become a hard
  architectural dependency or a blocker on the spine. Prototype against it when waitlist access
  opens; keep the git-worktree path as the always-available fallback.

## Design source: Figma Make

Kairo's UI — visuals and interaction — is prototyped in **Figma Make**, and that prototype *is* the
design spec for the GPUI app. Figma Make output is web/React; it does **not** import into GPUI. Treat
it as design reference (layout, motion, the feel being chased); translation to GPUI is manual. Since
the component layer is authored from the Figma design anyway, taking only the permissive framework
(not Zed's GPL component library) costs nothing — see Licensing below.

---

## Licensing, reuse & trademark

> Not legal advice — this records Zed's published statements and how the license mechanics apply, so
> the constraint travels with the design. Confirm anything load-bearing with counsel before
> distribution.

### The governing split (verified against crate manifests, 2026-06-11)

Zed is open source under a deliberate copyleft split ([zed.dev/software-overview](https://zed.dev/software-overview),
[Zed is now open source](https://zed.dev/blog/zed-is-now-open-source)):

- **Apache-2.0** — components intended for reuse, chiefly **`gpui`**. In the open-source
  announcement Zed states GPUI is Apache-licensed *specifically so you can build high-performance
  desktop apps on it and distribute them under any license you choose* (paraphrased). This is an
  explicit invitation that covers Kairo exactly.
- **GPL-3.0-or-later** — most of Zed, including the editor/component crates. Copyleft: a distributed
  work deriving from these must itself be GPL-3.0-or-later. Zed's framing: copyleft ensures
  "improvements benefit the entire community."
- **AGPL-3.0** — server-side/collaboration components (not relevant to Kairo).

Each crate's `Cargo.toml` `license =` field is authoritative; a few crates use
`license.workspace = true`, which inherits the workspace default (GPL). Enumerate the full set from a
checkout:

```bash
for f in crates/*/Cargo.toml; do
  name=$(rg -m1 -or '$1' '^name = "(.+)"' "$f")
  lic=$(rg -m1 -or '$1' '^license = "(.+)"' "$f")
  echo "${lic:-WORKSPACE_INHERIT(GPL)}	${name}"
done | sort
```

### Use directly — Apache-2.0 (verified)

| Crate | Role | Note |
|---|---|---|
| `gpui` | Framework: window, layout, GPU render, input, animation | `publish = true`, v0.2.2, homepage **gpui.rs** — now officially published, not just community mirrors |
| `gpui_macros` | Macros for gpui | git-only (`publish = false`) |
| `sum_tree` | Concurrency-friendly B-tree (data-structure foundation) | git-only |
| `util` | General utility structs/functions | git-only |
| `collections` | Standard collection types | git-only |
| `http_client` | HTTP client | git-only; Kairo will likely use `tonic`/`reqwest` for the tokeira gRPC contract anyway |

This is the honest extent of the permissive base: **framework + low-level infra.** Everything that
makes Zed look and behave like an editor is GPL.

### Must derive (or consciously accept GPL) — verified GPL-3.0-or-later

| Crate | What it gives | Permissive path for Kairo |
|---|---|---|
| `ui` | Zed's component library (buttons, lists, panels) | Build Kairo's own on `gpui` — the Figma-Make plan |
| `theme` | Theming / design tokens | Kairo's own tokens from the Figma design |
| `rope` | Rope text structure | `ropey` (MIT), or build on Apache `sum_tree` |
| `text` | Buffer / anchor model | Derive on `ropey` / `sum_tree` |
| `editor` | Full code/diff editing surface | Derive a minimal read/diff view on `gpui` |
| `terminal` / `terminal_view` | Embedded terminal | Use **`alacritty_terminal`** directly (the upstream crate Zed wraps; permissive) and render in `gpui` |
| `fuzzy` | Fuzzy matching | **`nucleo`** (MIT) |
| `picker` | Command-palette / picker UI | Build on `gpui` + `nucleo` |

Trap: **`rope` and `text` are GPL even though their dependency `sum_tree` is Apache.** A text buffer
is not a free pull — accept GPL, swap in `ropey`, or build on `sum_tree`.

### Trademark — the boundary that is separate from the code license

From the [Zed brand page](https://zed.dev/brand): the Zed name and logos are trademarks of Zed
Industries; they "depend as a business on control of [their] brand." Concretely:

- No use of the Zed name or logos in any way implying official connection/endorsement, or that could
  cause customer confusion.
- No use of their marks on merchandise without written consent.

Apache-2.0 on `gpui` grants **copyright** permission, **not** trademark rights. The product is
**Tokeira Kairo** — our own name and mark; it must not be branded as "Zed"-anything or use Zed's
logo. (Attribution of the Apache-2.0 NOTICE/LICENSE for `gpui` is still required in distribution.)

### Contributor agreement — only if we upstream

The [Zed CLA](https://zed.dev/cla) grants Zed broad rights over contributions you submit *to their
repo* and warrants they're yours. It places **no** obligation on a separate derivative product we
build and do not contribute back. It only matters if we choose to upstream patches to GPUI/Zed.

### Phased reuse posture (the strategic point)

Reuse is staged deliberately — early permissive reuse buys speed and shapes the product; deeper
derivation is a later, deliberate option, not a day-one commitment:

1. **Phase 1 — Mould on the permissive base.** Build Kairo on `gpui` + the Apache infra crates,
   authoring components from the Figma design. Kairo stays under **our chosen license** (we are free
   to keep it proprietary or pick any license — GPUI's Apache grant permits it). This phase
   maximises freedom to iterate on shape, branding, and product feel without copyleft obligations.
2. **Phase 2 — Direct effort to derived works, if/when it pays.** Once shape is settled and capacity
   allows, we may invest in deriving from GPL Zed crates (e.g. a richer editor/diff/terminal surface)
   **as a conscious license decision**: any distributed work built from those crates becomes
   GPL-3.0-or-later. That is a fork in the road taken on purpose — chosen when the engineering
   leverage of reusing mature GPL components outweighs staying permissive — never stumbled into by an
   accidental `use`.

Operating rules that keep the phases clean:

- **Isolate by license at the crate boundary.** Keep Apache-only dependencies in Kairo's core
  crates. If Phase 2 introduces GPL-derived code, quarantine it in clearly GPL-licensed crates so the
  copyleft surface is explicit and auditable, never viral-by-accident.
- **Prefer the permissive upstream of a GPL wrapper.** Where a GPL Zed crate wraps a permissive
  library (`terminal` → `alacritty_terminal`), depend on the upstream directly until/unless Phase 2.
- **Trademark holds across all phases.** Tokeira Kairo is the mark regardless of how much is derived.
- **Decide derivation against the latest reality.** GPUI is pre-1.0 and churns; re-check licenses and
  the build-vs-derive calculus at the point of the Phase-2 decision, not now.

---

## Prior art (Zed-derived / GPUI ecosystem)

Three Zed-derived references, verified 2026-06-11. They validate the standalone-GPUI-app path and
sharpen Kairo's wedge.

### `penso/arbor` — MIT — the closest reference (Kairo minus the spine)

A shipping "fully native app for agentic coding" built on **`gpui = "0.2.2"`** (crates.io, the same
official publish Kairo would use) and the **`zed-industries/alacritty` `alacritty_terminal` fork** —
i.e. the exact stack chosen above, proven in production, under a permissive (MIT) license.

What it already does (the layer Kairo would otherwise build): git worktrees created from
GitHub/GitLab issues, embedded PTY terminal (+ experimental `libghostty-vt`), side-by-side diffs, PR
review context, ACP agent chat (Claude/Codex/Gemini) plus OpenAI-compatible providers, and agent
working/waiting visibility over WebSocket. Architecture worth borrowing:

- **One daemon, four surfaces:** `arbor-httpd` (daemon) powers the GPUI desktop app, a web UI, a CLI
  (`arbor-cli`), and an **MCP server** (`arbor-mcp`). A single daemon-backed model is a strong shape
  for Kairo.
- **Remote outposts over SSH + mosh** (`arbor-ssh`, `arbor-mosh`) with persistent daemon-backed
  sessions — directly satisfies the "remotely perform on capable machines / robust parallel
  worktrees" requirement.
- Its own `arbor-symphony` is an **optional, lightweight** orchestration runtime.

**The wedge:** Arbor is Kairo's UI + workspace + remote-outpost layer already built; what it lacks is
a *durable* orchestration spine. Arbor *tracks* agents; Kairo *durably orchestrates* workflows
(history-as-authority, survives restart, DSQL — that is tokeira). So Arbor (a) de-risks the entire
Kairo UI/stack choice, (b) is **MIT**, so its patterns and code are legally studyable and reusable
with attribution — far friendlier than Zed's GPL editor crates, and (c) confirms the
`alacritty_terminal`-direct and daemon-serves-app+web+CLI+MCP decisions. Treat Arbor as the primary
implementation reference for Phase 1.

### `Glass-HQ/gpui` — Apache-2.0 — a GPUI fork with wider platform reach

Verified `license = "Apache-2.0"` (fork of Zed's `gpui`, same v0.2.2 lineage). Adds a **host-shell
model for iOS** (`hosts/ios`, a `cargo gpui` host tool) and advertises macOS/iOS/Linux/Windows/web.
Candidate dependency **only if** Kairo needs mobile/web reach beyond upstream `gpui`. Default remains
upstream `gpui 0.2.2` (officially published, the version Arbor uses, larger community); the Glass
fork is a fallback that carries fork-divergence cost.

### `Glass-HQ/Glass` — GPL-3.0-or-later — the editor-fork path we declined

Verified `license = "GPL-3.0-or-later"` (its primary crate is still internally `zed`, v0.233.0, by
Glass HQ). A full **hard fork of the Zed editor** (~200+ crates) rebranded as a browser + editor + AI
product. This is the heavy fork route the Decision table rejects for a cockpit; it stands as the
concrete example of maximal GPL derivation. The same org maintaining both an Apache GPUI extraction
(`Glass-HQ/gpui`) and a GPL editor fork (`Glass-HQ/Glass`) mirrors the exact license split Kairo
navigates.

### Naming note

Arbor lists **Conductor** (conductor.build, a macOS multi-agent orchestrator) among similar tools.
The NORTH-STAR placeholder name "Conductor" for the *orchestrator/spine* (open-decision #1) is
already a product in this space — retire that placeholder when naming the spine. (The cockpit name
**Tokeira Kairo** is unaffected.)

---

## Sequencing (the scope guard)

1. **Walking skeleton first** (`agentic-walking-skeleton`): TUI cockpit (the demo ships one), prove
   the spine — host the OpenAI sandbox demo on `tokeirad`, kill/resume durability. No GPUI yet.
2. **Tokeira Kairo (GPUI)** built against the *same* signals/queries/updates/visibility contract the
   TUI validated. Swap the surface, not the spine. Phase 1 (permissive) per Licensing above.
3. **DeltaDB** evaluated behind the workspace/memory seam once beta access is available.
4. **Phase 2 derivation** (optional, deliberate) if/when reusing GPL components earns its copyleft.

The discipline: an exciting editor-shaped cockpit must not displace the near-term milestone. Prove
durability with the TUI; grow Kairo around a spine that already works.

## Open sub-decisions (for the Kairo spec, when carved)

1. ~~Which Zed crates are reusable~~ — **resolved** (see Licensing): `gpui` + `gpui_macros` +
   `sum_tree` + `util` + `collections` + `http_client` are Apache-2.0 and reusable under any license;
   the component/editor/terminal/fuzzy layer is GPL and is derived-our-own (Phase 1) or consciously
   GPL-adopted (Phase 2). Remaining: confirm `gpui_util` and any `license.workspace` crates via the
   enumeration command before depending on them.
2. Cockpit ↔ `tokeirad` supervision shape: embedded child process vs. attach-to-running, and the
   health/restart policy.
3. Multi-workflow view model: how the spine's `waves` fan-out (NORTH-STAR §4) renders as concurrent
   nodes, and how decision points/gates surface for intervention.
4. Workspace/memory interface boundary that both git-worktrees and DeltaDB implement.
5. Figma Make → GPUI translation workflow (component mapping, theming, motion).
6. Kairo's own license choice for Phase 1 (proprietary vs. permissive vs. copyleft) and the
   crate-boundary isolation policy that protects the Phase-1/Phase-2 split.
