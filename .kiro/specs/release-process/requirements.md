# Requirements Document

## Introduction

This spec owns tokeira's **release-process governance**: how the compatibility claim and build
provenance are advanced and gated across releases. It captures the release-specific direction that was
authored in the earlier `temporal-compatibility` drafts (`requirements-orig.md` Req 5.4/5.5/6.x and
`design-orig.md` §7–§8) but trimmed from the live `temporal-compatibility` spec when that spec narrowed
to the queryable compatibility surface.

Concretely it covers four things:

1. **Build provenance enforcement** — release builds must carry traceable provenance.
2. **Version-pin monotonicity gates** — `TEMPORAL_PROTO_VERSION` and `TEMPORAL_SERVER_COMPAT` only move
   forward across releases unless an explicit override is recorded.
3. **The server-compat bump protocol** — a documented, tool-driven (`tkr compat bump`) workflow for
   advancing `TEMPORAL_SERVER_COMPAT`, with an auditable PR trail.
4. **The local CI substrate + handoff** — the release-governance checks run inside the Dagger-backed
   `tkr ci check` pipeline; remote-trigger wiring is deferred to `pipeline-foundation`.

The **broader tagging / release-management strategy** (cutting tagged releases, release channels,
changelog generation, crates.io/registry publication) is **explicitly out of scope** and deferred to a
later effort; this spec governs the *compatibility-claim and provenance* mechanics only.

### Source authority

This spec's contract is the earlier drafts it consolidates —
`.kiro/specs/temporal-compatibility/requirements-orig.md` (Req 5.4, 5.5, 6.1–6.3) and `design-orig.md`
(§7 Local CI pipeline, §8 Server compat bump command). Those drafts are **retired** (this spec captures
them; their original text remains in git history). The pins (`crates/tokeira-build-info/src/pinned.rs`)
and the matrix/CLI/Dagger substrate are owned by the live `temporal-compatibility` spec, which this spec
consumes.

## Glossary

- **`TOKEIRA_VERSION`** — semver of the `tokeirad` binary (from `Cargo.toml`); the tokeira release id.
- **`TEMPORAL_PROTO_VERSION`** — the vendored `temporalio/api` tag tokeira's wire mirrors (the wire
  contract); currently `v1.62.11`.
- **`TEMPORAL_SERVER_COMPAT`** — the Temporal server release whose behaviour tokeira claims (a *claim*,
  not a derivation); currently `1.31.0`. Moves independently of the proto pin.
- **Build provenance** — `TOKEIRA_GIT_SHA` and `TOKEIRA_SOURCE_TREE_HASH` stamped at build time,
  surfaced via `tokeirad --version`, the startup log, and the `GetSystemInfo` handshake.
- **Version pin** — one of the three constants in `crates/tokeira-build-info/src/pinned.rs`.
- **Monotonicity gate** — a CI check that fails if a pin moves backward across releases without an
  explicit override trailer.
- **Server-compat bump** — advancing `TEMPORAL_SERVER_COMPAT`, governed by the bump protocol.
- **Bump trigger** — one of three sanctioned reasons to initiate a bump (see Requirement 3).
- **`Server-Compat-Bump:` trailer** — the commit trailer that records and is mechanically validated for
  every bump commit.
- **`run_ci_checks`** — the pure pipeline function (Dagger-backed) that runs the release-governance
  checks; reusable by `pipeline-foundation` for remote triggers.

## Target State

In scope (becomes `Implemented`):

- Release builds fail (in CI) without valid provenance; the local debug loop is never blocked.
- `tkr ci check` runs proto- and server-compat **monotonicity** checks and a **bump-trailer** check
  against the last tagged release, failing on a backward move or a missing/mismatched trailer unless an
  explicit override trailer is present.
- `tkr compat bump --to <version>` drives the full bump protocol (preflight → evidence → mutate →
  publish) producing an auditable PR with the Upstream Releases / Disposition / Matrix-Delta / SDK
  evidence sections.
- The bump protocol's governance is documented in `AGENTS.md` and enforced by `CODEOWNERS` on
  `pinned.rs` (informational until branch protection lands with `pipeline-foundation`).

Out of scope (and why):

- **Remote CI triggers** (GitHub Actions, nightly cron, scheduled runners) — owned by
  `pipeline-foundation`; this spec ships the portable `run_ci_checks` substrate it will consume.
- **Tagging / release channels / changelog / registry publication** — the broad release-management
  strategy, deliberately deferred.
- The compatibility **matrix content**, the `GetSystemInfo` handshake, `tkr compat show/diff`, and the
  Dagger image build/publish pipelines — owned by `temporal-compatibility` / `image-lifecycle`.

## Evidence From Current Code

- Pins: `crates/tokeira-build-info/src/pinned.rs` (`TOKEIRA_VERSION`, `TEMPORAL_PROTO_VERSION`,
  `TEMPORAL_SERVER_COMPAT`). Provenance + `--version` + startup log already landed
  (`temporal-compatibility` tasks 1, 6, 12, 13).
- Dagger substrate to host the checks: `crates/tokeira-build` (image pipelines) + the `tkr` Dagger
  re-exec helper in `apps/tkr/src/commands/image/`.
- Source drafts (now retired, consolidated into this spec):
  `.kiro/specs/temporal-compatibility/{requirements,design}-orig.md` — recoverable from git history if
  the original wording is ever needed.

## Requirements

### Requirement 1: Release builds carry valid provenance

**User Story:** As a tokeira operator, I want every release binary to carry non-empty, traceable
provenance, so that `tokeirad --version` is always attributable to a specific source tree and toolchain.

#### Acceptance Criteria

1. WHEN the build profile is `release` AND the `CI` environment variable is set, THE build SHALL fail
   if `TOKEIRA_GIT_SHA` is empty.
2. WHEN the build profile is `release` AND not in CI, THE build SHALL warn (via `cargo::warning=`) but
   SHALL NOT fail; the binary carries provenance `dev`.
3. WHEN a debug build runs without provenance environment variables, THE `build.rs` SHALL resolve
   `git rev-parse --short=8 HEAD` directly and substitute the literal `dev` if git is unavailable.
4. THE image build pipeline (`image-lifecycle`) SHALL set `TOKEIRA_GIT_SHA` (`git rev-parse --short=8
   HEAD`, `-dirty` suffix when the tree is dirty) and `TOKEIRA_SOURCE_TREE_HASH` on the build container
   before `cargo build`, and SHALL fail if `TOKEIRA_GIT_SHA` cannot be resolved.

### Requirement 2: Version pins move forward only

**User Story:** As a tokeira operator, I want the proto and server-compat pins to only advance across
releases, so that a rollback can never silently reintroduce a protocol or behavioural regression.

#### Acceptance Criteria

1. WHEN `tkr ci check` runs, THE proto-monotonicity check SHALL compare `TEMPORAL_PROTO_VERSION` between
   the tip commit and the last tagged tokeira release, and SHALL fail if the tip value is semver-less.
2. WHEN `TEMPORAL_PROTO_VERSION` moves backward, THE check SHALL pass only if the commit message carries
   a `Proto-Downgrade: <reason>` trailer.
3. WHEN `TEMPORAL_SERVER_COMPAT` moves backward, THE check SHALL pass only if the commit message carries
   a `Server-Compat-Downgrade: <reason>` trailer.
4. THE checks SHALL run inside the Dagger-backed `run_ci_checks` pipeline (deterministic container), not
   as host shell scripts; remote-trigger wiring is out of scope (Requirement 6).

### Requirement 3: The server-compat bump protocol

**User Story:** As a tokeira maintainer, I want a documented, tool-driven protocol for advancing
`TEMPORAL_SERVER_COMPAT`, so that the claim stays close to upstream, matches a written audit trail, and
is never blocked by feature-coverage gaps the matrix already communicates honestly.

#### Acceptance Criteria

1. THE protocol SHALL recognise three sufficient bump triggers: (1) upstream adds behaviour tokeira
   already classifies `Implemented`/`Experimental`; (2) a matrix row moved to `Implemented` unblocking a
   claim; (3) calendar drift — upstream ≥6 releases ahead for ≥3 months AND the delta is entirely
   already-documented `Stubbed`/`Unsupported` features.
2. BUMPS SHALL be driven exclusively by `tkr compat bump --to <version>`; hand-editing `pinned.rs`
   outside it is a protocol violation.
3. EVERY bump commit SHALL carry a trailer matching `^Server-Compat-Bump: \d+\.\d+\.\d+ -> \d+\.\d+\.\d+,
   trigger: [123]$`; the old version SHALL equal `TEMPORAL_SERVER_COMPAT` at the parent and the new
   version SHALL equal it at the commit, both enforced by `tkr ci check`.
4. A bump PR SHALL modify only `pinned.rs` + its rationale comment (no matrix-row changes in the same
   PR) and its body SHALL include: the chosen trigger + justification; the **Upstream Releases table**
   (one row per Temporal release between old and new claim, with a release-notes link and a verbatim
   one-line quote); the **Disposition Table** (every upstream-introduced surface mapped to one tokeira
   disposition); the **Matrix Delta**; and the **SDK Test-Suite Evidence** section.
5. A bump SHALL NOT be blocked on 100% feature coverage of the claimed version; the feature matrix (not
   `TEMPORAL_SERVER_COMPAT`) is the authoritative record of what tokeira actually does.
6. THE `pinned.rs` rationale comment SHALL name the latest bump PR number and trigger, amended in the
   same commit. `CODEOWNERS` SHALL name `pinned.rs` as requiring owner-team approval (informational
   until branch protection lands with `pipeline-foundation`).
7. THE `AGENTS.md` working agreements SHALL carry a "Server compat bump protocol" subsection summarising
   the triggers, the command, the trailer requirement, and the CODEOWNERS gate.

### Requirement 4: `tkr compat bump` drives the protocol end to end

**User Story:** As a tokeira maintainer, I want one command to execute the bump protocol, so that the
audit trail is generated mechanically rather than by hand.

#### Acceptance Criteria

1. `tkr compat bump --to <version>` SHALL execute four phases — **preflight** (validate target newer,
   working tree clean, default branch, GitHub credentials/scopes), **evidence** (enumerate upstream
   releases, fetch release bodies, compute the matrix delta), **mutate** (branch, edit `pinned.rs`,
   commit with the trailer, run `tkr ci check`), **publish** (push, open the PR, rewrite the PR number
   into `pinned.rs`, amend) — and SHALL stop at the first phase whose preconditions fail.
2. THE command SHALL refuse to proceed (preflight error) when the target equals or is older than the
   current pin.
3. THE command SHALL support `--dry-run` (no mutation/push/PR), `--no-open` (push but do not open a PR),
   and a resume policy for re-running after a partial failure.
4. THE bump engine SHALL be a pure-as-possible phased library (`run_bump(request) -> BumpOutcome`)
   reusable by automation, with the CLI as a thin wrapper.

### Requirement 5: Baseline bump record

**User Story:** As a tokeira maintainer, I want the current pin to have a templated bump record, so that
future bump diffs have a starting disposition table.

#### Acceptance Criteria

1. Landing this spec SHALL include a retroactive "Bump PR 0" baseline as a committed markdown file
   (e.g. `docs/compat-bumps/0-baseline.md`) authoring a fully-templated body for the current
   `TEMPORAL_SERVER_COMPAT`, with the file documenting why it is a committed record rather than a real PR
   (the value predates the protocol).

### Requirement 6: CI substrate and remote-trigger handoff

**User Story:** As a tokeira maintainer, I want the release-governance checks shaped so a remote runner
can reuse them, so that local and remote verdicts never diverge.

#### Acceptance Criteria

1. THE checks SHALL be exposed as a pure function `run_ci_checks(request, dagger) -> CiCheckReport`
   taking a Dagger client abstraction, so `pipeline-foundation` can pass a differently-configured client.
2. `CiCheckReport` SHALL be `Serialize + Deserialize` so a remote runner can ship it without
   re-serialising check logic; `tkr ci check --json` SHALL emit it unmodified.
3. THIS spec SHALL NOT add any `.github/workflows/*.yml`, nightly cron, or scheduled runner; the local
   `tkr ci check` is the canonical pre-push gate until `pipeline-foundation` lands.
