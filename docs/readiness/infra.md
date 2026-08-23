# Release / Infrastructure — Delivery Readiness

> The tokeira **release process** direction: build
> provenance, version/proto/compat pinning, the compatibility-claim bump workflow, and the CI
> substrate. This is a **summary**; the authoritative spec for this work is
> [`.kiro/specs/release-process/`](../../.kiro/specs/release-process/requirements.md) (requirements +
> design + tasks), which consolidates the release-process direction from the earlier
> `temporal-compatibility` drafts. The broader **tagging / release-management strategy** (cutting tagged
> releases, channels, changelogs, registry publication) is deliberately **deferred**.
>
> The broader **tagging / release-management strategy** (when/how to cut tagged releases, channels,
> changelogs) is deliberately **deferred** to a later effort.

**Last updated:** 2026-06-22.

## The pinned contract (`crates/tokeira-build-info/src/pinned.rs`)

Three independent, compile-time pins; the startup log, the `GetSystemInfo` handshake, and
`tokeirad --version` all embed the same constants (no runtime computation):

- `TOKEIRA_VERSION` — semver of `tokeirad`, from `Cargo.toml` (e.g. `0.1.0`). The tokeira release id.
- `TEMPORAL_PROTO_VERSION` — vendored `temporalio/api` tag the wire mirrors (`v1.62.11`). **Wire** contract.
- `TEMPORAL_SERVER_COMPAT` — the Temporal server release whose **behaviour** tokeira claims (`1.31.0`).
  A *claim*, not a derivation; moves independently of the proto pin (proto may run ahead of behaviour).

## Build provenance

- `tokeira-build-info` derives provenance (`TOKEIRA_GIT_SHA`, `SOURCE_TREE_HASH`, toolchain) at build
  time; the image pipeline passes git metadata in.
- **Release builds must carry provenance:** in CI release builds, `build.rs` fails if `TOKEIRA_VERSION`
  is empty or (with `CI` set) `TOKEIRA_GIT_SHA` is empty. Non-CI release builds warn and stamp `dev`.

## Status

| Area | State | Notes / spec task |
|------|:-----:|-------------------|
| Pins + provenance + `--version` / startup log | ✅ Landed | `temporal-compatibility` tasks 1, 6, 12, 13. |
| `FEATURE_MATRIX` / SDK matrix (queryable) | ✅ | `temporal-compatibility-surface` complete. |
| Matrix-completeness / capability-consistency property tests | ⬜ | task 3. |
| Kernel `cfg_feature!` adoption | ⬜ | task 4. |
| Edge `dispatch_rpc` adoption (all handlers) | ⬜ | task 5. |
| Compatibility service (Buffa + connect-rust) | ⬜ | task 7. |
| `tkr compat show / diff` | ⬜ | task 8. |
| `tkr ci check / build` (Dagger) | ⬜ | task 9. |
| Dagger compatibility module | ⬜ | task 10. |
| Dagger versioned build + lockfile policy | ⬜ | task 11. |
| `tkr compat bump` (server-compat bump protocol) | ⬜ | design only (see below). |

## Proto-version monotonicity gate (direction)

A Dagger-backed check, invoked via `tkr ci check`, compares `TEMPORAL_PROTO_VERSION` between the tip
commit and the last tagged tokeira release. A semver downgrade **fails** unless the commit carries an
explicit `Proto-Downgrade: <reason>` trailer. A proto-bump PR must include regenerated types + an
updated matrix covering any new RPCs; removing a previously-defined RPC is flagged as a breaking wire
change. A proto bump does **not** require a `TEMPORAL_SERVER_COMPAT` bump (the two move independently).

## Server-compat bump protocol — `tkr compat bump --to <version>` (direction, not yet built)

A phased engine (`crates/tokeira-build/src/compat_bump/`) drives a `TEMPORAL_SERVER_COMPAT` bump:

- **Preflight** — load `pinned.rs`; validate the target is semver-newer; working tree clean; on the
  default branch; GitHub creds valid (`GET /user`).
- **Evidence** — enumerate upstream releases between the old and new claim (GitHub API via `octocrab`),
  build the **Upstream Releases table** (one row per release: release-notes link + a verbatim one-line
  quote) and the matrix delta.
- **Mutate** — branch; edit `pinned.rs`; commit with a `Server-Compat-Bump:` trailer; run `tkr ci check`.
- **Publish** — push; open a PR (octocrab) with the templated body.

**Bump triggers** (when to raise the claim): (1) a matrix row moved to `Implemented` unblocking a claim;
(2) deliberate calendar-drift catch-up when the upstream delta is entirely already-documented
`Stubbed`/`Unsupported` features; — the `FEATURE_MATRIX`, not `TEMPORAL_SERVER_COMPAT`, is authoritative
for what tokeira actually does.

## CI substrate + handoff

`tkr ci check` connects its own in-process Dagger session (the vendored `dagger-sdk`; no wrapper
process, no session environment variables) and calls `run_ci_checks(request, client) -> CiCheckReport`
(serde-serializable). Remote-trigger wiring (GitHub Actions, nightly) is **out of scope** here and owned
by the `pipeline-foundation` spec, which will call the same `run_ci_checks` so local and remote verdicts
never diverge.

## Source

Authoritative spec: [`.kiro/specs/release-process/`](../../.kiro/specs/release-process/requirements.md)
(requirements, design, tasks) — the release-governance work (provenance gate, monotonicity checks,
`tkr compat bump` engine, CI handoff). It consolidates the release-process direction previously drafted
in `temporal-compatibility/{requirements,design}-orig.md` (now retired). The pins, the `FEATURE_MATRIX`,
the handshake, and the `tkr compat show/diff` + Dagger substrate are owned by the live
`temporal-compatibility` spec, which `release-process` consumes.
