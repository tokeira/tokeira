# Spike: the first-party Dagger Rust SDK driving tokeira's builds

Evaluates whether the [iw/dagger](https://github.com/iw/dagger) Rust SDK
(`sdk/rust/v1.0.0-beta.11.rust.3`) with its composed v1.0.0-beta.11 engine
retires tokeira's hand-rolled Dagger plumbing: the `dagger-client` GraphQL
crate, the `DaggerClient` trait seam in `tokeira-build`, and the env-var +
re-exec session dance in `tkr`.

## SDK provenance

`vendor/` holds the two release artifacts, unpacked after verification
against the release's `SHA256SUMS` (committed beside them):
`dagger-sdk-1.0.0-beta.11.rust.3.crate` and its exact-version macro
companion. This follows the SDK's own documented install procedure —
evaluating that experience is part of the spike. The `[patch.crates-io]`
macro pin is the documented requirement.

## Engine bring-up (workspace devbox)

The release's engine is a complete `linux/amd64` OCI image with the Rust
SDK composed in. The workspace devbox (x86_64, mounted Docker service) runs
it natively:

```bash
# On the devbox — fetch the engine artifact (392MB, never committed),
# and verify it against the release's SHA256SUMS before loading:
curl -fL -o /tmp/engine.oci.tar <release asset URL for dagger-engine-v1.0.0-beta.11.rust.3-linux-amd64.oci.tar>
sha256sum /tmp/engine.oci.tar                              # must match SHA256SUMS
/vendor/docker/docker load -i /tmp/engine.oci.tar          # note the loaded image ref
/vendor/docker/docker run -d --name dagger-engine-rust-v3 --privileged <loaded image ref>
# The engine image ships its exact-matching CLI; extract it:
/vendor/docker/docker cp dagger-engine-rust-v3:/usr/local/bin/dagger /tmp/dagger-v3-cli
```

Every spike invocation then sets exactly two variables:

```bash
export _EXPERIMENTAL_DAGGER_RUNNER_HOST=docker-container://dagger-engine-rust-v3
export _EXPERIMENTAL_DAGGER_CLI_BIN=/tmp/dagger-v3-cli
```

These select the *fork* engine + CLI pair. They are engine-selection
configuration, not session plumbing — the SDK still owns session spawn,
auth, and lifecycle. (Implicit provisioning without them downloads the
pinned CLI from upstream `dl.dagger.io`, which does not carry fork
versions — a finding, not a defect: production would pin exactly like a
dev-engine setup does.)

## Probes

```bash
cargo run -- probe      # session lifecycle: connect, version, close
cargo run -- tokeirad   # tokeirad image, SDK-idiomatic (cache volumes, export_image)
cargo run -- tkp        # bound provisioner, honest inputs, deliberately cold
```

- **probe** — the envar-magic replacement test: no `DAGGER_SESSION_PORT`/
  `TOKEN`, no `dagger run` re-exec, no arg reconstruction.
- **tokeirad** — parity with `pipelines/build.rs`, minus what the old
  client forced: two `cache_volume` mounts replace the three-stage
  cargo-chef choreography, `export_image` loads the result engine-side (no
  tarball + `docker load` subprocess, no double export per tag), and build
  stdout is captured.
- **tkp** — parity with `pipelines/provisioner.rs` over the real assembled
  bound source and frozen snapshot (same `tokeira-build` machinery), same
  purity stance (cold, no cache mounts), with captured test output and a
  straight async `File::export`.

## Findings

Live runs on the workspace devbox (x86_64, 32 vCPU, mounted Docker), engine
`v1.0.0-beta.11.rust.3+c5f7dc77` as a pinned runner container. (First
measured against `rust.1`, which surfaced the two defects below; re-run
against `rust.3`, which resolves both.)

| Axis | Old plumbing | SDK (measured) | Verdict |
|---|---|---|---|
| Session UX | env-var pickup + two re-exec copies + one path with none | `connect()` in **216–432ms**, close clean, lifecycle < 1s; two *engine-selection* vars only | **Retires the re-exec dance outright** |
| Async | `reqwest::blocking` + `spawn_blocking` panic workaround | async end to end; ran inside tokio naturally | **Retires the workaround** |
| API surface | no cache volumes / stdout / sync; runtime downcasts | `cache_volume` + `with_mounted_cache`, `stdout()`/`stderr()`, `sync()`, typed handles — no downcasts anywhere | **Retires the trait seam's escape hatches** |
| Error fidelity | everything flattened to `BuildError::Validation` | typed `QueryError` chains; every defect this spike met surfaced with its full decode/exec chain intact | **Strict upgrade** |
| Image exit | tarball → `docker load` subprocess ×2 | `export_image` loads the built image engine-side in **4.05s** — `exportImage` is Void-typed, so this exercises the engine's JSON-null Void encoding and the SDK's strict null-only decode end to end | **Green; retires the tarball + `docker load` exit** |
| Perf (tokeirad) | cold by design; chef recompiles the whole workspace on any change; historical 7-min upload hang | upload **262ms cold / ~35ms warm**; compile **124s cold / 72.6s one-line incremental / 0.15–0.2s unchanged**; total unchanged rebuild **1.4s** | **Cache volumes beat chef where it matters (incremental)** |
| Version discipline | none (0.20+ floor in prose) | exact engine/CLI/revision pair enforced at connect; fork CLI extracted from the engine image (implicit `dl.dagger.io` provisioning cannot carry fork versions) | **Pin the engine+CLI pair as build inputs** |

**tkp probe (measured, fully cold):** real assembled bound source frozen as
the scoped cargo workspace (tree `5fbe2801…`, 272 files), the production
test step — `cargo test --locked -p <closure crate>` at the snapshot
root — run hermetically in-container and **passed in 158s** with 50KB of
captured output, then release build → strip → `File::export` → host-side
hash: **39.6MB binary, 329s end-to-end**. The typed `ExecError` (exit code,
command, both output streams, traceparent) named every failing test on the
first look — the old client's ceiling was "exit 101".

Defects surfaced by this spike, both since resolved:

1. **SDK/engine: `exportImage` Void encoding** — the `rust.1` engine
   answered Void-typed fields with `{}` while the binding, per the schema's
   prescription, expects null. Fixed in the `rust.3` pair: the engine
   encodes Void as JSON null and the SDK decodes strictly null-only, so a
   regression on either side refuses loudly instead of passing silently.
   The strict pair proved its worth immediately: it refused an interim
   release whose engine artifact had been built from a pre-fix revision —
   once as a wire-shape refusal, and again as a connect-time
   version/provenance mismatch — before any probe could quietly succeed.
2. **tokeira (was latent, production): the provisioner test step could not
   run against the frozen snapshot** — the snapshot carried the full
   workspace manifest but only the closure's member directories. Fixed by
   the scoped-workspace restructure (the snapshot is now a complete, valid
   cargo workspace; see `crates/tokeira-build`); the hermetic test-step run
   above is that fix exercised for real.

Operational notes:

- The devbox-synced tree carries no `.git`; the tkp probe's snapshot
  machinery needs one (a throwaway fixture commit suffices).
- Doc-agreement tests (code↔doc consistency) are vacuous against the
  frozen closure, which deliberately carries no doc trees — they gate on
  their doc tree's presence and stay hard failures wherever the tree
  exists.
- Re-vendoring between releases: cargo's `.crate` packaging normalizes
  file mtimes, so an rsync-style sync using size+mtime quick-checks can
  silently skip swapped vendor files of equal size — force-refresh mtimes
  (or checksum-sync) after a vendor swap.
- Verify every downloaded release artifact against the release's
  `SHA256SUMS` before use — engine tar included.
- A fully-cached rebuild replays execs without running them — captured
  stdout/stderr is legitimately empty on cache hits.
- The engine's internal build cache lives in the container's writable
  layer and grows by tens of GB across cold closure builds — prune it
  after heavy runs (`{engine{localCache{prune}}}` via the extracted CLI;
  a Void field, so the reply is the fix's own `null`).
- The engine container and extracted CLI do not survive a devbox
  pause/resume; repeat the bring-up.

## Verdict

The SDK retires every piece of the hand-rolled plumbing this spike set out
to test: the session dance (sub-second owned connect), the blocking-client
workaround (async end to end), the trait seam's downcasts (typed handles),
the error flattening (typed chains that diagnosed every real defect this
spike met), the image exit (engine-side `export_image`, green), and the
caching gap (cache volumes beat chef exactly where chef is weakest). Both
defects the first run surfaced are resolved and re-proven live; the
engine+CLI pinning procedure is the remaining adoption work item.
