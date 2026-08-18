# Spike: the first-party Dagger Rust SDK driving tokeira's builds

Evaluates whether the [iw/dagger](https://github.com/iw/dagger) Rust SDK
(`sdk/rust/v1.0.0-beta.11.rust.1`) with its composed v1.0.0-beta.11 engine
retires tokeira's hand-rolled Dagger plumbing: the `dagger-client` GraphQL
crate, the `DaggerClient` trait seam in `tokeira-build`, and the env-var +
re-exec session dance in `tkr`.

## SDK provenance

`vendor/` holds the two release artifacts, unpacked after verification
against the release's `SHA256SUMS` (committed beside them):
`dagger-sdk-1.0.0-beta.11.rust.1.crate` and its exact-version macro
companion. This follows the SDK's own documented install procedure —
evaluating that experience is part of the spike. The `[patch.crates-io]`
macro pin is the documented requirement.

## Engine bring-up (workspace devbox)

The release's engine is a complete `linux/amd64` OCI image with the Rust
SDK composed in. The workspace devbox (x86_64, mounted Docker service) runs
it natively:

```bash
# On the devbox — fetch the engine artifact (392MB, never committed):
curl -fL -o /tmp/engine.oci.tar <release asset URL for dagger-engine-v1.0.0-beta.11.rust.1-linux-amd64.oci.tar>
/vendor/docker/docker load -i /tmp/engine.oci.tar          # note the loaded image ref
/vendor/docker/docker run -d --name dagger-engine-rust-v1 --privileged <loaded image ref>
# The engine image ships its exact-matching CLI; extract it:
/vendor/docker/docker cp dagger-engine-rust-v1:/usr/local/bin/dagger /tmp/dagger-v1-cli
```

Every spike invocation then sets exactly two variables:

```bash
export _EXPERIMENTAL_DAGGER_RUNNER_HOST=docker-container://dagger-engine-rust-v1
export _EXPERIMENTAL_DAGGER_CLI_BIN=/tmp/dagger-v1-cli
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
`v1.0.0-beta.11.rust.1+501b57e0` as a pinned runner container.

| Axis | Old plumbing | SDK (measured) | Verdict |
|---|---|---|---|
| Session UX | env-var pickup + two re-exec copies + one path with none | `connect()` in **432ms**, close clean, whole lifecycle 823ms; two *engine-selection* vars only | **Retires the re-exec dance outright** |
| Async | `reqwest::blocking` + `spawn_blocking` panic workaround | async end to end; ran inside tokio naturally | **Retires the workaround** |
| API surface | no cache volumes / stdout / sync; runtime downcasts | `cache_volume` + `with_mounted_cache`, `stdout()`/`stderr()`, `sync()`, typed handles — no downcasts anywhere | **Retires the trait seam's escape hatches** |
| Error fidelity | everything flattened to `BuildError::Validation` | typed `QueryError` chains; the export defect below surfaced with its full decode chain intact | **Strict upgrade** |
| Image exit | tarball → `docker load` subprocess ×2 | `export_image` exists but **hits a beta.11 codegen defect** (binding decodes unit, engine returns map); tarball `export` fallback: 23.7MB in **0.98s** | **Defect to file upstream; export-hang class did not reproduce** |
| Perf (tokeirad) | cold by design; chef recompiles the whole workspace on any change; historical 7-min upload hang | upload **262ms cold / ~35ms warm**; compile **124s cold / 72.6s one-line incremental / 0.15–0.2s unchanged**; total unchanged rebuild **1.4s** | **Cache volumes beat chef where it matters (incremental)** |
| Version discipline | none (0.20+ floor in prose) | exact engine/CLI/revision pair enforced; fork CLI extracted from the engine image (implicit `dl.dagger.io` provisioning cannot carry fork versions) | **Pin the engine+CLI pair as build inputs** |

**tkp probe (measured):** real assembled bound source over the frozen
snapshot (tree `ad48e393…`, 272 files), in-container tests with captured
output (102s live, 273ms cache-replayed), release build → strip →
`File::export` → host-side hash: **49.7MB binary, 9.1s end-to-end on a
warm graph**. The typed `ExecError` (exit code, command, stdout/stderr
tails, traceparent) diagnosed both failures below on the first look —
the old client's ceiling was "exit 101".

Defects surfaced by this spike:

1. **SDK: `exportImage` decode mismatch** — the generated binding types
   the result as unit; the beta.11 engine answers with a map: "the GraphQL
   response has an invalid shape / invalid type: map, expected unit". Every
   other exercised call decoded cleanly. To file against iw/dagger.
2. **tokeira (latent, production): the provisioner test step cannot work
   against today's workspace.** `pipelines/provisioner.rs` runs
   `cargo test --locked -p <closure crate>` at the snapshot root, but the
   snapshot materializes only the closure's member directories while its
   root `Cargo.toml` lists every workspace member — cargo refuses on the
   first missing one (`tools/compatibility-docs`). Reproduced live; spun
   off for its own fix (identity implications: any manifest rewrite
   touches the source-closure digest contract).

Operational notes:

- The devbox-synced tree carries no `.git`; the tkp probe's snapshot
  machinery needs one (a throwaway fixture commit suffices).
- A fully-cached rebuild replays execs without running them — captured
  stdout/stderr is legitimately empty on cache hits.
- The engine container and extracted CLI do not survive a devbox
  pause/resume; repeat the bring-up.

## Verdict

The SDK retires every piece of the hand-rolled plumbing this spike set out
to test: the session dance (432ms owned connect), the blocking-client
workaround (async end to end), the trait seam's downcasts (typed handles),
the error flattening (typed chains that diagnosed two real defects), and
the caching gap (cache volumes beat chef exactly where chef is weakest).
One beta codegen defect (`exportImage`) and the engine+CLI pinning
procedure are the adoption work items.
