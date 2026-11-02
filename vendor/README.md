# Vendored third-party crates

## dagger-sdk / dagger-sdk-macros

The first-party Dagger Rust SDK ([iw/dagger](https://github.com/iw/dagger),
release `sdk/rust/v1.0.0-beta.11.rust.3`), vendored as the release's own
`.crate` artifacts, unpacked after verification against the release's
`SHA256SUMS` (committed beside them; the third entry attests the composed
engine's OCI image, fetched at engine bring-up rather than committed).

The workspace consumes `dagger-sdk` as a path dependency; the
`[patch.crates-io]` pin for `dagger-sdk-macros` in the workspace manifest is
the SDK's documented install requirement (the exact-version macro companion
must resolve to the vendored copy, never a registry lookup).

The extracted trees match the archives byte-for-byte except for two documented
manifest changes:

- An empty `[workspace]` table appended to each crate's `Cargo.toml`. Without an
  own workspace root, workspace discovery from these excluded packages escapes a
  nested checkout into the outer repository's manifest.
- The SDK's `zip` dependency is pinned to `=9.0.4` instead of `=8.2.0` to resolve
  RUSTSEC-2026-0429 and retire the older ZIP extraction advisory exceptions. The
  existing bounded regular-file reader is byte-identical to the reader in the
  checksum-verified `sdk/rust/v1.0.0-beta.11.rust.5` artifact, which uses 9.0.4.
  That release removes isolated-session and lock-mode APIs used by Tokeira, so
  this narrow dependency patch preserves the current SDK/engine/CLI contract.
  The root `Cargo.lock` records the consumed dependencies; the packaged lockfile
  remains upstream provenance and is not the workspace resolution.

To advance the SDK: download the new release's two `.crate` artifacts and
`SHA256SUMS`, verify (`shasum -a 256 -c`), replace these directories and the
checksum file wholesale, re-append the `[workspace]` fence to both
manifests, and update the release tag above. Retire the ZIP manifest patch once
the replacement release includes the fix and the session APIs Tokeira needs.
The engine + CLI pair are pinned per environment (see
`spikes/dagger-rust-sdk/README.md` for the bring-up recipe); SDK and engine
advance together — the SDK's compatibility validator refuses a mismatched
engine at connect.
