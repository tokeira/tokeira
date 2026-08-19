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

The extracted trees match the archives byte-for-byte with one documented
exception: an empty `[workspace]` table appended to each crate's
`Cargo.toml`. Without an own workspace root, workspace discovery from these
excluded packages escapes a nested checkout (a `.claude/worktrees/` clone
inside the main one) into the outer repository's manifest.

To advance the SDK: download the new release's two `.crate` artifacts and
`SHA256SUMS`, verify (`shasum -a 256 -c`), replace these directories and the
checksum file wholesale, re-append the `[workspace]` fence to both
manifests, and update the release tag above. The engine + CLI
pair are pinned per environment (see
`spikes/dagger-rust-sdk/README.md` for the bring-up recipe); SDK and engine
advance together — the SDK's compatibility validator refuses a mismatched
engine at connect.
