# Proto tree

This starter crate keeps the proto tree locally so the artifact is self-contained.

Recommended long-term repo move:

- place this tree at the workspace root as `proto/`
- sync `proto/upstream/temporal/...` from upstream Temporal
- keep `proto/tokeira/internal/...` owned by the Tokeira repo
- keep `crates/tokeira-proto/build.rs` pointing at the workspace-level tree
