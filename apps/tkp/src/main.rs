//! `tkp` — the Tokeira platform provisioner (transitional bundled binary).
//!
//! A specialized binary scoped to a single deployment's lifecycle (no operator /
//! global verbs — those live in `tkr`). The whole shell — verbs, binding gate,
//! operation lock, state envelope, revisions — lives in
//! `tokeira-provisioner-cli`; this binary only injects its platform realization.
//!
//! Transitional: this bundle carries BOTH `local` and `compose-syn`, resolved
//! per-deployment by directory content. The target is one binary per platform
//! (`tkp-compose`, `tkp-local`; task 15.4) — each `tokeira-provisioner-cli` plus
//! exactly one platform — at which point this bundled crate retires.

use anyhow::Result;

mod platform;

use platform::BundledPlatform;

#[tokio::main]
async fn main() -> Result<()> {
    tokeira_provisioner_cli::run(BundledPlatform).await
}
