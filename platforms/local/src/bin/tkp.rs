//! `tkp-local` — the local deployment provisioner (Req 14).
//!
//! `tokeira-provisioner-cli` (the platform-agnostic shell) composed with the
//! local realization. Local deployments run in-process in `tkr` (never
//! forwarded), so this binary's role is exercising the real shell end to end
//! without Docker — the Day-0/dev-loop substrate.

use anyhow::Result;
use tokeira_local_deployment::provisioner::LocalPlatform;

#[tokio::main]
async fn main() -> Result<()> {
    tokeira_provisioner_cli::run(LocalPlatform).await
}
