//! `tkp` — the compose deployment provisioner (Req 14).
//!
//! `tokeira-provisioner-cli` (the platform-agnostic shell) composed with this
//! platform's realization — the platform ships its own provisioner. The source
//! binary is named `tkp`; deployment create marries a copy into the
//! deployment directory as `tkp`, where the launcher runs it.

use anyhow::Result;
use tokeira_compose_deployment::provisioner::ComposeProvisioner;

#[tokio::main]
async fn main() -> Result<()> {
    tokeira_provisioner_cli::run(ComposeProvisioner).await
}
