//! `tkp config seed` — create-time server-config rendering.
//!
//! Invoked by `tkr deployment create` against the staging directory, after
//! the staged definition check and before the deployment is published. The
//! platform realizes the definition's config-document render with
//! provider-derived fields left blank and writes the result into the
//! deployment directory. Evaluation only: no provider access, no engine
//! state, no lock — nothing else can reach a staging directory. A platform
//! that has not adopted the render answers `NotApplicable`, which is
//! success here: the generic seeded document stays, and create proceeds.

use std::path::Path;

use anyhow::Result;

use crate::{ProvisionerPlatform, Realization};

pub(crate) async fn seed<P: ProvisionerPlatform>(
    platform: &P,
    deployment_dir: &Path,
) -> Result<()> {
    match platform.config_seed(deployment_dir).await? {
        Realization::Realized(()) => {
            println!(
                "[{}] seeded server configuration",
                platform.label(deployment_dir)
            );
            Ok(())
        }
        // An optional capability: absence keeps the generic seed and must not
        // fail create — unlike scale, where NotApplicable is a refusal.
        Realization::NotApplicable { reason } => {
            println!("config seed skipped: {reason}");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TestPlatform;

    #[tokio::test]
    async fn not_applicable_is_success_so_create_never_fails() {
        let tmp = tempfile::tempdir().unwrap();
        seed(&TestPlatform, tmp.path())
            .await
            .expect("NotApplicable keeps the generic seed and succeeds");
    }
}
