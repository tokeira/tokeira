//! The materialize plan: verified bytes → placement instructions.
//!
//! Fetch materializes a Deployment Publication into a deployment dir: the
//! definition documents at their recorded names, config-tree files at their
//! relative paths, and `tkp` from the verified engine artifact for the host
//! target with the manifest sidecar beside it — the placement shape of a
//! bundle create. The plan refuses before a byte lands; the caller (tkr)
//! executes it inside its existing atomic staging.

use std::path::Path;

use super::{error::Refusal, open::VerifiedPublication};
use crate::BUNDLE_MANIFEST_BASENAME;

/// One file the plan places, relative to the deployment root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// Deployment-relative destination path.
    pub relative_path: String,
    /// The verified target to read.
    pub target: String,
    /// Whether the file must be executable (`tkp`).
    pub executable: bool,
}

/// The ordered placements one fetch performs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializePlan {
    placements: Vec<Placement>,
}

impl MaterializePlan {
    /// Plan the materialization for `host` (a target triple). Refuses when
    /// the engine carries no artifact for the host.
    pub fn new(publication: &VerifiedPublication, host: &str) -> Result<Self, Refusal> {
        let claim = publication.claim();
        let mut placements = Vec::new();
        // Definition root + companions at their recorded names.
        placements.push(Placement {
            relative_path: claim.definition.root.clone(),
            target: claim.definition.root.clone(),
            executable: false,
        });
        for companion in &claim.definition.companions {
            let target = claim.companion_target(companion);
            placements.push(Placement {
                relative_path: target.clone(),
                target,
                executable: false,
            });
        }
        // Config trees at their relative paths (their target names).
        for target in publication.config_targets() {
            placements.push(Placement {
                relative_path: target.clone(),
                target: target.clone(),
                executable: false,
            });
        }
        // The engine: the host's artifact as `tkp`, manifest sidecar beside
        // it (exactly the shape bundle placement writes).
        let artifact = publication
            .artifacts()
            .iter()
            .find(|artifact| artifact.triple == host)
            .ok_or_else(|| Refusal::HostTargetUnsupported {
                host: host.to_string(),
                available: publication
                    .artifacts()
                    .iter()
                    .map(|artifact| artifact.triple.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
            })?;
        placements.push(Placement {
            relative_path: "tkp".to_string(),
            target: artifact.target.clone(),
            executable: true,
        });
        placements.push(Placement {
            relative_path: BUNDLE_MANIFEST_BASENAME.to_string(),
            target: claim.engine.manifest.clone(),
            executable: false,
        });
        Ok(Self { placements })
    }

    /// The placements, in write order.
    pub fn placements(&self) -> &[Placement] {
        &self.placements
    }

    /// Execute the plan into `dir` (the caller's staging directory —
    /// atomicity is the caller's rename). Every byte read is TUF-verified
    /// as it streams; any failure leaves the caller's staging to its
    /// existing cleanup.
    pub async fn materialize_into(
        &self,
        publication: &VerifiedPublication,
        dir: &Path,
    ) -> Result<(), Refusal> {
        for placement in &self.placements {
            let bytes = publication.read(&placement.target).await?;
            let path = dir.join(&placement.relative_path);
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|error| {
                    Refusal::TargetUnreadable {
                        target: placement.target.clone(),
                        error: format!("creating {}: {error}", parent.display()),
                    }
                })?;
            }
            tokio::fs::write(&path, &bytes)
                .await
                .map_err(|error| Refusal::TargetUnreadable {
                    target: placement.target.clone(),
                    error: format!("writing {}: {error}", path.display()),
                })?;
            #[cfg(unix)]
            if placement.executable {
                use std::os::unix::fs::PermissionsExt as _;
                tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
                    .await
                    .map_err(|error| Refusal::TargetUnreadable {
                        target: placement.target.clone(),
                        error: format!("marking {} executable: {error}", path.display()),
                    })?;
            }
        }
        Ok(())
    }
}
