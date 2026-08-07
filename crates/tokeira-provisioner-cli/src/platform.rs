//! The platform, as the framework knows one: identity, admission,
//! capabilities.
//!
//! [`BoundPlatform`] is a struct, not a trait: the platform-specific layer
//! already said everything varied (the declaration), so what remains is the
//! same for every platform — holding the identity pair the binary was built
//! as, deciding whether a deployment on disk belongs to this platform and
//! whether this binary may operate it, and answering identity and
//! capability questions. The engine and the shell ask; the platform never
//! drives anything, and the engine never reads deployment metadata itself.

use std::{path::Path, sync::Arc};

use anyhow::{Context, Result, bail};
use tokeira_orchestrator::{DefinitionFormatId, PlatformId};
use tokeira_platform::declaration::{
    DeploymentRef, InfraConstructor, Ops, PlatformDeclaration, ProviderExecution, Vocabulary,
};
use tokeira_provisioner::DeploymentBindingMetadata;

const METADATA_JSON: &str = "metadata.json";

/// One command's admitted deployment: the binding metadata and the
/// deployment coordinates every engine verb receives.
///
/// Produced once per command invocation, at the shell boundary — identity
/// is never re-derived, metadata never re-read, and the executable never
/// re-verified between the verbs of one command.
#[derive(Debug)]
pub struct Admitted {
    /// The deployment's recorded binding metadata, as admitted.
    pub metadata: DeploymentBindingMetadata,
    /// The deployment's coordinates: identity, never state.
    pub deployment_ref: DeploymentRef,
}

/// One platform, bound to the identity pair its binary was built as.
///
/// Constructed once at process start. Construction composes the authoring
/// vocabulary — a kind-name collision between the declared providers
/// refuses the binary here, naming both providers, before any deployment
/// is read.
pub struct BoundPlatform {
    id: PlatformId,
    format: DefinitionFormatId,
    declaration: PlatformDeclaration,
    vocabulary: Vocabulary,
}

impl std::fmt::Debug for BoundPlatform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundPlatform")
            .field("id", &self.id)
            .field("format", &self.format)
            .finish_non_exhaustive()
    }
}

impl BoundPlatform {
    /// Bind the declaration to the built-as identity pair.
    pub fn bind(
        id: &'static str,
        format: &'static str,
        declaration: PlatformDeclaration,
    ) -> Result<Self> {
        let vocabulary = declaration.vocabulary()?;
        Ok(Self {
            id: PlatformId::new(id)?,
            format: DefinitionFormatId::new(format)?,
            declaration,
            vocabulary,
        })
    }

    // ------------------------------------------------------------------
    // Admission: is this deployment ours, and may this binary operate it?
    // Platform ownership — called before the operation lock, before any
    // state is read.
    // ------------------------------------------------------------------

    /// Admit one deployment directory.
    ///
    /// Three agreements, all or nothing: the deployment metadata names this
    /// platform and this definition format; the placed provisioner bundle,
    /// when present, carries bound evidence for the same pair; and the
    /// bundle's integrity manifest verifies the running executable.
    ///
    /// Called once per command; the returned [`Admitted`] value threads
    /// through every engine verb the command drives.
    pub fn admit_deployment(&self, deployment_dir: &Path) -> Result<Admitted> {
        let metadata = self.metadata(deployment_dir)?;
        self.validate_bundle(deployment_dir, &metadata)?;
        let deployment_ref = DeploymentRef {
            name: metadata.name.clone(),
            dir: deployment_dir.to_path_buf(),
        };
        Ok(Admitted {
            metadata,
            deployment_ref,
        })
    }

    fn metadata(&self, deployment_dir: &Path) -> Result<DeploymentBindingMetadata> {
        let path = deployment_dir.join(METADATA_JSON);
        let bytes =
            std::fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let metadata: DeploymentBindingMetadata = serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to decode {}", path.display()))?;
        if metadata.platform != self.id {
            bail!(
                "deployment metadata selects platform `{}` but this provisioner is bound to `{}`",
                metadata.platform,
                self.id
            );
        }
        let definition = metadata.definition.as_ref().ok_or_else(|| {
            anyhow::anyhow!("bound deployment metadata records no definition format/path")
        })?;
        if definition.format != self.format {
            bail!(
                "deployment metadata selects definition format `{}` but this provisioner is bound to `{}`",
                definition.format,
                self.format
            );
        }
        Ok(metadata)
    }

    fn validate_bundle(
        &self,
        deployment_dir: &Path,
        metadata: &DeploymentBindingMetadata,
    ) -> Result<()> {
        let bundle_path = deployment_dir.join(tokeira_provisioner::BUNDLE_MANIFEST_BASENAME);
        if !bundle_path.is_file() {
            return Ok(());
        }
        let bundle: tokeira_provisioner::ProvisionerBundle = serde_json::from_slice(
            &std::fs::read(&bundle_path)
                .with_context(|| format!("failed to read {}", bundle_path.display()))?,
        )
        .with_context(|| format!("failed to decode {}", bundle_path.display()))?;
        bundle.validate_bound_evidence()?;
        let evidence = bundle.bound.as_ref().ok_or_else(|| {
            anyhow::anyhow!("placed provisioner bundle carries no bound platform/frontend evidence")
        })?;
        if evidence.platform != metadata.platform || evidence.format != self.format {
            bail!(
                "placed provisioner bundle selects platform/format `{}/{}` but deployment metadata records `{}/{}`",
                evidence.platform,
                evidence.format,
                metadata.platform,
                self.format
            );
        }
        let manifest = bundle.integrity_manifest();
        manifest.validate().map_err(|error| {
            anyhow::anyhow!("placed provisioner bundle has an invalid integrity manifest: {error}")
        })?;
        let executable =
            std::env::current_exe().context("failed to locate the running bound provisioner")?;
        let executable_bytes = std::fs::read(&executable)
            .with_context(|| format!("failed to read {}", executable.display()))?;
        manifest
            .verify_artifact(
                &executable_bytes,
                &tokeira_provisioner::Target(env!("TKP_TARGET").to_string()),
            )
            .map_err(|error| {
                anyhow::anyhow!(
                    "the running bound provisioner disagrees with its placed bundle: {error}"
                )
            })
    }

    // ------------------------------------------------------------------
    // Identity: answers derived from the binding. Per-deployment identity
    // lives on `Admitted` — no method here re-admits to answer a question.
    // ------------------------------------------------------------------

    /// The platform identity this binary was built as ("compose").
    pub fn id(&self) -> &PlatformId {
        &self.id
    }

    /// The definition format this binary was built as ("tkd").
    pub fn format(&self) -> &DefinitionFormatId {
        &self.format
    }

    // ------------------------------------------------------------------
    // The authoring surface and capabilities: what the declaration carries,
    // exposed read-only. Capability is presence — the shell renders a typed
    // refusal for an absent capability; nothing answers "not applicable".
    // ------------------------------------------------------------------

    /// The composed authoring vocabulary the engine evaluates against.
    pub fn vocabulary(&self) -> &Vocabulary {
        &self.vocabulary
    }

    /// The provider's execution seam: reachability probe and context
    /// installation, invoked by the engine.
    pub fn execution(&self) -> &dyn ProviderExecution {
        self.declaration.provider.execution.as_ref()
    }

    /// The provider's ops surface over running deployments (logs, port
    /// mappings), when it declares one. The shell calls it directly — these
    /// are questions about live containers, not lifecycle, so the engine is
    /// not in the path.
    pub fn ops(&self) -> Option<&dyn Ops> {
        self.declaration.provider.ops.as_deref()
    }

    /// The declared selections' infra-phase extension constructors, paired
    /// with each selection's namespace — the provider's first, then
    /// auxiliaries in declaration order. The deployment runs them inside
    /// `register_infra_extensions`; a selection without one contributes
    /// nothing.
    pub fn infra_constructors(&self) -> Vec<(&'static str, Arc<dyn InfraConstructor>)> {
        std::iter::once((
            self.declaration.provider.kinds.provider,
            self.declaration.provider.infra.clone(),
        ))
        .chain(
            self.declaration
                .auxiliary
                .iter()
                .map(|selection| (selection.provider, selection.infra.clone())),
        )
        .filter_map(|(namespace, constructor)| {
            constructor.map(|constructor| (namespace, constructor))
        })
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use tokeira_platform::declaration::{KindSet, ProviderExport};

    use super::*;

    #[derive(Debug)]
    struct NoProbe;

    #[async_trait::async_trait]
    impl ProviderExecution for NoProbe {
        async fn probe(
            &self,
            _deployment: &DeploymentRef,
        ) -> anyhow::Result<Option<tokeira_iac::PlatformIssue>> {
            Ok(None)
        }
    }

    fn declaration() -> PlatformDeclaration {
        PlatformDeclaration::on(ProviderExport {
            kinds: KindSet::new("test", Vec::new()),
            ops: None,
            execution: Box::new(NoProbe),
            infra: None,
        })
    }

    fn write_metadata(dir: &Path, platform: &str) {
        std::fs::write(
            dir.join("metadata.json"),
            serde_json::json!({
                "name": "demo",
                "id": "00000000-0000-0000-0000-000000000000",
                "platform": platform,
                "definition": {"format": "tkd", "path": "definition.tkd"}
            })
            .to_string(),
        )
        .unwrap();
    }

    #[test]
    fn admission_yields_the_value_every_verb_threads() {
        let dir = tempfile::tempdir().unwrap();
        write_metadata(dir.path(), "test");
        let platform = BoundPlatform::bind("test", "tkd", declaration()).unwrap();
        let admitted = platform.admit_deployment(dir.path()).unwrap();
        assert_eq!(admitted.metadata.name, "demo");
        assert_eq!(admitted.deployment_ref.name, "demo");
        assert_eq!(admitted.deployment_ref.dir, dir.path());
    }

    #[test]
    fn a_foreign_platform_is_refused_naming_both() {
        let dir = tempfile::tempdir().unwrap();
        write_metadata(dir.path(), "other");
        let platform = BoundPlatform::bind("test", "tkd", declaration()).unwrap();
        let error = platform
            .admit_deployment(dir.path())
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("`other`") && error.contains("`test`"),
            "{error}"
        );
    }
}
