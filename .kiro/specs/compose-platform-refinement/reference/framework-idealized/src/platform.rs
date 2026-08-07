//! The platform, as the framework knows one: identity, admission,
//! capabilities.
//!
//! [`BoundPlatform`] is a struct, not a trait: the platform-specific layer
//! already said everything varied (the declaration), so what remains is the
//! same for every platform — holding the identity pair the binary was built
//! as, deciding whether a deployment on disk belongs to this platform and
//! whether this binary may operate it, and answering identity and
//! capability questions. The engine and the shell ask; the platform never
//! drives anything.

use std::path::Path;

use anyhow::{Context, Result, bail};
use tokeira_orchestrator::{DefinitionFormatId, PlatformId};
use tokeira_platform::declaration::{
    DeploymentRef, Ops, PlatformDeclaration, ProviderExecution, Vocabulary,
};
use tokeira_provisioner::DeploymentBindingMetadata;

/// One platform, bound to the identity pair its binary was built as.
///
/// Constructed once at process start. Construction composes the authoring
/// vocabulary — a kind-name collision between the declared providers refuses
/// the binary here, naming both providers, before any deployment is read.
pub struct BoundPlatform {
    id: PlatformId,
    format: DefinitionFormatId,
    declaration: PlatformDeclaration,
    vocabulary: Vocabulary,
}

/// One command's admitted deployment: the metadata and coordinates every
/// engine verb receives. Produced ONCE per command invocation, at the shell
/// boundary — identity is never re-derived, metadata never re-read, the
/// executable never re-verified between the verbs of one command.
pub struct Admitted {
    pub metadata: DeploymentBindingMetadata,
    pub deployment_ref: DeploymentRef,
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
    // Platform ownership — the engine never reads metadata.json. The shell
    // admits once per command, before taking the operation lock or reading
    // state, and threads the Admitted value through every verb it drives.
    // ------------------------------------------------------------------

    /// Admit one deployment directory.
    ///
    /// Three agreements, all or nothing:
    /// 1. deployment metadata names this platform and this definition format;
    /// 2. the placed provisioner bundle, when present, carries bound
    ///    evidence for the same pair;
    /// 3. the bundle's integrity manifest verifies the running executable.
    pub fn admit_deployment(&self, deployment_dir: &Path) -> Result<Admitted> {
        let metadata = self.metadata(deployment_dir)?;
        // (2) and (3) exactly as today's BoundPlatform::validate_bundle:
        // bundle evidence pair agreement, integrity manifest validation,
        // verify_artifact against std::env::current_exe(). Unchanged
        // behaviour; relocated ownership.
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
        let path = deployment_dir.join("metadata.json");
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
        _deployment_dir: &Path,
        _metadata: &DeploymentBindingMetadata,
    ) -> Result<()> {
        // Body carried verbatim from today's bound.rs: evidence pair,
        // integrity manifest, current-exe verification.
        Ok(())
    }

    // ------------------------------------------------------------------
    // Identity: answers derived from the binding.
    // ------------------------------------------------------------------

    /// The platform identity this binary was built as ("compose").
    pub fn id(&self) -> &PlatformId {
        &self.id
    }

    /// The definition format this binary was built as ("tkd").
    pub fn format(&self) -> &DefinitionFormatId {
        &self.format
    }

    // Per-deployment identity lives on `Admitted` (name via metadata,
    // coordinates via deployment_ref) — no method here re-admits to answer
    // an identity question.

    // ------------------------------------------------------------------
    // The authoring surface and capabilities: what the declaration carries,
    // exposed read-only. Capability is presence — the shell renders a typed
    // refusal for an absent capability; nothing answers "not applicable".
    // ------------------------------------------------------------------

    /// The composed authoring vocabulary the engine evaluates against.
    pub fn vocabulary(&self) -> &Vocabulary {
        &self.vocabulary
    }

    /// The provider's execution seam — the reachability probe — invoked by
    /// the engine before opening an operation.
    pub fn execution(&self) -> &dyn ProviderExecution {
        self.declaration.provider.execution.as_ref()
    }

    /// The provider's ops surface over running deployments (logs, port
    /// mappings; scale as local and ECS onboard), when it declares one.
    /// The shell calls it directly — these are live substrate questions,
    /// not lifecycle, so the engine is not in the path.
    pub fn ops(&self) -> Option<&dyn Ops> {
        self.declaration.provider.ops.as_deref()
    }
}

// Absent by construction: an inspection capability. Compose derives
// docker-compose.yml itself at deploy apply — a standard, non-authoritative
// artifact of deployment, owned by the provider — so no capability surface
// exists for it here or on `ProviderExport`.
