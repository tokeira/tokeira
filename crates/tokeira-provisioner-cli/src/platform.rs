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

use std::{
    collections::{BTreeMap, btree_map::Entry},
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use tokeira_orchestrator::{DefinitionFormatId, PlatformId};
use tokeira_platform::{
    declaration::{
        DeploymentRef, Ops, PlatformDeclaration, PlatformExecution, PlatformIntegration,
    },
    definition::Namespace,
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

impl Admitted {
    /// The recorded desired source, from the admitted metadata. Admission
    /// guaranteed the definition record exists, so this cannot miss.
    pub fn config_source(&self) -> crate::ConfigSource {
        let definition = self
            .metadata
            .definition
            .as_ref()
            .expect("admission verified the definition record");
        crate::ConfigSource::recorded(definition.format.clone(), definition.path.clone())
    }
}

/// One platform, bound to the identity pair its binary was built as.
///
/// Constructed once at process start. The declaration's namespaces remain
/// frontend facts; binding verifies that each author-visible kind has one
/// decoder, but does not compose them into an execution registry.
pub struct BoundPlatform {
    id: PlatformId,
    format: DefinitionFormatId,
    declaration: PlatformDeclaration,
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
        validate_namespace_kinds(&declaration.namespaces)?;
        Ok(Self {
            id: PlatformId::new(id)?,
            format: DefinitionFormatId::new(format)?,
            declaration,
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

    /// Resource namespaces visible to the selected definition frontend.
    pub fn namespaces(&self) -> &[Namespace] {
        &self.declaration.namespaces
    }

    /// The platform's reachability seam, invoked by the engine.
    pub fn execution(&self) -> &dyn PlatformExecution {
        self.declaration.execution.as_ref()
    }

    /// The platform's ops surface over running deployments (logs, port
    /// mappings), when it declares one. The shell calls it directly — these
    /// are questions about live containers, not lifecycle, so the engine is
    /// not in the path.
    pub fn ops(&self) -> Option<&dyn Ops> {
        self.declaration.ops.as_deref()
    }

    /// The platform implementation delegated to by the described deployment.
    pub fn implementation(&self) -> Arc<dyn PlatformIntegration> {
        Arc::clone(&self.declaration.implementation)
    }

    /// Construct the platform that applies this deployment's service
    /// manifests.
    pub fn service_platform(
        &self,
        deployment: &DeploymentRef,
    ) -> Result<Box<dyn tokeira_deploy_engine::Platform>> {
        self.declaration.implementation.service_platform(deployment)
    }
}

/// Refuse declaration-order dispatch: a kind name must select exactly one
/// decoder before either frontend sees the namespace list. The map is thrown
/// away after validation; namespaces remain the sole authoring contract.
fn validate_namespace_kinds(namespaces: &[Namespace]) -> Result<()> {
    let mut owners = BTreeMap::new();
    for namespace in namespaces {
        for &kind in namespace.kinds {
            match owners.entry(kind) {
                Entry::Vacant(owner) => {
                    owner.insert(namespace.name);
                }
                Entry::Occupied(owner) if *owner.get() == namespace.name => {
                    bail!(
                        "authoring namespace `{}` advertises resource kind `{kind}` more than once",
                        namespace.name
                    );
                }
                Entry::Occupied(owner) => {
                    bail!(
                        "authoring namespaces `{}` and `{}` both advertise resource kind `{kind}`",
                        owner.get(),
                        namespace.name
                    );
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct NoProbe;

    #[async_trait::async_trait]
    impl PlatformExecution for NoProbe {
        async fn probe(
            &self,
            _deployment: &DeploymentRef,
        ) -> anyhow::Result<Option<tokeira_iac::PlatformIssue>> {
            Ok(None)
        }
    }

    #[derive(Debug)]
    struct NoIntegration;

    #[async_trait::async_trait]
    impl PlatformIntegration for NoIntegration {
        async fn register_infra_extensions(
            &self,
            _deployment: &DeploymentRef,
            _ctx: &mut tokeira_iac::ProvisionContext,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn register_deploy_extensions(
            &self,
            _deployment: &DeploymentRef,
            _ctx: &mut tokeira_deploy_engine::ServiceContext,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn register_image_extensions(
            &self,
            _deployment: &DeploymentRef,
            _ctx: &mut tokeira_deploy_engine::ImageContext,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn service_platform(
            &self,
            _deployment: &DeploymentRef,
        ) -> anyhow::Result<Box<dyn tokeira_deploy_engine::Platform>> {
            Ok(Box::new(NoServices))
        }
    }

    #[derive(Debug)]
    struct NoServices;

    #[async_trait::async_trait]
    impl tokeira_deploy_engine::Platform for NoServices {
        async fn apply_manifests(
            &self,
            _manifests: &[serde_json::Value],
        ) -> std::result::Result<usize, tokeira_deploy_engine::RuntimeError> {
            unreachable!("the platform test declares no services")
        }
    }

    fn declaration() -> PlatformDeclaration {
        declaration_with_namespaces(Vec::new())
    }

    fn declaration_with_namespaces(namespaces: Vec<Namespace>) -> PlatformDeclaration {
        PlatformDeclaration {
            namespaces,
            ops: None,
            execution: Box::new(NoProbe),
            implementation: Arc::new(NoIntegration),
        }
    }

    fn namespace(name: &'static str, kinds: &'static [&'static str]) -> Namespace {
        Namespace {
            name,
            kinds,
            defaults: None,
            decode: no_decode,
        }
    }

    fn no_decode(
        _name: &str,
        _value: tokeira_platform::author::LocatedValue,
    ) -> Option<
        std::result::Result<
            tokeira_platform::kind::DecodedKind,
            tokeira_platform::error::KindError,
        >,
    > {
        None
    }

    #[test]
    fn disjoint_namespace_kinds_bind_in_declaration_order() {
        let platform = BoundPlatform::bind(
            "test",
            "tkd",
            declaration_with_namespaces(vec![
                namespace("first", &["Alpha"]),
                namespace("second", &["Beta"]),
            ]),
        )
        .unwrap();

        assert_eq!(
            platform
                .namespaces()
                .iter()
                .map(|namespace| namespace.name)
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
    }

    #[test]
    fn a_kind_advertised_by_two_namespaces_is_refused_naming_all_three() {
        let error = BoundPlatform::bind(
            "test",
            "tkd",
            declaration_with_namespaces(vec![
                namespace("first", &["Shared"]),
                namespace("second", &["Shared"]),
            ]),
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("`Shared`"), "{error}");
        assert!(error.contains("`first`"), "{error}");
        assert!(error.contains("`second`"), "{error}");
    }

    #[test]
    fn a_kind_repeated_inside_one_namespace_is_refused() {
        let error = BoundPlatform::bind(
            "test",
            "tkd",
            declaration_with_namespaces(vec![namespace("repeated", &["Shared", "Shared"])]),
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("`Shared`"), "{error}");
        assert!(error.contains("`repeated`"), "{error}");
        assert!(error.contains("more than once"), "{error}");
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
