//! The Compose provider's execution seam and registration ingredient.
//!
//! [`ComposeExecution::probe`] answers whether the local Docker daemon can
//! be reached for this deployment — the degradable answer stated as data:
//! the framework's plan blocks on it and apply/destroy refuse. The answer
//! is point-in-time: a daemon dying after a passing probe surfaces through
//! the operation's own error path, where
//! [`ComposeError::DockerNotAvailable`] already maps to the same typed
//! platform issue (`From<ComposeError> for IacError`).
//!
//! [`ComposeInfraConstructor`] is the selection's infra-phase constructor,
//! run by the deployment's registration seam: it registers what Compose
//! resources read from the provision context — the Docker-backed
//! [`ComposePlatform`] (the compose-file ledger lives under the
//! framework-owned `state/` directory) and the compose-service recovery
//! hook. Its failures are errors — real ones, and equally a daemon dying
//! after a passing probe: the error root stays [`ComposeError`], so the
//! unreachable class renders as the same platform issue rather than a
//! blended message.
//!
//! [`ComposeServiceProjection`] and [`ComposeWorkloadConstructor`] are the
//! workload export: the projection recognizes this provider's services
//! among realized entries (by resource type) and rebuilds each
//! [`ComposeWorkload`](crate::ComposeWorkload) from the recorded desired
//! manifest; the constructor builds the same Docker-backed
//! [`ComposePlatform`] the infra side registers — one ledger, one
//! reconciliation mechanic, whichever plane drives it.

use std::sync::Arc;

use tokeira_platform::declaration::{
    DeployPlatform, DeployService, DeploymentRef, InfraConstructor, Projection, ProviderExecution,
    RealizedEntry, ServiceProjection, WorkloadConstructor,
};

use crate::{
    ComposeError, ComposePlatform, ComposeWorkload, docker_unreachable_issue, service_from_manifest,
};

/// Compose's execution seam implementation.
#[derive(Debug)]
pub struct ComposeExecution;

#[async_trait::async_trait]
impl ProviderExecution for ComposeExecution {
    async fn probe(
        &self,
        deployment: &DeploymentRef,
    ) -> anyhow::Result<Option<tokeira_iac::PlatformIssue>> {
        // The ledger-free ops handle suffices: reachability is a question
        // about the daemon, not about any operation artifact.
        let platform = match ComposePlatform::ops(&deployment.name) {
            Ok(platform) => platform,
            Err(error) => return degrade(error),
        };
        match platform.ensure_reachable().await {
            Ok(()) => Ok(None),
            Err(error) => degrade(error),
        }
    }
}

/// Compose's infra-phase extension constructor. The provider's own
/// namespace carries no attribute block — everything it needs is the
/// deployment's coordinates.
#[derive(Debug)]
pub struct ComposeInfraConstructor;

#[async_trait::async_trait]
impl InfraConstructor for ComposeInfraConstructor {
    async fn construct(
        &self,
        deployment: &DeploymentRef,
        _attributes: Option<&serde_json::Value>,
        ctx: &mut tokeira_iac::ProvisionContext,
    ) -> anyhow::Result<()> {
        // Recovered resources let refresh/destroy reconstruct a live service
        // from recorded state without the definition in hand.
        ctx.set_extension(tokeira_iac::ResourceRecovery::new(|state| {
            (state.resource_type.0 == "compose_service")
                .then(|| service_from_manifest(state.properties.clone()).ok())
                .flatten()
                .map(|service| Box::new(service) as Box<dyn tokeira_iac::Resource>)
        }));

        let ledger = deployment.dir.join("state/compose-services.yaml");
        let platform = ComposePlatform::connect(ledger, &deployment.name)?;
        ctx.set_extension(platform);
        Ok(())
    }
}

/// The compose service projection: every realized `compose_service` entry
/// is one of this provider's workloads.
///
/// The service model rebuilds from the recorded desired manifest — the same
/// canonical JSON the applier deserializes — so the projection invents
/// nothing. A `compose_service` entry without a decodable manifest is an
/// error, never a skip: a silently dropped service would read as "tear it
/// down" to the deploy engine.
#[derive(Debug)]
pub struct ComposeServiceProjection;

impl ServiceProjection for ComposeServiceProjection {
    fn project(&self, entries: &[RealizedEntry<'_>]) -> anyhow::Result<Projection> {
        let mut projection = Projection::default();
        for entry in entries {
            if entry.resource_type.0 != "compose_service" {
                continue;
            }
            let manifest = entry.manifest.ok_or_else(|| {
                anyhow::anyhow!(
                    "compose service `{}` realized without a desired manifest",
                    entry.resource_id.0
                )
            })?;
            let service = service_from_manifest(manifest.clone())?;
            projection.claimed.push(entry.resource_id.clone());
            projection.services.push(
                Arc::new(ComposeWorkload::new(service, entry.module)) as Arc<dyn DeployService>
            );
        }
        Ok(projection)
    }
}

/// The workload applier constructor: the same Docker-backed
/// [`ComposePlatform`] construction the infra constructor registers, built
/// for the deploy engine's apply. The ledger path is shared deliberately —
/// one recorded view of the deployment's services, whichever plane wrote
/// last.
#[derive(Debug)]
pub struct ComposeWorkloadConstructor;

#[async_trait::async_trait]
impl WorkloadConstructor for ComposeWorkloadConstructor {
    async fn construct(
        &self,
        deployment: &DeploymentRef,
        _attributes: Option<&serde_json::Value>,
    ) -> anyhow::Result<Box<dyn DeployPlatform>> {
        let ledger = deployment.dir.join("state/compose-services.yaml");
        Ok(Box::new(ComposePlatform::connect(
            ledger,
            &deployment.name,
        )?))
    }
}

/// Map a connection failure to the operator-facing issue when the error
/// class is the unreachable daemon; anything else is a real failure.
fn degrade(error: ComposeError) -> anyhow::Result<Option<tokeira_iac::PlatformIssue>> {
    match error {
        ComposeError::DockerNotAvailable {
            socket_path,
            evidence,
        } => Ok(Some(docker_unreachable_issue(&socket_path, &evidence))),
        other => Err(anyhow::Error::from(other)),
    }
}

#[cfg(test)]
mod tests {
    use tokeira_deploy_engine::Service as _;

    use super::*;
    use crate::{ComposeService, canonicalize_manifest};

    fn placed_service() -> ComposeService {
        ComposeService {
            name: "grafana".into(),
            image: "grafana/grafana-oss:12.4.3".into(),
            ports: vec!["3000:3000".into()],
            volumes: Vec::new(),
            environment: Default::default(),
            depends_on: vec!["mimir".into(), "loki".into()],
            healthcheck: None,
            command: Vec::new(),
            resource_dependencies: Vec::new(),
        }
    }

    fn entry<'a>(
        module: &'a str,
        resource_type: &str,
        id: &str,
        manifest: Option<&'a serde_json::Value>,
    ) -> RealizedEntry<'a> {
        RealizedEntry {
            module,
            resource_type: tokeira_iac::ResourceType::new(resource_type),
            resource_id: tokeira_iac::ResourceId(id.to_string()),
            manifest,
            dependencies: Vec::new(),
        }
    }

    // The projection partitions by resource type and rebuilds each service
    // from its recorded manifest: the workload's own manifests() yields the
    // very JSON it was rebuilt from, so the engine's hash and the applier's
    // reconcile speak one canonical form.
    #[test]
    fn compose_services_are_claimed_and_rebuilt_from_their_manifests() {
        let manifest = canonicalize_manifest(placed_service().to_manifest());
        let other = serde_json::json!({"path": "/tmp/state"});
        let entries = vec![
            entry(
                "observability",
                "compose_service",
                "compose/grafana",
                Some(&manifest),
            ),
            entry("local_state", "local_state_dir", "local/dir", Some(&other)),
        ];
        let projection = ComposeServiceProjection.project(&entries).unwrap();
        assert_eq!(
            projection.claimed,
            vec![tokeira_iac::ResourceId("compose/grafana".to_string())]
        );
        assert_eq!(projection.services.len(), 1);
        let service = &projection.services[0];
        assert_eq!(service.name(), "grafana");
        assert_eq!(service.module(), "observability");
        // Canonical form sorts the set-valued arrays, so the rebuilt
        // workload carries its start-ordering deps in sorted order — set
        // equality is the contract, and the engine orders topologically.
        assert_eq!(service.dependencies(), vec!["loki", "mimir"]);
        let rebuilt = service
            .manifests(&tokeira_deploy_engine::ServiceContext::default())
            .unwrap();
        assert_eq!(rebuilt, vec![manifest]);
    }

    // A compose service without a decodable manifest refuses the projection:
    // silently dropping it would read as "tear it down" to the deploy engine.
    #[test]
    fn a_compose_service_without_a_manifest_refuses() {
        let entries = vec![entry(
            "runtime",
            "compose_service",
            "compose/tokeirad",
            None,
        )];
        let error = ComposeServiceProjection
            .project(&entries)
            .expect_err("a missing manifest refuses");
        assert!(
            error.to_string().contains("without a desired manifest"),
            "unexpected: {error}"
        );
    }
}
