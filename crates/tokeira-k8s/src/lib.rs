//! Live Kubernetes platform for the tokeira provisioner (`tkp`).
//!
//! This crate is the runner-host bridge between the provider-agnostic IaC engine
//! (`tokeira-iac`) and a real Kubernetes API server. It sits in the runner-host
//! plane and owns two things:
//!
//! - [`KubePlatform`]: a `kube::Client` handle registered on a
//!   [`tokeira_iac::ProvisionContext`] via `set_extension`. Kubernetes-object
//!   resource kinds (defined by consuming platform crates such as
//!   `platforms/eks`) recover it in their `create`/`describe`/`delete` and route
//!   every mutation through it. This keeps EKS on the single `InfraEngine` apply
//!   path — there is deliberately no separate manifest-only channel
//!   (design → "Single apply path"), mirroring how compose containers apply via
//!   `ComposePlatform`.
//! - Shared, provider-agnostic manifest helpers ([`standard_labels`],
//!   [`build_node_pool`], `manifest_metadata`, `plan_manifests`) that
//!   consuming crates call to construct and classify manifests before apply.
//!
//! The crate depends only on `tokeira-iac` (for the [`NamespaceResource`]
//! `Resource` impl); it intentionally does not depend on the deploy engine or on
//! any concrete platform, so it stays reusable and free of any service-topology
//! knowledge.
//!
//! All server-side apply and delete traffic is attributed to the field manager
//! `FIELD_MANAGER` (`tkp`) so field ownership is stable and 409 conflicts are
//! actionable rather than silently clobbering another manager's fields.

mod apply;
mod logs;
mod namespace;
mod platform;
mod portforward;
mod scale;
mod watch;

pub use logs::LogOptions;
pub use namespace::{NamespaceConfig, NamespaceResource};
pub use platform::KubePlatform;
pub use portforward::{PortForwardConfig, PortForwardSession};
pub use scale::DeploymentStatus;
pub use watch::ReadinessState;

use std::collections::BTreeMap;

use serde::Serialize;

/// Field manager used for all server-side apply and delete operations.
///
/// Attributing writes to a stable manager (`tkp`, the tokeira provisioner) is
/// what lets Kubernetes report a field-ownership conflict when another actor has
/// taken ownership of a field this provisioner manages, instead of silently
/// overwriting it. Apply fails closed on a 409 conflict unless takeover is
/// explicitly requested via [`ApplyOptions::force_conflicts`] — this is the
/// review-before-mutation guarantee at the Kubernetes boundary.
pub(crate) const FIELD_MANAGER: &str = "tkp";

/// Errors surfaced by the Kubernetes platform's public API.
#[derive(Debug, thiserror::Error)]
pub enum K8sError {
    /// The API server could not be reached (no cluster, connectivity, or auth).
    ///
    /// Read-only `plan` tolerates this by omitting the platform from the
    /// context; `apply`/`destroy` require a reachable platform.
    #[error("kubernetes API server is unreachable: {0}")]
    Unreachable(String),
    /// A manifest lacked a field required to route it to an API endpoint
    /// (`kind`, `metadata.name`, or `apiVersion`).
    #[error("invalid manifest: {0}")]
    InvalidManifest(String),
    /// Any other failure from an underlying `kube`/provider call, preserving the
    /// original context chain.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Whether a Kubernetes resource is namespace-scoped or cluster-scoped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestScope {
    /// Lives inside a namespace (`metadata.namespace` present).
    Namespaced,
    /// Cluster-scoped (no `metadata.namespace`), e.g. a `NodePool`.
    Cluster,
}

/// Operator-facing metadata extracted from a manifest before apply.
///
/// This is the classification used by both the review/apply flow and the
/// dynamic API routing: presence of `namespace` is the sole
/// signal for namespaced vs. cluster scope, matching how the apply/delete/get
/// paths select their `Api` endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ManifestMetadata {
    /// The manifest's `apiVersion` (e.g. `apps/v1`, `v1`, `karpenter.sh/v1`).
    pub(crate) api_version: String,
    /// The manifest's `kind` (e.g. `Deployment`).
    pub(crate) kind: String,
    /// The manifest's `metadata.name`.
    pub(crate) name: String,
    /// The manifest's `metadata.namespace`, if any.
    pub(crate) namespace: Option<String>,
    /// Derived scope, driven solely by [`namespace`](Self::namespace) presence.
    pub(crate) scope: ManifestScope,
}

/// A deploy-time manifest paired with parsed metadata for review/apply flows.
#[derive(Debug, Clone, Serialize)]
pub struct PlannedManifest {
    /// Parsed classification of [`manifest`](Self::manifest).
    pub(crate) metadata: ManifestMetadata,
    /// The raw manifest body. Skipped in serialized review output because the
    /// review surface shows the classification, not the (potentially large)
    /// object body — the body is applied, not printed.
    #[serde(skip_serializing)]
    pub(crate) manifest: serde_json::Value,
}

impl PlannedManifest {
    /// Parse a raw manifest into a typed review/apply item.
    ///
    /// Fails with [`K8sError::InvalidManifest`] if the manifest is missing a
    /// field required to route it.
    pub(crate) fn try_from_value(manifest: serde_json::Value) -> Result<Self, K8sError> {
        let metadata = manifest_metadata(&manifest)?;
        Ok(Self { metadata, manifest })
    }
}

/// Server-side-apply behavior flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
pub struct ApplyOptions {
    /// Take over conflicting fields instead of failing closed on a 409 conflict.
    ///
    /// Off by default so a conflict is surfaced for review; enabling it is the
    /// explicit operator opt-in to seize field ownership.
    pub(crate) force_conflicts: bool,
}

/// Extract operator-facing metadata from a raw manifest.
///
/// The three required fields (`kind`, `metadata.name`, `apiVersion`) are exactly
/// those needed to build a `GroupVersionKind` and select an `Api`; a manifest
/// missing any of them cannot be routed, hence the hard error.
pub(crate) fn manifest_metadata(
    manifest: &serde_json::Value,
) -> Result<ManifestMetadata, K8sError> {
    let kind = manifest
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| K8sError::InvalidManifest("manifest missing 'kind'".into()))?;
    let name = manifest
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| K8sError::InvalidManifest("manifest missing 'metadata.name'".into()))?;
    let namespace = manifest
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    let api_version = manifest
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .ok_or_else(|| K8sError::InvalidManifest("manifest missing 'apiVersion'".into()))?;

    Ok(ManifestMetadata {
        api_version: api_version.to_owned(),
        kind: kind.to_owned(),
        name: name.to_owned(),
        scope: if namespace.is_some() {
            ManifestScope::Namespaced
        } else {
            ManifestScope::Cluster
        },
        namespace,
    })
}

/// Convert raw manifest JSON into typed planned manifests for review/apply.
///
/// All-or-nothing: if any manifest fails to parse, the whole batch fails, so a
/// malformed manifest can never be silently dropped from an apply set.
pub(crate) fn plan_manifests(
    manifests: Vec<serde_json::Value>,
) -> Result<Vec<PlannedManifest>, K8sError> {
    manifests
        .into_iter()
        .map(PlannedManifest::try_from_value)
        .collect()
}

/// Standard labels applied to tokeira-managed Kubernetes resources.
///
/// `app` doubles as the pod selector key that [`KubePlatform::logs`] and
/// [`KubePlatform::port_forward`] use to find a service's pods (`app={service}`),
/// so it must match the Deployment's pod-template `app` label — that coupling is
/// why the selector label is centralized here rather than duplicated per builder.
pub fn standard_labels(service_name: &str, project_name: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("app".into(), service_name.into()),
        ("app.kubernetes.io/name".into(), service_name.into()),
        ("app.kubernetes.io/part-of".into(), project_name.into()),
        ("app.kubernetes.io/managed-by".into(), FIELD_MANAGER.into()),
    ])
}

/// Build an EKS Auto Mode `NodePool` as a dynamic JSON resource.
///
/// Pins ARM64 (Graviton) instance families with on-demand capacity and
/// references the EKS Auto Mode **default** NodeClass. The `apiVersion`
/// (`karpenter.sh/v1`) and the NodeClass group (`eks.amazonaws.com`) are
/// ground-truthed to EKS Auto Mode conventions verified 2026-07 (requirements
/// "Topology currency"); Property 13 locks this shape, so do not "modernize"
/// these strings without re-verifying against the current EKS Auto Mode API.
pub fn build_node_pool(node_families: &[String]) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "karpenter.sh/v1",
        "kind": "NodePool",
        "metadata": { "name": "tokeira-graviton" },
        "spec": {
            "template": {
                "spec": {
                    "requirements": [
                        { "key": "kubernetes.io/arch", "operator": "In", "values": ["arm64"] },
                        { "key": "karpenter.sh/capacity-type", "operator": "In", "values": ["on-demand"] },
                        { "key": "node.kubernetes.io/instance-type", "operator": "In", "values": node_families }
                    ],
                    "nodeClassRef": {
                        "group": "eks.amazonaws.com",
                        "kind": "NodeClass",
                        "name": "default"
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_metadata_detects_namespaced() {
        let manifest = serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": { "name": "edge-api", "namespace": "tokeira-system" }
        });
        let meta = manifest_metadata(&manifest).expect("parses");
        assert_eq!(meta.kind, "Deployment");
        assert_eq!(meta.name, "edge-api");
        assert_eq!(meta.namespace.as_deref(), Some("tokeira-system"));
        assert_eq!(meta.scope, ManifestScope::Namespaced);
    }

    #[test]
    fn manifest_metadata_detects_cluster_scoped() {
        let manifest = serde_json::json!({
            "apiVersion": "karpenter.sh/v1",
            "kind": "NodePool",
            "metadata": { "name": "tokeira-graviton" }
        });
        let meta = manifest_metadata(&manifest).expect("parses");
        assert_eq!(meta.namespace, None);
        assert_eq!(meta.scope, ManifestScope::Cluster);
    }

    #[test]
    fn manifest_metadata_rejects_missing_kind() {
        let manifest = serde_json::json!({ "apiVersion": "v1", "metadata": { "name": "x" } });
        let err = manifest_metadata(&manifest).expect_err("must reject");
        assert!(matches!(err, K8sError::InvalidManifest(_)));
    }

    #[test]
    fn standard_labels_use_tkp_manager_and_app_selector() {
        let labels = standard_labels("edge-api", "tokeira");
        assert_eq!(labels["app"], "edge-api");
        assert_eq!(labels["app.kubernetes.io/name"], "edge-api");
        assert_eq!(labels["app.kubernetes.io/part-of"], "tokeira");
        assert_eq!(labels["app.kubernetes.io/managed-by"], "tkp");
    }

    // Checkpoint 1.6: the NodePool shape is the operator-visible contract locked
    // by Property 13 — assert every load-bearing field explicitly.
    #[test]
    fn node_pool_targets_graviton_on_demand_default_nodeclass() {
        let families = vec!["m8g".to_string(), "c8g".to_string(), "r8g".to_string()];
        let np = build_node_pool(&families);

        assert_eq!(np["apiVersion"], "karpenter.sh/v1");
        assert_eq!(np["kind"], "NodePool");

        let node_class = &np["spec"]["template"]["spec"]["nodeClassRef"];
        assert_eq!(node_class["group"], "eks.amazonaws.com");
        assert_eq!(node_class["kind"], "NodeClass");
        assert_eq!(node_class["name"], "default");

        let reqs = np["spec"]["template"]["spec"]["requirements"]
            .as_array()
            .expect("requirements is an array");
        let value_for = |key: &str| {
            reqs.iter()
                .find(|r| r["key"] == key)
                .unwrap_or_else(|| panic!("missing requirement {key}"))["values"]
                .clone()
        };
        assert_eq!(
            value_for("kubernetes.io/arch"),
            serde_json::json!(["arm64"])
        );
        assert_eq!(
            value_for("karpenter.sh/capacity-type"),
            serde_json::json!(["on-demand"])
        );
        assert_eq!(
            value_for("node.kubernetes.io/instance-type"),
            serde_json::json!(["m8g", "c8g", "r8g"])
        );
    }
}
