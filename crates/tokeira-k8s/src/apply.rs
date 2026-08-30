//! Server-side apply, get, and delete over the dynamic `kube` API.
//!
//! Every write is attributed to [`crate::FIELD_MANAGER`] (`tkp`) so field
//! ownership is stable and a 409 conflict is actionable. Applies fail closed on
//! a conflict unless [`ApplyOptions::force_conflicts`] is set (review before
//! mutation). Deletes run in reverse and tolerate not-found, making teardown
//! idempotent.
//!
//! The dynamic (`DynamicObject`) path is used for everything so a single code
//! path handles both typed core objects (Deployment, Service) and CRDs
//! (`NodePool`, external-secrets) without a per-kind match.

use anyhow::{Context, Result};
use kube::{
    Client,
    api::{Api, DeleteParams, DynamicObject, GroupVersionKind, Patch, PatchParams},
    discovery::ApiResource,
};
use tracing::{info, warn};

use crate::{ApplyOptions, FIELD_MANAGER, PlannedManifest, manifest_metadata, plan_manifests};

/// Server-side-apply a batch of raw manifests with default options.
pub(crate) async fn apply_manifests(
    client: &Client,
    manifests: &[serde_json::Value],
) -> Result<usize> {
    let planned =
        plan_manifests(manifests.to_vec()).context("failed to parse manifests for apply")?;
    apply_planned_manifests(client, &planned, ApplyOptions::default()).await
}

/// Server-side-apply pre-planned manifests with explicit options.
pub(crate) async fn apply_planned_manifests(
    client: &Client,
    manifests: &[PlannedManifest],
    options: ApplyOptions,
) -> Result<usize> {
    let mut applied = 0;
    for manifest in manifests {
        apply_manifest(client, manifest, options)
            .await
            .with_context(|| {
                format!(
                    "failed to apply {}/{}",
                    manifest.metadata.kind, manifest.metadata.name
                )
            })?;
        applied += 1;
    }
    info!(count = applied, "manifests applied");
    Ok(applied)
}

/// Apply a single manifest via server-side apply on the dynamic API.
async fn apply_manifest(
    client: &Client,
    planned: &PlannedManifest,
    options: ApplyOptions,
) -> Result<()> {
    let kind = planned.metadata.kind.as_str();
    let name = planned.metadata.name.as_str();
    let namespace = planned.metadata.namespace.as_deref();

    let gvk = parse_gvk(&planned.metadata.api_version, kind);
    let ar = ApiResource::from_gvk_with_plural(&gvk, &pluralize(kind));
    let patch_params = if options.force_conflicts {
        PatchParams::apply(FIELD_MANAGER).force()
    } else {
        PatchParams::apply(FIELD_MANAGER)
    };

    let api: Api<DynamicObject> = match namespace {
        Some(ns) => Api::namespaced_with(client.clone(), ns, &ar),
        None => Api::all_with(client.clone(), &ar),
    };

    match api
        .patch(name, &patch_params, &Patch::Apply(&planned.manifest))
        .await
    {
        Ok(_) => {
            info!(kind, name, namespace = ?namespace, "applied");
            Ok(())
        }
        // A 409 without `force` means another field manager owns a field we are
        // setting. Failing closed (rather than clobbering) is what upholds
        // review-before-mutation; takeover is opt-in via `force_conflicts`.
        Err(kube::Error::Api(ae)) if ae.code == 409 && !options.force_conflicts => {
            Err(anyhow::anyhow!(
                "field-manager conflict for {kind}/{name}: another manager owns a field being \
                 applied; re-run with force-conflicts enabled if takeover is intended"
            ))
        }
        Err(err) => Err(err.into()),
    }
}

/// Fetch a single object's live state as JSON, or `None` if it does not exist.
///
/// Used by Kubernetes-object resource `describe` to distinguish present from
/// absent against the real cluster.
pub(crate) async fn get_manifest(
    client: &Client,
    manifest: &serde_json::Value,
) -> Result<Option<serde_json::Value>> {
    let meta = manifest_metadata(manifest)?;
    let gvk = parse_gvk(&meta.api_version, &meta.kind);
    let ar = ApiResource::from_gvk_with_plural(&gvk, &pluralize(&meta.kind));
    let api: Api<DynamicObject> = match meta.namespace.as_deref() {
        Some(ns) => Api::namespaced_with(client.clone(), ns, &ar),
        None => Api::all_with(client.clone(), &ar),
    };
    match api.get_opt(&meta.name).await? {
        Some(obj) => Ok(Some(
            serde_json::to_value(obj).context("failed to serialize live k8s object")?,
        )),
        None => Ok(None),
    }
}

/// Check every field the desired manifest owns against one live object.
///
/// Server-side apply deliberately leaves provider-defaulted and controller-owned
/// fields outside the desired document. A whole-object equality check would
/// therefore report permanent drift; recursive subset comparison checks exactly
/// the surface attributed to `tkp` while ignoring live-only fields such as
/// `status`, resource versions, and allocated Service addresses.
pub(crate) fn desired_fields_match(desired: &serde_json::Value, live: &serde_json::Value) -> bool {
    match (desired, live) {
        (serde_json::Value::Object(desired), serde_json::Value::Object(live)) => {
            desired.iter().all(|(key, value)| {
                live.get(key)
                    .is_some_and(|live| desired_fields_match(value, live))
            })
        }
        (serde_json::Value::Array(desired), serde_json::Value::Array(live)) => {
            desired.len() == live.len()
                && desired
                    .iter()
                    .zip(live)
                    .all(|(desired, live)| desired_fields_match(desired, live))
        }
        _ => desired == live,
    }
}

/// Read and compare the complete desired manifest set.
///
/// Missing objects, changed owned fields, and objects whose live list shape no
/// longer matches are drift. Provider errors remain errors so callers can choose
/// the fail-closed behavior appropriate to their planning surface.
pub(crate) async fn manifests_current(
    client: &Client,
    manifests: &[serde_json::Value],
) -> Result<bool> {
    for desired in manifests {
        let Some(live) = get_manifest(client, desired).await? else {
            return Ok(false);
        };
        if !desired_fields_match(desired, &live) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Delete a batch of manifests in reverse order, ignoring not-found.
///
/// Reverse order (last applied deleted first) mirrors dependency-safe teardown;
/// a delete failure for one object is logged and does not abort the rest, so a
/// partially-torn-down set can still be cleaned up.
pub(crate) async fn delete_manifests(
    client: &Client,
    manifests: &[serde_json::Value],
) -> Result<usize> {
    let mut deleted = 0;
    for manifest in manifests.iter().rev() {
        match delete_manifest(client, manifest).await {
            Ok(true) => deleted += 1,
            Ok(false) => {}
            Err(e) => warn!(error = %e, "failed to delete resource, continuing"),
        }
    }
    info!(count = deleted, "manifests deleted");
    Ok(deleted)
}

/// Delete a single manifest. `Ok(true)` if deleted, `Ok(false)` if already gone.
async fn delete_manifest(client: &Client, manifest: &serde_json::Value) -> Result<bool> {
    let meta = manifest_metadata(manifest)?;
    let gvk = parse_gvk(&meta.api_version, &meta.kind);
    let ar = ApiResource::from_gvk_with_plural(&gvk, &pluralize(&meta.kind));
    let api: Api<DynamicObject> = match meta.namespace.as_deref() {
        Some(ns) => Api::namespaced_with(client.clone(), ns, &ar),
        None => Api::all_with(client.clone(), &ar),
    };
    match api.delete(&meta.name, &DeleteParams::default()).await {
        Ok(_) => {
            info!(kind = %meta.kind, name = %meta.name, "deleted");
            Ok(true)
        }
        Err(kube::Error::Api(ae)) if ae.code == 404 => {
            info!(kind = %meta.kind, name = %meta.name, "already absent");
            Ok(false)
        }
        Err(e) => Err(e.into()),
    }
}

/// Parse an `apiVersion` (`apps/v1`, `v1`, `karpenter.sh/v1`) + kind into a GVK.
///
/// A core-group object like `v1` has no `/`, so it maps to the empty group.
fn parse_gvk(api_version: &str, kind: &str) -> GroupVersionKind {
    match api_version.split_once('/') {
        Some((group, version)) => GroupVersionKind::gvk(group, version, kind),
        None => GroupVersionKind::gvk("", api_version, kind),
    }
}

/// Best-effort pluralization for the resource kinds tokeira applies.
///
/// The dynamic `ApiResource` needs the plural (URL) name. We hard-code the kinds
/// this platform emits (the fallback `{kind}s` covers the regular cases); it is
/// not a general English pluralizer, only a mapping for the manifests we build.
fn pluralize(kind: &str) -> String {
    let lower = kind.to_lowercase();
    match lower.as_str() {
        "namespace" => "namespaces".into(),
        "deployment" => "deployments".into(),
        "service" => "services".into(),
        "configmap" => "configmaps".into(),
        "secret" => "secrets".into(),
        "serviceaccount" => "serviceaccounts".into(),
        "role" => "roles".into(),
        "rolebinding" => "rolebindings".into(),
        "clusterrole" => "clusterroles".into(),
        "clusterrolebinding" => "clusterrolebindings".into(),
        "networkpolicy" => "networkpolicies".into(),
        "externalsecret" => "externalsecrets".into(),
        "secretstore" => "secretstores".into(),
        "nodepool" => "nodepools".into(),
        "nodeclass" => "nodeclasses".into(),
        "pod" => "pods".into(),
        "job" => "jobs".into(),
        "ingress" => "ingresses".into(),
        "daemonset" => "daemonsets".into(),
        "statefulset" => "statefulsets".into(),
        _ => format!("{lower}s"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_gvk_splits_group_and_version() {
        let gvk = parse_gvk("apps/v1", "Deployment");
        assert_eq!(gvk.group, "apps");
        assert_eq!(gvk.version, "v1");
        assert_eq!(gvk.kind, "Deployment");
    }

    #[test]
    fn parse_gvk_core_group_is_empty() {
        let gvk = parse_gvk("v1", "Namespace");
        assert_eq!(gvk.group, "");
        assert_eq!(gvk.version, "v1");
    }

    #[test]
    fn pluralize_handles_irregular_and_regular_kinds() {
        assert_eq!(pluralize("NetworkPolicy"), "networkpolicies");
        assert_eq!(pluralize("NodeClass"), "nodeclasses");
        assert_eq!(pluralize("Deployment"), "deployments");
        assert_eq!(pluralize("ConfigMap"), "configmaps");
    }

    #[test]
    fn desired_field_comparison_ignores_server_owned_fields() {
        let desired = serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "tokeirad",
                "labels": { "app": "tokeirad" }
            },
            "spec": {
                "replicas": 2,
                "template": { "spec": { "containers": [{ "name": "tokeirad" }] } }
            }
        });
        let live = serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "tokeirad",
                "labels": { "app": "tokeirad", "controller": "deployment" },
                "resourceVersion": "42"
            },
            "spec": {
                "replicas": 2,
                "strategy": { "type": "RollingUpdate" },
                "template": { "spec": { "containers": [{ "name": "tokeirad", "imagePullPolicy": "IfNotPresent" }] } }
            },
            "status": { "readyReplicas": 2 }
        });

        assert!(desired_fields_match(&desired, &live));
    }

    #[test]
    fn desired_field_comparison_detects_owned_scalar_and_list_drift() {
        let desired = serde_json::json!({
            "spec": {
                "replicas": 2,
                "containers": [{ "name": "server" }, { "name": "alloy" }]
            }
        });
        let changed_replicas = serde_json::json!({
            "spec": {
                "replicas": 3,
                "containers": [{ "name": "server" }, { "name": "alloy" }]
            }
        });
        let missing_sidecar = serde_json::json!({
            "spec": { "replicas": 2, "containers": [{ "name": "server" }] }
        });

        assert!(!desired_fields_match(&desired, &changed_replicas));
        assert!(!desired_fields_match(&desired, &missing_sidecar));
    }
}
