//! The engine-injected runtime context the operator's `deployment(cfg, cx)`
//! reads but never writes.
//!
//! `Cx` is identity + ambient state supplied by `tkp` (from the deployment
//! metadata), not an operator knob. The `.tkd` reaches it only through the
//! whitelisted fields the bridge exposes (`cx.project_name`, `cx.region`,
//! `cx.account_id`) — `deployment_dir` is host-path plumbing and is deliberately
//! **not** surfaced to the `.tkd`.
//!
//! Unlike the compose `Cx`, this context carries **no `Vol`/bind-mount helpers**:
//! Kubernetes objects are constructed by the kinds (`k8s-openapi` structs →
//! `serde_json::Value`), not assembled from host paths, so there is no path-free
//! volume vocabulary to expose (design → "context.rs … no bind-mount `Vol`
//! helpers").

use std::path::PathBuf;

/// Engine-provided context, read by the operator's `deployment(cfg, cx)`.
///
/// Constructed by `tkp` per invocation and injected into the interpreter. All
/// fields are read-only from the `.tkd`'s perspective; the bridge exposes only a
/// whitelisted subset as readable `cx.*` fields.
#[derive(Clone, Debug)]
pub struct Cx {
    /// Deployment identity, from `metadata.json` (set at `tkr deployment create`).
    /// Seeds resource names/tags and is readable as `cx.project_name`.
    pub project_name: String,
    /// The AWS region the deployment targets. `None` before an identity is
    /// recorded (e.g. a bare in-memory context in tests); readable as `cx.region`.
    pub region: Option<String>,
    /// The AWS account id, when known. Used where an ARN or Pod-Identity
    /// association needs it; `None` until resolved. Readable as `cx.account_id`.
    pub account_id: Option<String>,
    /// Host-supplied ambient directory for the deployment (state/config on disk).
    /// Never surfaced to the `.tkd` — it is realize-time plumbing only, kept off
    /// the operator surface so the definition stays hermetic (Proposal 003 §4).
    pub deployment_dir: PathBuf,
}
