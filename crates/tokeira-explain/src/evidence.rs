//! Evidence identity: every addressable fact in an explanation carries a
//! stable id that renderers, queries, and (eventually) agent citations
//! reference.
//!
//! Ids are **natural keys**, never ordinals: `change:observability::compose/
//! grafana`, not `C12`. Ordinals are stable only while iteration order is,
//! and the model's determinism property (identical inputs → byte-identical
//! serialization) demands identity that survives any construction order.
//! Natural keys also satisfy "same resource, same deployment and revision →
//! same id" by construction rather than by bookkeeping (operator-explanation Req 1.4).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A stable, human-legible identifier for one fact in one explanation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EvidenceId(String);

impl EvidenceId {
    /// The change to one resource: `change:{module}::{resource_id}`.
    pub(crate) fn change(module: &str, resource_id: &str) -> Self {
        Self(format!("change:{module}::{resource_id}"))
    }

    /// One uncertainty: `uncertainty:{reason_tag}:{subject}`.
    pub(crate) fn uncertainty(reason_tag: &str, subject: &EvidenceId) -> Self {
        Self(format!("uncertainty:{reason_tag}:{}", subject.0))
    }

    /// One operational impact: `impact:{class_tag}:{subject}`.
    pub(crate) fn impact(class_tag: &str, subject: &EvidenceId) -> Self {
        Self(format!("impact:{class_tag}:{}", subject.0))
    }

    /// The deployment itself: `deployment:{name}`.
    pub fn deployment(name: &str) -> Self {
        Self(format!("deployment:{name}"))
    }

    /// One platform issue: `issue:{component}` — the component name is the
    /// natural key (one issue per component per plan).
    pub fn issue(component: &str) -> Self {
        Self(format!("issue:{component}"))
    }

    /// One causal group: `group:{root_key}`, where the root key is the
    /// group's natural root identity (`revision-comparison:{n}`,
    /// `resource:{change id}`, `provisioner-advance`) — never a member
    /// ordinal, for the same determinism reason as every other id here.
    pub(crate) fn group(root_key: &str) -> Self {
        Self(format!("group:{root_key}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// What kind of fact an id resolves to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EvidenceKind {
    Deployment,
    Change,
    Uncertainty,
    Impact,
    CausalGroup,
    PlatformIssue,
}

/// The addressable collection of one explanation's evidence.
///
/// `BTreeMap` for the same reason as everywhere in this layer: iteration and
/// serialization order are functions of the keys, so construction order can
/// never leak into the artifact (§Evidence, Property 2).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceIndex(BTreeMap<EvidenceId, EvidenceKind>);

impl EvidenceIndex {
    pub(crate) fn insert(&mut self, id: EvidenceId, kind: EvidenceKind) {
        self.0.insert(id, kind);
    }

    /// Resolve an id to its kind. `None` is the caller's cue to treat a
    /// citation as invalid — never to render it anyway (operator-explanation Req 1.4).
    pub fn resolve(&self, id: &EvidenceId) -> Option<EvidenceKind> {
        self.0.get(id).copied()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn ids(&self) -> impl Iterator<Item = &EvidenceId> {
        self.0.keys()
    }
}
