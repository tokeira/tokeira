//! Transport-independent scoped Worker authorization.
//!
//! A [`WorkerScope`] is an attenuation attached to authenticated claims. It
//! grants a fixed code-owned set of Worker operations over one exact namespace,
//! one or more exact normal task queues, and one exact Deployment Version.
//! Ordinary Temporal roles never widen it.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokeira_types::WorkerTaskClass;

use crate::GlobPattern;

/// Exact resource boundary attached to a scoped Worker credential.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerScope {
    namespace: String,
    task_queues: BTreeSet<String>,
    deployment_name: String,
    build_id: String,
}

impl WorkerScope {
    /// Validate and normalize an operator- or issuer-authored Worker scope.
    pub fn try_new(
        namespace: String,
        task_queues: Vec<String>,
        deployment_name: String,
        build_id: String,
    ) -> Result<Self, WorkerScopeError> {
        validate_resource("namespace", &namespace)?;
        validate_resource("deployment_name", &deployment_name)?;
        validate_resource("build_id", &build_id)?;
        if task_queues.is_empty() {
            return Err(WorkerScopeError::EmptyTaskQueues);
        }
        let mut normalized = BTreeSet::new();
        for (index, task_queue) in task_queues.into_iter().enumerate() {
            validate_resource("task_queues", &task_queue).map_err(|source| {
                WorkerScopeError::TaskQueue {
                    index,
                    source: Box::new(source),
                }
            })?;
            if !normalized.insert(task_queue.clone()) {
                return Err(WorkerScopeError::DuplicateTaskQueue { task_queue });
            }
        }
        Ok(Self {
            namespace,
            task_queues: normalized,
            deployment_name,
            build_id,
        })
    }

    /// Exact namespace name authorized by this scope.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Normal task queues in deterministic lexical order.
    pub fn task_queues(&self) -> impl ExactSizeIterator<Item = &str> {
        self.task_queues.iter().map(String::as_str)
    }

    /// Exact Worker Deployment name authorized by this scope.
    pub fn deployment_name(&self) -> &str {
        &self.deployment_name
    }

    /// Exact Worker Build ID authorized by this scope.
    pub fn build_id(&self) -> &str {
        &self.build_id
    }

    /// Decide one preflight or fully resolved Worker target.
    ///
    /// This function performs no I/O and deliberately receives only normalized
    /// resource identity, never request bodies, task payloads, or credentials.
    pub fn authorize(
        &self,
        operation: WorkerOperation,
        namespace: &str,
        target: WorkerTarget<'_>,
    ) -> WorkerScopeDecision {
        if namespace != self.namespace {
            return WorkerScopeDecision::Deny(WorkerScopeDenyReason::Namespace);
        }
        if matches!(target, WorkerTarget::Preflight) {
            return WorkerScopeDecision::Allow;
        }
        match (operation.target_shape(), target) {
            (WorkerTargetShape::TaskQueue, WorkerTarget::TaskQueue { normal_task_queue }) => {
                self.authorize_queue(normal_task_queue)
            }
            (
                WorkerTargetShape::WorkflowPoll,
                WorkerTarget::VersionedTask {
                    normal_task_queue,
                    task_class: WorkerTaskClass::Workflow | WorkerTaskClass::Query,
                    deployment_name,
                    build_id,
                },
            ) => self.authorize_versioned(normal_task_queue, deployment_name, build_id),
            (
                WorkerTargetShape::Versioned(expected_class),
                WorkerTarget::VersionedTask {
                    normal_task_queue,
                    task_class,
                    deployment_name,
                    build_id,
                },
            ) if expected_class.is_none_or(|expected| expected == task_class) => {
                self.authorize_versioned(normal_task_queue, deployment_name, build_id)
            }
            _ => WorkerScopeDecision::Deny(WorkerScopeDenyReason::Operation),
        }
    }

    fn authorize_queue(&self, normal_task_queue: &str) -> WorkerScopeDecision {
        if self.task_queues.contains(normal_task_queue) {
            WorkerScopeDecision::Allow
        } else {
            WorkerScopeDecision::Deny(WorkerScopeDenyReason::Queue)
        }
    }

    fn authorize_versioned(
        &self,
        normal_task_queue: &str,
        deployment_name: &str,
        build_id: &str,
    ) -> WorkerScopeDecision {
        if !self.task_queues.contains(normal_task_queue) {
            WorkerScopeDecision::Deny(WorkerScopeDenyReason::Queue)
        } else if deployment_name != self.deployment_name || build_id != self.build_id {
            WorkerScopeDecision::Deny(WorkerScopeDenyReason::Version)
        } else {
            WorkerScopeDecision::Allow
        }
    }
}

fn validate_resource(field: &'static str, value: &str) -> Result<(), WorkerScopeError> {
    if value.trim().is_empty() {
        return Err(WorkerScopeError::Blank { field });
    }
    if value.contains('*') {
        return Err(WorkerScopeError::Wildcard { field });
    }
    Ok(())
}

/// Invalid Worker-scope construction.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum WorkerScopeError {
    /// A required exact resource was blank.
    #[error("{field} must not be blank")]
    Blank {
        /// Invalid field name.
        field: &'static str,
    },
    /// A scope resource used unsupported wildcard syntax.
    #[error("{field} must not contain wildcard syntax")]
    Wildcard {
        /// Invalid field name.
        field: &'static str,
    },
    /// No normal task queue was supplied.
    #[error("task_queues must contain at least one task queue")]
    EmptyTaskQueues,
    /// One task queue entry was invalid.
    #[error("task_queues[{index}]: {source}")]
    TaskQueue {
        /// Invalid entry index.
        index: usize,
        /// Underlying exact-resource failure.
        source: Box<WorkerScopeError>,
    },
    /// Duplicate queues are rejected rather than silently normalized away.
    #[error("task_queues contains duplicate entry {task_queue:?}")]
    DuplicateTaskQueue {
        /// Exact duplicate queue name.
        task_queue: String,
    },
}

/// Fixed Worker RPC authority understood by the scoped authorizer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WorkerOperation {
    /// Poll Workflow Tasks.
    PollWorkflowTaskQueue,
    /// Poll Activity Tasks.
    PollActivityTaskQueue,
    /// Poll Nexus Tasks.
    PollNexusTaskQueue,
    /// Complete a Workflow Task.
    RespondWorkflowTaskCompleted,
    /// Fail a Workflow Task.
    RespondWorkflowTaskFailed,
    /// Complete a Query task.
    RespondQueryTaskCompleted,
    /// Complete an Activity Task.
    RespondActivityTaskCompleted,
    /// Fail an Activity Task.
    RespondActivityTaskFailed,
    /// Cancel an Activity Task.
    RespondActivityTaskCanceled,
    /// Heartbeat an Activity Task.
    RecordActivityTaskHeartbeat,
    /// Complete a Nexus Task.
    RespondNexusTaskCompleted,
    /// Fail a Nexus Task.
    RespondNexusTaskFailed,
    /// Record one or more Worker heartbeats.
    RecordWorkerHeartbeat,
    /// Shut down the caller's bound Worker session.
    ShutdownWorker,
    /// Inspect readiness for one allowed normal task queue.
    DescribeTaskQueue,
}

impl WorkerOperation {
    fn target_shape(self) -> WorkerTargetShape {
        match self {
            Self::PollWorkflowTaskQueue => WorkerTargetShape::WorkflowPoll,
            Self::RespondWorkflowTaskCompleted | Self::RespondWorkflowTaskFailed => {
                WorkerTargetShape::Versioned(Some(WorkerTaskClass::Workflow))
            }
            Self::PollActivityTaskQueue
            | Self::RespondActivityTaskCompleted
            | Self::RespondActivityTaskFailed
            | Self::RespondActivityTaskCanceled
            | Self::RecordActivityTaskHeartbeat => {
                WorkerTargetShape::Versioned(Some(WorkerTaskClass::Activity))
            }
            Self::PollNexusTaskQueue
            | Self::RespondNexusTaskCompleted
            | Self::RespondNexusTaskFailed => {
                WorkerTargetShape::Versioned(Some(WorkerTaskClass::Nexus))
            }
            Self::RespondQueryTaskCompleted => {
                WorkerTargetShape::Versioned(Some(WorkerTaskClass::Query))
            }
            Self::RecordWorkerHeartbeat => WorkerTargetShape::Versioned(None),
            Self::ShutdownWorker | Self::DescribeTaskQueue => WorkerTargetShape::TaskQueue,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkerTargetShape {
    TaskQueue,
    WorkflowPoll,
    Versioned(Option<WorkerTaskClass>),
}

/// Normalized resource target for a scoped Worker authorization decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerTarget<'a> {
    /// API and namespace check before a token or resource is decoded.
    Preflight,
    /// One normal task queue, used by readiness and shutdown.
    TaskQueue {
        /// Stable application queue, never a sticky queue alias.
        normal_task_queue: &'a str,
    },
    /// Exact task origin or heartbeat version coordinates.
    VersionedTask {
        /// Stable application queue.
        normal_task_queue: &'a str,
        /// Worker task family; ignored only for Worker-heartbeat batches.
        task_class: WorkerTaskClass,
        /// Exact Worker Deployment name.
        deployment_name: &'a str,
        /// Exact Worker Build ID.
        build_id: &'a str,
    },
}

/// Worker-specific portion of a generic authorization target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkerCallTarget<'a> {
    /// Fixed Worker operation.
    pub operation: WorkerOperation,
    /// Preflight or resolved resource target.
    pub target: WorkerTarget<'a>,
}

/// Pure scoped-authorization result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerScopeDecision {
    /// The target is within the exact Worker scope.
    Allow,
    /// The target is outside the scope.
    Deny(WorkerScopeDenyReason),
}

/// Bounded denial classification safe for metric labels.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WorkerScopeDenyReason {
    /// The API or target shape is not allowed.
    Operation,
    /// The namespace differs.
    Namespace,
    /// The normal task queue is not allowed.
    Queue,
    /// Deployment or build differs.
    Version,
    /// Server-authored task origin differs.
    TaskOrigin,
    /// A Worker heartbeat element differs.
    Heartbeat,
    /// Caller-authored Worker process coordinates differ from its live session.
    WorkerSession,
    /// More than one distinct configured scope matched.
    AmbiguousMapping,
}

impl WorkerScopeDenyReason {
    /// Stable bounded label used by denial metrics.
    pub const fn metric_label(self) -> &'static str {
        match self {
            Self::Operation => "operation",
            Self::Namespace => "namespace",
            Self::Queue => "queue",
            Self::Version => "version",
            Self::TaskOrigin => "task_origin",
            Self::Heartbeat => "heartbeat",
            Self::WorkerSession => "worker_session",
            Self::AmbiguousMapping => "ambiguous_mapping",
        }
    }
}

/// One configured identity-pattern to Worker-scope mapping.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerScopeRule {
    pattern: GlobPattern,
    scope: WorkerScope,
}

impl WorkerScopeRule {
    /// Construct one validated mapping rule.
    pub fn new(
        pattern: impl Into<String>,
        scope: WorkerScope,
    ) -> Result<Self, crate::PatternError> {
        Ok(Self {
            pattern: GlobPattern::new(pattern)?,
            scope,
        })
    }
}

/// Validated Worker-scope mappings shared by JWT subjects and AWS IAM ARNs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerScopeRules {
    rules: Vec<WorkerScopeRule>,
}

impl WorkerScopeRules {
    /// Construct a ruleset from validated rules.
    pub fn new(rules: Vec<WorkerScopeRule>) -> Self {
        Self { rules }
    }

    /// Resolve matching rules without ever unioning distinct scopes.
    pub fn resolve(&self, identity: &str) -> Result<Option<WorkerScope>, ScopeConflict> {
        let mut resolved: Option<&WorkerScope> = None;
        for rule in self
            .rules
            .iter()
            .filter(|rule| rule.pattern.is_match(identity))
        {
            if let Some(existing) = resolved
                && existing != &rule.scope
            {
                return Err(ScopeConflict);
            }
            resolved = Some(&rule.scope);
        }
        Ok(resolved.cloned())
    }
}

/// Multiple distinct configured Worker scopes matched one identity.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("multiple distinct Worker scopes matched the authenticated identity")]
pub struct ScopeConflict;

/// Merge an optional signed and configured scope without choosing or unioning.
pub fn resolve_effective_scope(
    signed: Option<WorkerScope>,
    configured: Option<WorkerScope>,
) -> Result<Option<WorkerScope>, ScopeConflict> {
    match (signed, configured) {
        (None, None) => Ok(None),
        (Some(scope), None) | (None, Some(scope)) => Ok(Some(scope)),
        (Some(signed), Some(configured)) if signed == configured => Ok(Some(signed)),
        (Some(_), Some(_)) => Err(ScopeConflict),
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn scope(queues: Vec<String>) -> Result<WorkerScope, WorkerScopeError> {
        WorkerScope::try_new(
            "payments".to_owned(),
            queues,
            "payments".to_owned(),
            "2026.07.28".to_owned(),
        )
    }

    // Feature: scoped-worker-authorization, Property 1: Worker-Scope normalization and validation
    proptest! {
        #[test]
        fn property_worker_scope_normalization_and_validation(
            namespace in ".{0,12}",
            queues in prop::collection::vec(".{0,12}", 0..6),
            deployment in ".{0,12}",
            build_id in ".{0,12}",
        ) {
            let result = WorkerScope::try_new(
                namespace.clone(),
                queues.clone(),
                deployment.clone(),
                build_id.clone(),
            );
            let valid_resource = |value: &str| !value.trim().is_empty() && !value.contains('*');
            let unique = queues.iter().collect::<BTreeSet<_>>().len() == queues.len();
            let expected = valid_resource(&namespace)
                && !queues.is_empty()
                && queues.iter().all(|queue| valid_resource(queue))
                && unique
                && valid_resource(&deployment)
                && valid_resource(&build_id);
            prop_assert_eq!(result.is_ok(), expected);
            if let Ok(scope) = result {
                let ordered = scope.task_queues().collect::<Vec<_>>();
                prop_assert!(ordered.windows(2).all(|pair| pair[0] < pair[1]));
                let encoded = serde_json::to_string(&scope).expect("serialize scope");
                let decoded: WorkerScope =
                    serde_json::from_str(&encoded).expect("deserialize scope");
                prop_assert_eq!(decoded, scope);
            }
        }
    }

    // Feature: scoped-worker-authorization, Property 2: Scope-source resolution is non-composable
    proptest! {
        #[test]
        fn property_scope_source_resolution_is_non_composable(
            identity in "[a-z]{1,8}",
            first_matches in any::<bool>(),
            second_matches in any::<bool>(),
            scopes_equal in any::<bool>(),
        ) {
            let first = scope(vec!["queue-a".to_owned()]).expect("scope");
            let second = if scopes_equal {
                first.clone()
            } else {
                scope(vec!["queue-b".to_owned()]).expect("scope")
            };
            let miss = format!("never-{identity}");
            let rules = WorkerScopeRules::new(vec![
                WorkerScopeRule::new(
                    if first_matches { identity.clone() } else { miss.clone() },
                    first.clone(),
                ).expect("rule"),
                WorkerScopeRule::new(
                    if second_matches { identity.clone() } else { miss },
                    second,
                ).expect("rule"),
            ]);
            let result = rules.resolve(&identity);
            match (first_matches, second_matches, scopes_equal) {
                (false, false, _) => prop_assert_eq!(result, Ok(None)),
                (true, true, false) => prop_assert_eq!(result, Err(ScopeConflict)),
                _ => prop_assert!(matches!(result, Ok(Some(_)))),
            }
        }
    }
}
