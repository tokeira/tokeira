//! Worker versioning rule storage and evaluation.
//!
//! This module owns rule state, not transport policy. gRPC-specific
//! preconditions such as `CommitBuildId.force` are checked by the edge handler
//! before mutations reach this store. Keeping the store rule-only lets the
//! runtime publisher use the same resolver without depending on edge code.

use std::collections::{HashMap, HashSet};

use dashmap::DashMap;
use thiserror::Error;
use time::OffsetDateTime;
use tokeira_types::{BuildId, NamespaceId, TaskQueueName, WorkflowId};

#[derive(Clone, Debug, PartialEq)]
pub struct AssignmentRule {
    pub target_build_id: String,
    pub percentage_ramp: Option<f32>,
    pub create_time: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RedirectRule {
    pub source_build_id: String,
    pub target_build_id: String,
    pub create_time: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VersioningRules {
    pub assignment_rules: Vec<AssignmentRule>,
    pub redirect_rules: Vec<RedirectRule>,
    pub conflict_token: Vec<u8>,
}

impl Default for VersioningRules {
    fn default() -> Self {
        Self {
            assignment_rules: Vec::new(),
            redirect_rules: Vec::new(),
            conflict_token: encode_token(1),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum VersioningMutation {
    InsertAssignmentRule {
        rule: AssignmentRule,
        index: usize,
    },
    ReplaceAssignmentRule {
        rule: AssignmentRule,
        index: usize,
        force: bool,
    },
    DeleteAssignmentRule {
        index: usize,
        force: bool,
    },
    AddRedirectRule {
        rule: RedirectRule,
    },
    ReplaceRedirectRule {
        source_build_id: String,
        rule: RedirectRule,
    },
    DeleteRedirectRule {
        source_build_id: String,
    },
    CommitBuildId {
        build_id: String,
    },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum VersioningError {
    #[error("stale conflict token")]
    StaleConflictToken,
    #[error("rule index out of bounds")]
    OutOfBounds,
    #[error("build id must not be empty")]
    EmptyBuildId,
    #[error("operation would remove the last unconditional assignment rule")]
    LastUnconditionalRule,
    #[error("redirect source already exists")]
    DuplicateRedirectSource,
    #[error("redirect source does not exist")]
    UnknownRedirectSource,
    #[error("redirect cycle detected")]
    RedirectCycle,
    #[error("redirect chain is too deep")]
    RedirectChainTooDeep,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskReachabilityType {
    NewWorkflows,
    ExistingWorkflows,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskQueueReachability {
    pub task_queue: TaskQueueName,
    pub reachability: Vec<TaskReachabilityType>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildIdReachabilityResult {
    pub build_id: String,
    pub task_queue_reachability: Vec<TaskQueueReachability>,
}

#[derive(Default)]
pub struct VersioningRuleStore {
    rules: DashMap<(NamespaceId, TaskQueueName), VersioningRules>,
}

impl VersioningRuleStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_rules(
        &self,
        namespace_id: NamespaceId,
        task_queue: &TaskQueueName,
    ) -> VersioningRules {
        self.rules
            .entry((namespace_id, task_queue.clone()))
            .or_default()
            .clone()
    }

    pub fn apply_mutation(
        &self,
        namespace_id: NamespaceId,
        task_queue: &TaskQueueName,
        conflict_token: Vec<u8>,
        mutation: VersioningMutation,
        now: OffsetDateTime,
    ) -> Result<VersioningRules, VersioningError> {
        let mut entry = self
            .rules
            .entry((namespace_id, task_queue.clone()))
            .or_default();
        if entry.conflict_token != conflict_token {
            return Err(VersioningError::StaleConflictToken);
        }

        match mutation {
            VersioningMutation::InsertAssignmentRule { rule, index } => {
                validate_assignment_rule(&rule)?;
                let index = index.min(entry.assignment_rules.len());
                entry.assignment_rules.insert(index, rule);
            }
            VersioningMutation::ReplaceAssignmentRule { rule, index, force } => {
                validate_assignment_rule(&rule)?;
                if index >= entry.assignment_rules.len() {
                    return Err(VersioningError::OutOfBounds);
                }
                let mut candidate = entry.assignment_rules.clone();
                candidate[index] = rule;
                ensure_unconditional_remains(&candidate, force)?;
                entry.assignment_rules = candidate;
            }
            VersioningMutation::DeleteAssignmentRule { index, force } => {
                if index >= entry.assignment_rules.len() {
                    return Err(VersioningError::OutOfBounds);
                }
                let mut candidate = entry.assignment_rules.clone();
                candidate.remove(index);
                ensure_unconditional_remains(&candidate, force)?;
                entry.assignment_rules = candidate;
            }
            VersioningMutation::AddRedirectRule { mut rule } => {
                validate_redirect_rule(&rule)?;
                if entry
                    .redirect_rules
                    .iter()
                    .any(|existing| existing.source_build_id == rule.source_build_id)
                {
                    return Err(VersioningError::DuplicateRedirectSource);
                }
                rule.create_time = now;
                entry.redirect_rules.push(rule);
            }
            VersioningMutation::ReplaceRedirectRule {
                source_build_id,
                mut rule,
            } => {
                validate_redirect_rule(&rule)?;
                let Some(existing) = entry
                    .redirect_rules
                    .iter_mut()
                    .find(|existing| existing.source_build_id == source_build_id)
                else {
                    return Err(VersioningError::UnknownRedirectSource);
                };
                rule.create_time = now;
                *existing = rule;
            }
            VersioningMutation::DeleteRedirectRule { source_build_id } => {
                let Some(index) = entry
                    .redirect_rules
                    .iter()
                    .position(|rule| rule.source_build_id == source_build_id)
                else {
                    return Err(VersioningError::UnknownRedirectSource);
                };
                entry.redirect_rules.remove(index);
            }
            VersioningMutation::CommitBuildId { build_id } => {
                if build_id.is_empty() {
                    return Err(VersioningError::EmptyBuildId);
                }
                entry.assignment_rules.retain(|rule| {
                    rule.target_build_id != build_id
                        && !(is_unconditional(rule) && rule.target_build_id != build_id)
                });
                entry.assignment_rules.push(AssignmentRule {
                    target_build_id: build_id,
                    percentage_ramp: None,
                    create_time: now,
                });
            }
        }

        entry.conflict_token = increment_token(&entry.conflict_token);
        Ok(entry.clone())
    }

    pub fn evaluate_assignment(
        &self,
        namespace_id: NamespaceId,
        task_queue: &TaskQueueName,
        workflow_id: &WorkflowId,
    ) -> Option<BuildId> {
        let rules = self.get_rules(namespace_id, task_queue);
        rules
            .assignment_rules
            .iter()
            .find(|rule| assignment_applies(rule, workflow_id))
            .map(|rule| BuildId(rule.target_build_id.clone()))
    }

    pub fn resolve_redirect(
        &self,
        namespace_id: NamespaceId,
        task_queue: &TaskQueueName,
        build_id: &BuildId,
    ) -> Result<BuildId, VersioningError> {
        let rules = self.get_rules(namespace_id, task_queue);
        resolve_redirect_from_rules(&rules.redirect_rules, &build_id.0).map(BuildId)
    }

    pub fn all_task_queues_with_rules(&self) -> Vec<(NamespaceId, TaskQueueName)> {
        self.rules.iter().map(|entry| entry.key().clone()).collect()
    }
}

pub fn compute_reachability(
    build_id: &str,
    task_queue: TaskQueueName,
    assignment_rules: &[AssignmentRule],
    redirect_rules: &[RedirectRule],
) -> TaskQueueReachability {
    let mut new_reachable = HashSet::new();
    for rule in assignment_rules {
        if let Ok(target) = resolve_redirect_from_rules(redirect_rules, &rule.target_build_id) {
            new_reachable.insert(target);
        }
    }

    let mut reachability = Vec::new();
    if new_reachable.contains(build_id) {
        reachability.push(TaskReachabilityType::NewWorkflows);
    } else if redirect_rules
        .iter()
        .any(|rule| rule.target_build_id == build_id || rule.source_build_id == build_id)
    {
        reachability.push(TaskReachabilityType::ExistingWorkflows);
    }

    TaskQueueReachability {
        task_queue,
        reachability,
    }
}

fn validate_assignment_rule(rule: &AssignmentRule) -> Result<(), VersioningError> {
    if rule.target_build_id.is_empty() {
        return Err(VersioningError::EmptyBuildId);
    }
    Ok(())
}

fn validate_redirect_rule(rule: &RedirectRule) -> Result<(), VersioningError> {
    if rule.target_build_id.is_empty() || rule.source_build_id.is_empty() {
        return Err(VersioningError::EmptyBuildId);
    }
    Ok(())
}

fn ensure_unconditional_remains(
    rules: &[AssignmentRule],
    force: bool,
) -> Result<(), VersioningError> {
    if force || rules.iter().any(is_unconditional) {
        Ok(())
    } else {
        Err(VersioningError::LastUnconditionalRule)
    }
}

fn is_unconditional(rule: &AssignmentRule) -> bool {
    rule.percentage_ramp.is_none() || rule.percentage_ramp == Some(100.0)
}

fn assignment_applies(rule: &AssignmentRule, workflow_id: &WorkflowId) -> bool {
    let Some(ramp) = rule.percentage_ramp else {
        return true;
    };
    if ramp >= 100.0 {
        return true;
    }
    if ramp <= 0.0 {
        return false;
    }
    deterministic_bucket(&workflow_id.0) < (f64::from(ramp) * 100.0) as u64
}

pub(crate) fn deterministic_bucket(value: &str) -> u64 {
    // FNV-1a is stable across processes, unlike DefaultHasher's seeded state.
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash % 10_000
}

fn resolve_redirect_from_rules(
    redirect_rules: &[RedirectRule],
    build_id: &str,
) -> Result<String, VersioningError> {
    let redirects: HashMap<&str, &str> = redirect_rules
        .iter()
        .map(|rule| (rule.source_build_id.as_str(), rule.target_build_id.as_str()))
        .collect();
    let mut current = build_id;
    let mut visited = HashSet::new();
    for _ in 0..10 {
        let Some(next) = redirects.get(current).copied() else {
            return Ok(current.to_string());
        };
        if !visited.insert(current) {
            return Err(VersioningError::RedirectCycle);
        }
        current = next;
    }
    Err(VersioningError::RedirectChainTooDeep)
}

fn encode_token(value: u64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

fn decode_token(token: &[u8]) -> u64 {
    let mut bytes = [0_u8; 8];
    if token.len() == 8 {
        bytes.copy_from_slice(token);
    }
    u64::from_be_bytes(bytes)
}

fn increment_token(token: &[u8]) -> Vec<u8> {
    encode_token(decode_token(token).saturating_add(1))
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokeira_types::{BuildId, NamespaceId, TaskQueueName, WorkflowId};

    use super::{
        AssignmentRule, RedirectRule, TaskReachabilityType, VersioningError, VersioningMutation,
        VersioningRuleStore, assignment_applies, compute_reachability, is_unconditional,
    };

    fn assignment(build_id: &str) -> AssignmentRule {
        AssignmentRule {
            target_build_id: build_id.to_string(),
            percentage_ramp: None,
            create_time: OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn ramped_assignment(build_id: &str, percentage: f32) -> AssignmentRule {
        AssignmentRule {
            target_build_id: build_id.to_string(),
            percentage_ramp: Some(percentage),
            create_time: OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn redirect(source: &str, target: &str) -> RedirectRule {
        RedirectRule {
            source_build_id: source.to_string(),
            target_build_id: target.to_string(),
            create_time: OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn arb_build_id() -> impl Strategy<Value = String> {
        prop::collection::vec(prop::char::range('a', 'z'), 1..8)
            .prop_map(|chars| chars.into_iter().collect())
    }

    fn arb_assignment_rule() -> impl Strategy<Value = AssignmentRule> {
        (
            arb_build_id(),
            prop_oneof![
                Just(None),
                Just(Some(0.0)),
                Just(Some(25.0)),
                Just(Some(50.0)),
                Just(Some(100.0)),
            ],
        )
            .prop_map(|(target_build_id, percentage_ramp)| AssignmentRule {
                target_build_id,
                percentage_ramp,
                create_time: OffsetDateTime::UNIX_EPOCH,
            })
    }

    #[derive(Clone, Debug)]
    enum CrudOp {
        InsertAssignment { build_id: String, index: usize },
        ReplaceAssignment { build_id: String, index: usize },
        DeleteAssignment { index: usize },
        AddRedirect { source: String, target: String },
        ReplaceRedirect { source: String, target: String },
        DeleteRedirect { source: String },
        Commit { build_id: String },
    }

    fn arb_crud_op() -> impl Strategy<Value = CrudOp> {
        prop_oneof![
            (arb_build_id(), 0usize..6)
                .prop_map(|(build_id, index)| { CrudOp::InsertAssignment { build_id, index } }),
            (arb_build_id(), 0usize..6)
                .prop_map(|(build_id, index)| { CrudOp::ReplaceAssignment { build_id, index } }),
            (0usize..6).prop_map(|index| CrudOp::DeleteAssignment { index }),
            (arb_build_id(), arb_build_id())
                .prop_map(|(source, target)| { CrudOp::AddRedirect { source, target } }),
            (arb_build_id(), arb_build_id())
                .prop_map(|(source, target)| { CrudOp::ReplaceRedirect { source, target } }),
            arb_build_id().prop_map(|source| CrudOp::DeleteRedirect { source }),
            arb_build_id().prop_map(|build_id| CrudOp::Commit { build_id }),
        ]
    }

    fn token_value(token: &[u8]) -> u64 {
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(token);
        u64::from_be_bytes(bytes)
    }

    fn apply_model_op(
        assignments: &mut Vec<AssignmentRule>,
        redirects: &mut Vec<RedirectRule>,
        op: CrudOp,
    ) -> Result<VersioningMutation, (VersioningError, VersioningMutation)> {
        match op {
            CrudOp::InsertAssignment { build_id, index } => {
                let rule = assignment(&build_id);
                assignments.insert(index.min(assignments.len()), rule.clone());
                Ok(VersioningMutation::InsertAssignmentRule { rule, index })
            }
            CrudOp::ReplaceAssignment { build_id, index } => {
                let mutation = VersioningMutation::ReplaceAssignmentRule {
                    rule: assignment(&build_id),
                    index,
                    force: true,
                };
                if index >= assignments.len() {
                    return Err((VersioningError::OutOfBounds, mutation));
                }
                let rule = assignment(&build_id);
                assignments[index] = rule.clone();
                Ok(mutation)
            }
            CrudOp::DeleteAssignment { index } => {
                let mutation = VersioningMutation::DeleteAssignmentRule { index, force: true };
                if index >= assignments.len() {
                    return Err((VersioningError::OutOfBounds, mutation));
                }
                assignments.remove(index);
                Ok(mutation)
            }
            CrudOp::AddRedirect { source, target } => {
                let rule = redirect(&source, &target);
                let mutation = VersioningMutation::AddRedirectRule { rule: rule.clone() };
                if redirects
                    .iter()
                    .any(|redirect| redirect.source_build_id == source)
                {
                    return Err((VersioningError::DuplicateRedirectSource, mutation));
                }
                redirects.push(rule.clone());
                Ok(mutation)
            }
            CrudOp::ReplaceRedirect { source, target } => {
                let rule = redirect(&source, &target);
                let mutation = VersioningMutation::ReplaceRedirectRule {
                    source_build_id: source.clone(),
                    rule: rule.clone(),
                };
                let Some(existing) = redirects
                    .iter_mut()
                    .find(|redirect| redirect.source_build_id == source)
                else {
                    return Err((VersioningError::UnknownRedirectSource, mutation));
                };
                *existing = rule.clone();
                Ok(mutation)
            }
            CrudOp::DeleteRedirect { source } => {
                let mutation = VersioningMutation::DeleteRedirectRule {
                    source_build_id: source.clone(),
                };
                let Some(index) = redirects
                    .iter()
                    .position(|redirect| redirect.source_build_id == source)
                else {
                    return Err((VersioningError::UnknownRedirectSource, mutation));
                };
                redirects.remove(index);
                Ok(mutation)
            }
            CrudOp::Commit { build_id } => {
                assignments.retain(|rule| {
                    rule.target_build_id != build_id
                        && !(is_unconditional(rule) && rule.target_build_id != build_id)
                });
                assignments.push(assignment(&build_id));
                Ok(VersioningMutation::CommitBuildId { build_id })
            }
        }
    }

    fn independent_redirect_target(
        redirect_rules: &[RedirectRule],
        build_id: &str,
    ) -> Option<String> {
        let redirects: HashMap<&str, &str> = redirect_rules
            .iter()
            .map(|rule| (rule.source_build_id.as_str(), rule.target_build_id.as_str()))
            .collect();
        let mut current = build_id;
        let mut visited = HashSet::new();
        for _ in 0..10 {
            let Some(next) = redirects.get(current).copied() else {
                return Some(current.to_string());
            };
            if !visited.insert(current) {
                return None;
            }
            current = next;
        }
        None
    }

    proptest! {
        #[test]
        fn property_assignment_evaluation_determinism(
            rules in prop::collection::vec(arb_assignment_rule(), 0..16),
            workflow_id in arb_build_id(),
        ) {
            // Feature: edge-worker-versioning-transport, Property 1: Assignment rule evaluation determinism
            let store = VersioningRuleStore::default();
            let namespace_id = NamespaceId::new();
            let task_queue = TaskQueueName("q".to_string());
            let mut token = store.get_rules(namespace_id, &task_queue).conflict_token;
            for rule in rules.clone() {
                token = store
                    .apply_mutation(
                        namespace_id,
                        &task_queue,
                        token,
                        VersioningMutation::InsertAssignmentRule {
                            rule,
                            index: usize::MAX,
                        },
                        OffsetDateTime::UNIX_EPOCH,
                    )
                    .unwrap()
                    .conflict_token;
            }

            let workflow_id = WorkflowId(workflow_id);
            let first = store.evaluate_assignment(namespace_id, &task_queue, &workflow_id);
            let second = store.evaluate_assignment(namespace_id, &task_queue, &workflow_id);
            let expected = rules
                .iter()
                .find(|rule| assignment_applies(rule, &workflow_id))
                .map(|rule| BuildId(rule.target_build_id.clone()));

            prop_assert_eq!(first.clone(), second);
            prop_assert_eq!(first, expected);
        }

        #[test]
        fn property_redirect_chain_resolution(chain_len in 0usize..9) {
            // Feature: edge-worker-versioning-transport, Property 2: Redirect chain resolution
            let store = VersioningRuleStore::default();
            let namespace_id = NamespaceId::new();
            let task_queue = TaskQueueName("q".to_string());
            let mut token = store.get_rules(namespace_id, &task_queue).conflict_token;
            for idx in 0..chain_len {
                token = store
                    .apply_mutation(
                        namespace_id,
                        &task_queue,
                        token,
                        VersioningMutation::AddRedirectRule {
                            rule: redirect(&format!("b{idx}"), &format!("b{}", idx + 1)),
                        },
                        OffsetDateTime::UNIX_EPOCH,
                    )
                    .unwrap()
                    .conflict_token;
            }

            let expected = if chain_len == 0 {
                "b0".to_string()
            } else {
                format!("b{chain_len}")
            };
            prop_assert_eq!(
                store
                    .resolve_redirect(namespace_id, &task_queue, &BuildId("b0".to_string()))
                    .unwrap(),
                BuildId(expected)
            );
            prop_assert_eq!(
                store
                    .resolve_redirect(namespace_id, &task_queue, &BuildId("unrelated".to_string()))
                    .unwrap(),
                BuildId("unrelated".to_string())
            );

            let cycle_store = VersioningRuleStore::default();
            let cycle_queue = TaskQueueName("cycle".to_string());
            let mut cycle_token = cycle_store.get_rules(namespace_id, &cycle_queue).conflict_token;
            for (source, target) in [("a", "b"), ("b", "a")] {
                cycle_token = cycle_store
                    .apply_mutation(
                        namespace_id,
                        &cycle_queue,
                        cycle_token,
                        VersioningMutation::AddRedirectRule {
                            rule: redirect(source, target),
                        },
                        OffsetDateTime::UNIX_EPOCH,
                    )
                    .unwrap()
                    .conflict_token;
            }
            prop_assert_eq!(
                cycle_store
                    .resolve_redirect(namespace_id, &cycle_queue, &BuildId("a".to_string()))
                    .unwrap_err(),
                VersioningError::RedirectCycle
            );
        }

        #[test]
        fn property_conflict_token_monotonicity(build_ids in prop::collection::vec(arb_build_id(), 1..24)) {
            // Feature: edge-worker-versioning-transport, Property 3: Conflict token monotonicity
            let store = VersioningRuleStore::default();
            let namespace_id = NamespaceId::new();
            let task_queue = TaskQueueName("q".to_string());
            let mut token = store.get_rules(namespace_id, &task_queue).conflict_token;
            let stale_token = token.clone();
            let mut previous = token_value(&token);

            for build_id in build_ids {
                let rules = store
                    .apply_mutation(
                        namespace_id,
                        &task_queue,
                        token,
                        VersioningMutation::InsertAssignmentRule {
                            rule: assignment(&build_id),
                            index: usize::MAX,
                        },
                        OffsetDateTime::UNIX_EPOCH,
                    )
                    .unwrap();
                let current = token_value(&rules.conflict_token);
                prop_assert!(current > previous);
                previous = current;
                token = rules.conflict_token;
            }

            prop_assert_eq!(
                store
                    .apply_mutation(
                        namespace_id,
                        &task_queue,
                        stale_token,
                        VersioningMutation::InsertAssignmentRule {
                            rule: assignment("stale"),
                            index: 0,
                        },
                        OffsetDateTime::UNIX_EPOCH,
                    )
                    .unwrap_err(),
                VersioningError::StaleConflictToken
            );
        }

        #[test]
        fn property_rule_crud_correctness(ops in prop::collection::vec(arb_crud_op(), 0..64)) {
            // Feature: edge-worker-versioning-transport, Property 4: Rule CRUD correctness
            let store = VersioningRuleStore::default();
            let namespace_id = NamespaceId::new();
            let task_queue = TaskQueueName("q".to_string());
            let mut token = store.get_rules(namespace_id, &task_queue).conflict_token;
            let mut model_assignments = Vec::new();
            let mut model_redirects = Vec::new();

            for op in ops {
                let before = store.get_rules(namespace_id, &task_queue);
                match apply_model_op(&mut model_assignments, &mut model_redirects, op) {
                    Ok(mutation) => {
                        let rules = store
                            .apply_mutation(
                                namespace_id,
                                &task_queue,
                                token,
                                mutation,
                                OffsetDateTime::UNIX_EPOCH,
                            )
                            .unwrap();
                        prop_assert_eq!(rules.assignment_rules, model_assignments.clone());
                        prop_assert_eq!(rules.redirect_rules, model_redirects.clone());
                        token = rules.conflict_token;
                    }
                    Err((expected, mutation)) => {
                        prop_assert_eq!(
                            store
                                .apply_mutation(
                                    namespace_id,
                                    &task_queue,
                                    token.clone(),
                                    mutation,
                                    OffsetDateTime::UNIX_EPOCH,
                                )
                                .unwrap_err(),
                            expected
                        );
                        prop_assert_eq!(store.get_rules(namespace_id, &task_queue), before);
                    }
                }
            }
        }

        #[test]
        fn property_reachability_classification(
            assignment_indices in prop::collection::vec(0usize..8, 0..8),
            redirect_count in 0usize..8,
        ) {
            // Feature: edge-worker-versioning-transport, Property 5: Reachability classification
            let build_ids: Vec<String> = (0..9).map(|idx| format!("b{idx}")).collect();
            let assignments: Vec<_> = assignment_indices
                .into_iter()
                .map(|idx| assignment(&build_ids[idx]))
                .collect();
            let redirects: Vec<_> = (0..redirect_count)
                .map(|idx| redirect(&build_ids[idx], &build_ids[idx + 1]))
                .collect();

            for build_id in &build_ids {
                let reachability = compute_reachability(
                    build_id,
                    TaskQueueName("q".to_string()),
                    &assignments,
                    &redirects,
                );
                let new_reachable = assignments.iter().any(|rule| {
                    independent_redirect_target(&redirects, &rule.target_build_id)
                        .as_deref()
                        == Some(build_id.as_str())
                });
                let redirect_referenced = redirects.iter().any(|rule| {
                    rule.source_build_id == *build_id || rule.target_build_id == *build_id
                });
                let expected = if new_reachable {
                    vec![TaskReachabilityType::NewWorkflows]
                } else if redirect_referenced {
                    vec![TaskReachabilityType::ExistingWorkflows]
                } else {
                    Vec::new()
                };

                prop_assert_eq!(reachability.reachability, expected);
            }
        }
    }

    #[test]
    fn empty_rule_set_returns_empty_rules_and_initial_token() {
        let store = VersioningRuleStore::default();
        let rules = store.get_rules(NamespaceId::new(), &TaskQueueName("q".to_string()));

        assert!(rules.assignment_rules.is_empty());
        assert!(rules.redirect_rules.is_empty());
        assert_eq!(rules.conflict_token, 1_u64.to_be_bytes());
    }

    #[test]
    fn insert_assignment_with_oversized_index_appends() {
        let store = VersioningRuleStore::default();
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("q".to_string());
        let token = store.get_rules(namespace_id, &task_queue).conflict_token;

        let rules = store
            .apply_mutation(
                namespace_id,
                &task_queue,
                token,
                VersioningMutation::InsertAssignmentRule {
                    rule: assignment("build-a"),
                    index: usize::MAX,
                },
                OffsetDateTime::UNIX_EPOCH,
            )
            .unwrap();

        assert_eq!(rules.assignment_rules[0].target_build_id, "build-a");
    }

    #[test]
    fn replace_and_delete_out_of_bounds_are_rejected() {
        let store = VersioningRuleStore::default();
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("q".to_string());
        let token = store.get_rules(namespace_id, &task_queue).conflict_token;

        assert_eq!(
            store
                .apply_mutation(
                    namespace_id,
                    &task_queue,
                    token.clone(),
                    VersioningMutation::ReplaceAssignmentRule {
                        rule: assignment("build-a"),
                        index: 1,
                        force: true,
                    },
                    OffsetDateTime::UNIX_EPOCH,
                )
                .unwrap_err(),
            VersioningError::OutOfBounds
        );
        assert_eq!(
            store
                .apply_mutation(
                    namespace_id,
                    &task_queue,
                    token,
                    VersioningMutation::DeleteAssignmentRule {
                        index: 1,
                        force: true,
                    },
                    OffsetDateTime::UNIX_EPOCH,
                )
                .unwrap_err(),
            VersioningError::OutOfBounds
        );
    }

    #[test]
    fn empty_build_ids_are_rejected() {
        let store = VersioningRuleStore::default();
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("q".to_string());
        let token = store.get_rules(namespace_id, &task_queue).conflict_token;

        assert_eq!(
            store
                .apply_mutation(
                    namespace_id,
                    &task_queue,
                    token.clone(),
                    VersioningMutation::InsertAssignmentRule {
                        rule: assignment(""),
                        index: 0,
                    },
                    OffsetDateTime::UNIX_EPOCH,
                )
                .unwrap_err(),
            VersioningError::EmptyBuildId
        );
        assert_eq!(
            store
                .apply_mutation(
                    namespace_id,
                    &task_queue,
                    token,
                    VersioningMutation::AddRedirectRule {
                        rule: redirect("old", ""),
                    },
                    OffsetDateTime::UNIX_EPOCH,
                )
                .unwrap_err(),
            VersioningError::EmptyBuildId
        );
    }

    #[test]
    fn removing_last_unconditional_rule_requires_force() {
        let store = VersioningRuleStore::default();
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("q".to_string());
        let token = store.get_rules(namespace_id, &task_queue).conflict_token;
        let rules = store
            .apply_mutation(
                namespace_id,
                &task_queue,
                token,
                VersioningMutation::InsertAssignmentRule {
                    rule: assignment("build-a"),
                    index: 0,
                },
                OffsetDateTime::UNIX_EPOCH,
            )
            .unwrap();

        assert_eq!(
            store
                .apply_mutation(
                    namespace_id,
                    &task_queue,
                    rules.conflict_token.clone(),
                    VersioningMutation::DeleteAssignmentRule {
                        index: 0,
                        force: false,
                    },
                    OffsetDateTime::UNIX_EPOCH,
                )
                .unwrap_err(),
            VersioningError::LastUnconditionalRule
        );
        assert_eq!(
            store
                .apply_mutation(
                    namespace_id,
                    &task_queue,
                    rules.conflict_token,
                    VersioningMutation::ReplaceAssignmentRule {
                        rule: ramped_assignment("build-b", 50.0),
                        index: 0,
                        force: false,
                    },
                    OffsetDateTime::UNIX_EPOCH,
                )
                .unwrap_err(),
            VersioningError::LastUnconditionalRule
        );
    }

    #[test]
    fn force_allows_removing_last_unconditional_rule() {
        let store = VersioningRuleStore::default();
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("q".to_string());
        let token = store.get_rules(namespace_id, &task_queue).conflict_token;
        let rules = store
            .apply_mutation(
                namespace_id,
                &task_queue,
                token,
                VersioningMutation::InsertAssignmentRule {
                    rule: assignment("build-a"),
                    index: 0,
                },
                OffsetDateTime::UNIX_EPOCH,
            )
            .unwrap();

        let rules = store
            .apply_mutation(
                namespace_id,
                &task_queue,
                rules.conflict_token,
                VersioningMutation::DeleteAssignmentRule {
                    index: 0,
                    force: true,
                },
                OffsetDateTime::UNIX_EPOCH,
            )
            .unwrap();

        assert!(rules.assignment_rules.is_empty());
    }

    #[test]
    fn duplicate_redirect_source_is_rejected() {
        let store = VersioningRuleStore::default();
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("q".to_string());
        let token = store.get_rules(namespace_id, &task_queue).conflict_token;
        let rules = store
            .apply_mutation(
                namespace_id,
                &task_queue,
                token,
                VersioningMutation::AddRedirectRule {
                    rule: redirect("old", "new"),
                },
                OffsetDateTime::UNIX_EPOCH,
            )
            .unwrap();

        let err = store
            .apply_mutation(
                namespace_id,
                &task_queue,
                rules.conflict_token,
                VersioningMutation::AddRedirectRule {
                    rule: redirect("old", "newer"),
                },
                OffsetDateTime::UNIX_EPOCH,
            )
            .unwrap_err();

        assert_eq!(err, VersioningError::DuplicateRedirectSource);
    }

    #[test]
    fn absent_redirect_replace_and_delete_are_rejected() {
        let store = VersioningRuleStore::default();
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("q".to_string());
        let token = store.get_rules(namespace_id, &task_queue).conflict_token;

        assert_eq!(
            store
                .apply_mutation(
                    namespace_id,
                    &task_queue,
                    token.clone(),
                    VersioningMutation::ReplaceRedirectRule {
                        source_build_id: "old".to_string(),
                        rule: redirect("old", "new"),
                    },
                    OffsetDateTime::UNIX_EPOCH,
                )
                .unwrap_err(),
            VersioningError::UnknownRedirectSource
        );
        assert_eq!(
            store
                .apply_mutation(
                    namespace_id,
                    &task_queue,
                    token,
                    VersioningMutation::DeleteRedirectRule {
                        source_build_id: "old".to_string(),
                    },
                    OffsetDateTime::UNIX_EPOCH,
                )
                .unwrap_err(),
            VersioningError::UnknownRedirectSource
        );
    }

    #[test]
    fn redirect_create_time_is_server_authored_on_add_and_replace() {
        let store = VersioningRuleStore::default();
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("q".to_string());
        let token = store.get_rules(namespace_id, &task_queue).conflict_token;
        let add_time = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(10);
        let replace_time = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(20);
        let rules = store
            .apply_mutation(
                namespace_id,
                &task_queue,
                token,
                VersioningMutation::AddRedirectRule {
                    rule: redirect("old", "new"),
                },
                add_time,
            )
            .unwrap();
        assert_eq!(rules.redirect_rules[0].create_time, add_time);

        let rules = store
            .apply_mutation(
                namespace_id,
                &task_queue,
                rules.conflict_token,
                VersioningMutation::ReplaceRedirectRule {
                    source_build_id: "old".to_string(),
                    rule: redirect("old", "newer"),
                },
                replace_time,
            )
            .unwrap();
        assert_eq!(rules.redirect_rules[0].create_time, replace_time);
    }

    #[test]
    fn redirect_cycle_and_depth_limit_are_rejected() {
        let store = VersioningRuleStore::default();
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("q".to_string());
        let mut token = store.get_rules(namespace_id, &task_queue).conflict_token;
        for (source, target) in [("a", "b"), ("b", "a")] {
            token = store
                .apply_mutation(
                    namespace_id,
                    &task_queue,
                    token,
                    VersioningMutation::AddRedirectRule {
                        rule: redirect(source, target),
                    },
                    OffsetDateTime::UNIX_EPOCH,
                )
                .unwrap()
                .conflict_token;
        }
        assert_eq!(
            store
                .resolve_redirect(namespace_id, &task_queue, &BuildId("a".to_string()))
                .unwrap_err(),
            VersioningError::RedirectCycle
        );

        let store = VersioningRuleStore::default();
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("deep".to_string());
        let mut token = store.get_rules(namespace_id, &task_queue).conflict_token;
        for idx in 0..11 {
            token = store
                .apply_mutation(
                    namespace_id,
                    &task_queue,
                    token,
                    VersioningMutation::AddRedirectRule {
                        rule: redirect(&format!("b{idx}"), &format!("b{}", idx + 1)),
                    },
                    OffsetDateTime::UNIX_EPOCH,
                )
                .unwrap()
                .conflict_token;
        }
        assert_eq!(
            store
                .resolve_redirect(namespace_id, &task_queue, &BuildId("b0".to_string()))
                .unwrap_err(),
            VersioningError::RedirectChainTooDeep
        );
    }

    #[test]
    fn resolve_redirect_returns_original_when_no_rule_matches() {
        let store = VersioningRuleStore::default();
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("q".to_string());

        assert_eq!(
            store
                .resolve_redirect(namespace_id, &task_queue, &BuildId("build-a".to_string()))
                .unwrap(),
            BuildId("build-a".to_string())
        );
    }

    #[test]
    fn commit_build_id_rewrites_assignment_rules() {
        let store = VersioningRuleStore::default();
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("q".to_string());
        let token = store.get_rules(namespace_id, &task_queue).conflict_token;
        let rules = store
            .apply_mutation(
                namespace_id,
                &task_queue,
                token,
                VersioningMutation::InsertAssignmentRule {
                    rule: assignment("old"),
                    index: 0,
                },
                OffsetDateTime::UNIX_EPOCH,
            )
            .unwrap();

        let rules = store
            .apply_mutation(
                namespace_id,
                &task_queue,
                rules.conflict_token,
                VersioningMutation::CommitBuildId {
                    build_id: "new".to_string(),
                },
                OffsetDateTime::UNIX_EPOCH,
            )
            .unwrap();

        assert_eq!(rules.assignment_rules, vec![assignment("new")]);
    }

    #[test]
    fn assignment_reachability_follows_redirect_targets_for_new_workflows() {
        let reachability = compute_reachability(
            "new",
            TaskQueueName("q".to_string()),
            &[assignment("old")],
            &[redirect("old", "new")],
        );

        assert_eq!(
            reachability.reachability,
            vec![TaskReachabilityType::NewWorkflows]
        );
    }

    #[test]
    fn evaluate_assignment_uses_stable_workflow_bucket() {
        let store = VersioningRuleStore::default();
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("q".to_string());
        let token = store.get_rules(namespace_id, &task_queue).conflict_token;
        store
            .apply_mutation(
                namespace_id,
                &task_queue,
                token,
                VersioningMutation::InsertAssignmentRule {
                    rule: assignment("build-a"),
                    index: 0,
                },
                OffsetDateTime::UNIX_EPOCH,
            )
            .unwrap();

        assert_eq!(
            store
                .evaluate_assignment(
                    namespace_id,
                    &task_queue,
                    &WorkflowId("workflow-a".to_string())
                )
                .unwrap()
                .0,
            "build-a"
        );
    }
}
