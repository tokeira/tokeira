//! Namespace-scoped Workflow Rule persistence and protobuf translation.
//!
//! The edge validates and translates public rule messages, while the repository owns durable
//! namespace isolation, duplicate detection, capacity eviction, and retention. Automatic matching
//! is runtime-owned so every activity lifecycle path observes the same stored rule set.

use std::{fmt, sync::Arc};

use time::{Duration, OffsetDateTime};
use tokeira_proto::{
    conversions::common::to_proto_timestamp,
    public::temporal::api::rules::v1::{
        WorkflowRule, WorkflowRuleAction as ProtoWorkflowRuleAction, WorkflowRuleSpec,
        workflow_rule_action, workflow_rule_spec,
    },
};
use tokeira_storage::{RunRepository, WorkflowRuleCreateResult, WorkflowRuleDeleteResult};
use tokeira_types::{NamespaceId, WorkflowRuleAction, WorkflowRuleRecord, WorkflowRuleTrigger};

/// v1.31.0 namespace registry limit (`common/dynamicconfig/constants.go @ v1.31.0`).
const MAX_RULES_PER_NAMESPACE: usize = 10;

/// Failures returned by the durable workflow-rule registry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkflowRuleError {
    /// The supplied id already identifies a rule in the namespace.
    AlreadyExists,
    /// No stored rule has the supplied id.
    NotFound,
    /// The namespace has reached the configured rule limit.
    LimitExceeded,
    /// An expiration timestamp cannot be represented by the durable time type.
    InvalidExpiration,
    /// Durable storage failed before the operation completed.
    Storage(String),
}

/// Durable namespace rule registry used by WorkflowService CRUD handlers.
#[derive(Clone)]
pub struct WorkflowRuleStore {
    repo: Arc<dyn RunRepository>,
}

impl fmt::Debug for WorkflowRuleStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowRuleStore")
            .finish_non_exhaustive()
    }
}

impl WorkflowRuleStore {
    /// Attach the registry to the service's durable repository.
    pub fn new(repo: Arc<dyn RunRepository>) -> Self {
        Self { repo }
    }

    /// Create one namespace rule, rejecting duplicate ids atomically.
    pub async fn create(
        &self,
        namespace_id: NamespaceId,
        spec: WorkflowRuleSpec,
        identity: String,
        description: String,
        now: OffsetDateTime,
    ) -> Result<WorkflowRule, WorkflowRuleError> {
        let record = record_from_proto(spec, identity, description, now)?;
        match self
            .repo
            .create_workflow_rule(namespace_id, record.clone(), MAX_RULES_PER_NAMESPACE)
            .await
            .map_err(storage_error)?
        {
            WorkflowRuleCreateResult::Created => Ok(record_to_proto(record)),
            WorkflowRuleCreateResult::AlreadyExists => Err(WorkflowRuleError::AlreadyExists),
            WorkflowRuleCreateResult::LimitExceeded => Err(WorkflowRuleError::LimitExceeded),
        }
    }

    /// Read one stored namespace rule by id, including expired records.
    pub async fn describe(
        &self,
        namespace_id: NamespaceId,
        rule_id: &str,
    ) -> Result<WorkflowRule, WorkflowRuleError> {
        self.repo
            .get_workflow_rule(namespace_id, rule_id)
            .await
            .map_err(storage_error)?
            .map(record_to_proto)
            .ok_or(WorkflowRuleError::NotFound)
    }

    /// Delete one stored namespace rule by id, including expired records.
    pub async fn delete(
        &self,
        namespace_id: NamespaceId,
        rule_id: &str,
    ) -> Result<(), WorkflowRuleError> {
        match self
            .repo
            .delete_workflow_rule(namespace_id, rule_id)
            .await
            .map_err(storage_error)?
        {
            WorkflowRuleDeleteResult::Deleted => Ok(()),
            WorkflowRuleDeleteResult::NotFound => Err(WorkflowRuleError::NotFound),
        }
    }

    /// List every stored namespace rule in stable id order, including expired records.
    pub async fn list(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<WorkflowRule>, WorkflowRuleError> {
        self.repo
            .list_workflow_rules(namespace_id)
            .await
            .map(|rules| rules.into_iter().map(record_to_proto).collect())
            .map_err(storage_error)
    }
}

fn record_from_proto(
    spec: WorkflowRuleSpec,
    created_by_identity: String,
    description: String,
    create_time: OffsetDateTime,
) -> Result<WorkflowRuleRecord, WorkflowRuleError> {
    let trigger = match spec.trigger {
        Some(workflow_rule_spec::Trigger::ActivityStart(trigger)) => {
            WorkflowRuleTrigger::ActivityStart {
                predicate: trigger.predicate,
            }
        }
        None => WorkflowRuleTrigger::Unsupported,
    };
    let actions = spec
        .actions
        .into_iter()
        .map(|action| match action.variant {
            Some(workflow_rule_action::Variant::ActivityPause(_)) => {
                WorkflowRuleAction::ActivityPause
            }
            None => WorkflowRuleAction::Unsupported,
        })
        .collect();
    let expiration_time = spec
        .expiration_time
        .as_ref()
        .map(timestamp_to_offset)
        .transpose()?;
    Ok(WorkflowRuleRecord {
        id: spec.id,
        create_time,
        created_by_identity,
        description,
        trigger,
        visibility_query: spec.visibility_query,
        actions,
        expiration_time,
    })
}

fn record_to_proto(record: WorkflowRuleRecord) -> WorkflowRule {
    let trigger = match record.trigger {
        WorkflowRuleTrigger::ActivityStart { predicate } => {
            Some(workflow_rule_spec::Trigger::ActivityStart(
                workflow_rule_spec::ActivityStartingTrigger { predicate },
            ))
        }
        WorkflowRuleTrigger::Unsupported => None,
    };
    let actions = record
        .actions
        .into_iter()
        .map(|action| ProtoWorkflowRuleAction {
            variant: match action {
                WorkflowRuleAction::ActivityPause => {
                    Some(workflow_rule_action::Variant::ActivityPause(
                        workflow_rule_action::ActionActivityPause {},
                    ))
                }
                WorkflowRuleAction::Unsupported => None,
            },
        })
        .collect();
    WorkflowRule {
        create_time: Some(to_proto_timestamp(record.create_time)),
        spec: Some(WorkflowRuleSpec {
            id: record.id,
            trigger,
            visibility_query: record.visibility_query,
            actions,
            expiration_time: record.expiration_time.map(to_proto_timestamp),
        }),
        created_by_identity: record.created_by_identity,
        description: record.description,
    }
}

fn timestamp_to_offset(
    timestamp: &prost_types::Timestamp,
) -> Result<OffsetDateTime, WorkflowRuleError> {
    if !(0..1_000_000_000).contains(&timestamp.nanos) {
        return Err(WorkflowRuleError::InvalidExpiration);
    }
    OffsetDateTime::from_unix_timestamp(timestamp.seconds)
        .map(|value| value + Duration::nanoseconds(i64::from(timestamp.nanos)))
        .map_err(|_| WorkflowRuleError::InvalidExpiration)
}

fn storage_error(error: anyhow::Error) -> WorkflowRuleError {
    WorkflowRuleError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;
    use tokeira_proto::public::temporal::api::rules::v1::WorkflowRuleAction as ProtoAction;
    use tokeira_storage::InMemoryStore;
    use tokio::runtime::Builder;

    use super::*;

    fn pause_spec(id: &str, expiration_time: Option<OffsetDateTime>) -> WorkflowRuleSpec {
        WorkflowRuleSpec {
            id: id.to_string(),
            trigger: Some(workflow_rule_spec::Trigger::ActivityStart(
                workflow_rule_spec::ActivityStartingTrigger {
                    predicate: "ActivityType = 'activity'".to_string(),
                },
            )),
            visibility_query: String::new(),
            actions: vec![ProtoAction {
                variant: Some(workflow_rule_action::Variant::ActivityPause(
                    workflow_rule_action::ActionActivityPause {},
                )),
            }],
            expiration_time: expiration_time.map(to_proto_timestamp),
        }
    }

    #[tokio::test]
    async fn expired_rule_remains_describable_and_listed() {
        let store = WorkflowRuleStore::new(Arc::new(InMemoryStore::default()));
        let namespace_id = NamespaceId(uuid::Uuid::nil());
        let now = OffsetDateTime::UNIX_EPOCH;
        store
            .create(
                namespace_id,
                pause_spec("expired", Some(now - Duration::SECOND)),
                "creator".to_string(),
                "description".to_string(),
                now,
            )
            .await
            .expect("create should retain an already-expired rule");

        assert!(store.describe(namespace_id, "expired").await.is_ok());
        assert_eq!(store.list(namespace_id).await.expect("list").len(), 1);
    }

    #[tokio::test]
    async fn create_at_capacity_evicts_earliest_expiration_only_then() {
        let store = WorkflowRuleStore::new(Arc::new(InMemoryStore::default()));
        let namespace_id = NamespaceId(uuid::Uuid::nil());
        let now = OffsetDateTime::UNIX_EPOCH;
        for id in 0..MAX_RULES_PER_NAMESPACE {
            store
                .create(
                    namespace_id,
                    pause_spec(
                        &format!("rule-{id}"),
                        (id < 2).then_some(now + Duration::seconds(10 + id as i64)),
                    ),
                    String::new(),
                    String::new(),
                    now,
                )
                .await
                .expect("fill registry");
        }

        store
            .create(
                namespace_id,
                pause_spec("replacement", None),
                String::new(),
                String::new(),
                now,
            )
            .await
            .expect("earliest expiration supplies capacity");
        assert!(matches!(
            store.describe(namespace_id, "rule-0").await,
            Err(WorkflowRuleError::NotFound)
        ));
        assert!(store.describe(namespace_id, "rule-1").await.is_ok());
        assert!(store.describe(namespace_id, "replacement").await.is_ok());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        // Feature: workflow-rules, Property 1: namespace isolation and CRUD model
        fn property_namespace_rule_crud_matches_reference_model(
            operations in proptest::collection::vec((any::<bool>(), any::<bool>(), 0u8..8u8), 1..40),
        ) {
            let runtime = Builder::new_current_thread().enable_all().build()
                .expect("test runtime");
            runtime.block_on(async {
                let store = WorkflowRuleStore::new(Arc::new(InMemoryStore::default()));
                let namespaces = [
                    NamespaceId(uuid::Uuid::from_u128(1)),
                    NamespaceId(uuid::Uuid::from_u128(2)),
                ];
                let mut model = [BTreeMap::<String, ()>::new(), BTreeMap::<String, ()>::new()];
                for (create, second_namespace, id) in operations {
                    let namespace_index = usize::from(second_namespace);
                    let namespace_id = namespaces[namespace_index];
                    let rule_id = format!("rule-{id}");
                    if create {
                        let existed = model[namespace_index].contains_key(&rule_id);
                        let result = store.create(
                            namespace_id,
                            pause_spec(&rule_id, None),
                            String::new(),
                            String::new(),
                            OffsetDateTime::UNIX_EPOCH,
                        ).await;
                        if existed {
                            prop_assert_eq!(result, Err(WorkflowRuleError::AlreadyExists));
                        } else {
                            prop_assert!(result.is_ok());
                            model[namespace_index].insert(rule_id, ());
                        }
                    } else {
                        let existed = model[namespace_index].remove(&rule_id).is_some();
                        let result = store.delete(namespace_id, &rule_id).await;
                        if existed {
                            prop_assert_eq!(result, Ok(()));
                        } else {
                            prop_assert_eq!(result, Err(WorkflowRuleError::NotFound));
                        }
                    }
                    for (index, namespace_id) in namespaces.iter().copied().enumerate() {
                        let actual = store.list(namespace_id).await.expect("list");
                        let actual_ids = actual.into_iter().map(|rule| {
                            rule.spec.expect("stored spec").id
                        }).collect::<Vec<_>>();
                        let expected_ids = model[index].keys().cloned().collect::<Vec<_>>();
                        prop_assert_eq!(actual_ids, expected_ids);
                    }
                }
                Ok(())
            })?;
        }

        #[test]
        // Feature: workflow-rules, Property 5: rejection has no side effect
        fn property_rejected_rule_mutations_preserve_registry(
            namespace_seed in any::<u128>(),
        ) {
            let runtime = Builder::new_current_thread().enable_all().build()
                .expect("test runtime");
            runtime.block_on(async {
                let store = WorkflowRuleStore::new(Arc::new(InMemoryStore::default()));
                let namespace_id = NamespaceId(uuid::Uuid::from_u128(namespace_seed));
                for id in 0..MAX_RULES_PER_NAMESPACE {
                    store.create(
                        namespace_id,
                        pause_spec(&format!("rule-{id}"), None),
                        String::new(),
                        String::new(),
                        OffsetDateTime::UNIX_EPOCH,
                    ).await.expect("fill registry");
                }
                let before = store.list(namespace_id).await.expect("snapshot");
                prop_assert_eq!(
                    store.create(
                        namespace_id,
                        pause_spec("rule-0", None),
                        String::new(),
                        String::new(),
                        OffsetDateTime::UNIX_EPOCH,
                    ).await,
                    Err(WorkflowRuleError::AlreadyExists),
                );
                prop_assert_eq!(
                    store.create(
                        namespace_id,
                        pause_spec("over-limit", None),
                        String::new(),
                        String::new(),
                        OffsetDateTime::UNIX_EPOCH,
                    ).await,
                    Err(WorkflowRuleError::LimitExceeded),
                );
                prop_assert_eq!(
                    store.delete(namespace_id, "missing").await,
                    Err(WorkflowRuleError::NotFound),
                );
                prop_assert_eq!(store.list(namespace_id).await.expect("after"), before);
                Ok(())
            })?;
        }

        #[test]
        // Feature: workflow-rules, Property 7: expiration separates evaluation from retention
        fn property_expiration_does_not_change_proto_round_trip(
            seconds in -1_000_000i64..1_000_000i64,
        ) {
            let create_time = OffsetDateTime::UNIX_EPOCH;
            let expiration = OffsetDateTime::from_unix_timestamp(seconds)
                .expect("generated timestamp is valid");
            let record = record_from_proto(
                pause_spec("rule", Some(expiration)),
                "creator".to_string(),
                "description".to_string(),
                create_time,
            ).expect("valid proto record");
            let restored = record_from_proto(
                record_to_proto(record.clone()).spec.expect("spec"),
                record.created_by_identity.clone(),
                record.description.clone(),
                record.create_time,
            ).expect("restored record");
            prop_assert_eq!(restored, record);
        }
    }
}
