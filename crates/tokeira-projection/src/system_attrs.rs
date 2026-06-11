//! Temporal-standard visibility search-attribute catalog.
//!
//! The projection plane owns search-attribute typing because both the write-side
//! sink and read-side filter compiler need the same registry contract. These
//! tables mirror Temporal's v1.31.0 `sadefs` catalog so the edge can report the
//! same legal keys while projection can seed predefined attributes before
//! workflow histories start upserting them.

use anyhow::Result;
use tokeira_types::NamespaceId;

use crate::{store::VisibilityStore, types::SearchAttrType};

/// Classification used by Temporal's `NameTypeMap`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StandardSearchAttrCategory {
    /// Stored as dedicated visibility columns and resolved without the custom registry.
    System,
    /// Stored inside the search-attribute map and therefore registered per namespace.
    Predefined,
}

/// One standard search-attribute definition from Temporal v1.31.0.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StandardSearchAttr {
    /// Public search-attribute name accepted by Temporal visibility queries.
    pub name: &'static str,
    /// Indexed value type associated with `name`.
    pub attr_type: SearchAttrType,
    /// Whether the field is column-backed (`System`) or registry-backed (`Predefined`).
    pub category: StandardSearchAttrCategory,
}

/// Standard attributes returned by `NameTypeMap.System()` in Temporal v1.31.0.
///
/// `NameTypeMap.System()` merges `sadefs.System()` and `sadefs.Predefined()`;
/// WorkflowService `GetSearchAttributes` returns `.All()`, and OperatorService
/// `ListSearchAttributes` returns `.System()` as `system_attributes`
/// (`common/searchattribute/name_type_map.go` and
/// `service/frontend/operator_handler.go:499 @ v1.31.0`).
pub const STANDARD_SEARCH_ATTRIBUTES: &[StandardSearchAttr] = &[
    StandardSearchAttr {
        name: "WorkflowId",
        attr_type: SearchAttrType::Keyword,
        category: StandardSearchAttrCategory::System,
    },
    StandardSearchAttr {
        name: "RunId",
        attr_type: SearchAttrType::Keyword,
        category: StandardSearchAttrCategory::System,
    },
    StandardSearchAttr {
        name: "WorkflowType",
        attr_type: SearchAttrType::Keyword,
        category: StandardSearchAttrCategory::System,
    },
    StandardSearchAttr {
        name: "StartTime",
        attr_type: SearchAttrType::Datetime,
        category: StandardSearchAttrCategory::System,
    },
    StandardSearchAttr {
        name: "ExecutionTime",
        attr_type: SearchAttrType::Datetime,
        category: StandardSearchAttrCategory::System,
    },
    StandardSearchAttr {
        name: "CloseTime",
        attr_type: SearchAttrType::Datetime,
        category: StandardSearchAttrCategory::System,
    },
    StandardSearchAttr {
        name: "ExecutionStatus",
        attr_type: SearchAttrType::Keyword,
        category: StandardSearchAttrCategory::System,
    },
    StandardSearchAttr {
        name: "TaskQueue",
        attr_type: SearchAttrType::Keyword,
        category: StandardSearchAttrCategory::System,
    },
    StandardSearchAttr {
        name: "HistoryLength",
        attr_type: SearchAttrType::Int,
        category: StandardSearchAttrCategory::System,
    },
    StandardSearchAttr {
        name: "ExecutionDuration",
        attr_type: SearchAttrType::Int,
        category: StandardSearchAttrCategory::System,
    },
    StandardSearchAttr {
        name: "StateTransitionCount",
        attr_type: SearchAttrType::Int,
        category: StandardSearchAttrCategory::System,
    },
    StandardSearchAttr {
        name: "HistorySizeBytes",
        attr_type: SearchAttrType::Int,
        category: StandardSearchAttrCategory::System,
    },
    StandardSearchAttr {
        name: "ParentWorkflowId",
        attr_type: SearchAttrType::Keyword,
        category: StandardSearchAttrCategory::System,
    },
    StandardSearchAttr {
        name: "ParentRunId",
        attr_type: SearchAttrType::Keyword,
        category: StandardSearchAttrCategory::System,
    },
    StandardSearchAttr {
        name: "RootWorkflowId",
        attr_type: SearchAttrType::Keyword,
        category: StandardSearchAttrCategory::System,
    },
    StandardSearchAttr {
        name: "RootRunId",
        attr_type: SearchAttrType::Keyword,
        category: StandardSearchAttrCategory::System,
    },
    StandardSearchAttr {
        name: "TemporalChangeVersion",
        attr_type: SearchAttrType::KeywordList,
        category: StandardSearchAttrCategory::Predefined,
    },
    StandardSearchAttr {
        name: "BinaryChecksums",
        attr_type: SearchAttrType::KeywordList,
        category: StandardSearchAttrCategory::Predefined,
    },
    StandardSearchAttr {
        name: "BuildIds",
        attr_type: SearchAttrType::KeywordList,
        category: StandardSearchAttrCategory::Predefined,
    },
    StandardSearchAttr {
        name: "BatcherNamespace",
        attr_type: SearchAttrType::Keyword,
        category: StandardSearchAttrCategory::Predefined,
    },
    StandardSearchAttr {
        name: "BatcherUser",
        attr_type: SearchAttrType::Keyword,
        category: StandardSearchAttrCategory::Predefined,
    },
    StandardSearchAttr {
        name: "TemporalScheduledStartTime",
        attr_type: SearchAttrType::Datetime,
        category: StandardSearchAttrCategory::Predefined,
    },
    StandardSearchAttr {
        name: "TemporalScheduledById",
        attr_type: SearchAttrType::Keyword,
        category: StandardSearchAttrCategory::Predefined,
    },
    StandardSearchAttr {
        name: "TemporalSchedulePaused",
        attr_type: SearchAttrType::Bool,
        category: StandardSearchAttrCategory::Predefined,
    },
    StandardSearchAttr {
        name: "TemporalNamespaceDivision",
        attr_type: SearchAttrType::Keyword,
        category: StandardSearchAttrCategory::Predefined,
    },
    StandardSearchAttr {
        name: "TemporalPauseInfo",
        attr_type: SearchAttrType::KeywordList,
        category: StandardSearchAttrCategory::Predefined,
    },
    StandardSearchAttr {
        name: "TemporalReportedProblems",
        attr_type: SearchAttrType::KeywordList,
        category: StandardSearchAttrCategory::Predefined,
    },
    StandardSearchAttr {
        name: "TemporalWorkerDeploymentVersion",
        attr_type: SearchAttrType::Keyword,
        category: StandardSearchAttrCategory::Predefined,
    },
    StandardSearchAttr {
        name: "TemporalWorkflowVersioningBehavior",
        attr_type: SearchAttrType::Keyword,
        category: StandardSearchAttrCategory::Predefined,
    },
    StandardSearchAttr {
        name: "TemporalWorkerDeployment",
        attr_type: SearchAttrType::Keyword,
        category: StandardSearchAttrCategory::Predefined,
    },
    StandardSearchAttr {
        name: "TemporalUsedWorkerDeploymentVersions",
        attr_type: SearchAttrType::KeywordList,
        category: StandardSearchAttrCategory::Predefined,
    },
    StandardSearchAttr {
        name: "TemporalExternalPayloadCount",
        attr_type: SearchAttrType::Int,
        category: StandardSearchAttrCategory::Predefined,
    },
    StandardSearchAttr {
        name: "TemporalExternalPayloadSizeBytes",
        attr_type: SearchAttrType::Int,
        category: StandardSearchAttrCategory::Predefined,
    },
];

/// Seed predefined attributes that are stored in the search-attribute map.
///
/// System attributes are intentionally not registered: the filter compiler
/// resolves them before consulting the custom registry, matching Temporal's
/// split between column-backed `sadefs.System()` fields and map-backed
/// `sadefs.Predefined()` fields (`common/searchattribute/sadefs/constants.go
/// @ v1.31.0`).
pub async fn seed_predefined_search_attributes<S>(
    store: &S,
    namespace_id: NamespaceId,
) -> Result<()>
where
    S: VisibilityStore + ?Sized,
{
    for attr in STANDARD_SEARCH_ATTRIBUTES {
        if attr.category == StandardSearchAttrCategory::Predefined {
            store
                .register_attr(namespace_id, attr.name.to_owned(), attr.attr_type)
                .await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{memory::InMemoryVisibilityStore, store::VisibilityStore};
    use uuid::Uuid;

    #[tokio::test]
    async fn seeding_registers_predefined_but_not_system_attributes() {
        let store = InMemoryVisibilityStore::default();
        let namespace_id = NamespaceId(Uuid::from_u128(1));

        seed_predefined_search_attributes(&store, namespace_id)
            .await
            .expect("seed predefined attributes");

        assert!(
            store
                .resolve_attr(namespace_id, "TemporalChangeVersion")
                .await
                .expect("resolve predefined")
                .is_some()
        );
        assert!(
            store
                .resolve_attr(namespace_id, "WorkflowId")
                .await
                .expect("resolve system")
                .is_none()
        );
    }

    #[test]
    fn standard_catalog_contains_temporal_v1_31_system_and_predefined_keys() {
        let names = STANDARD_SEARCH_ATTRIBUTES
            .iter()
            .map(|attr| attr.name)
            .collect::<std::collections::BTreeSet<_>>();

        for required in [
            "ExecutionTime",
            "ExecutionDuration",
            "HistorySizeBytes",
            "ParentWorkflowId",
            "ParentRunId",
            "RootWorkflowId",
            "RootRunId",
            "TemporalWorkerDeploymentVersion",
            "TemporalExternalPayloadSizeBytes",
        ] {
            assert!(names.contains(required), "missing {required}");
        }
    }
}
