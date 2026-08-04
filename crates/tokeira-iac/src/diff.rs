//! Change computation: compare desired resources against persisted state.

use crate::{InternalChange, Resource, ResourceId, document::InfraState};

/// Compare desired resources against actual state and produce a list of changes.
///
/// - Resources in `desired` but not in `state` → Create
/// - Resources in both → delegate to `resource.diff()` (Update or NoChange)
/// - Resources in `state` but not in `desired` → Delete
pub fn compute_changes(
    desired: &[&dyn Resource],
    state: &InfraState,
    ctx: &crate::ProvisionContext,
) -> Vec<InternalChange> {
    let mut changes = Vec::new();
    let desired_ids: std::collections::HashSet<ResourceId> =
        desired.iter().map(|r| r.resource_id()).collect();

    for resource in desired {
        let rid = resource.resource_id();
        match state.resources.get(&rid) {
            Some(current) => changes.push(resource.diff(current, ctx)),
            None => changes.push(InternalChange::Create {
                resource_id: rid,
                resource_type: resource.resource_type(),
            }),
        }
    }

    for (rid, rs) in &state.resources {
        if !desired_ids.contains(rid) {
            changes.push(InternalChange::Delete {
                resource_id: rid.clone(),
                resource_type: rs.resource_type.clone(),
            });
        }
    }

    changes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProvisionContext, ResourceState, ResourceType, error::IacError};

    fn make_ctx() -> ProvisionContext {
        ProvisionContext::new("test", std::collections::HashMap::new())
    }

    struct FakeResource {
        id: ResourceId,
        rtype: ResourceType,
        deps: Vec<ResourceId>,
        update_detail: Option<String>,
    }

    #[async_trait::async_trait]
    impl crate::Resource for FakeResource {
        fn change_semantics(&self, _ctx: &crate::SemanticsContext<'_>) -> crate::ChangeSemantics {
            crate::ChangeSemantics::default()
        }
        fn resource_type(&self) -> ResourceType {
            self.rtype.clone()
        }
        fn resource_id(&self) -> ResourceId {
            self.id.clone()
        }
        fn dependencies(&self) -> Vec<ResourceId> {
            self.deps.clone()
        }
        fn module(&self) -> &str {
            "foundation"
        }
        async fn create(&self, _ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
            unimplemented!()
        }
        async fn update(
            &self,
            _current: &ResourceState,
            _ctx: &ProvisionContext,
        ) -> Result<ResourceState, IacError> {
            unimplemented!()
        }
        async fn delete(
            &self,
            _current: &ResourceState,
            _ctx: &ProvisionContext,
        ) -> Result<(), IacError> {
            unimplemented!()
        }
        async fn describe(
            &self,
            _ctx: &ProvisionContext,
        ) -> Result<crate::DescribeResult, IacError> {
            unimplemented!()
        }
        fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
            match &self.update_detail {
                Some(detail) => InternalChange::Update {
                    resource_id: self.id.clone(),
                    resource_type: self.rtype.clone(),
                    details: vec![crate::FieldDiff::observation(detail.clone())],
                },
                None => InternalChange::NoChange {
                    resource_id: self.id.clone(),
                },
            }
        }
    }

    fn make_state_entry(rid: &str, rtype: ResourceType) -> (ResourceId, ResourceState) {
        (
            ResourceId(rid.into()),
            ResourceState {
                resource_type: rtype,
                physical_id: format!("phys-{rid}"),
                properties: serde_json::json!({}),
                dependencies: vec![],
                created_at: "2025-01-01T00:00:00Z".into(),
                updated_at: "2025-01-01T00:00:00Z".into(),
                module: "foundation".into(),
            },
        )
    }

    #[test]
    fn detects_create_for_new_resources() {
        let state = InfraState::default();
        let r = FakeResource {
            id: ResourceId("vpc-main".into()),
            rtype: ResourceType::new("Vpc"),
            deps: vec![],
            update_detail: None,
        };
        let desired: Vec<&dyn crate::Resource> = vec![&r];
        let changes = compute_changes(&desired, &state, &make_ctx());
        assert_eq!(changes.len(), 1);
        assert!(
            matches!(&changes[0], InternalChange::Create { resource_id, .. } if resource_id.0 == "vpc-main")
        );
    }

    #[test]
    fn detects_no_change_for_unchanged_resources() {
        let mut state = InfraState::default();
        let (rid, rs) = make_state_entry("vpc-main", ResourceType::new("Vpc"));
        state.resources.insert(rid, rs);

        let r = FakeResource {
            id: ResourceId("vpc-main".into()),
            rtype: ResourceType::new("Vpc"),
            deps: vec![],
            update_detail: None,
        };
        let desired: Vec<&dyn crate::Resource> = vec![&r];
        let changes = compute_changes(&desired, &state, &make_ctx());
        assert_eq!(changes.len(), 1);
        assert!(matches!(&changes[0], InternalChange::NoChange { .. }));
    }

    #[test]
    fn detects_update_for_changed_resources() {
        let mut state = InfraState::default();
        let (rid, rs) = make_state_entry("sg-main", ResourceType::new("SecurityGroup"));
        state.resources.insert(rid, rs);

        let r = FakeResource {
            id: ResourceId("sg-main".into()),
            rtype: ResourceType::new("SecurityGroup"),
            deps: vec![],
            update_detail: Some("rules changed".into()),
        };
        let desired: Vec<&dyn crate::Resource> = vec![&r];
        let changes = compute_changes(&desired, &state, &make_ctx());
        assert_eq!(changes.len(), 1);
        assert!(
            matches!(&changes[0], InternalChange::Update { details, .. } if details[0].field == "rules changed")
        );
    }

    #[test]
    fn detects_delete_for_removed_resources() {
        let mut state = InfraState::default();
        let (rid, rs) = make_state_entry("old-bucket", ResourceType::new("S3Bucket"));
        state.resources.insert(rid, rs);

        let desired: Vec<&dyn crate::Resource> = vec![];
        let changes = compute_changes(&desired, &state, &make_ctx());
        assert_eq!(changes.len(), 1);
        assert!(
            matches!(&changes[0], InternalChange::Delete { resource_id, .. } if resource_id.0 == "old-bucket")
        );
    }

    #[test]
    fn mixed_create_update_delete_no_change() {
        let mut state = InfraState::default();
        for (id, rtype) in [
            ("vpc-main", ResourceType::new("Vpc")),
            ("sg-main", ResourceType::new("SecurityGroup")),
            ("old-bucket", ResourceType::new("S3Bucket")),
        ] {
            let (rid, rs) = make_state_entry(id, rtype);
            state.resources.insert(rid, rs);
        }

        let vpc = FakeResource {
            id: ResourceId("vpc-main".into()),
            rtype: ResourceType::new("Vpc"),
            deps: vec![],
            update_detail: None,
        };
        let sg = FakeResource {
            id: ResourceId("sg-main".into()),
            rtype: ResourceType::new("SecurityGroup"),
            deps: vec![],
            update_detail: Some("new rule".into()),
        };
        let eks = FakeResource {
            id: ResourceId("eks-cluster".into()),
            rtype: ResourceType::new("EksCluster"),
            deps: vec![],
            update_detail: None,
        };

        let desired: Vec<&dyn crate::Resource> = vec![&vpc, &sg, &eks];
        let changes = compute_changes(&desired, &state, &make_ctx());
        assert_eq!(changes.len(), 4);
        assert_eq!(
            changes
                .iter()
                .filter(|c| matches!(c, InternalChange::Create { .. }))
                .count(),
            1
        );
        assert_eq!(
            changes
                .iter()
                .filter(|c| matches!(c, InternalChange::Update { .. }))
                .count(),
            1
        );
        assert_eq!(
            changes
                .iter()
                .filter(|c| matches!(c, InternalChange::Delete { .. }))
                .count(),
            1
        );
        assert_eq!(
            changes
                .iter()
                .filter(|c| matches!(c, InternalChange::NoChange { .. }))
                .count(),
            1
        );
    }
}
