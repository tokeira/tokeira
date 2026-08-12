//! Reusable, typed author inputs for ECS-specific resources and services.
//!
//! This module is the ECS implementation crate's kind export, mirroring
//! `tokeira_aws::kinds`: each kind is the authored face of one concrete
//! resource or service owned by this crate. The namespace facts —
//! [`NAMESPACE`], [`KINDS`], [`decode`], [`namespace`] — are what the
//! platform declaration lists for the frontend.

pub mod dsql;
pub mod remote_state;
pub mod workload;

use tokeira_platform::{
    author::LocatedValue,
    definition::Namespace,
    error::KindError,
    kind::{self, DecodedKind},
};

use crate::{
    modules::{
        dsql::{AdoptedDsqlResource, DsqlIamRoleResource},
        remote_state::RemoteStateBucket as RemoteStateBucketResource,
    },
    services::EcsWorkload,
};

/// The namespace word: the normalized crate name definitions import from.
pub const NAMESPACE: &str = "tokeira_ecs";

/// The crate's author-visible kind names, each the word its resource or
/// service owns.
pub const KINDS: &[&str] = &[
    dsql::ENDPOINT_TYPE,
    dsql::ROLE_TYPE,
    workload::TYPE,
    remote_state::TYPE,
];

/// Decode one authored kind of this namespace; `None` when the name is not
/// ours.
pub fn decode(name: &str, value: LocatedValue) -> Option<Result<DecodedKind, KindError>> {
    Some(match name {
        n if n == dsql::ENDPOINT_TYPE => kind::decode_resource::<
            dsql::AdoptedDsqlEndpoint,
            AdoptedDsqlResource,
        >(dsql::ENDPOINT_TYPE, value),
        n if n == dsql::ROLE_TYPE => {
            kind::decode_resource::<dsql::DsqlIamRole, DsqlIamRoleResource>(dsql::ROLE_TYPE, value)
        }
        n if n == workload::TYPE => {
            kind::decode_service::<workload::Workload, EcsWorkload>(workload::TYPE, value)
        }
        n if n == remote_state::TYPE => kind::decode_resource::<
            remote_state::RemoteStateBucket,
            RemoteStateBucketResource,
        >(remote_state::TYPE, value),
        _ => return None,
    })
}

/// The assembled namespace for platform declarations.
pub fn namespace() -> Namespace {
    Namespace {
        name: NAMESPACE,
        kinds: KINDS,
        defaults: None,
        decode,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tokeira_platform::{author::LocatedValue, kind::PlacementContext};

    use super::*;

    // The namespace facts hold together: every listed name decodes here and
    // an unknown name is refused as not-ours rather than an error.
    #[test]
    fn every_listed_kind_decodes_and_unknown_names_refuse() {
        assert_eq!(
            KINDS,
            [
                "DsqlEndpoint",
                "DsqlIamRole",
                "EcsWorkload",
                "RemoteStateBucket"
            ]
        );
        for name in KINDS {
            let probe = decode(
                name,
                LocatedValue::new(tokeira_platform::author::ValueShape::Struct {
                    name: (*name).to_string(),
                    fields: Vec::new(),
                }),
            )
            .unwrap_or_else(|| panic!("kind `{name}` must belong to {NAMESPACE}"));
            if let Err(error) = probe {
                assert!(
                    !error.message.contains("unknown"),
                    "entry `{name}` failed as unknown: {}",
                    error.message
                );
            }
        }
        assert!(
            decode(
                "NotAnEcsKind",
                LocatedValue::new(tokeira_platform::author::ValueShape::Struct {
                    name: "NotAnEcsKind".to_string(),
                    fields: Vec::new(),
                }),
            )
            .is_none()
        );
    }

    fn placement(dependencies: Vec<tokeira_iac::ResourceId>) -> PlacementContext {
        PlacementContext {
            deployment_id: "demo".to_string(),
            deployment_dir: std::path::PathBuf::from("/tmp/demo"),
            definition_dir: std::path::PathBuf::from("/tmp/demo"),
            module: "dsql".to_string(),
            logical_id: "probe".to_string(),
            dependencies,
            dependency_content: BTreeMap::new(),
            tags: BTreeMap::new(),
        }
    }

    // Twin TYPE consts agree with the realized types, across both role
    // modes and the workload service twin.
    #[test]
    fn twin_type_consts_match_realized_types() {
        use tokeira_iac::Resource as _;
        use tokeira_platform::kind::Kind as _;

        let endpoint = dsql::AdoptedDsqlEndpoint {
            id: "dsql:management-endpoint".into(),
            endpoint_id: "vpce-123".into(),
        }
        .realize(&placement(Vec::new()))
        .expect("adopted endpoint");
        assert_eq!(endpoint.resource_type().0, dsql::ENDPOINT_TYPE);

        let preexisting = dsql::DsqlIamRole {
            id: "dsql:runtime-role".into(),
            mode: dsql::RoleMode::Preexisting {
                role_arn: "arn:aws:iam::1:role/adopted".into(),
            },
        }
        .realize(&placement(Vec::new()))
        .expect("preexisting role");
        assert_eq!(preexisting.resource_type().0, dsql::ROLE_TYPE);

        let managed = dsql::DsqlIamRole {
            id: "dsql:runtime-role".into(),
            mode: dsql::RoleMode::Managed {
                region: "eu-west-2".into(),
                role_name: "tokeira-dsql-runtime".into(),
                policy_name: "dsql-connect".into(),
                action: "dsql:DbConnect".into(),
            },
        }
        .realize(&placement(vec![tokeira_iac::ResourceId(
            "dsql:cluster".into(),
        )]))
        .expect("managed role");
        assert_eq!(managed.resource_type().0, dsql::ROLE_TYPE);

        let bucket = remote_state::RemoteStateBucket {
            region: "eu-west-2".into(),
            bucket: "demo-state-eu-west-2".into(),
            key_prefix: Some("demo/dev".into()),
        }
        .realize(&placement(Vec::new()))
        .expect("remote state bucket");
        assert_eq!(bucket.resource_type().0, remote_state::TYPE);

        let workload = workload::Workload {
            service: "tokeira-runtime".into(),
            config: crate::EcsConfig::default(),
        }
        .realize(&placement(Vec::new()))
        .expect("runtime workload");
        assert_eq!(
            tokeira_deploy_engine::Service::resource_type(&workload),
            workload::TYPE
        );
    }

    // A managed role without its cluster dependency and an unknown workload
    // name both refuse with actionable messages.
    #[test]
    fn refusals_name_the_missing_facts() {
        use tokeira_platform::kind::Kind as _;

        let role_error = dsql::DsqlIamRole {
            id: "dsql:runtime-role".into(),
            mode: dsql::RoleMode::Managed {
                region: "eu-west-2".into(),
                role_name: "tokeira-dsql-runtime".into(),
                policy_name: "dsql-connect".into(),
                action: "dsql:DbConnect".into(),
            },
        }
        .realize(&placement(Vec::new()))
        .expect_err("no cluster declared");
        assert!(
            role_error.message.contains("cluster"),
            "{}",
            role_error.message
        );

        let workload_error = workload::Workload {
            service: "tokeira-unknown".into(),
            config: crate::EcsConfig::default(),
        }
        .realize(&placement(Vec::new()))
        .expect_err("unknown workload");
        assert!(
            workload_error.message.contains("tokeira-runtime"),
            "the refusal lists the buildable set: {}",
            workload_error.message
        );
    }
}
