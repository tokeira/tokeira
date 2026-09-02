//! Reusable, typed author inputs for ECS-specific resources and services.
//!
//! This module is the ECS implementation crate's kind export, mirroring
//! `tokeira_aws::kinds`: each kind is the authored face of one concrete
//! resource or service owned by this crate. The namespace facts —
//! `NAMESPACE`, `KINDS`, `decode`, [`namespace`] — are what the
//! platform declaration lists for the frontend.

pub mod dsql;
pub mod roles;
pub mod workload;

use tokeira_platform::{
    author::LocatedValue,
    definition::Namespace,
    error::KindError,
    kind::{self, DecodedKind},
};

use crate::{
    modules::dsql::{AdoptedDsqlResource, DsqlIamRoleResource},
    roles::PlatformRoleResource,
    services::EcsWorkload,
};

/// The namespace word: the normalized crate name definitions import from.
pub(crate) const NAMESPACE: &str = "tokeira_ecs";

/// The crate's author-visible kind names, each the word its resource or
/// service owns.
pub(crate) const KINDS: &[&str] = &[
    dsql::ENDPOINT_TYPE,
    dsql::ROLE_TYPE,
    roles::EXECUTION_ROLE_TYPE,
    roles::TASK_ROLE_TYPE,
    workload::TYPE,
    roles::STORAGE_ROLE_TYPE,
];

/// Decode one authored kind of this namespace; `None` when the name is not
/// ours.
pub(crate) fn decode(name: &str, value: LocatedValue) -> Option<Result<DecodedKind, KindError>> {
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
        n if n == roles::TASK_ROLE_TYPE => kind::decode_resource::<
            roles::TaskRole,
            PlatformRoleResource,
        >(roles::TASK_ROLE_TYPE, value),
        n if n == roles::EXECUTION_ROLE_TYPE => kind::decode_resource::<
            roles::ExecutionRole,
            PlatformRoleResource,
        >(roles::EXECUTION_ROLE_TYPE, value),
        n if n == roles::STORAGE_ROLE_TYPE => kind::decode_resource::<
            roles::StorageRole,
            PlatformRoleResource,
        >(roles::STORAGE_ROLE_TYPE, value),
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
                "EcsExecutionRole",
                "EcsTaskRole",
                "EcsWorkload",
                "ObservabilityStorageRole",
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
            mode: dsql::RoleMode::Preexisting(dsql::PreexistingRole {
                role_arn: "arn:aws:iam::1:role/adopted".into(),
            }),
        }
        .realize(&placement(Vec::new()))
        .expect("preexisting role");
        assert_eq!(preexisting.resource_type().0, dsql::ROLE_TYPE);

        let managed = dsql::DsqlIamRole {
            id: "dsql:runtime-role".into(),
            mode: dsql::RoleMode::Managed(dsql::ManagedRole {
                region: "eu-west-2".into(),
                role_name: "tokeira-dsql-runtime".into(),
                policy_name: "dsql-connect".into(),
                action: "dsql:DbConnect".into(),
            }),
        }
        .realize(&placement(vec![tokeira_iac::ResourceId(
            "dsql:cluster".into(),
        )]))
        .expect("managed role");
        assert_eq!(managed.resource_type().0, dsql::ROLE_TYPE);

        let workload = workload::Workload {
            service: "tokeira-runtime".into(),
            environment: "dev".into(),
            region: "eu-west-2".into(),
            cluster: "tokeira".into(),
            image: "tokeirad:latest".into(),
            replicas: None,
            cpu: 1024,
            memory_mb: 2048,
            alloy_image: "grafana/alloy:v1.19.0".into(),
            aws_cli_image: "amazon/aws-cli:2.17.0".into(),
            busybox_image: "busybox:1.36".into(),
        }
        .realize(&placement(vec![
            tokeira_iac::ResourceId("iam-role-demo-tokeira-runtime-task".into()),
            tokeira_iac::ResourceId("demo-vpc".into()),
            tokeira_iac::ResourceId("sg-runtime".into()),
            tokeira_deployment::server_config::resource_id(),
        ]))
        .expect("runtime workload");
        let alloy_init = workload
            .task_definition
            .containers
            .iter()
            .find(|container| container.name == "alloy-config-init")
            .expect("alloy config init");
        assert!(
            alloy_init
                .command
                .join(" ")
                .contains("/demo/alloy/sidecar/tokeira-runtime"),
            "the workload and its Alloy parameter share deployment identity"
        );
        let wait_for = workload
            .task_definition
            .containers
            .iter()
            .find(|container| container.name == "wait-for-tokeira-controller")
            .expect("controller readiness gate");
        assert!(
            wait_for
                .command
                .join(" ")
                .contains("nc -z tokeira-controller 7240"),
            "readiness uses the Service Connect client alias"
        );
        assert_eq!(
            tokeira_deploy_engine::Service::resource_type(&workload),
            workload::TYPE
        );
    }

    // The definition's autoscaler policy must reach the task model used to
    // build the provider manifest.
    #[test]
    fn authored_autoscaler_resources_reach_the_realized_workload() {
        use tokeira_platform::kind::Kind as _;

        let workload = workload::Workload {
            service: "tokeira-autoscaler".into(),
            environment: "dev".into(),
            region: "eu-west-2".into(),
            cluster: "tokeira".into(),
            image: "autoscaler:authored".into(),
            replicas: Some(3),
            cpu: 512,
            memory_mb: 1024,
            alloy_image: "alloy:authored".into(),
            aws_cli_image: "aws-cli:authored".into(),
            busybox_image: "busybox:authored".into(),
        }
        .realize(&placement(vec![
            tokeira_iac::ResourceId("iam-role-demo-tokeira-autoscaler-task".into()),
            tokeira_iac::ResourceId("demo-vpc".into()),
            tokeira_iac::ResourceId("sg-control".into()),
        ]))
        .expect("autoscaler workload");

        assert_eq!(workload.task_definition.cpu, 512);
        assert_eq!(workload.task_definition.memory_mb, 1024);
        assert!(matches!(
            workload.scheduling,
            crate::services::EcsScheduling::Replica { desired_count: 3 }
        ));
        let primary = workload
            .task_definition
            .containers
            .iter()
            .find(|container| container.name == "tokeira-autoscaler")
            .expect("autoscaler primary container");
        assert_eq!(primary.image, "autoscaler:authored");
        assert!(
            workload
                .task_definition
                .containers
                .iter()
                .any(|container| container.name == "alloy" && container.image == "alloy:authored")
        );
        assert!(workload.task_definition.containers.iter().any(|container| {
            container.name == "alloy-config-init" && container.image == "aws-cli:authored"
        }));
        assert!(workload.task_definition.containers.iter().any(|container| {
            container.name.starts_with("wait-for-") && container.image == "busybox:authored"
        }));
    }

    #[test]
    fn invalid_authored_task_resources_refuse_before_manifest_derivation() {
        use tokeira_platform::kind::Kind as _;

        let error = workload::Workload {
            service: "tokeira-autoscaler".into(),
            environment: "dev".into(),
            region: "eu-west-2".into(),
            cluster: "tokeira".into(),
            image: "autoscaler:authored".into(),
            replicas: Some(1),
            cpu: 128,
            memory_mb: 256,
            alloy_image: "alloy:authored".into(),
            aws_cli_image: "aws-cli:authored".into(),
            busybox_image: "busybox:authored".into(),
        }
        .realize(&placement(Vec::new()))
        .expect_err("invalid task totals must be refused before unsigned subtraction");

        assert!(
            error
                .message
                .contains("invalid ECS workload `tokeira-autoscaler`")
        );
        assert!(error.message.contains("invalid ECS cpu/memory pair"));
    }

    // A managed role without its cluster dependency and an unknown workload
    // name both refuse with actionable messages.
    #[test]
    fn refusals_name_the_missing_facts() {
        use tokeira_platform::kind::Kind as _;

        let role_error = dsql::DsqlIamRole {
            id: "dsql:runtime-role".into(),
            mode: dsql::RoleMode::Managed(dsql::ManagedRole {
                region: "eu-west-2".into(),
                role_name: "tokeira-dsql-runtime".into(),
                policy_name: "dsql-connect".into(),
                action: "dsql:DbConnect".into(),
            }),
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
            environment: "dev".into(),
            region: "eu-west-2".into(),
            cluster: "tokeira".into(),
            image: "tokeirad:latest".into(),
            replicas: None,
            cpu: 1024,
            memory_mb: 2048,
            alloy_image: "grafana/alloy:v1.19.0".into(),
            aws_cli_image: "amazon/aws-cli:2.17.0".into(),
            busybox_image: "busybox:1.36".into(),
        }
        .realize(&placement(Vec::new()))
        .expect_err("unknown workload");
        assert!(
            workload_error.message.contains("tokeira-runtime"),
            "the refusal lists the buildable set: {}",
            workload_error.message
        );

        let workload_error = workload::Workload {
            service: "tokeira-runtime".into(),
            environment: "dev".into(),
            region: "eu-west-2".into(),
            cluster: "tokeira".into(),
            image: "tokeirad:latest".into(),
            replicas: None,
            cpu: 1024,
            memory_mb: 2048,
            alloy_image: "grafana/alloy:v1.19.0".into(),
            aws_cli_image: "amazon/aws-cli:2.17.0".into(),
            busybox_image: "busybox:1.36".into(),
        }
        .realize(&placement(Vec::new()))
        .expect_err("no task role declared");
        assert!(
            workload_error.message.contains("EcsTaskRole"),
            "{}",
            workload_error.message
        );
    }
}
