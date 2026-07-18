//! `tokeira-compose-syn` — the rust-via-`syn` compose deployment playground.
//!
//! Goal: experience the **author / operator divide** (Proposal 003), with full
//! fidelity to the hand-written `tokeira_compose_deployment::ComposeDeployment`.
//!
//! - **AUTHOR** (the Rust in this crate): the builder vocabulary ([`builder`]) +
//!   the kinds ([`kinds`]), wired *directly* to the engine constructs. No
//!   `Composition` IR, no `KindLibrary`, no `Realizer` trait.
//! - **OPERATOR** ([`definition`]): `config()` + `deployment(cfg, cx)` that may
//!   name *only* the author's vocabulary. Compiled as ordinary Rust for now;
//!   `syn`-interpreting that same file is the deferred foundation.

pub mod adapter;
pub mod bridge;
pub mod builder;
pub mod context;
pub mod definition;
pub mod interp;
pub mod kinds;
pub mod observability_config;
pub mod provisioner;

/// The default `.tkd` deployment definition this platform ships. `tkr deployment
/// create` writes it into the deployment directory as the config revision the
/// bound provisioner interprets; an operator edits it and re-applies.
pub const DEFAULT_TKD: &str = include_str!("../definition.tkd");

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{
        builder::{Deployment, Vol, WbValue},
        context::Cx,
        definition::{DsqlMode as CfgMode, Storage, config, deployment},
        kinds::{DsqlCluster, DsqlMode, DynamoDbTable, Service},
    };

    fn cx() -> Cx {
        Cx {
            project_name: "tokeira".into(),
            region: Some("eu-west-2".into()),
            deployment_dir: PathBuf::from("/tmp/deploy"),
        }
    }

    #[test]
    fn dsql_module_realizes_to_aws_resources() {
        let mut d = Deployment::new(&["default"]);
        let dsql = d.module("dsql", &["local_state"]);
        let cluster = d.resource(
            &dsql,
            "cluster",
            DsqlCluster {
                region: "eu-west-2".into(),
                mode: DsqlMode::Managed,
                endpoint: None,
                arn: None,
            },
        );
        d.resource(
            &dsql,
            "rate_limiter",
            DynamoDbTable {
                table: "tokeira-dsql-rate-limiter".into(),
                hash_key: "pk".into(),
                ttl: Some("ttl_epoch".into()),
            },
        );

        assert_eq!(
            d.module_deps("dsql"),
            Some(&["local_state".to_string()][..])
        );
        assert_eq!(d.resource_ids("dsql"), ["cluster", "rate_limiter"]);
        assert_eq!(cluster.output("endpoint").resource, "cluster");

        let resources = d.realize_module("dsql", &cx()).expect("dsql module");
        assert_eq!(resources.len(), 2);
        for r in &resources {
            assert!(!r.resource_id().0.is_empty());
        }
    }

    #[test]
    fn service_realizes_to_resource_and_workload() {
        let mut d = Deployment::new(&["default"]);
        let obs = d.module("observability", &["local_state"]);
        d.service(
            &obs,
            "mimir",
            Service {
                image: "grafana/mimir:3.0.6".into(),
                replicas: 1,
                publish: vec![9009],
                command: vec!["--config.file=/etc/mimir/mimir.yaml".into()],
                ..Service::EMPTY
            },
        );
        d.service(
            &obs,
            "grafana",
            Service {
                image: "grafana/grafana-oss:12.4.3".into(),
                replicas: 1,
                publish: vec![3000],
                env: vec![("GF_SECURITY_ADMIN_USER".into(), "admin".into())],
                needs: vec!["mimir".into()],
                ..Service::EMPTY
            },
        );

        let resources = d.realize_module("observability", &cx()).expect("module");
        assert_eq!(resources.len(), 2);
        assert_eq!(d.service_names(), ["mimir", "grafana"]);

        let workloads = d.realize_workloads(&cx());
        let names: Vec<&str> = workloads.iter().map(|w| w.name()).collect();
        assert_eq!(names, ["mimir", "grafana"]);
        let grafana = workloads.iter().find(|w| w.name() == "grafana").unwrap();
        assert_eq!(grafana.module(), "observability");
        assert_eq!(grafana.dependencies(), ["mimir"]);
    }

    #[test]
    fn config_uses_flat_storage() {
        let c = config();
        assert!(matches!(c.storage, Storage::InMemory));
        assert_eq!(c.tokeirad.grpc_port, 7233);

        // `Preexisting` carries endpoint/arn directly on the variant (flat, 003 §6).
        let pre = Storage::Dsql {
            region: "eu-west-2".into(),
            mode: CfgMode::Preexisting,
            endpoint: Some("x.dsql.eu-west-2.on.aws".into()),
            arn: Some("arn:aws:dsql:...".into()),
        };
        let Storage::Dsql {
            mode: CfgMode::Preexisting,
            endpoint: Some(endpoint),
            ..
        } = pre
        else {
            panic!("expected preexisting dsql with an endpoint");
        };
        assert_eq!(endpoint, "x.dsql.eu-west-2.on.aws");
    }

    #[test]
    fn deployment_builds_the_compose_shape() {
        let d = deployment(&config(), &cx()); // InMemory default

        assert_eq!(d.namespaces(), ["default"]);
        let mut services = d.service_names();
        services.sort();
        assert_eq!(services, ["alloy", "grafana", "loki", "mimir", "tokeirad"]);

        // InMemory → no dsql module and no writeback
        assert!(!d.module_names().contains(&"dsql"));
        assert!(d.writeback_entries().is_empty());

        // grafana's deploy deps are name-based
        let workloads = d.realize_workloads(&cx());
        let grafana = workloads.iter().find(|w| w.name() == "grafana").unwrap();
        assert_eq!(grafana.dependencies(), ["mimir", "loki"]);
    }

    #[test]
    fn dsql_deployment_adds_module_and_writeback() {
        let mut cfg = config();
        cfg.storage = Storage::Dsql {
            region: "eu-west-2".into(),
            mode: CfgMode::Managed,
            endpoint: None,
            arn: None,
        };
        let d = deployment(&cfg, &cx());

        assert!(d.module_names().contains(&"dsql"));
        assert_eq!(
            d.resource_ids("dsql"),
            ["cluster", "rate_limiter", "conn_lease"]
        );

        let keys: Vec<&str> = d
            .writeback_entries()
            .iter()
            .map(|(k, _)| k.as_str())
            .collect();
        assert_eq!(
            keys,
            [
                "infrastructure.storage",
                "infrastructure.dsql.endpoint",
                "infrastructure.dsql.region",
                "infrastructure.dsql.rate_limiter_table",
                "infrastructure.dsql.conn_lease_table",
            ]
        );
        // storage is a literal; endpoint is a resource output
        assert!(matches!(d.writeback_entries()[0].1, WbValue::Const(ref s) if s == "dsql"));
        assert!(matches!(d.writeback_entries()[1].1, WbValue::Output(_)));
    }

    #[test]
    fn to_compose_service_orders_base_then_server_config_then_aws() {
        // The relocated mechanics build volumes in a load-bearing order: declared
        // (base) volumes first, then the server-config mount, then the AWS edge.
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("tokeirad.toml"), "").unwrap();
        let cx = Cx {
            project_name: "p".into(),
            region: None,
            deployment_dir: tmp.path().to_path_buf(),
        };
        let svc = Service {
            image: "img".into(),
            replicas: 1,
            publish: vec![1],
            volumes: vec![Vol::State {
                sub: "base".into(),
                at: "/base".into(),
            }],
            server_config: true,
            aws: Some("eu-west-2".into()),
            ..Service::EMPTY
        };
        let cs = svc.to_compose_service("tokeirad", &cx);

        assert!(
            cs.volumes[0].ends_with(":/base"),
            "base first: {:?}",
            cs.volumes
        );
        assert!(
            cs.volumes[1].contains("tokeirad.toml"),
            "server_config second: {:?}",
            cs.volumes
        );
        assert!(
            cs.volumes[2].contains("/.aws:"),
            "aws last: {:?}",
            cs.volumes
        );
        assert_eq!(
            cs.environment["TOKEIRA_CONFIG"],
            "/etc/tokeira/tokeirad.toml"
        );
        assert_eq!(cs.environment["AWS_REGION"], "eu-west-2");
    }
}
