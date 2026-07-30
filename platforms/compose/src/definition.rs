//! The OPERATOR's deployment definition — compiled Rust for now (the `syn`-
//! interpreted `.tkd` form is the deferred foundation). It may name ONLY the
//! author's vocabulary ([`crate::builder`] + [`crate::kinds`]), nothing of the
//! engine directly — and nothing non-hermetic: no paths, no `std::env`, no
//! filesystem probes, no loops. Those mechanics live author-side now
//! (`crate::kinds::Service::to_compose_service` + the [`Cx`] helpers).
//!
//! `config()` is the operator surface; `deployment(cfg, cx)` is the structure.
//! Both reproduce `platforms/compose/src` faithfully.

use crate::{
    builder::Deployment,
    context::Cx,
    kinds::{
        DsqlCluster, DsqlMode as KindMode, DynamoDbTable, LocalStateDir, ObservabilityConfigFiles,
        Service,
    },
};

// ── Config: the operator surface ────────────────────────────────────────────

/// DSQL cluster lifecycle. Flat (003 §6): `Storage::Dsql` carries `endpoint`/`arn`
/// directly, so `Preexisting` is a plain marker.
#[derive(Debug)]
pub enum DsqlMode {
    Managed,
    Preexisting,
}

/// Storage backend — a create-time choice. `Dsql` carries the cluster identity
/// (region) and, for an adopted cluster, the `endpoint`/`arn` directly.
#[derive(Debug)]
pub enum Storage {
    InMemory,
    Dsql {
        region: String,
        mode: DsqlMode,
        endpoint: Option<String>,
        arn: Option<String>,
    },
}

#[derive(Debug)]
pub struct Tokeirad {
    pub image: String,
    pub replicas: u32,
    pub grpc_port: u16,
    pub metrics_port: u16,
}

#[derive(Debug)]
pub struct Backend {
    pub image: String,
    pub replicas: u32,
}

#[derive(Debug)]
pub struct Grafana {
    pub image: String,
    pub replicas: u32,
    pub port: u16,
    pub admin_password: String,
}

#[derive(Debug)]
pub struct Observability {
    pub mimir: Backend,
    pub loki: Backend,
    pub grafana: Grafana,
    pub alloy: Backend,
}

/// The operator surface — what an operator edits.
#[derive(Debug)]
pub struct Compose {
    pub storage: Storage,
    pub tokeirad: Tokeirad,
    pub observability: Observability,
}

/// The default config the operator edits (defaults mirror `ComposeConfig`).
pub fn config() -> Compose {
    Compose {
        storage: Storage::InMemory,
        tokeirad: Tokeirad {
            image: "tokeirad:latest".into(),
            replicas: 1,
            grpc_port: 7233,
            metrics_port: 9090,
        },
        observability: Observability {
            mimir: Backend {
                image: "grafana/mimir:3.0.6".into(),
                replicas: 1,
            },
            loki: Backend {
                image: "grafana/loki:3.7.1".into(),
                replicas: 1,
            },
            grafana: Grafana {
                image: "grafana/grafana-oss:12.4.3".into(),
                replicas: 1,
                port: 3000,
                admin_password: "admin".into(),
            },
            alloy: Backend {
                image: "grafana/alloy:v1.16.0".into(),
                replicas: 1,
            },
        },
    }
}

// ── Structure: the deployment ───────────────────────────────────────────────

/// Map the operator's config DSQL mode to the author kind's mode.
fn kind_mode(mode: &DsqlMode) -> KindMode {
    match mode {
        DsqlMode::Managed => KindMode::Managed,
        DsqlMode::Preexisting => KindMode::Preexisting,
    }
}

/// Build the deployment from config + context. Hermetic: only struct literals,
/// `vec!`, `if let`/`match`, `format!`, `cx.*` anchors, and the builder verbs —
/// no paths, env, or I/O (those moved author-side).
pub fn deployment(cfg: &Compose, cx: &Cx) -> Deployment {
    let mut d = Deployment::new(&["default"]);

    // bootstrap state
    let local_state = d.module("local_state", &[]);
    d.resource(&local_state, "dir", LocalStateDir);

    // dsql — only under persistent storage
    if let Storage::Dsql {
        region,
        mode,
        endpoint,
        arn,
    } = &cfg.storage
    {
        let dsql = d.module("dsql", &["local_state"]);
        let cluster = d.resource(
            &dsql,
            "cluster",
            DsqlCluster {
                region: region.clone(),
                mode: kind_mode(mode),
                endpoint: endpoint.clone(),
                arn: arn.clone(),
            },
        );
        let rate_limiter = d.resource(
            &dsql,
            "rate_limiter",
            DynamoDbTable {
                table: format!("{}-dsql-rate-limiter", cx.project_name),
                hash_key: "pk".into(),
                ttl: Some("ttl_epoch".into()),
            },
        );
        let conn_lease = d.resource(
            &dsql,
            "conn_lease",
            DynamoDbTable {
                table: format!("{}-dsql-conn-lease", cx.project_name),
                hash_key: "pk".into(),
                ttl: Some("ttl_epoch".into()),
            },
        );

        // writeback into the server config (collect_writeback analog)
        d.writeback("infrastructure.storage", "dsql");
        d.writeback(
            "infrastructure.dsql.endpoint",
            cluster.output("cluster_endpoint"),
        );
        d.writeback("infrastructure.dsql.region", region.clone());
        d.writeback(
            "infrastructure.dsql.rate_limiter_table",
            rate_limiter.output("table_name"),
        );
        d.writeback(
            "infrastructure.dsql.conn_lease_table",
            conn_lease.output("table_name"),
        );
    }

    // observability — config files + the four backend services
    let observability = d.module("observability", &["local_state"]);
    let o = &cfg.observability;
    d.resource(
        &observability,
        "config_files",
        ObservabilityConfigFiles {
            scrape_host: "tokeirad".into(),
            scrape_port: cfg.tokeirad.metrics_port,
            cluster: cx.project_name.clone(),
            deployment: cx.project_name.clone(),
            mimir_remote_write: "http://mimir:9009/api/v1/push".into(),
            loki_push: "http://loki:3100/loki/api/v1/push".into(),
            mimir_http_port: 9009,
            loki_http_port: 3100,
            retention_hours: 168,
        },
    );

    d.service(
        &observability,
        "mimir",
        Service {
            image: o.mimir.image.clone(),
            replicas: o.mimir.replicas,
            publish: vec![9009],
            volumes: vec![
                cx.state("mimir", "/data"),
                cx.config("mimir.yaml", "/etc/mimir/mimir.yaml"),
                cx.config("mimir/rules", "/data/mimir/rules"),
            ],
            command: vec!["--config.file=/etc/mimir/mimir.yaml".into()],
            ..Service::EMPTY
        },
    );
    d.service(
        &observability,
        "loki",
        Service {
            image: o.loki.image.clone(),
            replicas: o.loki.replicas,
            publish: vec![3100],
            volumes: vec![
                cx.state("loki", "/loki"),
                cx.config("loki.yaml", "/etc/loki/loki.yaml"),
            ],
            command: vec!["--config.file=/etc/loki/loki.yaml".into()],
            ..Service::EMPTY
        },
    );
    d.service(
        &observability,
        "grafana",
        Service {
            image: o.grafana.image.clone(),
            replicas: o.grafana.replicas,
            publish: vec![o.grafana.port],
            volumes: vec![
                cx.state("grafana", "/var/lib/grafana"),
                cx.config("grafana/provisioning", "/etc/grafana/provisioning/"),
                cx.config("grafana/dashboards", "/var/lib/grafana/dashboards/"),
            ],
            env: vec![
                ("GF_SECURITY_ADMIN_USER".into(), "admin".into()),
                (
                    "GF_SECURITY_ADMIN_PASSWORD".into(),
                    o.grafana.admin_password.clone(),
                ),
                ("GF_METRICS_ENABLED".into(), "true".into()),
            ],
            needs: vec!["mimir".into(), "loki".into()],
            ..Service::EMPTY
        },
    );
    d.service(
        &observability,
        "alloy",
        Service {
            image: o.alloy.image.clone(),
            replicas: o.alloy.replicas,
            publish: vec![4317, 4318],
            volumes: vec![
                cx.docker_sock(),
                cx.config("alloy.alloy", "/etc/alloy/config.alloy"),
            ],
            command: vec!["run".into(), "/etc/alloy/config.alloy".into()],
            needs: vec!["tokeirad".into(), "mimir".into(), "loki".into()],
            ..Service::EMPTY
        },
    );

    // runtime — tokeirad. The conditional server-config mount and the DSQL AWS
    // edge are author mechanics now: the operator only declares intent
    // (`server_config: true`, `aws: <region under DSQL>`).
    let runtime = d.module("runtime", &["local_state"]);
    d.service(
        &runtime,
        "tokeirad",
        Service {
            image: cfg.tokeirad.image.clone(),
            replicas: cfg.tokeirad.replicas,
            publish: vec![cfg.tokeirad.grpc_port, cfg.tokeirad.metrics_port],
            server_config: true,
            aws: match &cfg.storage {
                Storage::Dsql { region, .. } => Some(region.clone()),
                _ => None,
            },
            ..Service::EMPTY
        },
    );

    d
}
