//! Kubernetes manifest builders for tokeira's pod shape.
//!
//! Each builder constructs a `k8s-openapi` typed object and lowers it to a
//! `serde_json::Value` via a small `to_manifest` helper, which is the form the K8s-object
//! kinds hand to `KubePlatform` for server-side apply. Building typed structs
//! (rather than hand-writing JSON) keeps the manifests schema-checked at compile
//! time; the round-trip to `Value` is lossless (Property 9).
//!
//! The shape is the proven source pod shape (Alloy native sidecar, arm64
//! affinity, pod anti-affinity, Pod-Identity ServiceAccount) adapted to tokeira:
//!
//! - **Config, not env.** `tokeirad`/`tokeira-controller`/`tokeira-autoscaler`
//!   each read all configuration — including the entire DSQL contract — from a
//!   TOML file located by `--config` (`apps/tokeirad/src/lib.rs`,
//!   `apps/tokeira-controller`, `apps/tokeira-autoscaler`). So the pod mounts a
//!   config ConfigMap and passes `--config <path>`; the DSQL endpoint arrives in
//!   that file via writeback (Req 6.2/9), never as `TEMPORAL_SQL_*` env.
//! - **Per-pod broadcast address.** Only `tokeirad` advertises a membership
//!   endpoint; it gets `TOKEIRA_NODE_HOST` from the downward API (`status.podIP`),
//!   consumed by the `tokeira-config` env override (Req 6.1). A shared ConfigMap
//!   cannot carry a per-pod host, which is exactly why that override exists.
//! - **No headless Service.** Membership is controller-based (a node registers
//!   with `tokeira-controller` over gRPC), not gossip, so no peer-discovery
//!   headless Service is needed (design → membership).

use std::collections::BTreeMap;

use k8s_openapi::{
    Resource,
    api::{
        apps::v1::{Deployment, DeploymentSpec},
        core::v1::{
            Affinity, ConfigMap, ConfigMapVolumeSource, Container, ContainerPort, EnvVar,
            EnvVarSource, NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
            ObjectFieldSelector, PodAffinityTerm, PodAntiAffinity, PodSpec, PodTemplateSpec,
            ResourceRequirements, Service, ServiceAccount, ServicePort, ServiceSpec, Volume,
            VolumeMount, WeightedPodAffinityTerm,
        },
    },
    apimachinery::pkg::{
        api::resource::Quantity,
        apis::meta::v1::{LabelSelector, ObjectMeta},
        util::intstr::IntOrString,
    },
};
use tokeira_k8s::standard_labels;

/// Directory the config ConfigMap is mounted at; the config file sits directly
/// under it (e.g. `/etc/tokeira/tokeirad.toml`).
const CONFIG_MOUNT_DIR: &str = "/etc/tokeira";
/// Mount path for the Alloy sidecar's rendered config.
const ALLOY_MOUNT_DIR: &str = "/etc/alloy";

/// The input to the per-service manifest builders: everything that varies across
/// the three tokeira binaries (`tokeirad`, `tokeira-controller`,
/// `tokeira-autoscaler`) plus the observability services. The pod *shape* is
/// identical; only these fields differ.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceManifest {
    /// Kubernetes object name and `app` selector value, e.g. `tokeirad`.
    pub(crate) name: String,
    /// Namespace all the service's objects live in.
    pub(crate) namespace: String,
    /// Project name (label `app.kubernetes.io/part-of`).
    pub(crate) project: String,
    /// Container image reference.
    pub(crate) image: String,
    /// Desired replica count.
    pub(crate) replicas: u32,
    /// CPU request/limit as a Kubernetes quantity (e.g. `500m`, `2`).
    pub(crate) cpu: String,
    /// Memory request/limit as a Kubernetes quantity (e.g. `1Gi`).
    pub(crate) memory: String,
    /// gRPC port, if the service exposes one (`tokeira-autoscaler` does not).
    pub(crate) grpc_port: Option<u16>,
    /// Prometheus metrics port (all services expose one).
    pub(crate) metrics_port: u16,
    /// Pod-Identity ServiceAccount name the pod runs as.
    pub(crate) service_account: String,
    /// Alloy sidecar image.
    pub(crate) alloy_image: String,
    /// Name of the ConfigMap holding this service's config file.
    pub(crate) config_map: String,
    /// The config file's key within the ConfigMap (e.g. `tokeirad.toml`), also
    /// its filename under the config mount directory (`/etc/tokeira`). Passed to the binary as
    /// `--config <mount_dir>/<config_file>`.
    pub(crate) config_file: String,
    /// Whether the pod must advertise its own IP as the membership address.
    /// Only `tokeirad` (a runtime node) does; the controller is reached via its
    /// Service and the autoscaler has no inbound endpoint.
    pub(crate) advertise_node_host: bool,
}

/// The `app` selector for a service — matched by its Deployment, Service, and
/// pod-template label. Also the pod-log/port-forward selector (`standard_labels`).
fn app_selector(name: &str) -> BTreeMap<String, String> {
    BTreeMap::from([("app".to_string(), name.to_string())])
}

/// A single-quantity request=limit map (guaranteed QoS: requests == limits).
fn resource_quantities(cpu: &str, memory: &str) -> BTreeMap<String, Quantity> {
    BTreeMap::from([
        ("cpu".to_string(), Quantity(cpu.to_string())),
        ("memory".to_string(), Quantity(memory.to_string())),
    ])
}

/// A literal env var.
fn env(name: &str, value: &str) -> EnvVar {
    EnvVar {
        name: name.to_string(),
        value: Some(value.to_string()),
        value_from: None,
    }
}

/// A downward-API env var sourced from a pod field (e.g. `status.podIP`).
fn env_field_ref(name: &str, field_path: &str) -> EnvVar {
    EnvVar {
        name: name.to_string(),
        value: None,
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector {
                field_path: field_path.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }),
    }
}

/// The Alloy native sidecar: a Kubernetes 1.33 native sidecar, i.e. an init
/// container with `restartPolicy: Always` so it starts before and runs for the
/// lifetime of the main container (Req 6.1; Property 13). It reads its rendered
/// config from the `alloy-config` ConfigMap mounted read-only.
fn alloy_sidecar(spec: &ServiceManifest) -> Container {
    Container {
        name: "alloy".to_string(),
        image: Some(spec.alloy_image.clone()),
        // The `Always` restart policy on an init container is precisely what makes
        // it a native sidecar rather than a run-once init step — do not drop it.
        restart_policy: Some("Always".to_string()),
        args: Some(vec![
            "run".to_string(),
            format!("{ALLOY_MOUNT_DIR}/config.alloy"),
            "--stability.level=generally-available".to_string(),
        ]),
        volume_mounts: Some(vec![VolumeMount {
            name: "alloy-config".to_string(),
            mount_path: ALLOY_MOUNT_DIR.to_string(),
            read_only: Some(true),
            ..Default::default()
        }]),
        resources: Some(ResourceRequirements {
            requests: Some(resource_quantities("128m", "256Mi")),
            limits: Some(resource_quantities("128m", "256Mi")),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// arm64 node affinity + pod anti-affinity by `app` label — schedule on Graviton
/// and spread replicas across nodes. arm64 is `required` (the images are arm64);
/// the spread is `preferred` (best-effort, so a single-node dev cluster still
/// schedules).
fn affinity(name: &str) -> Affinity {
    Affinity {
        node_affinity: Some(NodeAffinity {
            required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                node_selector_terms: vec![NodeSelectorTerm {
                    match_expressions: Some(vec![NodeSelectorRequirement {
                        key: "kubernetes.io/arch".to_string(),
                        operator: "In".to_string(),
                        values: Some(vec!["arm64".to_string()]),
                    }]),
                    ..Default::default()
                }],
            }),
            ..Default::default()
        }),
        pod_anti_affinity: Some(PodAntiAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                WeightedPodAffinityTerm {
                    weight: 100,
                    pod_affinity_term: PodAffinityTerm {
                        label_selector: Some(LabelSelector {
                            match_labels: Some(app_selector(name)),
                            ..Default::default()
                        }),
                        topology_key: "kubernetes.io/hostname".to_string(),
                        ..Default::default()
                    },
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The main-container env: the config path plus, for a runtime node, the
/// downward-API broadcast address. Everything else (DSQL, ports, pool) is in the
/// mounted config file, so this list is deliberately tiny.
fn main_env(spec: &ServiceManifest) -> Vec<EnvVar> {
    let config_path = format!("{CONFIG_MOUNT_DIR}/{}", spec.config_file);
    let mut env_vars = vec![env("TOKEIRA_CONFIG", &config_path)];
    if spec.advertise_node_host {
        // `POD_IP` is informational; `TOKEIRA_NODE_HOST` is the value the
        // `tokeira-config` env override consumes so this pod advertises its own
        // reachable membership address (Req 6.1 / the N1 hook). Without it every
        // pod sharing the ConfigMap would advertise the same static host.
        env_vars.push(env_field_ref("POD_IP", "status.podIP"));
        env_vars.push(env_field_ref("TOKEIRA_NODE_HOST", "status.podIP"));
    }
    env_vars
}

/// Container ports: metrics always, gRPC when the service exposes one.
fn container_ports(spec: &ServiceManifest) -> Vec<ContainerPort> {
    let mut ports = vec![ContainerPort {
        name: Some("metrics".to_string()),
        container_port: i32::from(spec.metrics_port),
        ..Default::default()
    }];
    if let Some(grpc) = spec.grpc_port {
        ports.push(ContainerPort {
            name: Some("grpc".to_string()),
            container_port: i32::from(grpc),
            ..Default::default()
        });
    }
    ports
}

/// Build a service's `Deployment`: main container (`--config <mounted file>`) +
/// the Alloy native sidecar, arm64/anti-affinity, the Pod-Identity ServiceAccount,
/// and the config + alloy-config ConfigMap volumes.
pub(crate) fn deployment(spec: &ServiceManifest) -> serde_json::Value {
    let labels = standard_labels(&spec.name, &spec.project);
    let config_path = format!("{CONFIG_MOUNT_DIR}/{}", spec.config_file);

    let main = Container {
        name: spec.name.clone(),
        image: Some(spec.image.clone()),
        // The image entrypoint is the tokeira binary; every binary locates its
        // config via `--config <path>` (`tokeirad` clap; controller/autoscaler
        // `config_path_from_args`). This is the uniform, ground-truthed mechanism.
        args: Some(vec!["--config".to_string(), config_path]),
        ports: Some(container_ports(spec)),
        env: Some(main_env(spec)),
        resources: Some(ResourceRequirements {
            requests: Some(resource_quantities(&spec.cpu, &spec.memory)),
            limits: Some(resource_quantities(&spec.cpu, &spec.memory)),
            ..Default::default()
        }),
        volume_mounts: Some(vec![VolumeMount {
            name: "tokeira-config".to_string(),
            mount_path: CONFIG_MOUNT_DIR.to_string(),
            read_only: Some(true),
            ..Default::default()
        }]),
        ..Default::default()
    };

    let pod_spec = PodSpec {
        service_account_name: Some(spec.service_account.clone()),
        // Native sidecar: Alloy runs as an init container with restartPolicy Always.
        init_containers: Some(vec![alloy_sidecar(spec)]),
        containers: vec![main],
        affinity: Some(affinity(&spec.name)),
        volumes: Some(vec![
            Volume {
                name: "tokeira-config".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: spec.config_map.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Volume {
                name: "alloy-config".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: format!("alloy-config-{}", spec.name),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ]),
        ..Default::default()
    };

    let deployment = Deployment {
        metadata: ObjectMeta {
            name: Some(spec.name.clone()),
            namespace: Some(spec.namespace.clone()),
            labels: Some(labels.clone()),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(i32::from(u16::try_from(spec.replicas).unwrap_or(u16::MAX))),
            selector: LabelSelector {
                match_labels: Some(app_selector(&spec.name)),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    ..Default::default()
                }),
                spec: Some(pod_spec),
            },
            ..Default::default()
        }),
        ..Default::default()
    };

    to_manifest(&deployment)
}

/// Build a service's ClusterIP `Service`: gRPC (if any) + metrics, topology-aware
/// routing. No `LoadBalancer`/headless variant (private-only; controller-based
/// membership).
pub(crate) fn service(spec: &ServiceManifest) -> serde_json::Value {
    let labels = standard_labels(&spec.name, &spec.project);
    let mut ports = vec![ServicePort {
        name: Some("metrics".to_string()),
        port: i32::from(spec.metrics_port),
        target_port: Some(IntOrString::Int(i32::from(spec.metrics_port))),
        ..Default::default()
    }];
    if let Some(grpc) = spec.grpc_port {
        ports.push(ServicePort {
            name: Some("grpc".to_string()),
            port: i32::from(grpc),
            target_port: Some(IntOrString::Int(i32::from(grpc))),
            ..Default::default()
        });
    }

    let service = Service {
        metadata: ObjectMeta {
            name: Some(spec.name.clone()),
            namespace: Some(spec.namespace.clone()),
            labels: Some(labels),
            annotations: Some(BTreeMap::from([(
                "service.kubernetes.io/topology-mode".to_string(),
                "Auto".to_string(),
            )])),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".to_string()),
            ports: Some(ports),
            selector: Some(app_selector(&spec.name)),
            ..Default::default()
        }),
        ..Default::default()
    };

    to_manifest(&service)
}

/// Build the Pod-Identity `ServiceAccount` a service's pods run as.
pub(crate) fn service_account(name: &str, namespace: &str, project: &str) -> serde_json::Value {
    let sa = ServiceAccount {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some(standard_labels(name, project)),
            ..Default::default()
        },
        ..Default::default()
    };
    to_manifest(&sa)
}

/// Build a `ConfigMap` holding one config file (`data[file_name] = content`).
///
/// This is the vehicle for the server config: the hydrated `tokeirad.toml` (DSQL
/// endpoint filled by writeback) is the `content`, mounted at
/// `/etc/tokeira/<file_name>` and passed via `--config` (Req 6.2/9).
pub(crate) fn config_map(
    name: &str,
    namespace: &str,
    project: &str,
    file_name: &str,
    content: &str,
) -> serde_json::Value {
    let cm = ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some(standard_labels(name, project)),
            ..Default::default()
        },
        data: Some(BTreeMap::from([(
            file_name.to_string(),
            content.to_string(),
        )])),
        ..Default::default()
    };
    to_manifest(&cm)
}

/// Build the Karpenter `NodePool` (arm64/on-demand → EKS Auto Mode default
/// NodeClass). Delegates to the shared `tokeira-k8s` helper so the shape is
/// single-sourced (Property 13).
pub(crate) fn node_pool(node_families: &[String]) -> serde_json::Value {
    tokeira_k8s::build_node_pool(node_families)
}

/// Serialize a `k8s-openapi` object to a manifest `Value`, injecting the
/// `apiVersion`/`kind` that `k8s-openapi` keeps as `Resource` associated
/// constants rather than serialized fields. `KubePlatform`'s dynamic apply reads
/// `apiVersion`/`kind` from the value to route it, so they must be present.
fn to_manifest<T>(obj: &T) -> serde_json::Value
where
    T: serde::Serialize + Resource,
{
    // `to_value` is infallible for these well-formed structs (no non-string map
    // keys); a failure would be a bug in the object we just built.
    let mut value = serde_json::to_value(obj).expect("k8s-openapi object serializes to JSON");
    if let serde_json::Value::Object(map) = &mut value {
        map.insert(
            "apiVersion".to_string(),
            serde_json::Value::String(T::API_VERSION.to_string()),
        );
        map.insert(
            "kind".to_string(),
            serde_json::Value::String(T::KIND.to_string()),
        );
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokeirad_spec() -> ServiceManifest {
        ServiceManifest {
            name: "tokeirad".into(),
            namespace: "tokeira-system".into(),
            project: "tokeira".into(),
            image: "tokeirad:latest".into(),
            replicas: 3,
            cpu: "2".into(),
            memory: "4Gi".into(),
            grpc_port: Some(7233),
            metrics_port: 9090,
            service_account: "tokeirad".into(),
            alloy_image: "grafana/alloy:v1.16.0".into(),
            config_map: "tokeirad-config".into(),
            config_file: "tokeirad.toml".into(),
            advertise_node_host: true,
        }
    }

    #[test]
    fn deployment_carries_apiversion_kind_and_pod_shape() {
        let d = deployment(&tokeirad_spec());
        assert_eq!(d["apiVersion"], "apps/v1");
        assert_eq!(d["kind"], "Deployment");
        assert_eq!(d["spec"]["replicas"], 3);

        let pod = &d["spec"]["template"]["spec"];
        assert_eq!(pod["serviceAccountName"], "tokeirad");
        // The binary locates its config via `--config <mounted file>`.
        assert_eq!(
            pod["containers"][0]["args"],
            serde_json::json!(["--config", "/etc/tokeira/tokeirad.toml"])
        );

        // Alloy is a native sidecar: an init container with restartPolicy Always.
        let alloy = &pod["initContainers"][0];
        assert_eq!(alloy["name"], "alloy");
        assert_eq!(alloy["restartPolicy"], "Always");
    }

    #[test]
    fn tokeirad_advertises_pod_ip_via_downward_api() {
        let d = deployment(&tokeirad_spec());
        let env = d["spec"]["template"]["spec"]["containers"][0]["env"]
            .as_array()
            .expect("env array");
        let node_host = env
            .iter()
            .find(|e| e["name"] == "TOKEIRA_NODE_HOST")
            .expect("TOKEIRA_NODE_HOST env present");
        assert_eq!(
            node_host["valueFrom"]["fieldRef"]["fieldPath"],
            "status.podIP"
        );
    }

    #[test]
    fn autoscaler_has_no_grpc_port_and_no_node_host() {
        let spec = ServiceManifest {
            name: "tokeira-autoscaler".into(),
            grpc_port: None,
            advertise_node_host: false,
            config_file: "autoscaler.toml".into(),
            config_map: "tokeira-autoscaler-config".into(),
            service_account: "tokeira-autoscaler".into(),
            ..tokeirad_spec()
        };
        let d = deployment(&spec);
        let env = d["spec"]["template"]["spec"]["containers"][0]["env"]
            .as_array()
            .expect("env array");
        assert!(env.iter().all(|e| e["name"] != "TOKEIRA_NODE_HOST"));

        let svc = service(&spec);
        let ports = svc["spec"]["ports"].as_array().expect("ports");
        assert!(ports.iter().all(|p| p["name"] != "grpc"));
        assert_eq!(svc["spec"]["type"], "ClusterIP");
    }

    #[test]
    fn arm64_affinity_is_required() {
        let d = deployment(&tokeirad_spec());
        let terms = &d["spec"]["template"]["spec"]["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]
            ["nodeSelectorTerms"][0]["matchExpressions"][0];
        assert_eq!(terms["key"], "kubernetes.io/arch");
        assert_eq!(terms["values"], serde_json::json!(["arm64"]));
    }

    #[test]
    fn service_is_clusterip_never_loadbalancer() {
        let svc = service(&tokeirad_spec());
        assert_eq!(svc["kind"], "Service");
        assert_eq!(svc["spec"]["type"], "ClusterIP");
        assert_ne!(svc["spec"]["type"], "LoadBalancer");
    }

    #[test]
    fn config_map_holds_the_config_file() {
        let cm = config_map(
            "tokeirad-config",
            "tokeira-system",
            "tokeira",
            "tokeirad.toml",
            "[infrastructure]\nstorage = \"dsql\"\n",
        );
        assert_eq!(cm["apiVersion"], "v1");
        assert_eq!(cm["kind"], "ConfigMap");
        assert!(
            cm["data"]["tokeirad.toml"]
                .as_str()
                .unwrap()
                .contains("storage = \"dsql\"")
        );
    }

    // Property 9 (manifest round-trip): every generated manifest round-trips
    // through serde_json losslessly.
    // Feature: platform-eks, Property 9
    #[test]
    fn manifests_round_trip_through_serde_json() {
        let spec = tokeirad_spec();
        for manifest in [
            deployment(&spec),
            service(&spec),
            service_account("tokeirad", "tokeira-system", "tokeira"),
            config_map(
                "tokeirad-config",
                "tokeira-system",
                "tokeira",
                "tokeirad.toml",
                "x=1",
            ),
            node_pool(&["m8g".into(), "c8g".into(), "r8g".into()]),
        ] {
            let text = serde_json::to_string(&manifest).expect("serialize");
            let back: serde_json::Value = serde_json::from_str(&text).expect("deserialize");
            assert_eq!(manifest, back);
        }
    }

    // Property 13 (topology currency): the NodePool is `karpenter.sh/v1` referencing
    // the `eks.amazonaws.com` default NodeClass, with Graviton4 node families; the
    // Alloy sidecar is a native sidecar (asserted in
    // `deployment_carries_apiversion_kind_and_pod_shape`).
    // Feature: platform-eks, Property 13
    #[test]
    fn node_pool_is_current_graviton_auto_mode() {
        let np = node_pool(&["m8g".into(), "c8g".into(), "r8g".into()]);
        assert_eq!(np["apiVersion"], "karpenter.sh/v1");
        assert_eq!(np["kind"], "NodePool");
        let node_class = &np["spec"]["template"]["spec"]["nodeClassRef"];
        assert_eq!(node_class["group"], "eks.amazonaws.com");
        assert_eq!(node_class["name"], "default");
        let reqs = np["spec"]["template"]["spec"]["requirements"]
            .as_array()
            .expect("requirements array");
        let families = reqs
            .iter()
            .find(|r| r["key"] == "node.kubernetes.io/instance-type")
            .expect("instance-type requirement");
        assert_eq!(families["values"], serde_json::json!(["m8g", "c8g", "r8g"]));
    }
}
