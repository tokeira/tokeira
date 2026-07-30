//! The compose platform's [`HostBridge`] — the host adapter the shared
//! `tokeira-tkd` interpreter dispatches through. It owns the closed [`HostObj`]
//! handle enum, the per-kind constructors (the reflection Rust lacks), and the
//! builder-verb method shims. This is the only module here that both names
//! `crate::builder`/`crate::kinds` *and* the interpreter's value model — the seam
//! that keeps the interpreter itself engine-agnostic (Proposal 004 §11, §19).

use std::{cell::RefCell, rc::Rc};

use tokeira_tkd::{EvalError, FieldMapExt, HostBridge};

use crate::{
    builder::{self, Kind, ModuleRef, Output, ResourceRef, Vol, WbValue},
    context::Cx,
    kinds::{
        DsqlCluster, DsqlMode, DynamoDbTable, LocalStateDir, ObservabilityConfigFiles, Service,
    },
};

/// Compose's runtime value type (the shared `Value` monomorphised over its host).
type Value = tokeira_tkd::Value<HostObj>;
/// Compose's evaluated field map.
type FieldMap = tokeira_tkd::FieldMap<HostObj>;

/// The closed set of opaque author handles compose exposes. Dispatch keys on
/// [`HostKind`], so a receiver-type error is structural, never a runtime downcast.
#[derive(Clone)]
pub enum HostObj {
    Deployment(Rc<RefCell<builder::Deployment>>),
    Module(ModuleRef),
    Resource(ResourceRef),
    Output(Output),
    /// A constructed-but-unplaced kind; consumed once when handed to the builder.
    Kind(Rc<RefCell<Option<HostKindVal>>>),
    /// A logical volume anchor produced by `cx.state/config/docker_sock`.
    Vol(Vol),
    /// The injected runtime context.
    Cx(Rc<Cx>),
}

/// A constructed kind awaiting placement. `Service` is concrete (the builder's
/// `service()` takes `kinds::Service`); every other kind is a `Box<dyn Kind>`
/// placed via `resource_dyn`.
pub enum HostKindVal {
    Boxed(Box<dyn Kind>),
    Service(Service),
}

// Manual impl: `dyn Kind` carries no `Debug`; the variant is the signal.
impl std::fmt::Debug for HostKindVal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HostKindVal::Boxed(_) => f.write_str("HostKindVal::Boxed(..)"),
            HostKindVal::Service(svc) => f.debug_tuple("HostKindVal::Service").field(svc).finish(),
        }
    }
}

/// The dispatch tag for a [`HostObj`] — the method-table key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HostKind {
    Deployment,
    Module,
    Resource,
    Output,
    Kind,
    Vol,
    Cx,
}

impl HostObj {
    pub fn kind(&self) -> HostKind {
        match self {
            HostObj::Deployment(_) => HostKind::Deployment,
            HostObj::Module(_) => HostKind::Module,
            HostObj::Resource(_) => HostKind::Resource,
            HostObj::Output(_) => HostKind::Output,
            HostObj::Kind(_) => HostKind::Kind,
            HostObj::Vol(_) => HostKind::Vol,
            HostObj::Cx(_) => HostKind::Cx,
        }
    }
}

impl std::fmt::Debug for HostObj {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Host::{:?}", self.kind())
    }
}

/// Wrap a boxed kind as a take-once host handle.
fn host_boxed(kind: Box<dyn Kind>) -> HostObj {
    HostObj::Kind(Rc::new(RefCell::new(Some(HostKindVal::Boxed(kind)))))
}

/// Wrap a concrete service as a take-once host handle.
fn host_service(svc: Service) -> HostObj {
    HostObj::Kind(Rc::new(RefCell::new(Some(HostKindVal::Service(svc)))))
}

// ── host-typed coercions (the shim argument surface) ────────────────────────

fn arg<'a>(args: &'a [Value], i: usize, m: &str) -> Result<&'a Value, EvalError> {
    args.get(i)
        .ok_or_else(|| EvalError::new(format!("`{m}`: missing argument {i}")))
}

fn as_host_module(v: &Value) -> Result<ModuleRef, EvalError> {
    match v {
        Value::Host(HostObj::Module(m)) => Ok(m.clone()),
        v => Err(EvalError::new(format!(
            "expected a module handle, got {v:?}"
        ))),
    }
}

/// Consume an unplaced `Box<dyn Kind>` handle (take-once).
fn take_boxed_kind(v: &Value) -> Result<Box<dyn Kind>, EvalError> {
    match v {
        Value::Host(HostObj::Kind(cell)) => match cell.borrow_mut().take() {
            Some(HostKindVal::Boxed(b)) => Ok(b),
            Some(HostKindVal::Service(_)) => {
                Err(EvalError::new("expected a resource kind, got a service"))
            }
            None => Err(EvalError::new("kind handle already consumed")),
        },
        v => Err(EvalError::new(format!("expected a kind handle, got {v:?}"))),
    }
}

/// Consume an unplaced `kinds::Service` handle (take-once).
fn take_service(v: &Value) -> Result<Service, EvalError> {
    match v {
        Value::Host(HostObj::Kind(cell)) => match cell.borrow_mut().take() {
            Some(HostKindVal::Service(s)) => Ok(s),
            Some(HostKindVal::Boxed(_)) => {
                Err(EvalError::new("expected a service, got a resource kind"))
            }
            None => Err(EvalError::new("service handle already consumed")),
        },
        v => Err(EvalError::new(format!(
            "expected a service handle, got {v:?}"
        ))),
    }
}

/// The `volumes: vec![cx.state(..), ..]` field — a `[Vol]` of host anchors.
fn take_vols(f: &mut FieldMap, key: &str) -> Result<Vec<Vol>, EvalError> {
    match f.take(key)? {
        Value::Vec(items) => items
            .into_iter()
            .map(|v| match v {
                Value::Host(HostObj::Vol(vol)) => Ok(vol),
                other => Err(EvalError::new(format!(
                    "field `{key}`: expected [Vol], got element {other:?}"
                ))),
            })
            .collect(),
        v => Err(EvalError::new(format!(
            "field `{key}`: expected an array, got {v:?}"
        ))),
    }
}

// ── concrete kind builders (testable; comparable via the kinds' PartialEq) ──

pub(crate) fn build_dsql_cluster(f: &mut FieldMap) -> Result<DsqlCluster, EvalError> {
    let region = f.take_str("region")?;
    let mode = match f.take_enum("mode", "DsqlMode")?.as_str() {
        "Managed" => DsqlMode::Managed,
        "Preexisting" => DsqlMode::Preexisting,
        other => {
            return Err(EvalError::new(format!(
                "`DsqlMode` has no variant `{other}`"
            )));
        }
    };
    let endpoint = f.take_opt_str("endpoint")?;
    let arn = f.take_opt_str("arn")?;
    f.expect_empty()?;
    Ok(DsqlCluster {
        region,
        mode,
        endpoint,
        arn,
    })
}

pub(crate) fn build_dynamodb_table(f: &mut FieldMap) -> Result<DynamoDbTable, EvalError> {
    let table = f.take_str("table")?;
    let hash_key = f.take_str("hash_key")?;
    let ttl = f.take_opt_str("ttl")?;
    f.expect_empty()?;
    Ok(DynamoDbTable {
        table,
        hash_key,
        ttl,
    })
}

pub(crate) fn build_observability_config_files(
    f: &mut FieldMap,
) -> Result<ObservabilityConfigFiles, EvalError> {
    let r = ObservabilityConfigFiles {
        scrape_host: f.take_str("scrape_host")?,
        scrape_port: f.take_u16("scrape_port")?,
        cluster: f.take_str("cluster")?,
        deployment: f.take_str("deployment")?,
        mimir_remote_write: f.take_str("mimir_remote_write")?,
        loki_push: f.take_str("loki_push")?,
        mimir_http_port: f.take_u16("mimir_http_port")?,
        loki_http_port: f.take_u16("loki_http_port")?,
        retention_hours: f.take_u32("retention_hours")?,
    };
    f.expect_empty()?;
    Ok(r)
}

pub(crate) fn build_local_state_dir(f: &mut FieldMap) -> Result<LocalStateDir, EvalError> {
    f.expect_empty()?;
    Ok(LocalStateDir)
}

pub(crate) fn build_service(f: &mut FieldMap) -> Result<Service, EvalError> {
    let image = f.take_str("image")?;
    let replicas = f.take_u32("replicas")?;
    let publish = f.take_vec_u16("publish")?;
    let volumes = take_vols(f, "volumes")?;
    let env = f.take_pairs("env")?;
    let command = f.take_vec_str("command")?;
    let needs = f.take_vec_str("needs")?;
    let server_config = f.take_bool("server_config")?;
    let aws = f.take_opt_str("aws")?;
    f.expect_empty()?;
    Ok(Service {
        image,
        replicas,
        publish,
        volumes,
        env,
        command,
        needs,
        server_config,
        aws,
    })
}

/// The interpreter image of `kinds::Service::EMPTY` — the `..Service::EMPTY`
/// spread overlays explicit fields onto this. Must stay in lockstep with the real
/// const; a drift surfaces as a missing/leftover field at construction.
pub(crate) fn service_defaults() -> FieldMap {
    FieldMap::from([
        ("image".to_string(), Value::Str(String::new())),
        ("replicas".to_string(), Value::Int(0)),
        ("publish".to_string(), Value::Vec(Vec::new())),
        ("volumes".to_string(), Value::Vec(Vec::new())),
        ("env".to_string(), Value::Vec(Vec::new())),
        ("command".to_string(), Value::Vec(Vec::new())),
        ("needs".to_string(), Value::Vec(Vec::new())),
        ("server_config".to_string(), Value::Bool(false)),
        ("aws".to_string(), Value::Opt(None)),
    ])
}

/// Read a whitelisted field of the injected `Cx` (field syntax `cx.project_name`).
fn cx_field(cx: &Cx, field: &str) -> Result<Value, EvalError> {
    match field {
        "project_name" => Ok(Value::Str(cx.project_name.clone())),
        "region" => Ok(Value::Opt(
            cx.region.clone().map(|s| Box::new(Value::Str(s))),
        )),
        other => Err(EvalError::new(format!(
            "`Cx` has no readable field `{other}`"
        ))),
    }
}

// ── the bridge ──────────────────────────────────────────────────────────────

/// The compose platform's host bridge. Dispatch is direct `match` (the kind set
/// is small and fixed), replacing the compiled-era function-pointer registry.
#[derive(Clone, Default, Debug)]
pub struct ComposeBridge;

impl HostBridge for ComposeBridge {
    type Host = HostObj;
    type Cx = Cx;
    type Output = builder::Deployment;

    fn is_kind(&self, name: &str) -> bool {
        matches!(
            name,
            "DsqlCluster"
                | "DynamoDbTable"
                | "ObservabilityConfigFiles"
                | "LocalStateDir"
                | "Service"
        )
    }

    fn knows_method(&self, name: &str) -> bool {
        matches!(
            name,
            "module"
                | "resource"
                | "service"
                | "writeback"
                | "output"
                | "state"
                | "config"
                | "docker_sock"
        )
    }

    fn knows_assoc(&self, path: &str) -> bool {
        path == "Deployment::new"
    }

    fn kind_defaults(&self, name: &str) -> Option<FieldMap> {
        (name == "Service").then(service_defaults)
    }

    fn construct_kind(
        &self,
        name: &str,
        mut fields: FieldMap,
        _cx: &Cx,
    ) -> Result<HostObj, EvalError> {
        match name {
            "DsqlCluster" => Ok(host_boxed(Box::new(build_dsql_cluster(&mut fields)?))),
            "DynamoDbTable" => Ok(host_boxed(Box::new(build_dynamodb_table(&mut fields)?))),
            "ObservabilityConfigFiles" => Ok(host_boxed(Box::new(
                build_observability_config_files(&mut fields)?,
            ))),
            "LocalStateDir" => Ok(host_boxed(Box::new(build_local_state_dir(&mut fields)?))),
            "Service" => Ok(host_service(build_service(&mut fields)?)),
            other => Err(EvalError::new(format!("unknown kind `{other}`"))),
        }
    }

    fn assoc(&self, path: &str, args: Vec<Value>, _cx: &Cx) -> Result<HostObj, EvalError> {
        match path {
            "Deployment::new" => {
                let ns = arg(&args, 0, "Deployment::new")?.as_str_vec()?;
                let ns_ref: Vec<&str> = ns.iter().map(String::as_str).collect();
                Ok(HostObj::Deployment(Rc::new(RefCell::new(
                    builder::Deployment::new(&ns_ref),
                ))))
            }
            other => Err(EvalError::new(format!("unknown associated fn `{other}`"))),
        }
    }

    fn call_method(
        &self,
        recv: &HostObj,
        method: &str,
        args: Vec<Value>,
        _cx: &Cx,
    ) -> Result<Value, EvalError> {
        match (recv.kind(), method) {
            (HostKind::Deployment, "module") => {
                let HostObj::Deployment(d) = recv else {
                    unreachable!("subset proved Deployment receiver")
                };
                let name = arg(&args, 0, "module")?.as_str()?.to_string();
                let needs = arg(&args, 1, "module")?.as_str_vec()?;
                let needs_ref: Vec<&str> = needs.iter().map(String::as_str).collect();
                let m = d.borrow_mut().module(&name, &needs_ref);
                Ok(Value::Host(HostObj::Module(m)))
            }
            (HostKind::Deployment, "resource") => {
                let HostObj::Deployment(d) = recv else {
                    unreachable!("subset proved Deployment receiver")
                };
                let module = as_host_module(arg(&args, 0, "resource")?)?;
                let id = arg(&args, 1, "resource")?.as_str()?.to_string();
                let kind = take_boxed_kind(arg(&args, 2, "resource")?)?;
                let r = d.borrow_mut().resource_dyn(&module, &id, kind);
                Ok(Value::Host(HostObj::Resource(r)))
            }
            (HostKind::Deployment, "service") => {
                let HostObj::Deployment(d) = recv else {
                    unreachable!("subset proved Deployment receiver")
                };
                let module = as_host_module(arg(&args, 0, "service")?)?;
                let name = arg(&args, 1, "service")?.as_str()?.to_string();
                let svc = take_service(arg(&args, 2, "service")?)?;
                d.borrow_mut().service(&module, &name, svc);
                Ok(Value::Unit)
            }
            (HostKind::Deployment, "writeback") => {
                let HostObj::Deployment(d) = recv else {
                    unreachable!("subset proved Deployment receiver")
                };
                let key = arg(&args, 0, "writeback")?.as_str()?.to_string();
                let wb = match arg(&args, 1, "writeback")? {
                    Value::Str(s) => WbValue::Const(s.clone()),
                    Value::Host(HostObj::Output(o)) => WbValue::Output(o.clone()),
                    other => {
                        return Err(EvalError::new(format!(
                            "writeback value must be a string or a resource output, got {other:?}"
                        )));
                    }
                };
                d.borrow_mut().writeback(&key, wb);
                Ok(Value::Unit)
            }
            (HostKind::Resource, "output") => {
                let HostObj::Resource(r) = recv else {
                    unreachable!("subset proved Resource receiver")
                };
                let name = arg(&args, 0, "output")?.as_str()?;
                Ok(Value::Host(HostObj::Output(r.output(name))))
            }
            (HostKind::Cx, "state") => {
                let sub = arg(&args, 0, "state")?.as_str()?.to_string();
                let at = arg(&args, 1, "state")?.as_str()?.to_string();
                Ok(Value::Host(HostObj::Vol(Vol::State { sub, at })))
            }
            (HostKind::Cx, "config") => {
                let sub = arg(&args, 0, "config")?.as_str()?.to_string();
                let at = arg(&args, 1, "config")?.as_str()?.to_string();
                Ok(Value::Host(HostObj::Vol(Vol::Config { sub, at })))
            }
            (HostKind::Cx, "docker_sock") => Ok(Value::Host(HostObj::Vol(Vol::Raw(
                "/var/run/docker.sock:/var/run/docker.sock".to_string(),
            )))),
            (kind, m) => Err(EvalError::new(format!("`{kind:?}` has no method `{m}`"))),
        }
    }

    fn host_field(&self, host: &HostObj, field: &str) -> Result<Value, EvalError> {
        match host {
            HostObj::Cx(cx) => cx_field(cx, field),
            other => Err(EvalError::new(format!(
                "`{:?}` has no readable field `{field}`",
                other.kind()
            ))),
        }
    }

    fn cx_host(&self, cx: &Cx) -> HostObj {
        HostObj::Cx(Rc::new(cx.clone()))
    }

    fn finish(&self, ret: HostObj) -> Result<builder::Deployment, EvalError> {
        match ret {
            HostObj::Deployment(rc) => Rc::try_unwrap(rc)
                .map(RefCell::into_inner)
                .map_err(|_| EvalError::new("the deployment handle is still referenced at return")),
            other => Err(EvalError::new(format!(
                "deployment() must return a Deployment, got {other:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_tkd::value::{EnumPath, VariantBody};

    fn cx() -> Cx {
        Cx {
            project_name: "tokeira".into(),
            region: Some("us-east-1".into()),
            deployment_dir: "/tmp/deploy".into(),
        }
    }

    fn s(v: &str) -> Value {
        Value::Str(v.into())
    }

    fn enum_v(ty: &str, variant: &str) -> Value {
        Value::Enum {
            path: EnumPath {
                ty: ty.into(),
                segments: vec![ty.into(), variant.into()],
            },
            variant: variant.into(),
            body: VariantBody::Unit,
        }
    }

    #[test]
    fn dsql_cluster_round_trips() {
        let mut f = FieldMap::from([
            ("region".into(), s("eu-west-2")),
            ("mode".into(), enum_v("DsqlMode", "Managed")),
            ("endpoint".into(), Value::Opt(None)),
            ("arn".into(), Value::Opt(None)),
        ]);
        let built = build_dsql_cluster(&mut f).unwrap();
        assert_eq!(
            built,
            DsqlCluster {
                region: "eu-west-2".into(),
                mode: DsqlMode::Managed,
                endpoint: None,
                arn: None,
            }
        );
    }

    #[test]
    fn dsql_cluster_preexisting_carries_endpoint() {
        let mut f = FieldMap::from([
            ("region".into(), s("eu-west-2")),
            ("mode".into(), enum_v("DsqlMode", "Preexisting")),
            ("endpoint".into(), Value::Opt(Some(Box::new(s("x.on.aws"))))),
            ("arn".into(), Value::Opt(Some(Box::new(s("arn:..."))))),
        ]);
        let built = build_dsql_cluster(&mut f).unwrap();
        assert_eq!(built.mode, DsqlMode::Preexisting);
        assert_eq!(built.endpoint.as_deref(), Some("x.on.aws"));
    }

    #[test]
    fn observability_config_files_nine_fields_round_trip() {
        let mut f = FieldMap::from([
            ("scrape_host".into(), s("tokeirad")),
            ("scrape_port".into(), Value::Int(9090)),
            ("cluster".into(), s("tokeira")),
            ("deployment".into(), s("tokeira")),
            (
                "mimir_remote_write".into(),
                s("http://mimir:9009/api/v1/push"),
            ),
            ("loki_push".into(), s("http://loki:3100/loki/api/v1/push")),
            ("mimir_http_port".into(), Value::Int(9009)),
            ("loki_http_port".into(), Value::Int(3100)),
            ("retention_hours".into(), Value::Int(168)),
        ]);
        let built = build_observability_config_files(&mut f).unwrap();
        assert_eq!(
            built,
            ObservabilityConfigFiles {
                scrape_host: "tokeirad".into(),
                scrape_port: 9090,
                cluster: "tokeira".into(),
                deployment: "tokeira".into(),
                mimir_remote_write: "http://mimir:9009/api/v1/push".into(),
                loki_push: "http://loki:3100/loki/api/v1/push".into(),
                mimir_http_port: 9009,
                loki_http_port: 3100,
                retention_hours: 168,
            }
        );
    }

    #[test]
    fn service_empty_overlay_only_sets_listed_fields() {
        // emulate `Service { image, replicas, ..Service::EMPTY }`
        let mut f = service_defaults();
        f.insert("image".into(), s("tokeirad:latest"));
        f.insert("replicas".into(), Value::Int(2));
        let built = build_service(&mut f).unwrap();
        assert_eq!(
            built,
            Service {
                image: "tokeirad:latest".into(),
                replicas: 2,
                ..Service::EMPTY
            }
        );
    }

    #[test]
    fn unknown_field_is_rejected() {
        let mut f = FieldMap::from([
            ("table".into(), s("t")),
            ("hash_key".into(), s("pk")),
            ("ttl".into(), Value::Opt(None)),
            ("typo".into(), s("oops")),
        ]);
        let err = build_dynamodb_table(&mut f).unwrap_err();
        assert!(err.msg.contains("unknown field `typo`"), "{}", err.msg);
    }

    #[test]
    fn deployment_new_and_resource_shim_place_a_kind() {
        let bridge = ComposeBridge;
        let cx = cx();
        let dep = bridge
            .assoc("Deployment::new", vec![Value::Vec(vec![s("default")])], &cx)
            .unwrap();

        // d.module("dsql", &[])
        let module_v = bridge
            .call_method(&dep, "module", vec![s("dsql"), Value::Vec(vec![])], &cx)
            .unwrap();

        // d.resource(&dsql, "cluster", DsqlCluster { .. })
        let kind = bridge
            .construct_kind(
                "DsqlCluster",
                FieldMap::from([
                    ("region".into(), s("eu-west-2")),
                    ("mode".into(), enum_v("DsqlMode", "Managed")),
                    ("endpoint".into(), Value::Opt(None)),
                    ("arn".into(), Value::Opt(None)),
                ]),
                &cx,
            )
            .unwrap();
        bridge
            .call_method(
                &dep,
                "resource",
                vec![module_v, s("cluster"), Value::Host(kind)],
                &cx,
            )
            .unwrap();

        let HostObj::Deployment(d) = &dep else {
            panic!("expected deployment")
        };
        assert_eq!(d.borrow().resource_ids("dsql"), ["cluster"]);
    }

    #[test]
    fn kind_handle_is_take_once() {
        let v = Value::Host(
            build_local_state_dir(&mut FieldMap::new())
                .map(|k| {
                    HostObj::Kind(Rc::new(RefCell::new(Some(HostKindVal::Boxed(Box::new(k))))))
                })
                .unwrap(),
        );
        assert!(take_boxed_kind(&v).is_ok());
        assert!(take_boxed_kind(&v).is_err(), "second take must fail");
    }

    #[test]
    fn cx_methods_build_volumes() {
        let bridge = ComposeBridge;
        let cx = cx();
        let cx_host = bridge.cx_host(&cx);
        let v = bridge
            .call_method(&cx_host, "state", vec![s("mimir"), s("/data")], &cx)
            .unwrap();
        let Value::Host(HostObj::Vol(vol)) = v else {
            panic!("expected a Vol")
        };
        assert_eq!(
            vol,
            Vol::State {
                sub: "mimir".into(),
                at: "/data".into()
            }
        );

        let sock = bridge
            .call_method(&cx_host, "docker_sock", vec![], &cx)
            .unwrap();
        let Value::Host(HostObj::Vol(sock)) = sock else {
            panic!("expected a Vol")
        };
        assert_eq!(
            sock,
            Vol::Raw("/var/run/docker.sock:/var/run/docker.sock".into())
        );
    }

    #[test]
    fn cx_field_reads_whitelisted_only() {
        let bridge = ComposeBridge;
        let cx_host = bridge.cx_host(&cx());
        assert_eq!(
            bridge.host_field(&cx_host, "project_name").unwrap(),
            Value::Str("tokeira".into())
        );
        assert!(bridge.host_field(&cx_host, "deployment_dir").is_err());
    }
}
