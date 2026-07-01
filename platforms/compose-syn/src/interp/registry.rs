//! The host bridge — the reflection Rust lacks, written once per author type.
//!
//! Three name-keyed tables map `.tkd` syntax onto real author code: **kind ctors**
//! turn a struct-literal field map into a `Box<dyn Kind>`/`kinds::Service`;
//! **methods** dispatch the builder verbs (`d.module`/`.resource`/`.service`/
//! `.writeback`, `r.output`, `cx.state`/`config`/`docker_sock`) onto the real
//! objects; **assoc** covers `Deployment::new`. This module + [`super::value`] are
//! the only interpreter modules that name `crate::builder`/`crate::kinds`.

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
};

use crate::{
    builder::{self, Vol, WbValue},
    context::Cx,
    kinds::{
        DsqlCluster, DsqlMode, DynamoDbTable, LocalStateDir, ObservabilityConfigFiles, Service,
    },
};

use super::value::{
    EvalError, FieldMap, FieldMapExt, HostKind, HostObj, Value, host_boxed, host_service,
};

// ── concrete kind builders (testable; comparable via the kinds' PartialEq) ──

pub fn build_dsql_cluster(f: &mut FieldMap, _cx: &Cx) -> Result<DsqlCluster, EvalError> {
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

pub fn build_dynamodb_table(f: &mut FieldMap, _cx: &Cx) -> Result<DynamoDbTable, EvalError> {
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

pub fn build_observability_config_files(
    f: &mut FieldMap,
    _cx: &Cx,
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

pub fn build_local_state_dir(f: &mut FieldMap, _cx: &Cx) -> Result<LocalStateDir, EvalError> {
    f.expect_empty()?;
    Ok(LocalStateDir)
}

pub fn build_service(f: &mut FieldMap, _cx: &Cx) -> Result<Service, EvalError> {
    let image = f.take_str("image")?;
    let replicas = f.take_u32("replicas")?;
    let publish = f.take_vec_u16("publish")?;
    let volumes = f.take_vols("volumes")?;
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
pub fn service_defaults() -> FieldMap {
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

// ── ctor wrappers (FieldMap -> HostObj) ─────────────────────────────────────

fn ctor_dsql_cluster(mut f: FieldMap, cx: &Cx) -> Result<HostObj, EvalError> {
    Ok(host_boxed(Box::new(build_dsql_cluster(&mut f, cx)?)))
}
fn ctor_dynamodb_table(mut f: FieldMap, cx: &Cx) -> Result<HostObj, EvalError> {
    Ok(host_boxed(Box::new(build_dynamodb_table(&mut f, cx)?)))
}
fn ctor_observability_config_files(mut f: FieldMap, cx: &Cx) -> Result<HostObj, EvalError> {
    Ok(host_boxed(Box::new(build_observability_config_files(
        &mut f, cx,
    )?)))
}
fn ctor_local_state_dir(mut f: FieldMap, cx: &Cx) -> Result<HostObj, EvalError> {
    Ok(host_boxed(Box::new(build_local_state_dir(&mut f, cx)?)))
}
fn ctor_service(mut f: FieldMap, cx: &Cx) -> Result<HostObj, EvalError> {
    Ok(host_service(build_service(&mut f, cx)?))
}

// ── method shims ────────────────────────────────────────────────────────────

fn arg<'a>(args: &'a [Value], i: usize, m: &str) -> Result<&'a Value, EvalError> {
    args.get(i)
        .ok_or_else(|| EvalError::new(format!("`{m}`: missing argument {i}")))
}

fn m_deployment_module(recv: &HostObj, args: Vec<Value>, _cx: &Cx) -> Result<Value, EvalError> {
    let HostObj::Deployment(d) = recv else {
        unreachable!("subset proved Deployment receiver")
    };
    let name = arg(&args, 0, "module")?.as_str()?.to_string();
    let needs = arg(&args, 1, "module")?.as_str_vec()?;
    let needs_ref: Vec<&str> = needs.iter().map(String::as_str).collect();
    let m = d.borrow_mut().module(&name, &needs_ref);
    Ok(Value::Host(HostObj::Module(m)))
}

fn m_deployment_resource(recv: &HostObj, args: Vec<Value>, _cx: &Cx) -> Result<Value, EvalError> {
    let HostObj::Deployment(d) = recv else {
        unreachable!("subset proved Deployment receiver")
    };
    let module = arg(&args, 0, "resource")?.as_host_module()?;
    let id = arg(&args, 1, "resource")?.as_str()?.to_string();
    let kind = arg(&args, 2, "resource")?.take_boxed_kind()?;
    let r = d.borrow_mut().resource_dyn(&module, &id, kind);
    Ok(Value::Host(HostObj::Resource(r)))
}

fn m_deployment_service(recv: &HostObj, args: Vec<Value>, _cx: &Cx) -> Result<Value, EvalError> {
    let HostObj::Deployment(d) = recv else {
        unreachable!("subset proved Deployment receiver")
    };
    let module = arg(&args, 0, "service")?.as_host_module()?;
    let name = arg(&args, 1, "service")?.as_str()?.to_string();
    let svc = arg(&args, 2, "service")?.take_service()?;
    d.borrow_mut().service(&module, &name, svc);
    Ok(Value::Unit)
}

fn m_deployment_writeback(recv: &HostObj, args: Vec<Value>, _cx: &Cx) -> Result<Value, EvalError> {
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

fn m_resource_output(recv: &HostObj, args: Vec<Value>, _cx: &Cx) -> Result<Value, EvalError> {
    let HostObj::Resource(r) = recv else {
        unreachable!("subset proved Resource receiver")
    };
    let name = arg(&args, 0, "output")?.as_str()?;
    Ok(Value::Host(HostObj::Output(r.output(name))))
}

fn m_cx_state(_recv: &HostObj, args: Vec<Value>, _cx: &Cx) -> Result<Value, EvalError> {
    let sub = arg(&args, 0, "state")?.as_str()?.to_string();
    let at = arg(&args, 1, "state")?.as_str()?.to_string();
    Ok(Value::Host(HostObj::Vol(Vol::State { sub, at })))
}

fn m_cx_config(_recv: &HostObj, args: Vec<Value>, _cx: &Cx) -> Result<Value, EvalError> {
    let sub = arg(&args, 0, "config")?.as_str()?.to_string();
    let at = arg(&args, 1, "config")?.as_str()?.to_string();
    Ok(Value::Host(HostObj::Vol(Vol::Config { sub, at })))
}

fn m_cx_docker_sock(_recv: &HostObj, _args: Vec<Value>, _cx: &Cx) -> Result<Value, EvalError> {
    Ok(Value::Host(HostObj::Vol(Vol::Raw(
        "/var/run/docker.sock:/var/run/docker.sock".to_string(),
    ))))
}

fn a_deployment_new(args: Vec<Value>, _cx: &Cx) -> Result<HostObj, EvalError> {
    let ns = arg(&args, 0, "Deployment::new")?.as_str_vec()?;
    let ns_ref: Vec<&str> = ns.iter().map(String::as_str).collect();
    Ok(HostObj::Deployment(Rc::new(RefCell::new(
        builder::Deployment::new(&ns_ref),
    ))))
}

/// Read a whitelisted field of the injected `Cx` (field syntax `cx.project_name`).
pub fn cx_field(cx: &Cx, field: &str) -> Result<Value, EvalError> {
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

// ── the registry ────────────────────────────────────────────────────────────

type Ctor = fn(FieldMap, &Cx) -> Result<HostObj, EvalError>;
type Defaults = fn() -> FieldMap;
type Method = fn(&HostObj, Vec<Value>, &Cx) -> Result<Value, EvalError>;
type Assoc = fn(Vec<Value>, &Cx) -> Result<HostObj, EvalError>;

pub struct Registry {
    kinds: HashMap<&'static str, Ctor>,
    defaults: HashMap<&'static str, Defaults>,
    methods: HashMap<(HostKind, &'static str), Method>,
    assoc: HashMap<&'static str, Assoc>,
    method_names: HashSet<&'static str>,
}

impl Registry {
    /// The compose platform's host vocabulary.
    pub fn compose() -> Self {
        let mut kinds: HashMap<&'static str, Ctor> = HashMap::new();
        kinds.insert("DsqlCluster", ctor_dsql_cluster);
        kinds.insert("DynamoDbTable", ctor_dynamodb_table);
        kinds.insert("ObservabilityConfigFiles", ctor_observability_config_files);
        kinds.insert("LocalStateDir", ctor_local_state_dir);
        kinds.insert("Service", ctor_service);

        let mut defaults: HashMap<&'static str, Defaults> = HashMap::new();
        defaults.insert("Service", service_defaults);

        let mut methods: HashMap<(HostKind, &'static str), Method> = HashMap::new();
        methods.insert((HostKind::Deployment, "module"), m_deployment_module);
        methods.insert((HostKind::Deployment, "resource"), m_deployment_resource);
        methods.insert((HostKind::Deployment, "service"), m_deployment_service);
        methods.insert((HostKind::Deployment, "writeback"), m_deployment_writeback);
        methods.insert((HostKind::Resource, "output"), m_resource_output);
        methods.insert((HostKind::Cx, "state"), m_cx_state);
        methods.insert((HostKind::Cx, "config"), m_cx_config);
        methods.insert((HostKind::Cx, "docker_sock"), m_cx_docker_sock);

        let mut assoc: HashMap<&'static str, Assoc> = HashMap::new();
        assoc.insert("Deployment::new", a_deployment_new);

        let method_names = methods.keys().map(|(_, n)| *n).collect();

        Self {
            kinds,
            defaults,
            methods,
            assoc,
            method_names,
        }
    }

    pub fn is_kind(&self, name: &str) -> bool {
        self.kinds.contains_key(name)
    }

    pub fn kind_defaults(&self, name: &str) -> Option<FieldMap> {
        self.defaults.get(name).map(|f| f())
    }

    /// Construct a kind from its evaluated field map.
    pub fn construct_kind(
        &self,
        name: &str,
        fields: FieldMap,
        cx: &Cx,
    ) -> Result<HostObj, EvalError> {
        let ctor = self
            .kinds
            .get(name)
            .ok_or_else(|| EvalError::new(format!("unknown kind `{name}`")))?;
        ctor(fields, cx)
    }

    pub fn method(&self, kind: HostKind, name: &str) -> Option<Method> {
        self.methods.get(&(kind, name)).copied()
    }

    pub fn assoc(&self, path: &str) -> Option<Assoc> {
        self.assoc.get(path).copied()
    }

    /// Is `name` a method any host type exposes? (check-time validation)
    pub fn knows_method(&self, name: &str) -> bool {
        self.method_names.contains(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interp::value::{EnumPath, VariantBody};

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
        let built = build_dsql_cluster(&mut f, &cx()).unwrap();
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
        let built = build_dsql_cluster(&mut f, &cx()).unwrap();
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
        let built = build_observability_config_files(&mut f, &cx()).unwrap();
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
        let built = build_service(&mut f, &cx()).unwrap();
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
    fn service_with_vol_and_env_round_trips() {
        let mut f = service_defaults();
        f.insert("image".into(), s("grafana/mimir:3.0.6"));
        f.insert("replicas".into(), Value::Int(1));
        f.insert("publish".into(), Value::Vec(vec![Value::Int(9009)]));
        f.insert(
            "volumes".into(),
            Value::Vec(vec![Value::Host(HostObj::Vol(Vol::State {
                sub: "mimir".into(),
                at: "/data".into(),
            }))]),
        );
        f.insert(
            "env".into(),
            Value::Vec(vec![Value::Tuple(vec![s("K"), s("V")])]),
        );
        f.insert("command".into(), Value::Vec(vec![s("--config.file=/x")]));
        let built = build_service(&mut f, &cx()).unwrap();
        assert_eq!(built.image, "grafana/mimir:3.0.6");
        assert_eq!(built.publish, [9009]);
        assert_eq!(
            built.volumes,
            [Vol::State {
                sub: "mimir".into(),
                at: "/data".into()
            }]
        );
        assert_eq!(built.env, [("K".to_string(), "V".to_string())]);
    }

    #[test]
    fn unknown_field_is_rejected() {
        let mut f = FieldMap::from([
            ("table".into(), s("t")),
            ("hash_key".into(), s("pk")),
            ("ttl".into(), Value::Opt(None)),
            ("typo".into(), s("oops")),
        ]);
        let err = build_dynamodb_table(&mut f, &cx()).unwrap_err();
        assert!(err.msg.contains("unknown field `typo`"), "{}", err.msg);
    }

    #[test]
    fn deployment_new_and_resource_shim_place_a_kind() {
        let reg = Registry::compose();
        let cx = cx();
        let dep = a_deployment_new(vec![Value::Vec(vec![s("default")])], &cx).unwrap();

        // d.module("dsql", &[])
        let module_v = reg.method(HostKind::Deployment, "module").unwrap()(
            &dep,
            vec![s("dsql"), Value::Vec(vec![])],
            &cx,
        )
        .unwrap();

        // d.resource(&dsql, "cluster", DsqlCluster { .. })
        let kind = ctor_dsql_cluster(
            FieldMap::from([
                ("region".into(), s("eu-west-2")),
                ("mode".into(), enum_v("DsqlMode", "Managed")),
                ("endpoint".into(), Value::Opt(None)),
                ("arn".into(), Value::Opt(None)),
            ]),
            &cx,
        )
        .unwrap();
        reg.method(HostKind::Deployment, "resource").unwrap()(
            &dep,
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
        let kind = ctor_local_state_dir(FieldMap::new(), &cx()).unwrap();
        let v = Value::Host(kind);
        assert!(v.take_boxed_kind().is_ok());
        assert!(v.take_boxed_kind().is_err(), "second take must fail");
    }

    #[test]
    fn cx_methods_build_volumes() {
        let reg = Registry::compose();
        let cx = cx();
        let cx_host = HostObj::Cx(std::rc::Rc::new(Cx {
            project_name: cx.project_name.clone(),
            region: cx.region.clone(),
            deployment_dir: cx.deployment_dir.clone(),
        }));
        let v =
            reg.method(HostKind::Cx, "state").unwrap()(&cx_host, vec![s("mimir"), s("/data")], &cx)
                .unwrap();
        assert_eq!(
            v.as_vol().unwrap(),
            Vol::State {
                sub: "mimir".into(),
                at: "/data".into()
            }
        );

        let sock = reg.method(HostKind::Cx, "docker_sock").unwrap()(&cx_host, vec![], &cx).unwrap();
        assert_eq!(
            sock.as_vol().unwrap(),
            Vol::Raw("/var/run/docker.sock:/var/run/docker.sock".into())
        );
    }

    #[test]
    fn cx_field_reads_whitelisted_only() {
        let cx = cx();
        assert_eq!(
            cx_field(&cx, "project_name").unwrap(),
            Value::Str("tokeira".into())
        );
        assert!(cx_field(&cx, "deployment_dir").is_err());
    }
}
