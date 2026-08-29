//! The EKS platform's [`HostBridge`] — the host adapter the shared `tokeira-tkd`
//! interpreter dispatches through. It owns the closed [`HostObj`] handle enum,
//! the per-kind constructors (the reflection Rust lacks), and the builder-verb
//! method shims. This is the only `platforms/eks` module that both names
//! `crate::builder`/`crate::kinds`/`crate::manifests` *and* the interpreter's
//! value model — the seam that keeps the interpreter itself engine-agnostic
//! (Proposal 004 §11, §19).
//!
//! It mirrors `platforms/compose::bridge` with two deliberate simplifications
//! grounded in the EKS design:
//! - **No `Vol` variant / no `cx.state`/`config`/`docker_sock` methods.** EKS has
//!   no bind-mount vocabulary: Kubernetes objects are built by the kinds
//!   (`k8s-openapi` structs), not assembled from host paths (design → `HostObj`).
//! - **No concrete-typed workload handle.** The compose platform carries a `Service` kind
//!   as `HostKindVal::Service`; EKS has no `service()` builder verb — a tokeira
//!   service Deployment is a `Box<dyn Kind>` (`ServiceDeployment`) like every
//!   other K8s object — so the kind handle is a bare take-once `Box<dyn Kind>`.
//!
//! ## The config-struct vs. author-kind split (load-bearing)
//!
//! The interpreter decides "is this named type a config `Value::Struct` or an
//! opaque host handle?" via [`HostBridge::is_kind`]. Only the types listed there
//! are kinds. `ServiceManifest`/`IngressRule` are *config structs* declared in
//! the `.tkd`: they arrive as [`tokeira_platform_definition::tkd::Value::Struct`] and are decomposed
//! here into the concrete `manifests::ServiceManifest`/`kinds::IngressRule`. This
//! is why they must NOT appear in `is_kind` — misclassifying one would route it
//! through kind construction and fail.

use std::{cell::RefCell, rc::Rc};

use tokeira_platform_definition::tkd::{EvalError, FieldMapExt, HostBridge};

use crate::{
    builder::{self, Kind, ModuleRef, Output, ResourceRef, WbValue},
    context::Cx,
    kinds::{
        DsqlCluster, DsqlConnectionEndpoint, DynamoDbTable, EcrRepository, EksCluster, IamRole,
        IngressRule, Namespace, NodePool, PodIdentityAssociation, S3Bucket, SecretsManagerSecret,
        SecurityGroup, ServiceDeployment, Vpc, VpcEndpoint,
    },
    manifests::ServiceManifest,
};

/// The EKS platform's runtime value type (the shared `Value` over its host).
type Value = tokeira_platform_definition::tkd::Value<HostObj>;
/// The EKS platform's evaluated field map.
type FieldMap = tokeira_platform_definition::tkd::FieldMap<HostObj>;

/// The closed set of opaque author handles the EKS platform exposes. Dispatch
/// keys on [`HostKind`], so a receiver-type mismatch is structural, never a
/// runtime downcast (design → "no `Box<dyn Any>`").
///
/// `Clone` is derived: every payload is `Clone` (the builder/kind handles are
/// behind an `Rc`), which the interpreter relies on when it clones config values
/// for the retarget diff. `Debug` is hand-written because `Box<dyn Kind>` is not
/// `Debug`.
#[derive(Clone)]
pub enum HostObj {
    /// The deployment under construction (shared so builder verbs mutate in place).
    Deployment(Rc<RefCell<builder::Deployment>>),
    /// A declared-module handle returned by `d.module(..)`.
    Module(ModuleRef),
    /// A declared-resource handle returned by `d.resource(..)`.
    Resource(ResourceRef),
    /// A deferred resource-output handle returned by `r.output(..)`.
    Output(Output),
    /// A constructed-but-unplaced kind, consumed once when handed to `resource`.
    /// `Option` gives take-once semantics: a kind may be placed exactly once.
    Kind(Rc<RefCell<Option<Box<dyn Kind>>>>),
    /// The injected runtime context (`cx`).
    Cx(Rc<Cx>),
}

/// The dispatch tag for a [`HostObj`] — the method-table key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HostKind {
    Deployment,
    Module,
    Resource,
    Output,
    Kind,
    Cx,
}

impl HostObj {
    /// The dispatch tag (variant) of this handle.
    pub(crate) fn kind(&self) -> HostKind {
        match self {
            HostObj::Deployment(_) => HostKind::Deployment,
            HostObj::Module(_) => HostKind::Module,
            HostObj::Resource(_) => HostKind::Resource,
            HostObj::Output(_) => HostKind::Output,
            HostObj::Kind(_) => HostKind::Kind,
            HostObj::Cx(_) => HostKind::Cx,
        }
    }
}

impl std::fmt::Debug for HostObj {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A boxed kind is not `Debug`; the variant tag is all the interpreter's
        // error messages need (it never introspects a host's contents).
        write!(f, "Host::{:?}", self.kind())
    }
}

/// Wrap a boxed kind as a take-once host handle.
fn host_kind(kind: Box<dyn Kind>) -> HostObj {
    HostObj::Kind(Rc::new(RefCell::new(Some(kind))))
}

// ── host-typed coercions (the shim argument surface) ────────────────────────

/// Positional argument access with a method-named error (subset already proved
/// arity for well-formed input; this guards the residual bridge-internal path).
fn arg<'a>(args: &'a [Value], i: usize, m: &str) -> Result<&'a Value, EvalError> {
    args.get(i)
        .ok_or_else(|| EvalError::new(format!("`{m}`: missing argument {i}")))
}

/// Coerce a value to a module handle (the first arg of `resource`).
fn as_host_module(v: &Value) -> Result<ModuleRef, EvalError> {
    match v {
        Value::Host(HostObj::Module(m)) => Ok(m.clone()),
        v => Err(EvalError::new(format!(
            "expected a module handle, got {v:?}"
        ))),
    }
}

/// Consume an unplaced `Box<dyn Kind>` handle (take-once). A second take is an
/// error: a constructed kind is a linear resource — placing it twice would alias
/// one builder entry into two modules.
fn take_boxed_kind(v: &Value) -> Result<Box<dyn Kind>, EvalError> {
    match v {
        Value::Host(HostObj::Kind(cell)) => cell
            .borrow_mut()
            .take()
            .ok_or_else(|| EvalError::new("kind handle already consumed")),
        v => Err(EvalError::new(format!("expected a kind handle, got {v:?}"))),
    }
}

/// Unwrap a config `Value::Struct`, asserting its declared type. Used for the
/// nested config structs a kind embeds (`ServiceManifest`, `IngressRule`) — the
/// analog of a `FieldMapExt` taker for a struct-typed field.
fn struct_fields(v: Value, expect_ty: &str) -> Result<FieldMap, EvalError> {
    match v {
        Value::Struct { ty, fields } if ty == expect_ty => Ok(fields),
        Value::Struct { ty, .. } => Err(EvalError::new(format!(
            "expected struct `{expect_ty}`, got `{ty}`"
        ))),
        other => Err(EvalError::new(format!(
            "expected struct `{expect_ty}`, got {other:?}"
        ))),
    }
}

/// An `Option<u16>` field (`grpc_port`). `FieldMapExt` ships `take_opt_str` but no
/// `Option<u16>`, so this fills the one numeric-optional the manifests need.
fn take_opt_u16(f: &mut FieldMap, key: &str) -> Result<Option<u16>, EvalError> {
    match f.take(key)? {
        Value::Opt(None) => Ok(None),
        Value::Opt(Some(b)) => match *b {
            Value::Int(n) => u16::try_from(n)
                .map(Some)
                .map_err(|_| EvalError::new(format!("field `{key}`: {n} out of u16 range"))),
            v => Err(EvalError::new(format!(
                "field `{key}`: expected Option<u16>, got Some({v:?})"
            ))),
        },
        v => Err(EvalError::new(format!(
            "field `{key}`: expected Option<u16>, got {v:?}"
        ))),
    }
}

/// An `i32` field (`password_length`). Small, positive in practice; range-checked
/// at the edge like the `FieldMapExt` `u16`/`u32` takers.
fn take_i32(f: &mut FieldMap, key: &str) -> Result<i32, EvalError> {
    match f.take(key)? {
        Value::Int(n) => i32::try_from(n)
            .map_err(|_| EvalError::new(format!("field `{key}`: {n} out of i32 range"))),
        v => Err(EvalError::new(format!(
            "field `{key}`: expected integer, got {v:?}"
        ))),
    }
}

/// The `ingress: vec![IngressRule { .. }, ..]` field — a `[IngressRule]` of config
/// structs, each decomposed into `kinds::IngressRule`.
fn take_ingress_rules(f: &mut FieldMap, key: &str) -> Result<Vec<IngressRule>, EvalError> {
    match f.take(key)? {
        Value::Vec(items) => items
            .into_iter()
            .map(|v| {
                let mut fields = struct_fields(v, "IngressRule")?;
                let rule = IngressRule {
                    description: fields.take_str("description")?,
                    protocol: fields.take_str("protocol")?,
                    from_port: fields.take_u16("from_port")?,
                    to_port: fields.take_u16("to_port")?,
                    source: fields.take_str("source")?,
                };
                fields.expect_empty()?;
                Ok(rule)
            })
            .collect(),
        v => Err(EvalError::new(format!(
            "field `{key}`: expected an array, got {v:?}"
        ))),
    }
}

// ── concrete kind builders (testable; comparable via the kinds' PartialEq) ──
//
// Each consumes the evaluated field map and a trailing `expect_empty()` closes
// the total-coverage contract: an unknown/misspelled `.tkd` field is rejected
// (the `syn` analog of serde `deny_unknown_fields`, Property 8).

pub(crate) fn build_vpc(f: &mut FieldMap) -> Result<Vpc, EvalError> {
    let r = Vpc {
        cidr: f.take_str("cidr")?,
        availability_zones: f.take_vec_str("availability_zones")?,
    };
    f.expect_empty()?;
    Ok(r)
}

pub(crate) fn build_vpc_endpoint(f: &mut FieldMap) -> Result<VpcEndpoint, EvalError> {
    let r = VpcEndpoint {
        service: f.take_str("service")?,
        interface: f.take_bool("interface")?,
    };
    f.expect_empty()?;
    Ok(r)
}

pub(crate) fn build_security_group(f: &mut FieldMap) -> Result<SecurityGroup, EvalError> {
    let suffix = f.take_str("suffix")?;
    let description = f.take_str("description")?;
    let ingress = take_ingress_rules(f, "ingress")?;
    f.expect_empty()?;
    Ok(SecurityGroup {
        suffix,
        description,
        ingress,
    })
}

pub(crate) fn build_iam_role(f: &mut FieldMap) -> Result<IamRole, EvalError> {
    let r = IamRole {
        suffix: f.take_str("suffix")?,
        trust_policy: f.take_str("trust_policy")?,
        inline_policies: f.take_pairs("inline_policies")?,
        managed_policy_arns: f.take_vec_str("managed_policy_arns")?,
    };
    f.expect_empty()?;
    Ok(r)
}

pub(crate) fn build_eks_cluster(f: &mut FieldMap) -> Result<EksCluster, EvalError> {
    let r = EksCluster {
        version: f.take_str("version")?,
        namespace: f.take_str("namespace")?,
        kms_key_arn: f.take_opt_str("kms_key_arn")?,
        deletion_protection: f.take_bool("deletion_protection")?,
        bootstrap_admin_permissions: f.take_bool("bootstrap_admin_permissions")?,
        cluster_admin_principal_arn: f.take_opt_str("cluster_admin_principal_arn")?,
    };
    f.expect_empty()?;
    Ok(r)
}

pub(crate) fn build_pod_identity_association(
    f: &mut FieldMap,
) -> Result<PodIdentityAssociation, EvalError> {
    let r = PodIdentityAssociation {
        namespace: f.take_str("namespace")?,
        service_account: f.take_str("service_account")?,
        role_suffix: f.take_str("role_suffix")?,
    };
    f.expect_empty()?;
    Ok(r)
}

pub(crate) fn build_dsql_cluster(f: &mut FieldMap) -> Result<DsqlCluster, EvalError> {
    // Mode is *inferred* from endpoint/arn presence in the kind's `realize`
    // (Req 5.4), so the config surface carries only the two optionals — there is
    // no `mode` field to author (unlike the compose platform's explicit `DsqlMode`).
    let r = DsqlCluster {
        endpoint: f.take_opt_str("endpoint")?,
        arn: f.take_opt_str("arn")?,
    };
    f.expect_empty()?;
    Ok(r)
}

pub(crate) fn build_dsql_connection_endpoint(
    f: &mut FieldMap,
) -> Result<DsqlConnectionEndpoint, EvalError> {
    // A unit struct: authored as the bare name `DsqlConnectionEndpoint`, so its
    // field map is empty.
    f.expect_empty()?;
    Ok(DsqlConnectionEndpoint)
}

pub(crate) fn build_dynamodb_table(f: &mut FieldMap) -> Result<DynamoDbTable, EvalError> {
    let r = DynamoDbTable {
        table: f.take_str("table")?,
        hash_key: f.take_str("hash_key")?,
        ttl: f.take_opt_str("ttl")?,
    };
    f.expect_empty()?;
    Ok(r)
}

pub(crate) fn build_s3_bucket(f: &mut FieldMap) -> Result<S3Bucket, EvalError> {
    let r = S3Bucket {
        bucket: f.take_str("bucket")?,
        versioning: f.take_bool("versioning")?,
    };
    f.expect_empty()?;
    Ok(r)
}

pub(crate) fn build_secrets_manager_secret(
    f: &mut FieldMap,
) -> Result<SecretsManagerSecret, EvalError> {
    let name = f.take_str("name")?;
    let username = f.take_str("username")?;
    let password_length = take_i32(f, "password_length")?;
    f.expect_empty()?;
    Ok(SecretsManagerSecret {
        name,
        username,
        password_length,
    })
}

pub(crate) fn build_ecr_repository(f: &mut FieldMap) -> Result<EcrRepository, EvalError> {
    let r = EcrRepository {
        name: f.take_str("name")?,
    };
    f.expect_empty()?;
    Ok(r)
}

pub(crate) fn build_namespace(f: &mut FieldMap) -> Result<Namespace, EvalError> {
    let r = Namespace {
        name: f.take_str("name")?,
    };
    f.expect_empty()?;
    Ok(r)
}

pub(crate) fn build_node_pool(f: &mut FieldMap) -> Result<NodePool, EvalError> {
    let r = NodePool {
        node_families: f.take_vec_str("node_families")?,
    };
    f.expect_empty()?;
    Ok(r)
}

/// Decompose a `ServiceManifest` config struct's fields into the concrete
/// `manifests::ServiceManifest`. `grpc_port` is the one `Option<u16>`.
pub(crate) fn build_service_manifest(mut f: FieldMap) -> Result<ServiceManifest, EvalError> {
    let name = f.take_str("name")?;
    let namespace = f.take_str("namespace")?;
    let project = f.take_str("project")?;
    let image = f.take_str("image")?;
    let replicas = f.take_u32("replicas")?;
    let cpu = f.take_str("cpu")?;
    let memory = f.take_str("memory")?;
    let grpc_port = take_opt_u16(&mut f, "grpc_port")?;
    let metrics_port = f.take_u16("metrics_port")?;
    let service_account = f.take_str("service_account")?;
    let alloy_image = f.take_str("alloy_image")?;
    let config_map = f.take_str("config_map")?;
    let config_file = f.take_str("config_file")?;
    let advertise_node_host = f.take_bool("advertise_node_host")?;
    f.expect_empty()?;
    Ok(ServiceManifest {
        name,
        namespace,
        project,
        image,
        replicas,
        cpu,
        memory,
        grpc_port,
        metrics_port,
        service_account,
        alloy_image,
        config_map,
        config_file,
        advertise_node_host,
    })
}

pub(crate) fn build_service_deployment(f: &mut FieldMap) -> Result<ServiceDeployment, EvalError> {
    // `spec` is a nested config struct; `config_content`/`alloy_config_content`
    // are rendered server/Alloy config the `.tkd` builds (via `format!` from
    // `cfg`/`cx`) — the DSQL endpoint arrives in `config_content` because
    // `hydrate_config` fills `cfg.dsql.endpoint` before the cluster-module
    // interpret (design → K8s manifests / writeback).
    let spec_v = f.take("spec")?;
    let spec = build_service_manifest(struct_fields(spec_v, "ServiceManifest")?)?;
    let config_content = f.take_str("config_content")?;
    let alloy_config_content = f.take_str("alloy_config_content")?;
    f.expect_empty()?;
    Ok(ServiceDeployment {
        spec,
        config_content,
        alloy_config_content,
    })
}

/// Read a whitelisted field of the injected `Cx` (field syntax `cx.project_name`).
/// `deployment_dir` is deliberately absent — it is realize-time host plumbing,
/// off the operator surface so the `.tkd` stays hermetic (Proposal 003 §4).
fn cx_field(cx: &Cx, field: &str) -> Result<Value, EvalError> {
    match field {
        "project_name" => Ok(Value::Str(cx.project_name.clone())),
        "region" => Ok(Value::Opt(
            cx.region.clone().map(|s| Box::new(Value::Str(s))),
        )),
        "account_id" => Ok(Value::Opt(
            cx.account_id.clone().map(|s| Box::new(Value::Str(s))),
        )),
        other => Err(EvalError::new(format!(
            "`Cx` has no readable field `{other}`"
        ))),
    }
}

// ── the bridge ──────────────────────────────────────────────────────────────

/// The EKS platform's host bridge. Dispatch is direct `match` (the kind set is
/// fixed), matching the compose platform's post-refactor shape.
#[derive(Clone, Default, Debug)]
pub struct EksBridge;

impl HostBridge for EksBridge {
    type Host = HostObj;
    type Cx = Cx;
    type Output = builder::Deployment;

    fn is_kind(&self, name: &str) -> bool {
        // Only author kinds — the types that realize to an `iac::Resource`.
        // `ServiceManifest`/`IngressRule` are config structs (see module doc) and
        // are intentionally excluded.
        matches!(
            name,
            "Vpc"
                | "VpcEndpoint"
                | "SecurityGroup"
                | "IamRole"
                | "EksCluster"
                | "PodIdentityAssociation"
                | "DsqlCluster"
                | "DsqlConnectionEndpoint"
                | "DynamoDbTable"
                | "S3Bucket"
                | "SecretsManagerSecret"
                | "EcrRepository"
                | "Namespace"
                | "NodePool"
                | "ServiceDeployment"
        )
    }

    fn knows_method(&self, name: &str) -> bool {
        // No `service`/`state`/`config`/`docker_sock`: EKS has no workload verb
        // and no bind-mount vocabulary.
        matches!(name, "module" | "resource" | "writeback" | "output")
    }

    fn knows_assoc(&self, path: &str) -> bool {
        path == "Deployment::new"
    }

    fn kind_defaults(&self, _name: &str) -> Option<FieldMap> {
        // No EKS kind has an `..EMPTY` spread default (the compose platform used it only
        // for `Service`, which has no EKS analog). Every field is authored.
        None
    }

    fn construct_kind(
        &self,
        name: &str,
        mut fields: FieldMap,
        _cx: &Cx,
    ) -> Result<HostObj, EvalError> {
        // Each arm builds the concrete kind and boxes it as a take-once handle;
        // the interpreter cannot name the concrete type it just constructed.
        let boxed: Box<dyn Kind> = match name {
            "Vpc" => Box::new(build_vpc(&mut fields)?),
            "VpcEndpoint" => Box::new(build_vpc_endpoint(&mut fields)?),
            "SecurityGroup" => Box::new(build_security_group(&mut fields)?),
            "IamRole" => Box::new(build_iam_role(&mut fields)?),
            "EksCluster" => Box::new(build_eks_cluster(&mut fields)?),
            "PodIdentityAssociation" => Box::new(build_pod_identity_association(&mut fields)?),
            "DsqlCluster" => Box::new(build_dsql_cluster(&mut fields)?),
            "DsqlConnectionEndpoint" => Box::new(build_dsql_connection_endpoint(&mut fields)?),
            "DynamoDbTable" => Box::new(build_dynamodb_table(&mut fields)?),
            "S3Bucket" => Box::new(build_s3_bucket(&mut fields)?),
            "SecretsManagerSecret" => Box::new(build_secrets_manager_secret(&mut fields)?),
            "EcrRepository" => Box::new(build_ecr_repository(&mut fields)?),
            "Namespace" => Box::new(build_namespace(&mut fields)?),
            "NodePool" => Box::new(build_node_pool(&mut fields)?),
            "ServiceDeployment" => Box::new(build_service_deployment(&mut fields)?),
            other => return Err(EvalError::new(format!("unknown kind `{other}`"))),
        };
        Ok(host_kind(boxed))
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
            // The `.tkd`'s `deployment()` returns the mutated builder; unwrap the
            // shared handle. A live extra reference at return means the `.tkd`
            // stashed the deployment somewhere — reject rather than clone.
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
    use std::path::PathBuf;

    use super::*;

    fn cx() -> Cx {
        Cx {
            project_name: "tokeira".into(),
            region: Some("eu-west-2".into()),
            account_id: Some("123456789012".into()),
            deployment_dir: PathBuf::from("/tmp/deploy"),
        }
    }

    fn s(v: &str) -> Value {
        Value::Str(v.into())
    }

    fn strct(ty: &str, fields: &[(&str, Value)]) -> Value {
        Value::Struct {
            ty: ty.into(),
            fields: fields.iter().cloned().map(|(k, v)| (k.into(), v)).collect(),
        }
    }

    #[test]
    fn vpc_round_trips() {
        let mut f = FieldMap::from([
            ("cidr".into(), s("10.0.0.0/16")),
            (
                "availability_zones".into(),
                Value::Vec(vec![s("eu-west-2a"), s("eu-west-2b")]),
            ),
        ]);
        assert_eq!(
            build_vpc(&mut f).unwrap(),
            Vpc {
                cidr: "10.0.0.0/16".into(),
                availability_zones: vec!["eu-west-2a".into(), "eu-west-2b".into()],
            }
        );
    }

    #[test]
    fn security_group_decomposes_nested_ingress_structs() {
        let ingress = Value::Vec(vec![strct(
            "IngressRule",
            &[
                ("description", s("api")),
                ("protocol", s("tcp")),
                ("from_port", Value::Int(7233)),
                ("to_port", Value::Int(7233)),
                ("source", s("self")),
            ],
        )]);
        let mut f = FieldMap::from([
            ("suffix".into(), s("eks-nodes-sg")),
            ("description".into(), s("eks nodes")),
            ("ingress".into(), ingress),
        ]);
        let built = build_security_group(&mut f).unwrap();
        assert_eq!(built.suffix, "eks-nodes-sg");
        assert_eq!(built.ingress.len(), 1);
        assert_eq!(built.ingress[0].from_port, 7233);
        assert_eq!(built.ingress[0].source, "self");
    }

    #[test]
    fn dsql_cluster_carries_only_optionals() {
        let mut managed = FieldMap::from([
            ("endpoint".into(), Value::Opt(None)),
            ("arn".into(), Value::Opt(None)),
        ]);
        assert_eq!(
            build_dsql_cluster(&mut managed).unwrap(),
            DsqlCluster {
                endpoint: None,
                arn: None,
            }
        );

        let mut adopt = FieldMap::from([
            ("endpoint".into(), Value::Opt(Some(Box::new(s("x.on.aws"))))),
            ("arn".into(), Value::Opt(None)),
        ]);
        assert_eq!(
            build_dsql_cluster(&mut adopt).unwrap().endpoint.as_deref(),
            Some("x.on.aws")
        );
    }

    #[test]
    fn dsql_connection_endpoint_is_a_unit_kind() {
        let mut f = FieldMap::new();
        assert_eq!(
            build_dsql_connection_endpoint(&mut f).unwrap(),
            DsqlConnectionEndpoint
        );
    }

    #[test]
    fn service_deployment_decomposes_spec_struct_and_content() {
        let spec = strct(
            "ServiceManifest",
            &[
                ("name", s("tokeirad")),
                ("namespace", s("tokeira-system")),
                ("project", s("tokeira")),
                ("image", s("tokeirad:latest")),
                ("replicas", Value::Int(3)),
                ("cpu", s("2")),
                ("memory", s("4Gi")),
                ("grpc_port", Value::Opt(Some(Box::new(Value::Int(7233))))),
                ("metrics_port", Value::Int(9090)),
                ("service_account", s("tokeirad")),
                ("alloy_image", s("grafana/alloy:v1.16.0")),
                ("config_map", s("tokeirad-config")),
                ("config_file", s("tokeirad.toml")),
                ("advertise_node_host", Value::Bool(true)),
            ],
        );
        let mut f = FieldMap::from([
            ("spec".into(), spec),
            ("config_content".into(), s("[infrastructure]\n")),
            ("alloy_config_content".into(), s("logging {}\n")),
        ]);
        let built = build_service_deployment(&mut f).unwrap();
        assert_eq!(built.spec.name, "tokeirad");
        assert_eq!(built.spec.grpc_port, Some(7233));
        assert!(built.spec.advertise_node_host);
        assert_eq!(built.config_content, "[infrastructure]\n");
        assert_eq!(built.alloy_config_content, "logging {}\n");
    }

    #[test]
    fn unknown_field_is_rejected() {
        let mut f = FieldMap::from([
            ("bucket".into(), s("tokeira-mimir")),
            ("versioning".into(), Value::Bool(false)),
            ("typo".into(), s("oops")),
        ]);
        let err = build_s3_bucket(&mut f).unwrap_err();
        assert!(err.msg.contains("unknown field `typo`"), "{}", err.msg);
    }

    #[test]
    fn deployment_new_and_resource_shim_place_a_kind() {
        let bridge = EksBridge;
        let cx = cx();
        let dep = bridge
            .assoc(
                "Deployment::new",
                vec![Value::Vec(vec![s("tokeira-system")])],
                &cx,
            )
            .unwrap();

        let module_v = bridge
            .call_method(
                &dep,
                "module",
                vec![s("foundation"), Value::Vec(vec![])],
                &cx,
            )
            .unwrap();

        let kind = bridge
            .construct_kind(
                "DsqlCluster",
                FieldMap::from([
                    ("endpoint".into(), Value::Opt(None)),
                    ("arn".into(), Value::Opt(None)),
                ]),
                &cx,
            )
            .unwrap();
        let resource_v = bridge
            .call_method(
                &dep,
                "resource",
                vec![module_v, s("cluster"), Value::Host(kind)],
                &cx,
            )
            .unwrap();

        // `r.output("private_hostname")` yields a deferred Output handle.
        let Value::Host(resource_h) = &resource_v else {
            panic!("resource() returns a host handle")
        };
        let out = bridge
            .call_method(resource_h, "output", vec![s("private_hostname")], &cx)
            .unwrap();
        assert!(matches!(out, Value::Host(HostObj::Output(_))));

        let HostObj::Deployment(d) = &dep else {
            panic!("expected deployment")
        };
        assert_eq!(d.borrow().resource_ids("foundation"), ["cluster"]);
    }

    #[test]
    fn writeback_records_const_and_output() {
        let bridge = EksBridge;
        let cx = cx();
        let dep = bridge
            .assoc(
                "Deployment::new",
                vec![Value::Vec(vec![s("tokeira-system")])],
                &cx,
            )
            .unwrap();
        let module_v = bridge
            .call_method(
                &dep,
                "module",
                vec![s("foundation"), Value::Vec(vec![])],
                &cx,
            )
            .unwrap();
        let kind = bridge
            .construct_kind("DsqlConnectionEndpoint", FieldMap::new(), &cx)
            .unwrap();
        let resource_v = bridge
            .call_method(
                &dep,
                "resource",
                vec![module_v, s("endpoint"), Value::Host(kind)],
                &cx,
            )
            .unwrap();
        let Value::Host(resource_h) = &resource_v else {
            panic!("resource() returns a host handle")
        };
        let out = bridge
            .call_method(resource_h, "output", vec![s("private_hostname")], &cx)
            .unwrap();

        bridge
            .call_method(
                &dep,
                "writeback",
                vec![s("infrastructure.storage"), s("dsql")],
                &cx,
            )
            .unwrap();
        bridge
            .call_method(
                &dep,
                "writeback",
                vec![s("infrastructure.dsql.endpoint"), out],
                &cx,
            )
            .unwrap();

        let HostObj::Deployment(d) = &dep else {
            panic!("expected deployment")
        };
        let entries = d.borrow();
        let entries = entries.writeback_entries();
        assert_eq!(entries[0].0, "infrastructure.storage");
        assert!(matches!(&entries[0].1, WbValue::Const(v) if v == "dsql"));
        assert_eq!(entries[1].0, "infrastructure.dsql.endpoint");
        assert!(matches!(&entries[1].1, WbValue::Output(_)));
    }

    #[test]
    fn kind_handle_is_take_once() {
        let v = Value::Host(host_kind(Box::new(EcrRepository {
            name: "tokeirad".into(),
        })));
        assert!(take_boxed_kind(&v).is_ok());
        assert!(take_boxed_kind(&v).is_err(), "second take must fail");
    }

    #[test]
    fn cx_field_reads_whitelisted_only() {
        let bridge = EksBridge;
        let cx_host = bridge.cx_host(&cx());
        assert_eq!(
            bridge.host_field(&cx_host, "project_name").unwrap(),
            Value::Str("tokeira".into())
        );
        assert_eq!(
            bridge.host_field(&cx_host, "region").unwrap(),
            Value::Opt(Some(Box::new(Value::Str("eu-west-2".into()))))
        );
        assert_eq!(
            bridge.host_field(&cx_host, "account_id").unwrap(),
            Value::Opt(Some(Box::new(Value::Str("123456789012".into()))))
        );
        // `deployment_dir` is host plumbing, never on the operator surface.
        assert!(bridge.host_field(&cx_host, "deployment_dir").is_err());
    }

    #[test]
    fn finish_unwraps_deployment() {
        let bridge = EksBridge;
        let cx = cx();
        let dep = bridge
            .assoc(
                "Deployment::new",
                vec![Value::Vec(vec![s("tokeira-system")])],
                &cx,
            )
            .unwrap();
        let HostObj::Deployment(_) = &dep else {
            panic!("expected deployment")
        };
        let built = bridge.finish(dep).unwrap();
        assert_eq!(built.namespaces(), ["tokeira-system"]);
    }

    // Guards against a future edit reclassifying a config struct as a kind, which
    // would route `ServiceManifest`/`IngressRule` through kind construction.
    #[test]
    fn config_structs_are_not_kinds() {
        let bridge = EksBridge;
        assert!(!bridge.is_kind("ServiceManifest"));
        assert!(!bridge.is_kind("IngressRule"));
        assert!(bridge.is_kind("ServiceDeployment"));
        assert!(bridge.is_kind("Vpc"));
    }
}
