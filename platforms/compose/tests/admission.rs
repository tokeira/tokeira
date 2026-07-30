//! Config admission (Proposal 004 Phase 4): `#[create]` create-time-immutability
//! (a changed `storage` is a retarget, refused) and `#[require]` constraints.

use std::path::PathBuf;

use tokeira_compose_deployment::{
    context::Cx,
    interp::{interpret, retarget_check},
};

const SRC: &str = include_str!("../definition.tkd");

fn cx() -> Cx {
    Cx {
        project_name: "tokeira".into(),
        region: Some("us-east-1".into()),
        deployment_dir: PathBuf::from("/tmp/x"),
    }
}

fn dsql_src(region: &str) -> String {
    SRC.replace(
        "storage: Storage::InMemory,",
        &format!(
            "storage: Storage::Dsql {{ region: \"{region}\".into(), mode: DsqlMode::Managed, endpoint: None, arn: None }},"
        ),
    )
}

#[test]
fn changing_create_time_storage_is_a_retarget() {
    let cx = cx();
    let (_, in_memory) = interpret(SRC, &cx).unwrap();
    let (_, dsql) = interpret(&dsql_src("us-east-1"), &cx).unwrap();

    // storage is `#[create]` — flipping InMemory -> Dsql is a retarget, refused.
    let err = retarget_check(SRC, &in_memory, &dsql).expect_err("storage change must be refused");
    assert!(
        err.iter()
            .any(|m| m.contains("storage") && m.contains("retarget")),
        "{err:?}"
    );

    // identity is fine
    assert!(retarget_check(SRC, &in_memory, &in_memory).is_ok());
}

#[test]
fn changing_a_non_create_field_reconciles() {
    let cx = cx();
    let (_, base) = interpret(SRC, &cx).unwrap();

    // tokeirad.replicas is NOT `#[create]` — bumping it is an ordinary apply.
    let bumped_src = SRC.replace(
        "            replicas: 1,\n            grpc_port",
        "            replicas: 3,\n            grpc_port",
    );
    assert_ne!(bumped_src, SRC, "the replicas substitution must apply");
    let (_, bumped) = interpret(&bumped_src, &cx).unwrap();

    assert!(
        retarget_check(SRC, &base, &bumped).is_ok(),
        "replicas should reconcile"
    );
}

const REQUIRE_OK: &str = r#"
#[require(replicas > 0)]
struct Compose { replicas: u32 }
fn config() -> Compose { Compose { replicas: 1 } }
fn deployment(cfg: &Compose, cx: &Cx) -> Deployment { Deployment::new(&["default"]) }
"#;

const REQUIRE_FAIL: &str = r#"
#[require(replicas > 0)]
struct Compose { replicas: u32 }
fn config() -> Compose { Compose { replicas: 0 } }
fn deployment(cfg: &Compose, cx: &Cx) -> Deployment { Deployment::new(&["default"]) }
"#;

#[test]
fn require_constraint_passes() {
    assert!(interpret(REQUIRE_OK, &cx()).is_ok());
}

#[test]
fn require_constraint_failure_is_rejected() {
    let err = match interpret(REQUIRE_FAIL, &cx()) {
        Ok(_) => panic!("the constraint must fail"),
        Err(e) => e,
    };
    assert!(err.msg.contains("constraint failed"), "{}", err.msg);
}
