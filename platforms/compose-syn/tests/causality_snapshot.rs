//! The desired-snapshot seam (explanation-causality Requirement 1): revision
//! comparison is a comparison of values, so a snapshot must be canonical —
//! authored order of set-valued fields cannot leak into equality, or
//! desired-vs-desired comparison re-imports the port-order roulette as
//! phantom definition edits. Property 7 pins that; the two-paths test pins
//! Requirement 1.6 (working definition and retained revision share one code
//! path); the broken-source test pins the located verdict (Requirement 1.3).

use std::path::PathBuf;

use proptest::prelude::*;
use tokeira_compose_syn::{
    DEFAULT_TKD,
    builder::{Deployment, Vol},
    context::Cx,
    interp,
    kinds::Service,
    provisioner::ComposeSynPlatform,
};
use tokeira_provisioner_cli::{DesiredSnapshot, ProvisionerPlatform, Realization};

fn cx() -> Cx {
    Cx {
        project_name: "canon".into(),
        region: Some("us-east-1".into()),
        deployment_dir: PathBuf::from("/tmp/causality-snapshot"),
    }
}

/// Realize a one-service deployment whose set-valued fields carry the given
/// authored order. Everything else held constant, so snapshot equality is
/// exactly order-insensitivity.
fn snapshot_with(publish: Vec<u16>, volumes: Vec<Vol>, needs: Vec<String>) -> DesiredSnapshot {
    let mut deployment = Deployment::new(&["default"]);
    let module = deployment.module("observability", &[]);
    deployment.service(
        &module,
        "svc",
        Service {
            image: "img:1".into(),
            replicas: 1,
            publish,
            volumes,
            env: vec![("A".into(), "1".into()), ("B".into(), "2".into())],
            command: vec!["run".into()],
            needs,
            server_config: false,
            aws: None,
        },
    );
    deployment.desired_snapshot(&cx())
}

#[test]
fn the_reference_definition_snapshots_deterministically() {
    let cx = cx();
    let (deployment, _config) = interp::interpret(DEFAULT_TKD, &cx).expect("reference parses");
    let first = deployment.desired_snapshot(&cx);
    assert!(!first.is_empty(), "the reference definition has resources");
    assert_eq!(first, deployment.desired_snapshot(&cx));
}

proptest! {
    // Feature: explanation-causality, Property 7
    //
    // Snapshot canonicality: for any authored order of the set-valued service
    // fields (ports, volumes, deploy-ordering needs), the desired snapshot is
    // the same value.
    #[test]
    fn property_7_snapshot_canonicality(
        (ports, shuffled_ports, subs, shuffled_subs, needs, shuffled_needs) in (
            prop::collection::vec(1024u16..9999, 1..5),
            prop::collection::vec("[a-z]{1,8}", 1..5),
            prop::collection::vec("[a-z]{1,8}", 0..4),
        )
            .prop_flat_map(|(ports, subs, needs)| (
                Just(ports.clone()),
                Just(ports).prop_shuffle(),
                Just(subs.clone()),
                Just(subs).prop_shuffle(),
                Just(needs.clone()),
                Just(needs).prop_shuffle(),
            ))
    ) {
        let volumes = |subs: &[String]| -> Vec<Vol> {
            subs.iter()
                .map(|sub| Vol::State { sub: sub.clone(), at: format!("/var/{sub}") })
                .collect()
        };
        prop_assert_eq!(
            snapshot_with(ports, volumes(&subs), needs),
            snapshot_with(shuffled_ports, volumes(&shuffled_subs), shuffled_needs),
            "authored order leaked into the snapshot"
        );
    }
}

// Requirement 1.6: the working definition and a retained revision copy of the
// same content produce equal snapshots through the platform seam — one code
// path, exercised end to end.
#[tokio::test]
async fn the_working_and_retained_paths_produce_one_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let working = dir.join("definition.tkd");
    let retained_dir = dir.join("state/config-revisions/4");
    std::fs::create_dir_all(&retained_dir).unwrap();
    let retained = retained_dir.join("definition.tkd");
    std::fs::write(&working, DEFAULT_TKD).unwrap();
    std::fs::write(&retained, DEFAULT_TKD).unwrap();

    let platform = ComposeSynPlatform;
    let realize = |path: PathBuf| async move {
        match platform.desired_snapshot(dir, &path).await.unwrap() {
            Realization::Realized(snapshot) => snapshot,
            Realization::NotApplicable { reason } => panic!("compose-syn snapshots: {reason}"),
        }
    };
    let from_working = realize(working).await;
    let from_retained = realize(retained).await;
    assert!(!from_working.is_empty());
    assert_eq!(from_working, from_retained);
}

// Requirement 1.3: a source that does not interpret returns the located
// verdict, never a partial snapshot.
#[tokio::test]
async fn a_broken_source_returns_the_located_verdict() {
    let tmp = tempfile::tempdir().unwrap();
    let broken = tmp.path().join("definition.tkd");
    std::fs::write(&broken, "this is not a definition").unwrap();

    let err = ComposeSynPlatform
        .desired_snapshot(tmp.path(), &broken)
        .await
        .expect_err("a broken source fails the snapshot");
    assert!(
        err.to_string().contains("does not verify"),
        "the verdict names the failure: {err}"
    );
}
