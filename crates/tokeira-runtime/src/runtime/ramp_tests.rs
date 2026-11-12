//! Ramp routing oracles from go-farm v0.0.0-20240924180020-3414d57e47da and
//! `common/worker_versioning/worker_versioning.go @ v1.31.0`.
//! Expected choices and IEEE-754 bits are fixed reference outputs, independent
//! of the implementation under test.

use proptest::{prelude::*, test_runner::FileFailurePersistence};
use tokeira_storage::{BuildId, DeploymentName, StoredRoutingConfig, WorkerDeploymentVersionKey};
use tokeira_types::WorkflowId;

use super::{
    ramp::{fingerprint32, threshold},
    workflow_task::routing_config_target,
};

const NAMED_PERCENTAGES: [f32; 13] = [
    0.0, 0.001, 0.5, 1.0, 10.0, 25.0, 33.3, 50.0, 66.6, 75.0, 99.9, 99.999, 100.0,
];
const NUMBERED_PERCENTAGES: [f32; 4] = [10.0, 33.3, 50.0, 99.9];
const BOUNDARY_IDS: [&str; 2] = ["ramp-boundary-396149606", "ramp-boundary-562216123"];

fn named_vectors() -> Vec<(String, u32, u64, &'static str)> {
    vec![
        (
            "a".to_owned(),
            0x3c973d4d,
            0x4037ab13f2140000,
            "CCCCCRRRRRRRR",
        ),
        (
            "wf-1".to_owned(),
            0xe8ed8b69,
            0x4056bf329d410000,
            "CCCCCCCCCCRRR",
        ),
        (
            "order-12345".to_owned(),
            0xdec7565c,
            0x4055c1776efc0000,
            "CCCCCCCCCCRRR",
        ),
        (
            "0b8b0c4a-6a4e-4d1f-9a40-3c2b6f0e7d11".to_owned(),
            0xd001336a,
            0x4054501e055a0000,
            "CCCCCCCCCCRRR",
        ),
        (
            "café".to_owned(),
            0xdc7524d9,
            0x4055877099310000,
            "CCCCCCCCCCRRR",
        ),
        (
            "日本語のワークフロー".to_owned(),
            0x6e9355e9,
            0x404598c6c7820000,
            "CCCCCCCRRRRRR",
        ),
        (
            "🚀".to_owned(),
            0x641d2f78,
            0x40438db345700000,
            "CCCCCCCRRRRRR",
        ),
        (
            "workflow-id-that-is-longer-than-twenty-four-bytes".to_owned(),
            0xf34eb36c,
            0x4057c2af858c0000,
            "CCCCCCCCCCRRR",
        ),
        (
            "x".repeat(255),
            0x3bff09c4,
            0x40376f9fd0900000,
            "CCCCCRRRRRRRR",
        ),
    ]
}

const NUMBERED_VECTORS: [(u32, u64, &str); 100] = [
    (0x309df644, 0x4032fdb432900000, "CRRR"),
    (0xe8ed8b69, 0x4056bf329d410000, "CCCR"),
    (0xc9465fb2, 0x4053a7df58620000, "CCCR"),
    (0x01feb9dd, 0x3fe8f0134a800000, "RRRR"),
    (0x689082d1, 0x40446c398cd20000, "CCRR"),
    (0xdfdd4720, 0x4055dc9bf2200000, "CCCR"),
    (0x27227112, 0x402e92e856100000, "CRRR"),
    (0x9f43265a, 0x404f1b1d7d940000, "CCCR"),
    (0x09131a0b, 0x400c5bb162600000, "RRRR"),
    (0x8bf4e248, 0x404b55d432100000, "CCCR"),
    (0x991e1144, 0x404de7df5f480000, "CCCR"),
    (0xc9fb6ed3, 0x4053b98dd29b0000, "CCCR"),
    (0x6b7c885e, 0x4044fe52a25c0000, "CCRR"),
    (0xc2ad248f, 0x405302e891f70000, "CCCR"),
    (0x345810a1, 0x403472667ee40000, "CRRR"),
    (0x5997bbc8, 0x40417fa2ad100000, "CCRR"),
    (0x3587f291, 0x4034e91ac0a40000, "CRRR"),
    (0xdeff6816, 0x4055c6f12a260000, "CCCR"),
    (0x3de50322, 0x40382d7539480000, "CRRR"),
    (0x28821a8e, 0x402fa5a4bef00000, "CRRR"),
    (0x82ed9504, 0x404992671ac80000, "CCCR"),
    (0x71700d9f, 0x404627e2a90e0000, "CCRR"),
    (0x57c6f46c, 0x404124dbbd180000, "CCRR"),
    (0x07063cf5, 0x4005f37e7da00000, "RRRR"),
    (0x0bad9806, 0x40123f3d89600000, "RRRR"),
    (0x2d412705, 0x4031ad733df40000, "CRRR"),
    (0xe9d2bc37, 0x4056d594615f0000, "CCCR"),
    (0x2043125b, 0x4029346657180000, "CRRR"),
    (0x81c1a021, 0x404957d146720000, "CCCR"),
    (0x68fecb6a, 0x404481c3bab40000, "CCRR"),
    (0x54ff142e, 0x404099d1f0fc0000, "CRRR"),
    (0x9d859fec, 0x404ec4193c180000, "CCCR"),
    (0x78003ea9, 0x4047700c3d020000, "CCRR"),
    (0x750b9891, 0x4046dc43cc520000, "CCRR"),
    (0x7541e203, 0x4046e6de24960000, "CCRR"),
    (0xd11bfcec, 0x40546bbbb30c0000, "CCCR"),
    (0x333cd76f, 0x403403c4275c0000, "CRRR"),
    (0xb94a5a2b, 0x40521842ce330000, "CCCR"),
    (0xd3d12bf5, 0x4054af6d4aed0000, "CCCR"),
    (0x31c8c773, 0x4033726de8ec0000, "CRRR"),
    (0x03c7b18d, 0x3ff7a015b1400000, "RRRR"),
    (0xb5aa375b, 0x4051bd9f67e30000, "CCCR"),
    (0x130b67a2, 0x401dc1d1ed200000, "RRRR"),
    (0xf1bd0a6e, 0x40579b7604be0000, "CCCR"),
    (0xe952d686, 0x4056c916f3160000, "CCCR"),
    (0xc892a521, 0x4053965220390000, "CCCR"),
    (0x6dab62b8, 0x40456b7947f00000, "CCRR"),
    (0x6a6a5f19, 0x4044c8c692e20000, "CCRR"),
    (0x38e440ae, 0x4036392943f80000, "CRRR"),
    (0x01a7b6c0, 0x3fe4b06c60000000, "RRRR"),
    (0x7b168b9d, 0x40480a6744aa0000, "CCRR"),
    (0x1a524276, 0x40249043ec300000, "CRRR"),
    (0x267783bb, 0x402e0d5eea180000, "CRRR"),
    (0x75c6f916, 0x404700dca64c0000, "CCRR"),
    (0x6e3a83a2, 0x4045876db5a40000, "CCRR"),
    (0xee7c2ec3, 0x40574a20910b0000, "CCCR"),
    (0xc1afafcb, 0x4052ea282ad30000, "CCCR"),
    (0x349a30f2, 0x40348c3b1e880000, "CRRR"),
    (0x25cc1666, 0x402d87717fb00000, "CRRR"),
    (0xadd5624c, 0x4050f9d6996c0000, "CCCR"),
    (0xf108f319, 0x405789dfbd710000, "CCCR"),
    (0x6cba9e96, 0x40453c72f94c0000, "CCRR"),
    (0xc425426e, 0x405327a37cbe0000, "CCCR"),
    (0xe2aee88e, 0x40562314b5de0000, "CCCR"),
    (0x3530f61c, 0x4034c72022f00000, "CRRR"),
    (0xb8f0da24, 0x40520f854d840000, "CCCR"),
    (0x1ed0ec64, 0x40281338ae200000, "CRRR"),
    (0x3bd1716e, 0x40375dd04ef80000, "CRRR"),
    (0x590459b6, 0x404162d9858c0000, "CCRR"),
    (0x3a977a6c, 0x4036e32bd2300000, "CRRR"),
    (0x516cbd77, 0x403fce7a027c0000, "CRRR"),
    (0x094ab31c, 0x400d096fb7800000, "RRRR"),
    (0x9c9fdc35, 0x404e9739025a0000, "CCCR"),
    (0x2a84ca15, 0x40309bdef0340000, "CRRR"),
    (0xb2241ea0, 0x40516586fda00000, "CCCR"),
    (0x35c56d03, 0x4035011e952c0000, "CRRR"),
    (0x1c255a62, 0x4025fd2e9c900000, "CRRR"),
    (0xc75da861, 0x4053782571790000, "CCCR"),
    (0x7547dfc6, 0x4046e809b4ac0000, "CCRR"),
    (0xeb4b4ea0, 0x4056fa5aada00000, "CCCR"),
    (0xe0c963ee, 0x4055f3aac23e0000, "CCCR"),
    (0x733c0175, 0x404681b848da0000, "CCRR"),
    (0x8afe0e71, 0x404b259ed2120000, "CCCR"),
    (0xd6b96a98, 0x4054f81b68d80000, "CCCR"),
    (0x55a6277a, 0x4040ba73b5d40000, "CCRR"),
    (0x9ff1422a, 0x404f3d1eec340000, "CCCR"),
    (0x9968cf81, 0x404df67887320000, "CCCR"),
    (0x95c8d5c9, 0x404d4139c1420000, "CCCR"),
    (0xdfca191f, 0x4055dabc74070000, "CCCR"),
    (0xadf855ef, 0x4050fd4064570000, "CCCR"),
    (0xfdd23b21, 0x4058c987c6390000, "CCCR"),
    (0xe3b17150, 0x40563c5410d00000, "CCCR"),
    (0xb5861e6d, 0x4051ba18f8a50000, "CCCR"),
    (0x5c44f774, 0x4042057854a80000, "CCRR"),
    (0xbbf44398, 0x40525ada99d80000, "CCCR"),
    (0xf1f492ac, 0x4057a0e252cc0000, "CCCR"),
    (0x1a4bcb5e, 0x40248b36e1700000, "CRRR"),
    (0x13c77162, 0x401ee7a129200000, "RRRR"),
    (0x4ce3d6e9, 0x403e08fff3040000, "CRRR"),
    (0x2e733f30, 0x40322504aec00000, "CRRR"),
];

fn routing_config(percentage: f32, to_unversioned: bool) -> StoredRoutingConfig {
    StoredRoutingConfig {
        current_version: Some(WorkerDeploymentVersionKey {
            deployment_name: DeploymentName("deployment".into()),
            build_id: BuildId("current".into()),
        }),
        ramping_version: (!to_unversioned).then(|| WorkerDeploymentVersionKey {
            deployment_name: DeploymentName("deployment".into()),
            build_id: BuildId("ramping".into()),
        }),
        ramping_to_unversioned: to_unversioned,
        ramping_version_percentage: percentage,
        ..Default::default()
    }
}

#[test]
fn routing_ramp_reference_choices() {
    let mut cases = Vec::new();
    for (id, _, _, choices) in named_vectors() {
        for (percentage, choice) in NAMED_PERCENTAGES.into_iter().zip(choices.bytes()) {
            cases.push((id.clone(), percentage, choice == b'R'));
        }
    }
    for (index, (_, _, choices)) in NUMBERED_VECTORS.iter().enumerate() {
        for (percentage, choice) in NUMBERED_PERCENTAGES.into_iter().zip(choices.bytes()) {
            cases.push((format!("wf-{index}"), percentage, choice == b'R'));
        }
    }
    for id in BOUNDARY_IDS {
        cases.push((id.to_owned(), 50.0, true));
        cases.push((id.to_owned(), 49.999996_f32, false));
    }
    assert_eq!(cases.len(), 521);
    let mut failures = Vec::new();
    for to_unversioned in [false, true] {
        for (id, percentage, ramping) in &cases {
            let config = routing_config(*percentage, to_unversioned);
            let actual = routing_config_target(&config, &WorkflowId(id.clone()));
            let expected = if *ramping {
                (!to_unversioned).then_some("ramping")
            } else {
                Some("current")
            };
            if actual.as_ref().map(|version| version.build_id.as_str()) != expected {
                failures.push(format!(
                    "id={id:?}, percentage={percentage:?}, to_unversioned={to_unversioned}: \
                     expected {expected:?}, got {actual:?}"
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} routing cases failed:\n{}",
        failures.len(),
        cases.len() * 2,
        failures.join("\n")
    );
}

#[test]
fn empty_workflow_id_routes_deterministically() {
    let id = WorkflowId(String::new());
    let reference_threshold = 100.0 * (f64::from(0xdc56d17a_u32) / (f64::from(u32::MAX) + 1.0));
    for percentage in [50.0_f32, 99.9_f32] {
        let config = routing_config(percentage, false);
        let expected = if reference_threshold <= f64::from(percentage) {
            "ramping"
        } else {
            "current"
        };
        for _ in 0..100 {
            assert_eq!(
                routing_config_target(&config, &id)
                    .as_ref()
                    .map(|v| v.build_id.as_str()),
                Some(expected)
            );
        }
    }
}

#[test]
fn fingerprint32_pattern_vectors() {
    let expected = [
        0xdc56d17a, 0x3c973d4d, 0x417330fd, 0x2f635ec7, 0x98b51e95, 0xa3f366ac, 0x0f813aa4,
        0x21deb6d7, 0xfd7ec8b9, 0x6f98dc86, 0xf2669361, 0x8dc87967, 0x697662d2, 0x6adbd09c,
        0x3afd9afb, 0xe8f53e30, 0x4dac30a0, 0x2b2ee471, 0x016d02f7, 0x912bb990, 0xed51ea9d,
        0x64d84b74, 0xf46cbab4, 0x0766ebb2, 0x0d4652a4, 0x6e629503, 0x95d555d6, 0x0527b214,
        0x0f598415, 0x398373aa, 0x14179c54, 0x870c1f58, 0x2099f533, 0xffdbb7d7, 0xb552c381,
        0x894bc32e, 0x01d81796, 0xc3f936fc, 0x15517c8c, 0x3360eacf, 0x6e2cecb9, 0x121e9c33,
        0x94ad6cb8, 0x4e170aa3, 0x24b79cb5, 0x1b65fd16, 0x7b6059af, 0x43919ee6, 0xbedf3954,
        0xdaf64a9b, 0xead51bbb, 0xdcc50a29, 0x118f39f2, 0x712f1978, 0xb4665df8, 0x43a0bce4,
        0xe824b136, 0x836efe79, 0x8d83d5fc, 0x63f579d8, 0xdf6f2929, 0xfe4d8183, 0x587aff64,
        0x0d9530b6, 0x80ec20a7, 0x0d7af72a, 0xfbd5388a, 0xc25aa832, 0x4cbeb1c4, 0xf966a847,
        0xc0a20355, 0x29b57646, 0xc76f1f46, 0x6ede1a6f, 0xdfb42caa, 0x038c4095, 0x3396b9fc,
        0x73e12861, 0xd68d03b2, 0x90501187, 0xe43dc80b, 0x16d18d77, 0xa95b727f, 0xf6505f9d,
        0xd6a1c00a, 0xad22406a, 0xe7f44b37, 0x03605c53, 0x8769635d, 0x4bab044e, 0xce10e81c,
        0x95156931, 0xbdb11fa8, 0x72693b18, 0xb6afb9c0, 0x1a0498a0, 0xe5eeba79, 0x01efcda1,
        0xb668add9, 0xd93dbc03, 0x51d4d9f2,
    ];
    for (len, expected) in expected.into_iter().enumerate() {
        let input = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:/"
            .iter()
            .copied()
            .cycle()
            .take(len)
            .collect::<Vec<_>>();
        assert_eq!(fingerprint32(&input), expected, "length {len}");
    }
}

#[test]
fn fingerprint32_high_bit_vectors() {
    let expected = [
        0xdc56d17a, 0x2fc8696b, 0xb5dd8733, 0x5cd1b13e, 0xbea6ffaf, 0x53cccc38, 0xc92b6193,
        0x23b8e9c8, 0x7c64efee, 0xab67c704, 0xadb0e638, 0xd9adbb29, 0xdc327b57, 0x9e7b26de,
        0xb14314eb, 0xa04b6093, 0xf8159314, 0x13cf0ec7, 0x848930f8, 0xefd9386c, 0xbc1a6c76,
        0x6c5fc71d, 0xf43a4702, 0x0ed2b8fd, 0x51f72aa0, 0x1ade4378, 0x0f1f1821, 0x3916a77b,
        0x19a2b7c8, 0xd7c79bd5, 0x87e003e2, 0x20970dcf, 0xf6302e0b, 0xebbba595, 0x273f71d4,
        0xefb8c771, 0x7c7e05ca, 0x653dca6b, 0x553ab5b2, 0x2773a658, 0x5b464e76,
    ];
    for (len, expected) in expected.into_iter().enumerate() {
        let input = (0..len).map(|i| (0x80 + 37 * i) as u8).collect::<Vec<_>>();
        assert_eq!(fingerprint32(&input), expected, "length {len}");
    }
}

#[test]
fn fingerprint32_workflow_id_vectors() {
    for (id, hash, _, _) in named_vectors() {
        assert_eq!(fingerprint32(id.as_bytes()), hash, "{id:?}");
    }
    for (index, (hash, _, _)) in NUMBERED_VECTORS.iter().enumerate() {
        let id = format!("wf-{index}");
        assert_eq!(fingerprint32(id.as_bytes()), *hash, "{id:?}");
    }
    for id in BOUNDARY_IDS {
        assert_eq!(fingerprint32(id.as_bytes()), 0x80000000, "{id:?}");
    }
}

#[test]
fn ramp_threshold_reference_bits() {
    for (id, _, bits, _) in named_vectors() {
        assert_eq!(threshold(&id).to_bits(), bits, "{id:?}");
    }
    for (index, (_, bits, _)) in NUMBERED_VECTORS.iter().enumerate() {
        let id = format!("wf-{index}");
        assert_eq!(threshold(&id).to_bits(), *bits, "{id:?}");
    }
    for id in BOUNDARY_IDS {
        assert_eq!(threshold(id).to_bits(), 0x4049000000000000, "{id:?}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(concat!(
            env!("CARGO_MANIFEST_DIR"), "/src/runtime/ramp_tests.proptest-regressions"
        )))),
        ..ProptestConfig::default()
    })]

    // Increasing a ramp percentage never returns a selected workflow to current.
    #[test]
    fn ramp_selection_is_monotone(
        id in prop::collection::vec(any::<char>(), 0..256).prop_map(String::from_iter),
        p in 0.0_f32..=100.0,
        q in 0.0_f32..=100.0,
    ) {
        let id = WorkflowId(id);
        for to_unversioned in [false, true] {
            let low = routing_config_target(&routing_config(p.min(q), to_unversioned), &id);
            let high = routing_config_target(&routing_config(p.max(q), to_unversioned), &id);
            let ramping = (!to_unversioned).then_some("ramping");
            if low.as_ref().map(|v| v.build_id.as_str()) == ramping {
                prop_assert_eq!(high.as_ref().map(|v| v.build_id.as_str()), ramping);
            }
        }
    }

    // Nonpositive percentages select current; 100 selects the configured ramp target.
    #[test]
    fn ramp_selection_endpoints(
        id in prop::collection::vec(any::<char>(), 0..256).prop_map(String::from_iter),
    ) {
        let id = WorkflowId(id);
        for to_unversioned in [false, true] {
            for percentage in [-1.0, 0.0, 100.0] {
                let actual = routing_config_target(&routing_config(percentage, to_unversioned), &id);
                let expected = if percentage == 100.0 {
                    (!to_unversioned).then_some("ramping")
                } else {
                    Some("current")
                };
                prop_assert_eq!(actual.as_ref().map(|v| v.build_id.as_str()), expected);
            }
        }
    }
}
