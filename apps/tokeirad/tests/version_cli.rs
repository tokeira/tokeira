use std::process::Command;

#[test]
fn version_cli_output_is_deterministic() {
    assert_deterministic_output(&["--version"]);
    assert_deterministic_output(&["--version", "--verbose"]);
    assert_deterministic_output(&["--version", "--json"]);
}

#[test]
fn version_cli_names_wire_identity_and_temporal_pins() {
    let output = String::from_utf8(run_tokeirad(&["--version"])).expect("UTF-8 version output");

    for value in [
        tokeira_build_info::SERVER_VERSION,
        tokeira_build_info::TOKEIRA_GIT_SHA,
        tokeira_build_info::TEMPORAL_PROTO_VERSION,
        tokeira_build_info::TEMPORAL_SERVER_COMPAT,
    ] {
        assert!(output.contains(value), "version output missing {value}");
    }
}

fn assert_deterministic_output(args: &[&str]) {
    let first = run_tokeirad(args);
    let second = run_tokeirad(args);

    assert_eq!(first, second, "version output changed for args {args:?}");
}

fn run_tokeirad(args: &[&str]) -> Vec<u8> {
    let output = Command::new(env!("CARGO_BIN_EXE_tokeirad"))
        .args(args)
        .output()
        .expect("tokeirad process should start");

    assert!(
        output.status.success(),
        "tokeirad {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}
