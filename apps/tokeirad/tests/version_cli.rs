use std::process::Command;

#[test]
fn version_cli_output_is_deterministic() {
    assert_deterministic_output(&["--version"]);
    assert_deterministic_output(&["--version", "--verbose"]);
    assert_deterministic_output(&["--version", "--json"]);
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
