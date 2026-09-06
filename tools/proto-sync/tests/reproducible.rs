//! Running the pinned generator against the vendored protos reproduces the
//! checked-in bindings byte for byte; `check` regenerates into a scratch
//! directory and touches nothing under the repository.

use std::process::Command;

// Feature: tonic-0-14-grpc-stack, Property 12: regeneration is reproducible
#[test]
fn checked_in_bindings_match_the_pinned_generator() {
    let output = Command::new(env!("CARGO_BIN_EXE_proto-sync"))
        .arg("check")
        .output()
        .expect("run proto-sync check");
    assert!(
        output.status.success(),
        "proto-sync check failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
