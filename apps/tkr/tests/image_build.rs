//! Integration test: `tkr image build` end-to-end.
//!
//! Runs the compiled `tkr` binary against a live Dagger session and a
//! local Docker daemon, then confirms the resulting image exists. Gated
//! behind the `integration-test` feature so unit-test runs on CI don't
//! require either dependency.
//!
//! Invoke with:
//!   cargo test -p tkr --features integration-test -- --ignored builds_tokeirad_image_end_to_end

#[cfg(feature = "integration-test")]
mod integration {
    use std::process::Command;

    #[test]
    #[ignore = "requires Dagger and a local Docker daemon"]
    fn builds_tokeirad_image_end_to_end() {
        let build = Command::new(env!("CARGO_BIN_EXE_tkr"))
            .args(["image", "build"])
            .status()
            .expect("runs tkr image build");
        assert!(build.success());

        let inspect = Command::new("docker")
            .args(["image", "inspect", "tokeirad:latest"])
            .status()
            .expect("runs docker image inspect");
        assert!(inspect.success());
    }
}
