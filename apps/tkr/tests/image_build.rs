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
