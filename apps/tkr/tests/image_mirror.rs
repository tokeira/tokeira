//! Integration test: `tkr image mirror` against LocalStack ECR.
//!
//! Creates an ECS deployment in a sandboxed `XDG_STATE_HOME`, runs
//! `tkr image mirror` twice, and asserts:
//!
//! 1. The deployment's `deployment.toml` writeback is idempotent
//!    (second run produces byte-identical config).
//! 2. Every mirrored repository ends up in LocalStack ECR with a
//!    lifecycle policy attached.
//!
//! Gated behind the `integration-test` feature so unit-test runs don't
//! drag in LocalStack / AWS / Dagger. Invoke with:
//!   cargo test -p tkr --features integration-test -- --ignored mirrors_canonical_images_into_localstack_ecr

#[cfg(feature = "integration-test")]
mod integration {
    use std::{env, fs, process::Command};

    #[test]
    #[ignore = "requires LocalStack ECR, AWS endpoint configuration, and Dagger"]
    fn mirrors_canonical_images_into_localstack_ecr() {
        let endpoint =
            env::var("AWS_ENDPOINT_URL_ECR").expect("set AWS_ENDPOINT_URL_ECR for LocalStack ECR");
        let state = tempfile::tempdir().expect("creates temporary state directory");
        let xdg_state = state.path().join("xdg");
        let deployment = "image-mirror-localstack";

        let create = Command::new(env!("CARGO_BIN_EXE_tkr"))
            .env("XDG_STATE_HOME", &xdg_state)
            .args([
                "deployment",
                "create",
                "--name",
                deployment,
                "--platform",
                "ecs",
                "--storage",
                "in-memory",
            ])
            .status()
            .expect("creates ECS deployment");
        assert!(create.success());

        let first = Command::new(env!("CARGO_BIN_EXE_tkr"))
            .env("XDG_STATE_HOME", &xdg_state)
            .args(["--deployment", &deployment, "image", "mirror", "--yes"])
            .status()
            .expect("runs first tkr image mirror");
        assert!(first.success());

        let deployment_toml = xdg_state.join("tokeira/tkr/image-mirror-localstack/deployment.toml");
        let first_config = fs::read_to_string(&deployment_toml).expect("reads deployment.toml");

        let second = Command::new(env!("CARGO_BIN_EXE_tkr"))
            .env("XDG_STATE_HOME", &xdg_state)
            .args(["--deployment", &deployment, "image", "mirror", "--yes"])
            .status()
            .expect("runs second tkr image mirror");
        assert!(second.success());

        let second_config = fs::read_to_string(&deployment_toml).expect("reads deployment.toml");
        assert_eq!(first_config, second_config);

        for repo in [
            "tokeira/mimir",
            "tokeira/loki",
            "tokeira/grafana",
            "tokeira/alloy",
            "tokeira/aws-cli",
            "tokeira/busybox",
        ] {
            let describe = Command::new("aws")
                .args([
                    "--endpoint-url",
                    &endpoint,
                    "ecr",
                    "describe-repositories",
                    "--repository-names",
                    repo,
                ])
                .status()
                .expect("runs aws ecr describe-repositories");
            assert!(describe.success());

            let policy = Command::new("aws")
                .args([
                    "--endpoint-url",
                    &endpoint,
                    "ecr",
                    "get-lifecycle-policy",
                    "--repository-name",
                    repo,
                ])
                .status()
                .expect("runs aws ecr get-lifecycle-policy");
            assert!(policy.success());
        }
    }
}
