//! Structural assertions for the managed embedded DSQL crate boundaries.

use std::{fs, path::Path};

#[test]
fn kernel_dependency_and_source_surface_remains_pure() {
    let workspace_crates = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("engine crate has a crates parent");
    let kernel = workspace_crates.join("tokeira-kernel");
    let manifest = fs::read_to_string(kernel.join("Cargo.toml")).expect("read kernel manifest");
    for forbidden in [
        "tokio",
        "sqlx",
        "aws-sdk",
        "opentelemetry",
        "metrics",
        "dsql-integration",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "kernel manifest gained forbidden runtime surface {forbidden}"
        );
    }

    let mut sources = Vec::new();
    collect_rust_sources(&kernel.join("src"), &mut sources);
    for source in sources {
        let contents = fs::read_to_string(&source).expect("read kernel source");
        for forbidden in [
            "tokio::",
            "sqlx::",
            "aws_sdk_",
            "opentelemetry::",
            "metrics::",
            "tracing::",
        ] {
            assert!(
                !contents.contains(forbidden),
                "{} gained forbidden runtime surface {forbidden}",
                source.display()
            );
        }
    }
}

#[test]
fn credentialed_sql_and_aws_tests_are_non_default_and_sleep_free() {
    let engine_manifest: toml::Value =
        toml::from_str(include_str!("../Cargo.toml")).expect("engine manifest remains valid TOML");
    let defaults = engine_manifest["features"]["default"]
        .as_array()
        .expect("engine default feature list");
    assert!(
        defaults
            .iter()
            .all(|feature| feature.as_str() != Some("dsql-integration")),
        "credentialed DSQL integration must not be a default feature"
    );

    let live_aws = include_str!("live_managed_dsql.rs");
    let sql_ownership = include_str!("../../tokeira-storage/tests/dsql_embedded_ownership.rs");
    assert!(live_aws.starts_with("#![cfg(feature = \"dsql-integration\")]"));
    assert!(sql_ownership.starts_with("#![cfg(feature = \"dsql-integration\")]"));
    assert!(live_aws.contains("#[ignore = \"creates and destroys a billable Aurora DSQL cluster"));
    assert!(live_aws.contains("TOKEIRA_LIVE_MANAGED_DSQL_ACK"));

    for source in [
        live_aws,
        sql_ownership,
        include_str!("embedded_telemetry.rs"),
    ] {
        assert!(
            !source.contains("tokio::time::sleep") && !source.contains("std::thread::sleep"),
            "managed embedded integration tests must synchronize without sleeps"
        );
    }
}

fn collect_rust_sources(directory: &Path, sources: &mut Vec<std::path::PathBuf>) {
    for entry in fs::read_dir(directory).expect("read source directory") {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            collect_rust_sources(&path, sources);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            sources.push(path);
        }
    }
}
