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

/// Every `(name, version)` pair the workspace lock resolves.
fn locked_packages() -> Vec<(String, String)> {
    let lock = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../Cargo.lock")
            .canonicalize()
            .expect("workspace lock path resolves"),
    )
    .expect("read the workspace lock");
    let mut packages = Vec::new();
    let mut name = None;
    for line in lock.lines() {
        if line == "[[package]]" {
            name = None;
        } else if let Some(value) = line.strip_prefix("name = ") {
            name = Some(value.trim_matches('"').to_owned());
        } else if let Some(value) = line.strip_prefix("version = ")
            && let Some(name) = name.take()
        {
            packages.push((name, value.trim_matches('"').to_owned()));
        }
    }
    packages
}

fn versions_of<'a>(packages: &'a [(String, String)], name: &str) -> Vec<&'a str> {
    packages
        .iter()
        .filter(|(candidate, _)| candidate == name)
        .map(|(_, version)| version.as_str())
        .collect()
}

// Feature: tonic-0-14-grpc-stack, Property 1: one stack in the lock
#[test]
fn workspace_resolves_one_grpc_and_http_stack() {
    let packages = locked_packages();

    // Exactly one version each, on the target line.
    for (name, line) in [
        ("tonic", "0.14."),
        ("prost", "0.14."),
        ("prost-types", "0.14."),
        ("hyper-util", "0.1."),
        ("tower-http", "0.6."),
    ] {
        let versions = versions_of(&packages, name);
        assert_eq!(
            versions.len(),
            1,
            "{name} must resolve to exactly one version, found {versions:?}"
        );
        assert!(
            versions[0].starts_with(line),
            "{name} must be on the {line}x line, found {}",
            versions[0]
        );
    }

    // Nothing from the legacy stack remains.
    for (name, legacy_line) in [
        ("tonic", "0.11."),
        ("tonic-web", "0.11."),
        ("tonic-reflection", "0.11."),
        ("tonic-build", "0.11."),
        ("prost", "0.12."),
        ("prost-types", "0.12."),
        ("prost-reflect", "0.12."),
        ("axum", "0.6."),
        ("tower-http", "0.4."),
        ("hyper-timeout", "0.4."),
    ] {
        assert!(
            !versions_of(&packages, name)
                .iter()
                .any(|version| version.starts_with(legacy_line)),
            "{name} {legacy_line}x must not be in the lock"
        );
    }

    // The AWS SDK's legacy HTTPS client keeps hyper 0.14 (with its http 0.2
    // and http-body 0.4), h2 0.3, hyper-rustls 0.24, and rustls 0.21 alive
    // only while the DSQL connector is pinned below 0.2, because that
    // connector takes `aws-sdk-dsql`'s default features. Once the connector
    // moves, this exception ends and all six must be gone.
    let connector_below_0_2 = versions_of(&packages, "aurora-dsql-sqlx-connector")
        .iter()
        .any(|version| version.starts_with("0.1."));
    for (name, legacy_line) in [
        ("hyper", "0.14."),
        ("http", "0.2."),
        ("http-body", "0.4."),
        ("h2", "0.3."),
        ("hyper-rustls", "0.24."),
        ("rustls", "0.21."),
    ] {
        let present = versions_of(&packages, name)
            .iter()
            .any(|version| version.starts_with(legacy_line));
        assert!(
            !present || connector_below_0_2,
            "{name} {legacy_line}x is in the lock without the DSQL connector exception"
        );
    }
}
