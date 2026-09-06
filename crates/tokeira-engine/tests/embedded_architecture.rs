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
    let connector_iam = include_str!("../../tokeira-storage/tests/dsql_connector_iam.rs");
    assert!(live_aws.starts_with("#![cfg(feature = \"dsql-integration\")]"));
    assert!(sql_ownership.starts_with("#![cfg(feature = \"dsql-integration\")]"));
    assert_eq!(
        connector_iam.lines().next(),
        Some("#![cfg(feature = \"dsql-integration\")]")
    );
    assert!(live_aws.contains("#[ignore = \"creates and destroys a billable Aurora DSQL cluster"));
    assert!(live_aws.contains("TOKEIRA_LIVE_MANAGED_DSQL_ACK"));

    for source in [
        live_aws,
        sql_ownership,
        connector_iam,
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
// Feature: dsql-connector-sqlx-09, Property 1: one SQLx line and no legacy client
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
        ("sqlx", "0.9."),
        ("sqlx-core", "0.9."),
        ("sqlx-postgres", "0.9."),
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

    // The legacy AWS HTTPS client left with connector 0.2. Re-enabling
    // aws-smithy-runtime/tls-rustls must fail here on any future dependency move.
    // http 0.2 and http-body 0.4 remain SDK type dependencies, regardless of
    // HTTP client: aws-sdk-dsql 1.55.0 and aws-smithy-runtime 1.12.1 require
    // them unconditionally in their Cargo.toml files.
    for (name, legacy_line) in [
        ("hyper", "0.14."),
        ("h2", "0.3."),
        ("hyper-rustls", "0.24."),
        ("tokio-rustls", "0.24."),
        ("rustls", "0.21."),
        ("rustls-webpki", "0.101."),
        ("webpki-roots", "0.26."),
    ] {
        let present = versions_of(&packages, name)
            .iter()
            .any(|version| version.starts_with(legacy_line));
        assert!(
            !present,
            "{name} {legacy_line}x is in the lock; the legacy AWS client is back"
        );
    }
    assert_eq!(
        versions_of(&packages, "aurora-dsql-sqlx-connector"),
        ["0.2.2"]
    );
}

// Feature: dsql-connector-sqlx-09, Property 4: the log bridge is explicit and resolved
#[test]
fn connector_log_bridge_is_declared_and_resolved() {
    let manifest: toml::Value = toml::from_str(include_str!("../../../Cargo.toml"))
        .expect("workspace manifest remains valid TOML");
    let features = manifest["workspace"]["dependencies"]["tracing-subscriber"]["features"]
        .as_array()
        .expect("tracing-subscriber declares features");
    assert!(
        features
            .iter()
            .any(|feature| feature.as_str() == Some("tracing-log"))
    );

    let lock: toml::Value = toml::from_str(include_str!("../../../Cargo.lock"))
        .expect("workspace lock remains valid TOML");
    let packages = lock["package"].as_array().expect("lock contains packages");
    let subscriber = packages
        .iter()
        .find(|package| package["name"].as_str() == Some("tracing-subscriber"))
        .expect("lock resolves tracing-subscriber");
    assert!(
        subscriber["dependencies"]
            .as_array()
            .expect("subscriber dependencies")
            .iter()
            .any(|dependency| dependency
                .as_str()
                .is_some_and(|name| name.split_whitespace().next() == Some("tracing-log")))
    );
    assert_eq!(versions_of(&locked_packages(), "tracing-log").len(), 1);
}

#[test]
fn dynamic_sql_attestations_are_counted_and_explained() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut sources = Vec::new();
    for directory in [
        "crates/tokeira-storage/src",
        "crates/tokeira-projection/src",
        "apps/tkr/src",
    ] {
        collect_rust_sources(&root.join(directory), &mut sources);
    }
    let mut attestations = 0;
    for source in sources {
        let contents = fs::read_to_string(&source).expect("read SQL source");
        let lines: Vec<_> = contents.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if line.trim_start().starts_with("use ") || !line.contains("AssertSqlSafe(") {
                continue;
            }
            attestations += line.matches("AssertSqlSafe(").count();
            assert!(
                lines[index.saturating_sub(4)..index]
                    .iter()
                    .any(|previous| previous.trim_start().starts_with("// SQL safety:")),
                "{}:{} has an unexplained SQL attestation",
                source.display(),
                index + 1
            );
        }
    }
    assert_eq!(
        attestations, 11,
        "new dynamic SQL requires an explicit audit"
    );
}
