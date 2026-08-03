//! The shipped example definitions run end-to-end through the full pipeline.

use spike_monty_tkdp::{lower::LowerOptions, run};

fn plan(source: &str, label: &str) -> String {
    match run(source, label, &LowerOptions::default()) {
        Ok(Ok(outcome)) => outcome.value,
        Ok(Err(failure)) => panic!("execution failed:\n{failure}"),
        Err(diags) => panic!("preflight rejected: {diags:?}"),
    }
}

#[test]
fn compose_example_renders_in_memory_plan() {
    let value = plan(include_str!("../examples/compose.tkdp"), "compose.tkdp");
    for needle in [
        "'namespaces': ['default']",
        "'name': 'local_state'",
        "'value': 'LocalStateDir()'",
        "'name': 'tokeirad'",
        "Tokeirad(image='tokeirad:latest', replicas=1, grpc_port=7233, metrics_port=9090)",
        "Observability(mimir_image='grafana/mimir:3.0.6'",
    ] {
        assert!(value.contains(needle), "missing {needle} in:\n{value}");
    }
    // The in-memory variant must not have provisioned DSQL.
    assert!(!value.contains("'dsql'"), "{value}");
}

#[test]
fn managed_dsql_example_takes_the_guarded_case() {
    let value = plan(
        include_str!("../examples/managed-dsql.tkdp"),
        "managed-dsql.tkdp",
    );
    for needle in [
        "'name': 'dsql'",
        "DsqlCluster(region='eu-west-2', mode=DsqlMode(name='managed'))",
    ] {
        assert!(value.contains(needle), "missing {needle} in:\n{value}");
    }
    assert!(!value.contains("local_state"), "{value}");
}
