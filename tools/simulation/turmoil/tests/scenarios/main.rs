//! The turmoil scenarios, one module per area of the engine they exercise.
//!
//! Each scenario builds a topology, gives it a workload, drives it with a
//! fault schedule and checks the trace against an oracle. They share one test
//! binary so the engine is linked once; nextest still runs each test in its
//! own process. Run one area with a filter, for instance
//! `cargo nextest run --test scenarios -E 'test(/^leases::/)'`.

mod dispatch;
mod harness;
mod leases;
mod outbound;
mod routing;

/// The seed every scenario runs with unless it says otherwise.
const SEED: u64 = 0x7e_6b_1a;

/// Engine logs, filtered by `RUST_LOG`, for diagnosing a failing run.
fn logs() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}
