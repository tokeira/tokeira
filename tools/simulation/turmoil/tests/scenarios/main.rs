//! The turmoil scenarios, one module per area of the engine they exercise.
//!
//! Each scenario builds a topology, gives it a workload, drives it with a
//! fault schedule and checks the trace against an oracle. They share one test
//! binary so the engine is linked once; nextest still runs each test in its
//! own process. Run one area with a filter, for instance
//! `cargo nextest run --test scenarios -E 'test(/^leases::/)'`, and every
//! scenario on another seed with `TURMOIL_SEED`.

use std::env;

mod dispatch;
mod harness;
mod leases;
mod outbound;
mod routing;

/// The seed scenarios run with when `TURMOIL_SEED` is unset.
const DEFAULT_SEED: u64 = 0x7e_6b_1a;

/// The seed every scenario runs with: `TURMOIL_SEED`, a whole number, or
/// [`DEFAULT_SEED`]. A sweep runs the scenarios once per seed, so an
/// assertion that holds only for the default seed shows up as a failing seed.
fn seed() -> u64 {
    match env::var("TURMOIL_SEED") {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("TURMOIL_SEED must be a whole number, got {value}")),
        Err(env::VarError::NotPresent) => DEFAULT_SEED,
        Err(error) => panic!("TURMOIL_SEED is unreadable: {error}"),
    }
}

/// Engine logs, filtered by `RUST_LOG`, for diagnosing a failing run.
fn logs() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}
