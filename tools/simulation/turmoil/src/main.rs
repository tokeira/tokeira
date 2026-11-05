//! Print the trace of one simulated run, for reading rather than asserting.
//!
//! Usage: `turmoil-sim [seed]`. The seed defaults to the one the tests use.

// A command-line probe: its output is the product, so stdout is intended.
#![allow(clippy::print_stdout)]

use anyhow::Context as _;

const DEFAULT_SEED: u64 = 0x7e_6b_1a;

fn main() -> anyhow::Result<()> {
    let seed = match std::env::args().nth(1) {
        Some(arg) => arg
            .parse()
            .with_context(|| format!("seed must be a whole number, got {arg}"))?,
        None => DEFAULT_SEED,
    };
    let trace = turmoil_sim::run_one_activity_workflow(seed)?;
    println!("seed {seed}");
    for step in &trace.steps {
        println!("{step}");
    }
    Ok(())
}
