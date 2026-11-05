//! Print the trace of one simulated run, for reading rather than asserting.
//!
//! Usage: `turmoil-sim [seed] [timer]`. The seed defaults to the one the tests
//! use. With `timer`, it runs step three's timer workflow; otherwise step
//! one's activity workflow.

// A command-line probe: its output is the product, so stdout is intended.
#![allow(clippy::print_stdout)]

use anyhow::{Context as _, bail};

const DEFAULT_SEED: u64 = 0x7e_6b_1a;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let seed = match args.next() {
        Some(arg) => arg
            .parse()
            .with_context(|| format!("seed must be a whole number, got {arg}"))?,
        None => DEFAULT_SEED,
    };
    let trace = match args.next().as_deref() {
        None => turmoil_sim::run_one_activity_workflow(seed)?,
        Some("timer") => turmoil_sim::timer::run_timer_workflow(seed)?,
        Some(other) => bail!("the scenario is `timer` or nothing, got {other}"),
    };
    println!("seed {seed}");
    for step in &trace.steps {
        println!("{step}");
    }
    Ok(())
}
