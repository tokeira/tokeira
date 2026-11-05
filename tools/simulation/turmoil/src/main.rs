//! Print the trace of one simulated run, for reading rather than asserting.
//!
//! Usage: `turmoil-sim [seed] [one-activity|timer]`. The seed defaults to the
//! one the scenarios use, and the workload to `one-activity`. Both run on the
//! single-node topology with no faults.

// A command-line probe: its output is the product, so stdout is intended.
#![allow(clippy::print_stdout)]

use anyhow::{Context as _, bail};
use turmoil_sim::{topology, workload};

const DEFAULT_SEED: u64 = 0x7e_6b_1a;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let seed = match args.next() {
        Some(arg) => arg
            .parse()
            .with_context(|| format!("seed must be a whole number, got {arg}"))?,
        None => DEFAULT_SEED,
    };
    let mut run = topology::single_node(seed);
    match args.next().as_deref() {
        None | Some("one-activity") => run.worker(workload::one_activity),
        Some("timer") => run.worker(workload::timer),
        Some(other) => bail!("the workload is one-activity or timer, got {other}"),
    }
    let trace = run.drive([])?;
    println!("seed {seed}");
    print!("{trace}");
    Ok(())
}
