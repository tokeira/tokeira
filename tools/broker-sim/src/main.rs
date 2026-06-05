//! Delivery-broker simulator entry point.
//!
//! Parses the shared `placement-sim` CLI vocabulary plus a `--bug=<name>` flag,
//! then runs the bounded-exhaustive checker and/or the seeded stress simulator,
//! aggregating into a harness `Report`. Exits non-zero on any failure. See
//! `.kiro/specs/delivery-broker-simulator/` for the design and invariants.

use sim_harness::{cli, run_seed, CliSpec, Report};

use broker_sim::{
    bug::InjectedBug, exhaustive, invariants, model::BrokerCfg, model_machine::BrokerModel,
};

fn main() {
    let spec = CliSpec {
        extra_flags: vec![],
        extra_value_flags: vec!["--bug"],
    };
    let args = cli::parse(&spec);
    let bug = args
        .values
        .get("bug")
        .and_then(|v| InjectedBug::from_flag(v));
    if args.values.contains_key("bug") && bug.is_none() {
        eprintln!(
            "unknown --bug value; expected token-before-commit | drop-expired-sticky | no-dedup-on-republish"
        );
        std::process::exit(2);
    }

    let mut failed = false;

    if args.run_exhaustive {
        match exhaustive::run_with_bug(bug, args.exhaustive_depth) {
            Ok(report) => {
                println!("bounded exhaustive checker: ok");
                println!("  depth:             {}", args.exhaustive_depth);
                println!("  states explored:   {}", report.states_explored);
                println!("  transitions tried: {}", report.transitions_tried);
            }
            Err(ce) => {
                // A counterexample is the SUCCESS case when a bug is injected.
                if let Some(bug) = bug {
                    println!(
                        "bounded exhaustive checker: bug correctly falsified (expected {})",
                        bug.expected_violation()
                    );
                    print!("{ce}");
                } else {
                    eprint!("{ce}");
                    failed = true;
                }
            }
        }
    }

    if args.run_stress {
        let registry = invariants::registry();
        let mut report = Report::new(registry.initial_outcomes());
        for seed in 1..=args.seeds {
            let model = BrokerModel::new(BrokerCfg::default(), args.ops, args.time_ms, bug);
            let seed_report = run_seed(model, seed, args.time_ms, &registry, args.verbose);
            report.add_seed(seed_report);
        }
        report.print("seeded stress simulator:");
        if bug.is_none() && !report.overall_passed() {
            failed = true;
        }
    }

    if bug.is_some() {
        println!(
            "note: --bug was set; a falsification above is the expected outcome, not a regression"
        );
    }

    if failed {
        std::process::exit(1);
    }
}
