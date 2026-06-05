//! Shared CLI scaffolding matching the `placement-sim` flag vocabulary.
//!
//! Parsing is hand-rolled (no `clap`) so the harness keeps the zero-dependency
//! posture `placement-sim` established. A consuming simulator declares any
//! extra flags (e.g. a buggy-mode flag) via [`CliSpec`]; unknown flags panic
//! with a message, exactly as `placement-sim` does.

use std::collections::HashSet;

/// Declares simulator-specific flags beyond the shared set. Extra flags are
/// treated as booleans (presence = set) and surfaced in [`CliArgs::flags`].
#[derive(Clone, Debug, Default)]
pub struct CliSpec {
    /// Boolean flags the model recognises, e.g. `"--bug=token-before-commit"`
    /// is handled as a value flag separately; simple presence flags go here.
    pub extra_flags: Vec<&'static str>,
    /// Value-bearing flags the model recognises, e.g. `"--bug"`.
    pub extra_value_flags: Vec<&'static str>,
}

/// Parsed shared arguments plus any model-specific flags that were set.
#[derive(Clone, Debug)]
pub struct CliArgs {
    /// Number of seeds for stress mode.
    pub seeds: u64,
    /// Events/operations per seed.
    pub ops: usize,
    /// Simulated time bound in milliseconds.
    pub time_ms: u64,
    /// Emit a per-event trace.
    pub verbose: bool,
    /// Maximum depth for the exhaustive checker.
    pub exhaustive_depth: usize,
    /// Whether to run the seeded stress mode.
    pub run_stress: bool,
    /// Whether to run the bounded-exhaustive mode.
    pub run_exhaustive: bool,
    /// Model-specific presence flags that were set (without the `--`).
    pub flags: HashSet<String>,
    /// Model-specific value flags that were set: name (without `--`) → value.
    pub values: std::collections::BTreeMap<String, String>,
}

impl Default for CliArgs {
    fn default() -> Self {
        Self {
            seeds: 250,
            ops: 800,
            time_ms: 6_000,
            verbose: false,
            exhaustive_depth: 12,
            run_stress: true,
            run_exhaustive: true,
            flags: HashSet::new(),
            values: std::collections::BTreeMap::new(),
        }
    }
}

/// Parse process args (skipping argv[0]) against the shared vocabulary plus the
/// model's extra flags. Panics on an unrecognised flag or a missing value.
pub fn parse(spec: &CliSpec) -> CliArgs {
    parse_from(std::env::args().skip(1), spec)
}

/// Testable core: parse an arbitrary argument iterator.
pub fn parse_from<I>(args: I, spec: &CliSpec) -> CliArgs
where
    I: IntoIterator<Item = String>,
{
    let args: Vec<String> = args.into_iter().collect();
    let mut out = CliArgs::default();
    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        match arg {
            "--verbose" => out.verbose = true,
            "--random-only" => out.run_exhaustive = false,
            "--exhaustive-only" => out.run_stress = false,
            "--seeds" => {
                out.seeds = take_value(&args, &mut i, "--seeds")
                    .parse()
                    .expect("--seeds integer")
            }
            "--ops" => {
                out.ops = take_value(&args, &mut i, "--ops")
                    .parse()
                    .expect("--ops integer")
            }
            "--time-ms" => {
                out.time_ms = take_value(&args, &mut i, "--time-ms")
                    .parse()
                    .expect("--time-ms integer")
            }
            "--exhaustive-depth" => {
                out.exhaustive_depth = take_value(&args, &mut i, "--exhaustive-depth")
                    .parse()
                    .expect("--exhaustive-depth integer")
            }
            other => {
                // Try `--name=value` form against the model's value flags first.
                if let Some((name, value)) =
                    other.strip_prefix("--").and_then(|r| r.split_once('='))
                {
                    if spec
                        .extra_value_flags
                        .contains(&format!("--{name}").as_str())
                    {
                        out.values.insert(name.to_string(), value.to_string());
                        i += 1;
                        continue;
                    }
                }
                // Then `--name value` form.
                if spec.extra_value_flags.contains(&other) {
                    let value = take_value(&args, &mut i, other);
                    let name = other.trim_start_matches("--").to_string();
                    out.values.insert(name, value);
                    i += 1;
                    continue;
                }
                // Then a model presence flag.
                if spec.extra_flags.contains(&other) {
                    out.flags.insert(other.trim_start_matches("--").to_string());
                } else {
                    panic!("unknown argument: {other}");
                }
            }
        }
        i += 1;
    }
    out
}

fn take_value(args: &[String], i: &mut usize, flag: &str) -> String {
    *i += 1;
    args.get(*i)
        .unwrap_or_else(|| panic!("{flag} requires a value"))
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> CliSpec {
        CliSpec {
            extra_flags: vec!["--no-warmup"],
            extra_value_flags: vec!["--bug"],
        }
    }

    fn args(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn defaults_match_placement_sim() {
        let a = parse_from(args(&[]), &spec());
        assert_eq!(a.seeds, 250);
        assert_eq!(a.ops, 800);
        assert_eq!(a.time_ms, 6_000);
        assert_eq!(a.exhaustive_depth, 12);
        assert!(a.run_stress && a.run_exhaustive);
    }

    #[test]
    fn parses_shared_flags_and_mode_toggles() {
        let a = parse_from(
            args(&[
                "--seeds",
                "10",
                "--ops",
                "50",
                "--time-ms",
                "1000",
                "--random-only",
                "--verbose",
            ]),
            &spec(),
        );
        assert_eq!(a.seeds, 10);
        assert_eq!(a.ops, 50);
        assert_eq!(a.time_ms, 1000);
        assert!(a.run_stress && !a.run_exhaustive);
        assert!(a.verbose);
    }

    #[test]
    fn exhaustive_only_disables_stress() {
        let a = parse_from(
            args(&["--exhaustive-only", "--exhaustive-depth", "9"]),
            &spec(),
        );
        assert!(!a.run_stress && a.run_exhaustive);
        assert_eq!(a.exhaustive_depth, 9);
    }

    #[test]
    fn model_presence_and_value_flags() {
        let a = parse_from(
            args(&["--no-warmup", "--bug", "token-before-commit"]),
            &spec(),
        );
        assert!(a.flags.contains("no-warmup"));
        assert_eq!(
            a.values.get("bug").map(String::as_str),
            Some("token-before-commit")
        );
    }

    #[test]
    fn value_flag_equals_form() {
        let a = parse_from(args(&["--bug=drop-expired-sticky"]), &spec());
        assert_eq!(
            a.values.get("bug").map(String::as_str),
            Some("drop-expired-sticky")
        );
    }

    #[test]
    #[should_panic(expected = "unknown argument")]
    fn unknown_flag_panics() {
        parse_from(args(&["--nope"]), &spec());
    }
}
