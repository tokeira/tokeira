//! `tkdp` — spike CLI over the `.tkdp` pipeline.
//!
//! ```text
//! tkdp check <file>                      preflight only
//! tkdp lower <file> [--faithful-exhaustion]   print the generated program
//! tkdp run   <file> [--faithful-exhaustion] [--show-generated]
//! ```
//!
//! Exit codes: 0 success, 1 definition rejected or failed, 2 usage.

// CLI binary: terminal output is the product (same allowance the workspace
// grants its CLI crates).
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::{path::PathBuf, process::ExitCode};

use spike_monty_tkdp::{build_program, check, diagnostics, lower::LowerOptions, run};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match parse_args(&args) {
        Ok(cmd) => dispatch(cmd),
        Err(msg) => {
            eprintln!("{msg}");
            eprintln!(
                "usage: tkdp <check|lower|run> <file.tkdp> [--faithful-exhaustion] [--show-generated]"
            );
            ExitCode::from(2)
        }
    }
}

struct Cmd {
    verb: Verb,
    file: PathBuf,
    faithful_exhaustion: bool,
    show_generated: bool,
}

enum Verb {
    Check,
    Lower,
    Run,
}

fn parse_args(args: &[String]) -> Result<Cmd, String> {
    let mut verb = None;
    let mut file = None;
    let mut faithful_exhaustion = false;
    let mut show_generated = false;
    for arg in args {
        match arg.as_str() {
            "check" | "lower" | "run" if verb.is_none() => {
                verb = Some(match arg.as_str() {
                    "check" => Verb::Check,
                    "lower" => Verb::Lower,
                    _ => Verb::Run,
                });
            }
            "--faithful-exhaustion" => faithful_exhaustion = true,
            "--show-generated" => show_generated = true,
            other if !other.starts_with('-') && file.is_none() => {
                file = Some(PathBuf::from(other));
            }
            other => return Err(format!("unexpected argument `{other}`")),
        }
    }
    Ok(Cmd {
        verb: verb.ok_or("missing subcommand")?,
        file: file.ok_or("missing definition file")?,
        faithful_exhaustion,
        show_generated,
    })
}

fn dispatch(cmd: Cmd) -> ExitCode {
    let label = cmd.file.display().to_string();
    let source = match std::fs::read_to_string(&cmd.file) {
        Ok(source) => source,
        Err(err) => {
            eprintln!("error: cannot read {label}: {err}");
            return ExitCode::from(1);
        }
    };
    let options = LowerOptions {
        strict_exhaustion: !cmd.faithful_exhaustion,
    };

    match cmd.verb {
        Verb::Check => match check(&source) {
            Ok(preflight) => {
                let e = preflight.entrypoints;
                println!(
                    "ok: {label} (config: {}, deployment: {})",
                    present(e.has_config),
                    present(e.has_deployment)
                );
                ExitCode::SUCCESS
            }
            Err(diags) => {
                eprint!("{}", diagnostics::render(&label, &source, &diags));
                ExitCode::from(1)
            }
        },
        Verb::Lower => match build_program(&source, &label, &options) {
            Ok(program) => {
                print!("{}", program.text);
                ExitCode::SUCCESS
            }
            Err(diags) => {
                eprint!("{}", diagnostics::render(&label, &source, &diags));
                ExitCode::from(1)
            }
        },
        Verb::Run => {
            if cmd.show_generated {
                match build_program(&source, &label, &options) {
                    Ok(program) => {
                        println!("--- generated program ---");
                        print!("{}", program.text);
                        println!("--- end generated program ---");
                    }
                    Err(diags) => {
                        eprint!("{}", diagnostics::render(&label, &source, &diags));
                        return ExitCode::from(1);
                    }
                }
            }
            match run(&source, &label, &options) {
                Ok(Ok(outcome)) => {
                    if !outcome.output.is_empty() {
                        print!("{}", outcome.output);
                    }
                    println!("{}", outcome.value);
                    ExitCode::SUCCESS
                }
                Ok(Err(failure)) => {
                    eprint!("{failure}");
                    ExitCode::from(1)
                }
                Err(diags) => {
                    eprint!("{}", diagnostics::render(&label, &source, &diags));
                    ExitCode::from(1)
                }
            }
        }
    }
}

fn present(present: bool) -> &'static str {
    if present { "yes" } else { "no" }
}
