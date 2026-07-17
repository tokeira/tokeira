//! Periodic disk hygiene for the multi-agent setup.
//!
//! Steady-state disk is: the main checkout's `target/`, one `target/` per live
//! worktree, and the kache store (LRU-capped). `tidy` trims the edges:
//!
//! 1. remove tkw worktrees whose branch is fully merged (their `target/` dirs
//!    are where the real gigabytes are) and prune stale worktree metadata;
//! 2. `cargo sweep` stale artifacts out of every live checkout — including
//!    *inside* Claude's and the ChatGPT app's managed worktrees, which is safe
//!    because sweeping deletes artifacts, never worktrees;
//! 3. `kache gc` the shared store (LRU to its cap, then an age pass);
//! 4. report what came back.
//!
//! Every step is best-effort: a missing optional tool is reported and skipped,
//! and one failure never aborts the rest. Safe to run while builds are live —
//! store GC takes kache's own cross-process lock.

use std::{path::Path, process::Command};

use anyhow::Result;

use crate::{repo::Repo, worktree};

pub(crate) fn run(sweep_days: u32, kache_age: &str) -> Result<()> {
    let repo = Repo::discover()?;

    println!("=== tkw tidy ===");

    println!("--- 1/4: sweeping merged tkw worktrees");
    if let Err(error) = worktree::clean_with(&repo, None) {
        println!("  (clean failed: {error})");
    }

    println!("--- 2/4: pruning stale build artifacts (cargo sweep, >{sweep_days}d)");
    if installed("cargo-sweep") {
        let days = sweep_days.to_string();
        sweep(&["--time", &days], &repo.main_root);
        for managed in [
            repo.worktrees_dir.clone(),
            repo.main_root.join(".claude").join("worktrees"),
            repo.codex_home.join("worktrees"),
        ] {
            if managed.is_dir() {
                sweep(&["--recursive", "--time", &days], &managed);
            }
        }
    } else {
        println!("  (cargo-sweep not installed — run: cargo install cargo-sweep)");
    }

    println!("--- 3/4: kache store GC");
    if installed("kache") {
        run_reporting("kache", &["gc"]);
        run_reporting("kache", &["gc", "--max-age", kache_age]);
    } else {
        println!("  (kache not installed)");
    }

    println!("--- 4/4: state");
    if installed("kache") {
        run_reporting("kache", &["stats"]);
    }
    let home = crate::repo::home_dir()?;
    let home_str = home.to_string_lossy().into_owned();
    run_reporting("df", &["-h", &home_str]);
    println!("=== done ===");
    Ok(())
}

fn installed(binary: &str) -> bool {
    // `--version` is the one flag every tool here supports without side
    // effects; spawn failure means "not on PATH".
    Command::new(binary)
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn sweep(args: &[&str], path: &Path) {
    let path_str = path.to_string_lossy().into_owned();
    let mut full: Vec<&str> = vec!["sweep"];
    full.extend(args);
    full.push(&path_str);
    run_reporting("cargo", &full);
}

/// Run a hygiene step with inherited stdio so its output lands in the report;
/// failures are noted and swallowed.
fn run_reporting(binary: &str, args: &[&str]) {
    match Command::new(binary).args(args).status() {
        Ok(status) if status.success() => {}
        Ok(status) => println!("  ({binary} {} exited with {status})", args.join(" ")),
        Err(error) => println!("  ({binary} {} failed to start: {error})", args.join(" ")),
    }
}
