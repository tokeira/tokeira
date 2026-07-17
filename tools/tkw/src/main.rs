//! `tkw` — Tokeira worktree and fleet-hygiene tool.
//!
//! One binary serving the concurrent-agents practice (docs/concurrent-agents.md):
//!
//! - `new`/`ls`/`rm`/`clean` manage **tkw-owned** agent worktrees at a sibling
//!   directory of the main checkout (for Kiro CLI and manual sessions; Claude
//!   Code and the ChatGPT app create and manage their own worktrees).
//! - `tidy` is the periodic disk-hygiene pass: sweep merged tkw worktrees,
//!   prune stale metadata, drop stale build artifacts, GC the kache store.
//! - `hook` implements the Claude Code / Kiro hook commands in Rust so the
//!   committed hook configuration stays a bare command with no shell logic.
//!
//! The ownership rule, stated once and enforced in code: tkw only ever
//! *removes* worktrees it created (under its own directory). Worktrees under
//! `.claude/worktrees/` and `$CODEX_HOME/worktrees/` have their own managed
//! lifecycles; tkw reports them and may sweep stale artifacts inside them, but
//! never removes them.

mod hook;
mod includes;
mod repo;
mod tidy;
mod worktree;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "tkw",
    about = "Tokeira worktree + fleet hygiene tool (see docs/concurrent-agents.md)",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create an agent worktree at the sibling worktree dir, on branch `agent/<name>`.
    New {
        /// Worktree name (also names the branch: `agent/<name>`).
        name: String,
        /// Base ref for the new branch. Defaults to the main checkout's HEAD.
        #[arg(long)]
        base: Option<String>,
    },
    /// List every worktree of the repository, annotated with its owner.
    Ls,
    /// Remove a tkw-owned worktree (refuses others; branch is kept).
    Rm {
        /// Worktree name under the tkw worktree directory.
        name: String,
        /// Remove even if the worktree has local changes.
        #[arg(long)]
        force: bool,
    },
    /// Remove tkw-owned worktrees whose branch is fully merged into the base.
    Clean {
        /// Merge base to test against. Defaults to the main checkout's current branch.
        #[arg(long)]
        base: Option<String>,
    },
    /// Periodic disk hygiene: clean + prune + cargo sweep + kache gc + report.
    Tidy {
        /// Remove build artifacts untouched for this many days (cargo sweep).
        #[arg(long, default_value_t = 14)]
        sweep_days: u32,
        /// Age bound passed to `kache gc --max-age`.
        #[arg(long, default_value = "14d")]
        kache_age: String,
    },
    /// Hook entry points invoked by Claude Code / Kiro hook configuration.
    #[command(subcommand)]
    Hook(HookCommand),
}

#[derive(Subcommand)]
enum HookCommand {
    /// Format the edited Rust file (hook context JSON on stdin). Never blocks.
    PostEdit,
    /// Finish-green gate: `cargo check --workspace`; exit 2 on failure.
    Stop,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::New { name, base } => worktree::new(&name, base.as_deref()),
        Command::Ls => worktree::ls(),
        Command::Rm { name, force } => worktree::rm(&name, force),
        Command::Clean { base } => worktree::clean(base.as_deref()),
        Command::Tidy {
            sweep_days,
            kache_age,
        } => tidy::run(sweep_days, &kache_age),
        Command::Hook(hook_command) => {
            // Hooks communicate through exit codes (Claude Code blocks a Stop on
            // exit 2), so they bypass `Result` and exit directly.
            let code = match hook_command {
                HookCommand::PostEdit => hook::post_edit(),
                HookCommand::Stop => hook::stop(),
            };
            std::process::exit(code);
        }
    }
}
