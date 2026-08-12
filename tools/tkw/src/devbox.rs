//! Remote build offload onto a Namespace Devbox.
//!
//! Owns the sync-and-run loop documented in docs/agents/namespace-devboxes.md:
//! rsync the *current worktree* to the box's persistent volume, execute cargo
//! there over plain SSH, stream output back, and propagate the exit code.
//! Compute moves; authority does not — remote runs produce verdicts and
//! diagnostics only. Artifacts are per-target-triple and never return to the
//! local `target/` or the kache store.
//!
//! Invariants:
//!
//! - The sync excludes are hardcoded, not configurable: `.env*` because
//!   `.worktreeinclude` deliberately copies gitignored secrets into every
//!   worktree and secrets never leave the machine (AGENTS.md §10.3); `.git`
//!   because a linked worktree's `.git` is a pointer file into the local
//!   common dir and is meaningless remotely; `target/` because artifacts are
//!   platform-local in both directions.
//! - Each worktree syncs to its own remote directory under `/workspaces`
//!   (the Devbox persistent volume — SSH sessions land there, and state under
//!   it survives stop/resume), so one box can serve the whole fleet without
//!   two agents clobbering each other's tree.
//! - The box is reached through the plain SSH host `<name>.devbox.namespace`
//!   that `devbox create`/`configure-ssh` writes into `~/.ssh/config`; this
//!   module does not depend on the `devbox` CLI being installed.

use std::{
    path::PathBuf,
    process::Command,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};

use crate::repo::git_capture_in;

/// Hygiene excludes applied to every sync. See the module invariants for the
/// reason each entry exists; do not make these configurable.
const SYNC_EXCLUDES: &[&str] = &["target/", ".env*", ".git"];

/// The §10.4 finish-green bar, in order (AGENTS.md §10.4). fmt runs in
/// `--check` form because the remote copy is a verification target, not the
/// editable tree — formatting mutations belong on the local side.
/// `{nightly}` is replaced with the dated nightly installed on the box, so the
/// toolchain pin keeps a single home (CI's `NIGHTLY_FMT_TOOLCHAIN`) and the
/// box simply mirrors it at provisioning time.
const BAR_STEPS: &[(&str, &str)] = &[
    ("fmt", "cargo +{nightly} fmt --all -- --check"),
    ("lint", "cargo lint --locked"),
    ("check", "cargo check --workspace --locked"),
    ("test", "cargo test --workspace --locked"),
    (
        "doc",
        "RUSTDOCFLAGS=\"-D warnings\" cargo doc --workspace --no-deps --locked",
    ),
];

/// A resolved sync/run target: which box, which local tree, which remote dir.
struct Target {
    /// SSH host from the Namespace-managed `~/.ssh/config` include.
    host: String,
    /// Root of the worktree tkw was invoked from (not the main checkout —
    /// each agent offloads its own tree).
    worktree_root: PathBuf,
    /// Per-worktree directory on the box's persistent volume.
    remote_dir: String,
}

impl Target {
    fn resolve(box_name: Option<&str>) -> Result<Self> {
        let box_name = match box_name {
            Some(name) => name.to_string(),
            None => std::env::var("TKW_DEVBOX").ok().unwrap_or_default(),
        };
        if box_name.is_empty() {
            bail!(
                "no devbox selected: pass --box <name> or set TKW_DEVBOX \
                 (the box name from `devbox create`, e.g. tok-bar-1)"
            );
        }
        let toplevel = git_capture_in(std::path::Path::new("."), &["rev-parse", "--show-toplevel"])
            .context("not inside a git worktree (tkw devbox syncs the current worktree)")?;
        let worktree_root = PathBuf::from(toplevel.trim());
        let name = worktree_root
            .file_name()
            .context("worktree root has no directory name")?
            .to_string_lossy()
            .into_owned();
        Ok(Self {
            host: format!("{box_name}.devbox.namespace"),
            worktree_root,
            remote_dir: format!("/workspaces/{name}"),
        })
    }
}

/// Sync the current worktree to its per-worktree directory on the box.
pub(crate) fn sync(box_name: Option<&str>) -> Result<()> {
    let target = Target::resolve(box_name)?;
    sync_target(&target)
}

/// Sync, then run `command` in the remote copy, streaming output.
/// Returns the remote exit code for main to propagate.
pub(crate) fn run(box_name: Option<&str>, command: &[String]) -> Result<i32> {
    let target = Target::resolve(box_name)?;
    sync_target(&target)?;
    let inner = command
        .iter()
        .map(|argument| shell_quote(argument))
        .collect::<Vec<_>>()
        .join(" ");
    ssh_stream(&target, &inner)
}

/// Sync, then run the §10.4 bar remotely, timing each step and stopping at
/// the first failure. Returns the failing step's exit code, or 0.
pub(crate) fn bar(box_name: Option<&str>) -> Result<i32> {
    let target = Target::resolve(box_name)?;
    sync_target(&target)?;

    let toolchains = ssh_capture(&target, "rustup toolchain list")?;
    let Some(nightly) = parse_nightly(&toolchains) else {
        bail!(
            "no nightly toolchain on {}: provision the box per \
             docs/agents/namespace-devboxes.md (rustup toolchain install <pinned nightly>)",
            target.host
        );
    };

    let mut timings: Vec<(&str, Duration)> = Vec::new();
    for (step_name, step_template) in BAR_STEPS {
        let script = step_template.replace("{nightly}", &nightly);
        println!("tkw devbox bar: {step_name} — {script}");
        let started = Instant::now();
        let code = ssh_stream(&target, &script)?;
        let elapsed = started.elapsed();
        timings.push((step_name, elapsed));
        if code != 0 {
            print_bar_summary(&timings, Some(step_name));
            return Ok(code);
        }
    }
    print_bar_summary(&timings, None);
    Ok(0)
}

fn sync_target(target: &Target) -> Result<()> {
    println!(
        "tkw devbox: syncing {} -> {}:{}",
        target.worktree_root.display(),
        target.host,
        target.remote_dir
    );
    let mut rsync = Command::new("rsync");
    rsync.arg("-a").arg("--delete");
    for exclude in SYNC_EXCLUDES {
        rsync.arg("--exclude").arg(exclude);
    }
    // Trailing slashes: sync the *contents* of the worktree into the remote
    // directory, creating it on first sync.
    rsync
        .arg(format!("{}/", target.worktree_root.display()))
        .arg(format!("{}:{}/", target.host, target.remote_dir));
    let status = rsync.status().context("failed to spawn rsync")?;
    if !status.success() {
        bail!("rsync to {} failed with {status}", target.host);
    }
    Ok(())
}

/// Wrap a remote command so it runs inside the synced tree with cargo on
/// PATH. Non-interactive SSH skips login profiles, so the cargo env file must
/// be sourced explicitly; both known layouts are tried (the Namespace base
/// image installs rustup system-wide under /usr/local, a stock rustup under
/// ~/.cargo) and a missing file is harmless.
fn remote_script(target: &Target, inner: &str) -> String {
    format!(
        "[ -f /usr/local/cargo/env ] && . /usr/local/cargo/env; \
         [ -f \"$HOME/.cargo/env\" ] && . \"$HOME/.cargo/env\"; \
         cd {} && {inner}",
        shell_quote(&target.remote_dir)
    )
}

/// Run a script on the box, streaming stdout/stderr to the user.
/// Exit 255 is ssh's own transport failure and becomes an error with
/// remediation; every other code is the remote command's verdict.
fn ssh_stream(target: &Target, inner: &str) -> Result<i32> {
    let status = Command::new("ssh")
        .arg("-o")
        .arg("BatchMode=yes")
        .arg(&target.host)
        .arg(remote_script(target, inner))
        .status()
        .context("failed to spawn ssh")?;
    let code = status.code().unwrap_or(1);
    if code == 255 {
        bail!(
            "ssh to {} failed — does the devbox exist, and has `devbox create` \
             (or `devbox configure-ssh`) written it into ~/.ssh/config?",
            target.host
        );
    }
    Ok(code)
}

/// Run a script on the box, capturing stdout (the transport banner goes to
/// stderr and is passed through).
fn ssh_capture(target: &Target, inner: &str) -> Result<String> {
    let output = Command::new("ssh")
        .arg("-o")
        .arg("BatchMode=yes")
        .arg(&target.host)
        .arg(remote_script(target, inner))
        .output()
        .context("failed to spawn ssh")?;
    if !output.status.success() {
        bail!(
            "ssh {} failed: {}",
            target.host,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// POSIX single-quote escaping: the only metacharacter inside single quotes
/// is the quote itself, closed-escaped-reopened as `'\''`.
fn shell_quote(argument: &str) -> String {
    format!("'{}'", argument.replace('\'', "'\\''"))
}

/// Pick the dated nightly from `rustup toolchain list` output. Lines look
/// like `nightly-2026-06-16-x86_64-unknown-linux-gnu (active)`; the first
/// whitespace-delimited token is a valid `cargo +<toolchain>` argument.
fn parse_nightly(toolchain_list: &str) -> Option<String> {
    toolchain_list
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("nightly-"))
        .filter_map(|line| line.split_whitespace().next())
        .map(str::to_string)
        .next()
}

fn print_bar_summary(timings: &[(&str, Duration)], failed: Option<&str>) {
    println!("tkw devbox bar:");
    let mut total = Duration::ZERO;
    for (step_name, elapsed) in timings {
        total += *elapsed;
        let verdict = if failed == Some(*step_name) {
            "FAILED"
        } else {
            "ok"
        };
        println!(
            "  {step_name:<6} {:>8}  {verdict}",
            format_duration(*elapsed)
        );
    }
    println!("  total  {:>8}", format_duration(total));
}

/// Human wall-clock: `41.3s` under a minute, `4m16s` above.
fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs_f64();
    if seconds < 60.0 {
        format!("{seconds:.1}s")
    } else {
        let minutes = (seconds / 60.0).floor() as u64;
        let rest = (seconds - (minutes as f64) * 60.0).round() as u64;
        format!("{minutes}m{rest:02}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_quote_wraps_and_escapes() {
        assert_eq!(shell_quote("plain"), "'plain'");
        assert_eq!(shell_quote(""), "''");
        assert_eq!(shell_quote("a b"), "'a b'");
        assert_eq!(shell_quote("it's"), "'it'\\''s'");
    }

    #[test]
    fn parse_nightly_finds_dated_toolchain() {
        let listing = "1.96-x86_64-unknown-linux-gnu (active, default)\n\
                       nightly-2026-06-16-x86_64-unknown-linux-gnu\n";
        assert_eq!(
            parse_nightly(listing),
            Some("nightly-2026-06-16-x86_64-unknown-linux-gnu".to_string())
        );
    }

    #[test]
    fn parse_nightly_ignores_stable_only_listings() {
        assert_eq!(
            parse_nightly("1.96-x86_64-unknown-linux-gnu (default)\n"),
            None
        );
    }

    #[test]
    fn parse_nightly_strips_annotations() {
        let listing = "nightly-2026-06-16-x86_64-unknown-linux-gnu (active)\n";
        assert_eq!(
            parse_nightly(listing),
            Some("nightly-2026-06-16-x86_64-unknown-linux-gnu".to_string())
        );
    }

    #[test]
    fn format_duration_switches_units_at_a_minute() {
        assert_eq!(format_duration(Duration::from_millis(500)), "0.5s");
        assert_eq!(format_duration(Duration::from_secs(59)), "59.0s");
        assert_eq!(format_duration(Duration::from_secs(256)), "4m16s");
    }
}
