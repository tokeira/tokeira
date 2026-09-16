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
//!   that `devbox create`/`configure-ssh` writes into `~/.ssh/config`. The
//!   data-plane commands — sync, run, bar, markers — therefore need no
//!   `devbox` CLI. Only `down` does, because stopping a box is a control-plane
//!   operation with no SSH equivalent; it degrades to printing the command
//!   when the CLI is absent rather than making the rest of the module depend
//!   on it.
//! - Activity markers are owned by [`Marker`] and pool leases by [`Lease`],
//!   not by the caller's discipline. Both keep a remote resource alive for as
//!   long as they exist, so their lifetimes belong to guards that cannot
//!   forget rather than to conventions that can.

use std::{
    path::PathBuf,
    process::{Command, Output},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};

use crate::repo::git_capture_in;

/// Hygiene excludes applied to every sync. See the module invariants for the
/// reason each entry exists; do not make these configurable.
const SYNC_EXCLUDES: &[&str] = &["target/", ".env*", ".git"];

/// Namespace's activity directory: a Devbox counts as busy, and so is kept
/// running, while any file exists here.
/// <https://namespace.so/docs/guides/devbox/long-running-tasks>
const MARKER_DIR: &str = "/.namespace/tasks";

/// Prefix identifying a marker this tool owns. Both the names it writes and
/// the sweep it runs are scoped to it, so a marker created by hand or by
/// another tool is never swept — the rule is to remove only your own exact
/// file.
const MARKER_PREFIX: &str = "tkw-";

/// Age at which an unrefreshed `tkw-` marker is treated as leaked and swept.
///
/// A guard covers every exit path this process can observe, but not the ones
/// it cannot: SIGKILL, the host sleeping, a session ending mid-run. A marker
/// left by one of those would hold its box awake indefinitely, so the bound
/// exists to make that finite rather than to be accurate. It must therefore
/// exceed the longest step that cannot refresh itself — `bar` re-touches per
/// step, but a single `devbox run` may execute for hours.
const MARKER_STALE_MINUTES: u32 = 240;

/// Create-time shape of a pooled box, matching the recipe in
/// docs/agents/namespace-devboxes.md. Deliberately not configurable: a pool
/// whose members differ in size or volume gives runs whose timing depends on
/// which box they happened to land on. A one-off that needs a different shape
/// is created by hand and addressed with `--box`.
const POOL_SIZE: &str = "m";
const POOL_VOLUME_GB: &str = "100";
const POOL_IDLE_TIMEOUT: &str = "15m";
const POOL_IMAGE: &str = "builtin:base";

/// Age at which a held lease is assumed stranded and released.
///
/// A lease covers one run, and nothing releases it if the process dies
/// unobserved. A stranded lease does not keep its box awake — that is the
/// marker's job — but `devbox acquire` skips a box whose lease is still held,
/// so the next run builds *another* box and the pool grows without bound. Same
/// bound and same reasoning as [`MARKER_STALE_MINUTES`].
const LEASE_STALE_MINUTES: i64 = 240;

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
    // nextest runs one process per test: cross-test races on process-global
    // state (tracing's callsite interest cache being the diagnosed case —
    // tracing-span-lifecycle-hygiene spec, Req 5) are structurally
    // impossible, where `cargo test`'s in-process parallelism made them a
    // per-run coin flip on high-core-count boxes.
    ("test", "cargo nextest run --workspace --locked"),
    // nextest does not run doctests; they keep their own step.
    ("doctest", "cargo test --workspace --doc --locked"),
    (
        "doc",
        "RUSTDOCFLAGS=\"-D warnings\" cargo doc --workspace --no-deps --locked",
    ),
];

/// Resolve the bare box name, independent of any worktree. The control plane
/// (`devbox shutdown`) addresses a box by this name, where SSH addresses it by
/// the derived host — conflating the two is a live mistake, so they are
/// separate functions.
fn resolve_box(box_name: Option<&str>) -> Result<String> {
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
    Ok(box_name)
}

/// Resolve the SSH host for a box. Marker inspection and every data-plane
/// command address a box this way, from any directory.
fn resolve_host(box_name: Option<&str>) -> Result<String> {
    Ok(format!("{}.devbox.namespace", resolve_box(box_name)?))
}

/// A resolved sync/run target: which box, which local tree, which remote dir.
struct Target {
    /// SSH host from the Namespace-managed `~/.ssh/config` include.
    host: String,
    /// Root of the worktree tkw was invoked from (not the main checkout —
    /// each agent offloads its own tree).
    worktree_root: PathBuf,
    /// Directory name of the worktree; also names this run's activity marker.
    worktree_name: String,
    /// Per-worktree directory on the box's persistent volume.
    remote_dir: String,
}

impl Target {
    fn resolve(box_name: Option<&str>) -> Result<Self> {
        let host = resolve_host(box_name)?;
        let toplevel = git_capture_in(std::path::Path::new("."), &["rev-parse", "--show-toplevel"])
            .context("not inside a git worktree (tkw devbox syncs the current worktree)")?;
        let worktree_root = PathBuf::from(toplevel.trim());
        let worktree_name = worktree_root
            .file_name()
            .context("worktree root has no directory name")?
            .to_string_lossy()
            .into_owned();
        Ok(Self {
            host,
            remote_dir: format!("/workspaces/{worktree_name}"),
            worktree_root,
            worktree_name,
        })
    }
}

/// Shell that claims this run's marker, sweeping leaked ones on the way in.
/// Both happen in one round trip, so the sweep adds no latency of its own.
fn claim_script(path: &str) -> String {
    format!(
        "find {dir} -maxdepth 1 -type f -name {pattern} -mmin +{MARKER_STALE_MINUTES} \
         -print -delete; touch {path}",
        dir = shell_quote(MARKER_DIR),
        pattern = shell_quote(&format!("{MARKER_PREFIX}*")),
        path = shell_quote(path),
    )
}

/// Shell that releases a marker. `-f` because the goal is the file's absence,
/// not a successful unlink — a marker already swept is a success, not an error.
fn release_script(path: &str) -> String {
    format!("rm -f {}", shell_quote(path))
}

/// An activity marker on a box, owned for the lifetime of one run.
///
/// Namespace treats a Devbox as busy while any file exists under
/// [`MARKER_DIR`], which is what keeps a box alive across the gaps between a
/// bar's separate SSH invocations. The hazard is the marker outliving the run:
/// the box then stays awake indefinitely, and nothing in the protocol detects
/// the omission.
///
/// Ownership therefore lives in the type rather than in the caller's care:
///
/// - [`Drop`] releases the marker on every exit path this process can see —
///   success, error, panic — and reports a failed release loudly, since a
///   release that fails silently is indistinguishable from one that worked.
/// - [`Marker::claim`] sweeps `tkw-` markers older than
///   [`MARKER_STALE_MINUTES`] first, so the next run on a box repairs the
///   previous one's leak. That covers what `Drop` structurally cannot: a
///   SIGKILL, a sleeping host, a session abandoned mid-run.
/// - The [`MARKER_PREFIX`] scopes both the name and the sweep, so a marker
///   made by hand or by another agent is never swept.
struct Marker {
    host: String,
    path: String,
}

impl Marker {
    /// Sweep leaked markers, then claim this run's own.
    ///
    /// Best effort by design: a box that refuses the marker still runs the
    /// work, since failing the run would turn a bounded risk into a certain
    /// one. The warning carries the diagnosis instead.
    fn claim(target: &Target) -> Option<Self> {
        // Worktree plus pid: one box may serve several worktrees at once, and
        // concurrent runs must never collide on — or clean up — each other's
        // marker.
        let path = format!(
            "{MARKER_DIR}/{MARKER_PREFIX}{}-{}",
            target.worktree_name,
            std::process::id()
        );
        let outcome = ssh_plain(&target.host, &claim_script(&path));
        let reason = match outcome {
            Ok(output) if output.status.success() => {
                for swept in String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .map(str::trim)
                    .filter(|line| !line.is_empty())
                {
                    println!(
                        "tkw devbox: swept leaked activity marker {swept} \
                         (unrefreshed for over {MARKER_STALE_MINUTES} minutes)"
                    );
                }
                return Some(Self {
                    host: target.host.clone(),
                    path,
                });
            }
            Ok(output) => String::from_utf8_lossy(&output.stderr).trim().to_string(),
            Err(error) => error.to_string(),
        };
        eprintln!(
            "tkw devbox: WARNING could not claim an activity marker on {} ({reason}).\n  \
             The run continues, but the box may idle-stop between steps.",
            target.host
        );
        None
    }

    /// Shell prefixed to a long step so the marker's mtime tracks live work.
    /// The sweep reads mtime, and a cold bar can outlast a short bound; this
    /// also recreates a marker that was swept, which is the right repair.
    fn refresh(&self) -> String {
        format!("touch {}; ", shell_quote(&self.path))
    }

    /// A failed release is reported with the exact repair rather than
    /// swallowed: the next run's sweep would reclaim the marker eventually,
    /// but only after its bound has elapsed.
    fn warn_stranded(&self, reason: &str) {
        eprintln!(
            "tkw devbox: WARNING activity marker {} may still exist on {} ({}).\n  \
             It keeps the box awake and billable. Remove it with:\n    \
             ssh {} 'rm -f {}'",
            self.path,
            self.host,
            reason.trim(),
            self.host,
            self.path
        );
    }
}

impl Drop for Marker {
    fn drop(&mut self) {
        match ssh_plain(&self.host, &release_script(&self.path)) {
            Ok(output) if output.status.success() => {}
            Ok(output) => self.warn_stranded(&String::from_utf8_lossy(&output.stderr)),
            Err(error) => self.warn_stranded(&error.to_string()),
        }
    }
}

/// List every activity marker on a box, whoever owns it.
///
/// Any marker keeps its box awake, so "the marker was removed" has to be
/// checkable rather than taken on trust. Hand-made markers are included
/// deliberately: [`Marker`] accounts for its own, and those are the ones with
/// nothing else watching them.
pub(crate) fn markers(box_name: Option<&str>) -> Result<()> {
    let host = resolve_host(box_name)?;
    let script = format!(
        "find {dir} -maxdepth 1 -type f -exec ls -ld --time-style=long-iso {{}} +",
        dir = shell_quote(MARKER_DIR),
    );
    let output = ssh_plain(&host, &script).context("failed to spawn ssh")?;
    // A listing that failed must never read as "no markers": a false all-clear
    // is the exact failure this command exists to catch.
    if !output.status.success() {
        bail!(
            "could not list activity markers on {host}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let listing = String::from_utf8_lossy(&output.stdout);
    let listing = listing.trim();
    if listing.is_empty() {
        println!("tkw devbox: no activity markers on {host} — it is free to idle-stop");
    } else {
        println!("tkw devbox: activity markers on {host} (each one keeps it billable):");
        for line in listing.lines() {
            println!("  {line}");
        }
    }
    Ok(())
}

/// Days from 1970-01-01 for a proleptic-Gregorian date (Howard Hinnant's
/// `days_from_civil`). Timestamp arithmetic is wanted only to age a lease by
/// hours, which does not justify pulling a date crate into a fleet tool.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Seconds since the Unix epoch for an RFC 3339 UTC timestamp such as
/// `2026-09-16T20:56:12.664353Z`. Fractional seconds and the zone suffix are
/// ignored: leases are aged in hours.
///
/// Returns `None` on anything that does not match the shape exactly. That
/// strictness is the safety property — a partial parse yielding a small epoch
/// would date the lease to 1970, look infinitely stale, and release a lease
/// that is in active use.
fn rfc3339_to_epoch_seconds(timestamp: &str) -> Option<i64> {
    let bytes = timestamp.as_bytes();
    if bytes.len() < 19
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let field = |range: std::ops::Range<usize>| timestamp.get(range)?.parse::<i64>().ok();
    let year = field(0..4)?;
    let month = field(5..7)?;
    let day = field(8..10)?;
    let hour = field(11..13)?;
    let minute = field(14..16)?;
    let second = field(17..19)?;
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&minute)
        || !(0..=60).contains(&second)
    {
        return None;
    }
    Some(days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second)
}

/// Carve the JSON payload out of `devbox list -o json`, which decorates the
/// stream with human notices — an update banner after the array, an empty-fleet
/// message before it — so the whole stream is not valid JSON.
fn extract_json_array(output: &str) -> Option<&str> {
    let start = output.find('[')?;
    let end = output.rfind(']')?;
    (end > start).then(|| output.get(start..=end))?
}

/// Box name and lease id out of `devbox acquire`'s summary, which prints the
/// exact release command for the lease it just took.
///
/// The JSON listing deliberately does not substitute for this. With two agents
/// acquiring the same tag at once, "the most recently acquired lease" may be
/// the other agent's, and releasing it would hand a box away mid-run. The
/// printed command is the only statement of *which lease is ours*.
fn parse_acquire_output(output: &str) -> Option<(String, String)> {
    const NEEDLE: &str = "devbox release ";
    for line in output.lines() {
        let Some(index) = line.find(NEEDLE) else {
            continue;
        };
        let (name, tail) = line[index + NEEDLE.len()..].split_once(" --lease_id=")?;
        let lease_id = tail.split_whitespace().next()?;
        return Some((name.trim().to_string(), lease_id.trim().to_string()));
    }
    None
}

/// Every currently-held lease on `tag`, as (box name, lease id, acquired at).
/// A lease carrying `released_at` is already back in the pool and is skipped.
fn held_leases(listing: &serde_json::Value, tag: &str) -> Vec<(String, String, i64)> {
    let Some(entries) = listing.as_array() else {
        return Vec::new();
    };
    entries
        .iter()
        .filter_map(|entry| {
            let lease = entry.get("pool_lease")?;
            if lease.get("tag")?.as_str()? != tag {
                return None;
            }
            if lease
                .get("released_at")
                .is_some_and(|value| value.is_string())
            {
                return None;
            }
            Some((
                entry.get("name")?.as_str()?.to_string(),
                lease.get("lease_id")?.as_str()?.to_string(),
                rfc3339_to_epoch_seconds(lease.get("acquired_at")?.as_str()?)?,
            ))
        })
        .collect()
}

/// Run a `devbox` control-plane subcommand, capturing its output. A missing
/// CLI is reported with its remedy rather than as a bare spawn failure.
fn devbox_capture(arguments: &[&str]) -> Result<Output> {
    Command::new("devbox")
        .args(arguments)
        .output()
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                anyhow::anyhow!(
                    "the `devbox` CLI is not on PATH, which pooled boxes require. \
                     Install it (curl -fsSL get.namespace.so/devbox/install.sh | bash), \
                     or address a box directly with --box <name>."
                )
            } else {
                anyhow::Error::new(error).context("failed to spawn `devbox`")
            }
        })
}

/// The current devbox listing, parsed.
fn list_boxes() -> Result<serde_json::Value> {
    let output = devbox_capture(&["list", "-o", "json"])?;
    let text = String::from_utf8_lossy(&output.stdout);
    let Some(array) = extract_json_array(&text) else {
        // An empty fleet prints a notice and no array; that is not a failure.
        return Ok(serde_json::Value::Array(Vec::new()));
    };
    serde_json::from_str(array).context("could not parse `devbox list -o json`")
}

/// A lease on a pooled devbox, held for the lifetime of one run.
///
/// `devbox acquire <tag>` reuses a box already carrying the tag when its lease
/// is free, and builds one only when none is — so the pool keeps its warm
/// `target/` across runs while each run still has an explicit end. An
/// `--ephemeral` box would give the same explicit end but discard its storage
/// on stop, making every run a cold rebuild.
///
/// The failure mode is a lease outliving its run — the marker hazard one level
/// up, and answered the same way. [`Drop`] releases on every exit the process
/// can observe, and [`Lease::acquire`] first sweeps leases held past
/// [`LEASE_STALE_MINUTES`], so the next run repairs the last one's strand.
struct Lease {
    box_name: String,
    lease_id: String,
}

impl Lease {
    /// Sweep stranded leases on `tag`, then take one for this run.
    fn acquire(tag: &str) -> Result<Self> {
        Self::sweep_stranded(tag)?;
        println!("tkw devbox: acquiring a `{tag}` box (creating one if the pool is empty)");
        let output = devbox_capture(&[
            "acquire",
            tag,
            "--size",
            POOL_SIZE,
            "--volume_size_gb",
            POOL_VOLUME_GB,
            "--auto_stop_idle_timeout",
            POOL_IDLE_TIMEOUT,
            "--image",
            POOL_IMAGE,
            "--no_checkout",
            "--purpose",
            "tkw pooled build box",
        ])?;
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if !output.status.success() {
            bail!("`devbox acquire {tag}` failed: {}", text.trim());
        }
        // Without the release command we hold a lease we cannot name, so this
        // fails loudly rather than running the work and stranding it silently.
        // The sweep reclaims it on the next run either way.
        let Some((box_name, lease_id)) = parse_acquire_output(&text) else {
            bail!(
                "`devbox acquire {tag}` did not print a release command, so the lease it \
                 took cannot be identified.\n  Release it by hand (`devbox list` shows the \
                 name), or wait for the next run to sweep it after \
                 {LEASE_STALE_MINUTES} minutes."
            );
        };
        println!("tkw devbox: leased {box_name} from pool `{tag}`");
        Ok(Self { box_name, lease_id })
    }

    /// Release leases on `tag` held past the staleness bound. Best effort: a
    /// sweep that cannot run must not stop the run that triggered it.
    fn sweep_stranded(tag: &str) -> Result<()> {
        let listing = list_boxes()?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_secs() as i64);
        for (box_name, lease_id, acquired_at) in held_leases(&listing, tag) {
            if now - acquired_at <= LEASE_STALE_MINUTES * 60 {
                continue;
            }
            match devbox_capture(&["release", &box_name, "--lease_id", &lease_id]) {
                Ok(output) if output.status.success() => println!(
                    "tkw devbox: released stranded lease on {box_name} \
                     (held over {LEASE_STALE_MINUTES} minutes)"
                ),
                Ok(output) => eprintln!(
                    "tkw devbox: WARNING could not release stranded lease on {box_name}: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
                Err(error) => {
                    eprintln!("tkw devbox: WARNING sweeping stranded leases failed: {error}");
                }
            }
        }
        Ok(())
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let released = devbox_capture(&["release", &self.box_name, "--lease_id", &self.lease_id]);
        let reason = match released {
            Ok(output) if output.status.success() => {
                println!("tkw devbox: released {} back to the pool", self.box_name);
                return;
            }
            Ok(output) => String::from_utf8_lossy(&output.stderr).trim().to_string(),
            Err(error) => error.to_string(),
        };
        eprintln!(
            "tkw devbox: WARNING {} may still be leased ({reason}).\n  \
             A held lease makes the next acquire build another box. Release it with:\n    \
             devbox release {} --lease_id {}",
            self.box_name, self.box_name, self.lease_id
        );
    }
}

/// Take a lease when a pool tag was given, or nothing when a box was named
/// directly. `--box` and `--tag` are mutually exclusive at the CLI, so this is
/// the whole of the choice.
fn acquire_if_pooled(tag: Option<&str>) -> Result<Option<Lease>> {
    // Read here rather than through clap's `env`, which needs a cargo feature
    // this workspace does not enable; `TKW_DEVBOX` resolves the same way.
    let from_environment = std::env::var("TKW_DEVBOX_TAG").ok();
    let tag = tag.or(from_environment.as_deref().filter(|tag| !tag.is_empty()));
    match tag {
        Some(tag) => Lease::acquire(tag).map(Some),
        None => Ok(None),
    }
}

/// The leased box if there is one, else the explicitly named box. A lease
/// always wins: holding one and then addressing a different box would leave
/// the leased box held but unused for the length of the run.
fn leased_or_named<'a>(lease: Option<&'a Lease>, box_name: Option<&'a str>) -> Option<&'a str> {
    lease.map(|lease| lease.box_name.as_str()).or(box_name)
}

/// Stop a box now instead of waiting out its idle timeout.
///
/// A box stays up from the moment the work finishes until its idle timeout
/// fires, so every run leaves a tail. The timeout bounds that tail; stopping
/// deliberately removes it.
///
/// Stopping has no SSH equivalent — it is a control-plane operation — so this
/// is the one command that wants the `devbox` CLI. A missing CLI prints the
/// command to run rather than failing opaquely: what matters is that the box
/// stops, not that tkw is what stops it.
pub(crate) fn down(box_name: Option<&str>) -> Result<()> {
    let box_name = resolve_box(box_name)?;
    println!("tkw devbox: stopping {box_name}");
    let outcome = Command::new("devbox")
        .arg("shutdown")
        .arg(&box_name)
        .arg("--force")
        .status();
    match outcome {
        Ok(status) if status.success() => {
            println!("tkw devbox: {box_name} stopped — Devbox Minutes stop accruing now");
            Ok(())
        }
        Ok(status) => bail!("`devbox shutdown {box_name} --force` failed with {status}"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => bail!(
            "the `devbox` CLI is not on PATH, so tkw cannot stop {box_name}.\n  \
             Install it (curl -fsSL get.namespace.so/devbox/install.sh | bash), \
             or stop the box yourself:\n    devbox shutdown {box_name} --force"
        ),
        Err(error) => Err(error).context("failed to spawn `devbox`"),
    }
}

/// Sync the current worktree to its per-worktree directory on the box.
pub(crate) fn sync(box_name: Option<&str>) -> Result<()> {
    let target = Target::resolve(box_name)?;
    sync_target(&target)
}

/// Sync, then run `command` in the remote copy, streaming output.
/// Returns the remote exit code for main to propagate.
pub(crate) fn run(box_name: Option<&str>, tag: Option<&str>, command: &[String]) -> Result<i32> {
    // Declared before the marker so it drops *after* it: the box goes back to
    // the pool only once the marker held on it has been released.
    let lease = acquire_if_pooled(tag)?;
    let target = Target::resolve(leased_or_named(lease.as_ref(), box_name))?;
    // Claimed before the sync so the sweep still runs when rsync fails, and
    // held to the end of the function so `?` and panics both release it.
    let _marker = Marker::claim(&target);
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
pub(crate) fn bar(box_name: Option<&str>, tag: Option<&str>) -> Result<i32> {
    // Declared before the marker so it drops after it — see `run`.
    let lease = acquire_if_pooled(tag)?;
    let target = Target::resolve(leased_or_named(lease.as_ref(), box_name))?;
    let marker = Marker::claim(&target);
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
        // Each step re-touches the marker so a bar longer than the staleness
        // bound — a cold workspace test is minutes, a cold full bar tens —
        // is never mistaken by the next run's sweep for a leak.
        let script = match &marker {
            Some(marker) => format!("{}{script}", marker.refresh()),
            None => script,
        };
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

/// Run a script on the box without the working-directory and cargo-env
/// wrapper. Marker upkeep addresses the box itself, and has to work before the
/// first sync has created the remote directory that [`remote_script`] would
/// `cd` into.
fn ssh_plain(host: &str, script: &str) -> std::io::Result<Output> {
    Command::new("ssh")
        .arg("-o")
        .arg("BatchMode=yes")
        .arg(host)
        .arg(script)
        .output()
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
    fn claim_script_sweeps_only_tkw_markers_before_touching_its_own() {
        let script = claim_script("/.namespace/tasks/tkw-some-worktree-4242");
        assert!(
            script.contains("-name 'tkw-*'"),
            "sweep is prefix-scoped: {script}"
        );
        assert!(
            script.contains(&format!("-mmin +{MARKER_STALE_MINUTES}")),
            "sweep is age-bounded: {script}"
        );
        // Directories are excluded: `-delete` fails on a non-empty one, and a
        // marker is always a file.
        assert!(
            script.contains("-type f"),
            "sweep matches files only: {script}"
        );
        let sweep = script.find("find").expect("sweep present");
        let touch = script.find("touch").expect("claim present");
        assert!(sweep < touch, "sweep precedes claim: {script}");
    }

    #[test]
    fn control_plane_addresses_the_box_by_name_and_ssh_by_host() {
        // `devbox shutdown` takes the bare name; ssh takes the derived host.
        // Passing one where the other belongs fails at a distance.
        assert_eq!(resolve_box(Some("tok-a")).unwrap(), "tok-a");
        assert_eq!(
            resolve_host(Some("tok-a")).unwrap(),
            "tok-a.devbox.namespace"
        );
    }

    #[test]
    fn release_script_tolerates_an_already_swept_marker() {
        assert_eq!(
            release_script("/.namespace/tasks/tkw-a-1"),
            "rm -f '/.namespace/tasks/tkw-a-1'"
        );
    }

    #[test]
    fn marker_scripts_quote_paths_with_shell_metacharacters() {
        // Worktree directory names reach the marker name verbatim.
        let script = claim_script("/.namespace/tasks/tkw-it's a tree-7");
        assert!(
            script.contains(r"'/.namespace/tasks/tkw-it'\''s a tree-7'"),
            "{script}"
        );
    }

    #[test]
    fn refresh_prefixes_a_touch_that_recreates_a_swept_marker() {
        let marker = Marker {
            host: "box.devbox.namespace".to_string(),
            path: "/.namespace/tasks/tkw-a-1".to_string(),
        };
        assert_eq!(marker.refresh(), "touch '/.namespace/tasks/tkw-a-1'; ");
        // Prefix form, so the step it guards still reports its own exit code.
        assert!(marker.refresh().ends_with("; "));
        std::mem::forget(marker); // Drop would SSH to a host that does not exist.
    }

    #[test]
    fn days_from_civil_anchors_on_the_epoch() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
        assert_eq!(days_from_civil(2000, 3, 1), 11017);
        // 2000 is a leap year and 1900 is not: the century rule is the part of
        // this algorithm an ad-hoc version gets wrong.
        assert_eq!(
            days_from_civil(2000, 3, 1) - days_from_civil(2000, 2, 28),
            2
        );
    }

    #[test]
    fn rfc3339_parses_the_shape_devbox_emits() {
        // Exactly as `pool_lease.acquired_at` appears in `devbox list -o json`.
        let epoch = rfc3339_to_epoch_seconds("2026-09-16T20:56:12.664353Z").unwrap();
        assert_eq!(
            epoch,
            days_from_civil(2026, 9, 16) * 86_400 + 20 * 3_600 + 56 * 60 + 12
        );
        assert_eq!(rfc3339_to_epoch_seconds("1970-01-01T00:00:00Z"), Some(0));
    }

    #[test]
    fn rfc3339_rejects_rather_than_guesses() {
        // A partial parse would date the lease to 1970, making a live lease
        // look infinitely stale and releasing a box out from under a run.
        for malformed in [
            "",
            "2026-09-16",
            "2026/09/16T20:56:12Z",
            "2026-09-16 20:56:12Z",
            "2026-13-16T20:56:12Z",
            "2026-09-32T20:56:12Z",
            "2026-09-16T25:56:12Z",
            "abcd-ef-ghTij:kl:mnZ",
        ] {
            assert_eq!(
                rfc3339_to_epoch_seconds(malformed),
                None,
                "should reject {malformed:?}"
            );
        }
    }

    #[test]
    fn extract_json_array_ignores_the_cli_banners() {
        // Real shape: an empty-fleet notice before, an update banner after.
        let noisy = "No devbox available yet.\n[\n{\"name\":\"a\"}\n]\n\n A new version \
                     of devbox is available: 0.0.187.\n";
        assert_eq!(extract_json_array(noisy), Some("[\n{\"name\":\"a\"}\n]"));
        assert_eq!(extract_json_array("no array here"), None);
    }

    #[test]
    fn parse_acquire_output_reads_the_printed_release_command() {
        // Captured verbatim from `devbox acquire`, decoration and all.
        let output = " Reused devbox \"tok-probe\" for tag \"tokeira-bar\" ───────────── \n\
                      \x20devbox release tok-probe --lease_id=dxl_examplelease000000000000    \n\
                      \x20ssh tok-probe.devbox.namespace\n";
        assert_eq!(
            parse_acquire_output(output),
            Some((
                "tok-probe".to_string(),
                "dxl_examplelease000000000000".to_string()
            ))
        );
        assert_eq!(parse_acquire_output("nothing useful here"), None);
    }

    /// Builds a listing in the shape `devbox list -o json` returns.
    fn listing(entries: &[(&str, &str, Option<&str>, &str)]) -> serde_json::Value {
        serde_json::Value::Array(
            entries
                .iter()
                .map(|(name, tag, released_at, acquired_at)| {
                    let mut lease = serde_json::Map::new();
                    lease.insert("tag".into(), (*tag).into());
                    lease.insert("lease_id".into(), format!("lease-{name}").into());
                    lease.insert("acquired_at".into(), (*acquired_at).into());
                    if let Some(released_at) = released_at {
                        lease.insert("released_at".into(), (*released_at).into());
                    }
                    let mut entry = serde_json::Map::new();
                    entry.insert("name".into(), (*name).into());
                    entry.insert("pool_lease".into(), lease.into());
                    serde_json::Value::Object(entry)
                })
                .collect(),
        )
    }

    #[test]
    fn held_leases_skip_released_and_foreign_tags() {
        let listing = listing(&[
            ("held", "ours", None, "2026-09-16T20:00:00Z"),
            (
                "returned",
                "ours",
                Some("2026-09-16T21:00:00Z"),
                "2026-09-16T20:00:00Z",
            ),
            ("other-pool", "theirs", None, "2026-09-16T20:00:00Z"),
        ]);
        let held = held_leases(&listing, "ours");
        assert_eq!(held.len(), 1, "only the held lease on our tag: {held:?}");
        assert_eq!(held[0].0, "held");
        assert_eq!(held[0].1, "lease-held");
    }

    #[test]
    fn held_leases_tolerate_entries_without_a_pool_lease() {
        // A hand-created box carries no `pool_lease` at all.
        let listing = serde_json::json!([{"name": "hand-made"}]);
        assert!(held_leases(&listing, "ours").is_empty());
        assert!(held_leases(&serde_json::json!({}), "ours").is_empty());
    }

    #[test]
    fn format_duration_switches_units_at_a_minute() {
        assert_eq!(format_duration(Duration::from_millis(500)), "0.5s");
        assert_eq!(format_duration(Duration::from_secs(59)), "59.0s");
        assert_eq!(format_duration(Duration::from_secs(256)), "4m16s");
    }
}
