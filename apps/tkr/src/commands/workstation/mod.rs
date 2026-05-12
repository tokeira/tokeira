//! `tkr workstation` command handlers.

use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokeira_remote_workstation::engine::{
    GithubRepo, UpOutcome, Workstation, WorkstationError, WorkstationProfile,
};
use tokio::process::Command;

use crate::cli::{GithubKeyAction, WorkstationAction};

mod secret_scan;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeployKeyEntry {
    repo: String,
    key_id: String,
    read_only: bool,
    created_at: String,
    removed_at: Option<String>,
}

pub async fn run(action: WorkstationAction, json: bool) -> Result<()> {
    match action {
        WorkstationAction::Up {
            profile,
            workstation,
            cache_volume_gib,
            repo_volume_gib,
            root_volume_gib,
            instance_type,
            region,
            subnet_id,
        } => {
            let mut profile = load_profile(&profile)?;
            apply_profile_overrides(
                &mut profile,
                cache_volume_gib,
                repo_volume_gib,
                root_volume_gib,
                instance_type,
                region,
                subnet_id,
            );

            // Check if we have local state for an existing workstation
            let ws_id = workstation
                .clone()
                .or_else(|| read_latest().ok())
                .unwrap_or_else(|| format!("ws-{}", uuid::Uuid::new_v4()));

            let state_dir =
                tokeira_remote_workstation::provision::state_dir_for(&ws_id);

            if state_dir.join("infra").exists() {
                // Existing workstation — resume via operational engine
                let engine = Workstation::new(profile.region.clone()).await?;
                let outcome = engine.up(&profile, Some(&ws_id)).await?;
                write_latest(outcome_workstation_id(&outcome))?;
                print_up_outcome(&outcome, json)?;
            } else {
                // Fresh create — use IaC engine
                println!("creating workstation {ws_id}...");
                let engine = Workstation::new(profile.region.clone()).await?;

                // Pre-flight: discover subnet, VPC, AZ, AMI
                let preflight = engine.preflight(&profile).await?;

                let bootstrap_ctx =
                    tokeira_remote_workstation::bootstrap::BootstrapContext {
                        workstation_id: ws_id.clone(),
                        bootstrap_fingerprint: String::new(), // computed below
                        profile: profile.clone(),
                        cache_volume_id: String::new(), // filled by IaC
                        repo_volume_id: String::new(),  // filled by IaC
                        rust_toolchain_toml: read_rust_toolchain_toml(),
                    };
                let fingerprint = tokeira_remote_workstation::bootstrap::fingerprint(
                    &profile,
                    &bootstrap_ctx.rust_toolchain_toml,
                );
                let user_data = tokeira_remote_workstation::bootstrap::render(
                    &tokeira_remote_workstation::bootstrap::BootstrapContext {
                        bootstrap_fingerprint: fingerprint.clone(),
                        ..bootstrap_ctx
                    },
                );
                let user_data_base64 =
                    base64::engine::general_purpose::STANDARD.encode(user_data.as_bytes());

                let module_config =
                    tokeira_remote_workstation::module::WorkstationModuleConfig {
                        workstation_id: ws_id.clone(),
                        instance_type: profile.instance_type.clone(),
                        ami_id: preflight.ami_id,
                        subnet_id: preflight.subnet_id,
                        vpc_id: preflight.vpc_id,
                        availability_zone: preflight.availability_zone,
                        root_volume_gib: profile.root_volume_gib,
                        cache_volume_gib: profile.cache_volume_gib,
                        repo_volume_gib: profile.repo_volume_gib,
                        user_data_base64,
                        region: profile.region.clone(),
                    };

                let aws_config = aws_config::defaults(
                    aws_config::BehaviorVersion::latest(),
                )
                .region(aws_config::Region::new(profile.region.clone()))
                .load()
                .await;
                let aws_clients = tokeira_aws::AwsClients::new(&aws_config);

                let _state =
                    tokeira_remote_workstation::provision::provision_workstation(
                        module_config,
                        aws_clients,
                        &state_dir,
                        |_ctx| {
                            // TODO: install TUI progress reporters here
                        },
                    )
                    .await?;

                write_latest(&ws_id)?;
                println!("workstation {ws_id} created");
            }
            Ok(())
        }
        WorkstationAction::Stop { workstation } => {
            let id = resolve_workstation_id(workstation.as_deref())?;
            let profile = WorkstationProfile::c8gd_rust();
            let engine = Workstation::new(profile.region).await?;
            engine.stop(&id).await?;
            println!("stopped workstation {id}; /work/target and /work/sccache were ephemeral");
            Ok(())
        }
        WorkstationAction::Destroy { workstation, yes } => {
            let id = resolve_workstation_id(workstation.as_deref())?;
            confirm_destroy(&id, yes)?;

            let profile = WorkstationProfile::c8gd_rust();
            let state_dir =
                tokeira_remote_workstation::provision::state_dir_for(&id);

            if state_dir.join("infra").exists() {
                // Use IaC engine for clean reverse-order destroy
                println!("destroying workstation {id}...");

                // We need the module config to enumerate resources for destroy.
                // For destroy, the actual values (AMI, subnet, etc.) don't matter
                // because we're deleting by physical ID from state. But we need
                // the workstation_id to generate the correct resource names.
                let module_config =
                    tokeira_remote_workstation::module::WorkstationModuleConfig {
                        workstation_id: id.clone(),
                        instance_type: profile.instance_type.clone(),
                        ami_id: String::new(),
                        subnet_id: String::new(),
                        vpc_id: String::new(),
                        availability_zone: String::new(),
                        root_volume_gib: profile.root_volume_gib,
                        cache_volume_gib: profile.cache_volume_gib,
                        repo_volume_gib: profile.repo_volume_gib,
                        user_data_base64: String::new(),
                        region: profile.region.clone(),
                    };

                let aws_config = aws_config::defaults(
                    aws_config::BehaviorVersion::latest(),
                )
                .region(aws_config::Region::new(profile.region.clone()))
                .load()
                .await;
                let aws_clients = tokeira_aws::AwsClients::new(&aws_config);

                tokeira_remote_workstation::provision::destroy_workstation(
                    module_config,
                    aws_clients,
                    &state_dir,
                    |_ctx| {
                        // TODO: install TUI progress reporters here
                    },
                )
                .await?;

                // Remove local state directory
                let _ = fs::remove_dir_all(&state_dir);
                clear_latest_if_matches(&id)?;
                println!("destroyed workstation {id}");
            } else {
                // Fallback: use operational engine (for workstations created
                // before the IaC rewrite)
                let engine = Workstation::new(profile.region).await?;
                engine.destroy(&id).await?;
                println!("destroyed workstation {id}");
            }
            Ok(())
        }
        WorkstationAction::Ssh { workstation } => {
            let id = resolve_workstation_id(workstation.as_deref())?;
            let profile = WorkstationProfile::c8gd_rust();
            let engine = Workstation::new(profile.region).await?;
            let code = engine.start_interactive_session(&id).await?;
            if code == 0 {
                Ok(())
            } else {
                bail!("SSM session exited with status {code}")
            }
        }
        WorkstationAction::RemoteExec {
            workstation,
            cwd,
            yes_secret_in_command,
            command,
        } => {
            if command.is_empty() {
                bail!("remote-exec requires a command");
            }
            if let Some(found) = secret_scan::scan(&command) {
                confirm_secret(&found, yes_secret_in_command)?;
            }
            let id = resolve_workstation_id(workstation.as_deref())?;
            let profile = WorkstationProfile::c8gd_rust();
            let engine = Workstation::new(profile.region).await?;
            let code = engine
                .remote_exec(
                    &id,
                    &cwd,
                    &command,
                    tokio::io::stdout(),
                    tokio::io::stderr(),
                )
                .await?;
            if code == 0 {
                Ok(())
            } else {
                bail!("remote command exited with status {code}")
            }
        }
        WorkstationAction::Status { workstation } => {
            let id = resolve_workstation_id(workstation.as_deref())?;
            let profile = WorkstationProfile::c8gd_rust();
            let engine = Workstation::new(profile.region).await?;
            let status = engine.status(&id).await?;
            if json {
                println!("{}", serde_json::to_string(&status)?);
            } else {
                println!("workstation: {}", status.workstation_id);
                println!("state: {}", status.state);
                println!("instance: {} {}", status.instance_id, status.instance_type);
                println!("region: {}", status.region);
                println!(
                    "cost rate: {}",
                    status
                        .hourly_cost_usd
                        .map(|rate| format!("${rate:.5}/hour"))
                        .unwrap_or_else(|| "unknown (not in local table)".to_string())
                );
                println!(
                    "cumulative uptime: {:.2} hours",
                    status.cumulative_uptime_hours
                );
                println!(
                    "cache volume: {} {} {}",
                    status.cache_volume_id.as_deref().unwrap_or("unknown"),
                    status
                        .cache_volume_gib
                        .map(|size| format!("{size} GiB"))
                        .unwrap_or_else(|| "unknown size".to_string()),
                    status
                        .cache_volume_state
                        .as_deref()
                        .unwrap_or("unknown state")
                );
                println!(
                    "repo volume: {} {} {}",
                    status.repo_volume_id.as_deref().unwrap_or("unknown"),
                    status
                        .repo_volume_gib
                        .map(|size| format!("{size} GiB"))
                        .unwrap_or_else(|| "unknown size".to_string()),
                    status
                        .repo_volume_state
                        .as_deref()
                        .unwrap_or("unknown state")
                );
            }
            Ok(())
        }
        WorkstationAction::List => {
            let profile = WorkstationProfile::c8gd_rust();
            let engine = Workstation::new(profile.region).await?;
            let rows = engine.list().await?;
            if json {
                println!("{}", serde_json::to_string(&rows)?);
            } else {
                for row in rows {
                    let rate = row
                        .hourly_cost_usd
                        .map(|value| format!("${value:.5}/hour"))
                        .unwrap_or_else(|| "unknown".to_string());
                    println!(
                        "{}\t{}\t{}\t{}\t{}",
                        row.workstation_id, row.state, row.region, row.instance_type, rate
                    );
                }
            }
            Ok(())
        }
        WorkstationAction::Bootstrap { workstation } => {
            let id = resolve_workstation_id(workstation.as_deref())?;
            let profile = WorkstationProfile::c8gd_rust();
            let engine = Workstation::new(profile.region.clone()).await?;
            let drift = engine.bootstrap(&id, &profile).await?;
            println!("{drift:?}");
            Ok(())
        }
        WorkstationAction::Idle { workstation, defer } => {
            let id = resolve_workstation_id(workstation.as_deref())?;
            let profile = WorkstationProfile::c8gd_rust();
            let engine = Workstation::new(profile.region).await?;
            let duration: Duration = defer.map(Into::into).unwrap_or(Duration::from_secs(7200));
            let until = Utc::now()
                + chrono::Duration::from_std(duration)
                    .context("idle defer duration is outside chrono range")?;
            engine.idle_defer(&id, until).await?;
            println!(
                "deferred idle shutdown for workstation {id} until {}",
                until.to_rfc3339()
            );
            Ok(())
        }
        WorkstationAction::GithubKey { action } => run_github_key(action).await,
    }
}

fn load_profile(name: &str) -> Result<WorkstationProfile> {
    WorkstationProfile::by_name(name).ok_or_else(|| anyhow!("unknown workstation profile {name}"))
}

fn apply_profile_overrides(
    profile: &mut WorkstationProfile,
    cache_volume_gib: Option<u32>,
    repo_volume_gib: Option<u32>,
    root_volume_gib: Option<u32>,
    instance_type: Option<String>,
    region: Option<String>,
    subnet_id: Option<String>,
) {
    if let Some(value) = cache_volume_gib {
        profile.cache_volume_gib = value;
    }
    if let Some(value) = repo_volume_gib {
        profile.repo_volume_gib = value;
    }
    if let Some(value) = root_volume_gib {
        profile.root_volume_gib = value;
    }
    if let Some(value) = instance_type {
        profile.instance_type = value;
    }
    if let Some(value) = region {
        profile.region = value;
    }
    if let Some(value) = subnet_id {
        profile.subnet_id = Some(value);
    }
}

fn resolve_workstation_id(override_id: Option<&str>) -> Result<String> {
    if let Some(id) = override_id {
        return Ok(id.to_string());
    }
    let path = state_root()?.join(".latest");
    let value = fs::read_to_string(&path).with_context(|| {
        format!(
            "no --workstation supplied and {} is missing",
            path.display()
        )
    })?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{} is empty; pass --workstation explicitly", path.display());
    }
    Ok(trimmed.to_string())
}

fn state_root() -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("HOME is not set"))?;
    Ok(home.join(".tokeira").join("workstations"))
}

fn write_latest(id: &str) -> Result<()> {
    let root = state_root()?;
    fs::create_dir_all(&root)?;
    fs::write(root.join(".latest"), id)?;
    Ok(())
}

fn read_latest() -> Result<String> {
    let path = state_root()?.join(".latest");
    let value = fs::read_to_string(&path)?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!(".latest is empty");
    }
    Ok(trimmed.to_string())
}

fn clear_latest_if_matches(id: &str) -> Result<()> {
    if let Ok(current) = read_latest() {
        if current == id {
            let _ = fs::remove_file(state_root()?.join(".latest"));
        }
    }
    Ok(())
}

fn read_rust_toolchain_toml() -> String {
    tokeira_remote_workstation::engine::read_rust_toolchain_toml()
}

fn outcome_workstation_id(outcome: &UpOutcome) -> &str {
    match outcome {
        UpOutcome::Created { handle, .. }
        | UpOutcome::Resumed { handle, .. }
        | UpOutcome::AlreadyRunning { handle } => &handle.workstation_id,
    }
}

fn print_up_outcome(outcome: &UpOutcome, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(outcome)?);
        return Ok(());
    }
    match outcome {
        UpOutcome::Created {
            handle,
            repo_clone_warning,
            ..
        } => {
            println!("created workstation {}", handle.workstation_id);
            if let Some(warning) = repo_clone_warning
                && !warning.trim().is_empty()
            {
                println!("repo clone warning: {}", warning.trim());
            }
        }
        UpOutcome::Resumed {
            handle,
            bootstrap_drift,
            repo_clone_warning,
        } => {
            println!("resumed workstation {}", handle.workstation_id);
            println!("bootstrap: {bootstrap_drift:?}");
            if let Some(warning) = repo_clone_warning
                && !warning.trim().is_empty()
            {
                println!("repo clone warning: {}", warning.trim());
            }
        }
        UpOutcome::AlreadyRunning { handle } => {
            println!("workstation {} already running", handle.workstation_id);
        }
    }
    Ok(())
}

fn confirm_secret(found: &secret_scan::SecretMatch, yes: bool) -> Result<()> {
    eprintln!(
        "The command looks like it contains a secret ({} at bytes {}..{}). SSM Run Command invocations are logged to CloudTrail with the full command text. Use `tkr workstation ssh` for interactive secret entry instead. Proceed anyway? [y/N]",
        found.pattern, found.start, found.end
    );
    if yes {
        return Ok(());
    }
    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    if matches!(answer.trim(), "y" | "Y" | "yes" | "YES") {
        Ok(())
    } else {
        Err(WorkstationError::SecretInCommand.into())
    }
}

fn confirm_destroy(workstation_id: &str, yes: bool) -> Result<()> {
    if yes {
        return Ok(());
    }
    eprint!(
        "Destroy workstation {workstation_id}? This deletes the instance AND both EBS volumes permanently. [y/N]: "
    );
    io::stderr().flush()?;
    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    if matches!(answer.trim(), "y" | "Y" | "yes" | "YES") {
        Ok(())
    } else {
        bail!("aborted workstation destroy")
    }
}

async fn run_github_key(action: GithubKeyAction) -> Result<()> {
    match action {
        GithubKeyAction::Add {
            workstation,
            repo,
            read_only,
        } => {
            ensure_gh().await?;
            let id = resolve_workstation_id(workstation.as_deref())?;
            let repo =
                GithubRepo::parse(&repo.ok_or_else(|| anyhow!("--repo owner/name is required"))?)?;
            let profile = WorkstationProfile::c8gd_rust();
            let engine = Workstation::new(profile.region).await?;
            let public_key = engine.github_key_add(&id, &repo, read_only).await?;
            let key_id = gh_add_key(&repo, &id, &public_key, read_only).await?;
            if !read_only {
                engine.github_key_configure(&id, &repo).await?;
            }
            append_deploy_key(&id, &repo, &key_id, read_only)?;
            println!("added deploy key {key_id} for {}/{}", repo.owner, repo.name);
            Ok(())
        }
        GithubKeyAction::Remove { workstation, repo } => {
            ensure_gh().await?;
            let id = resolve_workstation_id(workstation.as_deref())?;
            let repo =
                GithubRepo::parse(&repo.ok_or_else(|| anyhow!("--repo owner/name is required"))?)?;
            let entry = find_live_deploy_key(&id, &repo)?.ok_or_else(|| {
                anyhow!(
                    "no live deploy key recorded for {}/{}",
                    repo.owner,
                    repo.name
                )
            })?;
            gh_delete_key(&repo, &entry.key_id).await?;
            let profile = WorkstationProfile::c8gd_rust();
            let engine = Workstation::new(profile.region).await?;
            engine.github_key_remove(&id, &repo).await?;
            append_deploy_key_remove(&id, &entry)?;
            println!(
                "removed workstation key material for {}/{}",
                repo.owner, repo.name
            );
            Ok(())
        }
        GithubKeyAction::List { workstation } => {
            ensure_gh().await?;
            let id = resolve_workstation_id(workstation.as_deref())?;
            let entries = live_deploy_keys(&id)?;
            let remote = github_remote_keys(&id, &entries).await?;
            for entry in &entries {
                let status = if remote
                    .iter()
                    .any(|remote| remote.repo == entry.repo && remote.key_id == entry.key_id)
                {
                    "live"
                } else {
                    "orphan-local"
                };
                println!("{}\t{}\t{}", entry.repo, entry.key_id, status);
            }
            for remote in remote {
                if !entries
                    .iter()
                    .any(|entry| entry.repo == remote.repo && entry.key_id == remote.key_id)
                {
                    println!("{}\t{}\torphan-remote", remote.repo, remote.key_id);
                }
            }
            Ok(())
        }
    }
}

async fn ensure_gh() -> Result<()> {
    which::which("gh").context("gh CLI is required for workstation github-key")?;
    let status = Command::new("gh")
        .args(["auth", "status"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .context("failed to invoke gh auth status")?;
    if !status.success() {
        return Err(WorkstationError::GhCliUnauthenticated.into());
    }
    Ok(())
}

async fn gh_add_key(
    repo: &GithubRepo,
    workstation_id: &str,
    public_key: &str,
    read_only: bool,
) -> Result<String> {
    let output = Command::new("gh")
        .args([
            "api",
            "--method",
            "POST",
            &format!("repos/{}/{}/keys", repo.owner, repo.name),
            "-f",
            &format!("title=tokeira-workstation-{workstation_id}"),
            "-f",
            &format!("key={}", public_key.trim()),
            "-F",
            &format!("read_only={read_only}"),
        ])
        .stdout(Stdio::piped())
        .output()
        .await
        .context("failed to invoke gh api")?;
    if !output.status.success() {
        bail!("gh api failed: {}", String::from_utf8_lossy(&output.stderr));
    }
    let json: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    json.get("id")
        .and_then(|value| value.as_i64())
        .map(|value| value.to_string())
        .ok_or_else(|| anyhow!("GitHub deploy-key response did not include id"))
}

async fn gh_delete_key(repo: &GithubRepo, key_id: &str) -> Result<()> {
    let status = Command::new("gh")
        .args([
            "api",
            "--method",
            "DELETE",
            &format!("repos/{}/{}/keys/{key_id}", repo.owner, repo.name),
        ])
        .status()
        .await
        .context("failed to invoke gh api")?;
    if status.success() {
        Ok(())
    } else {
        bail!(
            "failed to delete GitHub deploy key {key_id}; remove it manually at https://github.com/{}/{}/settings/keys",
            repo.owner,
            repo.name
        )
    }
}

fn append_deploy_key(
    workstation_id: &str,
    repo: &GithubRepo,
    key_id: &str,
    read_only: bool,
) -> Result<()> {
    let dir = state_root()?.join(workstation_id);
    fs::create_dir_all(&dir)?;
    let entry = DeployKeyEntry {
        repo: format!("{}/{}", repo.owner, repo.name),
        key_id: key_id.to_string(),
        read_only,
        created_at: Utc::now().to_rfc3339(),
        removed_at: None,
    };
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("deploy-keys.jsonl"))?;
    writeln!(file, "{}", serde_json::to_string(&entry)?)?;
    Ok(())
}

fn append_deploy_key_remove(workstation_id: &str, entry: &DeployKeyEntry) -> Result<()> {
    let dir = state_root()?.join(workstation_id);
    fs::create_dir_all(&dir)?;
    let entry = DeployKeyEntry {
        repo: entry.repo.clone(),
        key_id: entry.key_id.clone(),
        read_only: entry.read_only,
        created_at: entry.created_at.clone(),
        removed_at: Some(Utc::now().to_rfc3339()),
    };
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("deploy-keys.jsonl"))?;
    writeln!(file, "{}", serde_json::to_string(&entry)?)?;
    Ok(())
}

fn find_live_deploy_key(workstation_id: &str, repo: &GithubRepo) -> Result<Option<DeployKeyEntry>> {
    let repo_name = format!("{}/{}", repo.owner, repo.name);
    Ok(live_deploy_keys(workstation_id)?
        .into_iter()
        .find(|entry| entry.repo == repo_name))
}

fn live_deploy_keys(workstation_id: &str) -> Result<Vec<DeployKeyEntry>> {
    let path = state_root()?.join(workstation_id).join("deploy-keys.jsonl");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let contents = fs::read_to_string(path)?;
    parse_live_deploy_keys(&contents)
}

fn parse_live_deploy_keys(contents: &str) -> Result<Vec<DeployKeyEntry>> {
    let mut entries = Vec::new();
    for line in contents.lines() {
        let entry: DeployKeyEntry = serde_json::from_str(line)?;
        if entry.removed_at.is_some() {
            entries.retain(|existing: &DeployKeyEntry| {
                !(existing.repo == entry.repo && existing.key_id == entry.key_id)
            });
        } else {
            entries.push(entry);
        }
    }
    Ok(entries)
}

async fn github_remote_keys(
    workstation_id: &str,
    entries: &[DeployKeyEntry],
) -> Result<Vec<DeployKeyEntry>> {
    let mut repos = entries
        .iter()
        .map(|entry| entry.repo.clone())
        .collect::<Vec<_>>();
    repos.sort();
    repos.dedup();

    let mut remote = Vec::new();
    for repo in repos {
        let Some((owner, name)) = repo.split_once('/') else {
            continue;
        };
        let output = Command::new("gh")
            .args(["api", &format!("repos/{owner}/{name}/keys")])
            .stdout(Stdio::piped())
            .output()
            .await
            .with_context(|| format!("failed to list GitHub deploy keys for {repo}"))?;
        if !output.status.success() {
            bail!("gh api failed: {}", String::from_utf8_lossy(&output.stderr));
        }
        let values: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout)?;
        for value in values {
            let title = value
                .get("title")
                .and_then(|title| title.as_str())
                .unwrap_or_default();
            if title != format!("tokeira-workstation-{workstation_id}") {
                continue;
            }
            if let Some(key_id) = value.get("id").and_then(|id| id.as_i64()) {
                remote.push(DeployKeyEntry {
                    repo: repo.clone(),
                    key_id: key_id.to_string(),
                    read_only: value
                        .get("read_only")
                        .and_then(|read_only| read_only.as_bool())
                        .unwrap_or(false),
                    created_at: value
                        .get("created_at")
                        .and_then(|created_at| created_at.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    removed_at: None,
                });
            }
        }
    }
    Ok(remote)
}

#[cfg(test)]
mod tests {
    use super::parse_live_deploy_keys;

    #[test]
    fn deploy_key_registry_round_trips_add_and_remove_events() {
        let contents = r#"{"repo":"openai/tokeira","key_id":"1","read_only":false,"created_at":"2026-05-11T00:00:00Z","removed_at":null}
{"repo":"openai/other","key_id":"2","read_only":true,"created_at":"2026-05-11T00:00:01Z","removed_at":null}
{"repo":"openai/tokeira","key_id":"1","read_only":false,"created_at":"2026-05-11T00:00:00Z","removed_at":"2026-05-11T00:01:00Z"}"#;
        let live = parse_live_deploy_keys(contents).expect("registry should parse");
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].repo, "openai/other");
        assert_eq!(live[0].key_id, "2");
        assert!(live[0].read_only);
    }
}
