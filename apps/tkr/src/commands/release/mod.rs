//! `tkr release` — generic Plan, confirmed apply, fragment, and verify commands.

mod changie;

use std::{
    fs,
    io::{self, IsTerminal as _, Write as _},
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result};
use dagger_sdk::LockMode;
use tokeira_build::{
    PackageOutcome, PlannedRegistryState, RegistryCredential, ReleaseApiCredential, ReleaseConfig,
    ReleaseError, ReleaseNotesOutcome, ReleaseNotesRequest, ReleaseNotesResult, ReleasePlan,
    ReleasePublishRequest, ReleaseReport, ReleaseVerifyRequest, RepositoryIdentity, TrainState,
    create_release_notes, observe_release_object, plan_release_with_dagger,
    publish_and_verify_release, require_apply_admission, verify_release,
};
use uuid::Uuid;

use crate::cli::ReleaseCommand;

use super::image::ci_dagger_session;

/// Dispatch one release sub-verb.
pub(crate) async fn run(command: ReleaseCommand, global_json: bool) -> Result<()> {
    match command {
        ReleaseCommand::Fragment {
            workspace_root,
            kind,
            body,
        } => {
            let root = resolve_workspace(workspace_root)?;
            let path = changie::create_fragment(&root, kind.as_deref(), body.as_deref()).await?;
            if global_json {
                println!(
                    "{}",
                    serde_json::to_string(&serde_json::json!({ "fragment": path }))?
                );
            } else {
                println!("created {}", path.display());
            }
            Ok(())
        }
        ReleaseCommand::Plan {
            workspace_root,
            version,
            base_ref,
            output,
        } => {
            run_plan(
                resolve_workspace(workspace_root)?,
                &version,
                base_ref.as_deref(),
                output.as_deref(),
                global_json,
            )
            .await
        }
        ReleaseCommand::Apply {
            workspace_root,
            plan,
            token_env,
            yes,
        } => {
            run_apply(
                workspace_root,
                &plan,
                token_env.as_deref(),
                yes,
                global_json,
            )
            .await
        }
        ReleaseCommand::Verify {
            workspace_root,
            version,
            output,
        } => {
            run_verify(
                resolve_workspace(workspace_root)?,
                &version,
                output.as_deref(),
                global_json,
            )
            .await
        }
    }
}

async fn run_apply(
    explicit_root: Option<PathBuf>,
    plan_path: &Path,
    token_env: Option<&str>,
    yes: bool,
    json: bool,
) -> Result<()> {
    let bytes = fs::read(plan_path).map_err(|source| {
        anyhow::anyhow!(ReleaseError::Plan {
            reason: format!("could not read {}: {source}", plan_path.display()),
        })
    })?;
    let stored: ReleasePlan = serde_json::from_slice(&bytes).map_err(|source| {
        anyhow::anyhow!(ReleaseError::Plan {
            reason: format!("could not parse {}: {source}", plan_path.display()),
        })
    })?;
    stored
        .validate_digest()
        .map_err(|error| anyhow::anyhow!(error))?;
    let root = resolve_workspace(explicit_root.or_else(|| Some(stored.workspace_root.clone())))?;
    if root != stored.workspace_root {
        return Err(anyhow::anyhow!(ReleaseError::WorkspaceMismatch {
            expected: stored.workspace_root.clone(),
            observed: root,
        }));
    }
    let repository = repository_identity(&root)?;
    let client = ci_dagger_session(&root, LockMode::Frozen).await?;
    let recomputed = plan_release_with_dagger(
        &root,
        &stored.target_version,
        Some(&stored.base_commit),
        repository,
        &client,
    )
    .await
    .context("recompute the release Plan")?;
    require_apply_admission(&stored, &recomputed, true).map_err(|error| anyhow::anyhow!(error))?;
    require_release_confirmation(&recomputed, yes, io::stdin().is_terminal())
        .map_err(|error| anyhow::anyhow!(error))?;

    let upload_required = recomputed
        .packages
        .iter()
        .any(|package| matches!(package.registry, PlannedRegistryState::Absent));
    let registry_credential = if upload_required {
        let name = token_env.ok_or_else(|| {
            anyhow::anyhow!(ReleaseError::CredentialMissing {
                name: "<not selected; pass --token-env>".to_owned(),
            })
        })?;
        let value = std::env::var(name).map_err(|_| {
            anyhow::anyhow!(ReleaseError::CredentialMissing {
                name: name.to_owned(),
            })
        })?;
        Some(RegistryCredential::new(value).map_err(|error| anyhow::anyhow!(error))?)
    } else {
        None
    };
    let publish_request = ReleasePublishRequest {
        plan: recomputed,
        registry_credential,
    };
    let parity = publish_and_verify_release(&publish_request, &client)
        .await
        .map_err(|error| anyhow::anyhow!(error))?;
    drop(publish_request);
    client
        .close()
        .await
        .context("close the publish-and-parity Dagger invocation")?;

    let observation_client = ci_dagger_session(&root, LockMode::Frozen).await?;
    let existing = observe_release_object(
        &parity.train.repository.slug,
        &parity.tag.tag,
        &observation_client,
    )
    .await
    .context("observe the public release object")?;
    observation_client
        .close()
        .await
        .context("close the read-only release observation")?;
    if let Some(existing) = existing {
        if existing.notes_sha256 != parity.release_notes_sha256
            || existing.target != parity.tag.commit
        {
            return Err(anyhow::anyhow!(ReleaseError::ReleaseConflict {
                tag: parity.tag.tag.clone(),
                reason: format!(
                    "observed target {} and notes digest {}; expected target {} and notes digest {}",
                    existing.target,
                    existing.notes_sha256,
                    parity.tag.commit,
                    parity.release_notes_sha256
                ),
            }));
        }
        let report = ReleaseReport {
            schema_version: parity.schema_version,
            train: parity.train,
            state: TrainState::Complete,
            packages: parity.packages,
            tag: parity.tag,
            release_notes: ReleaseNotesResult {
                outcome: ReleaseNotesOutcome::ExistingVerified,
                sha256: existing.notes_sha256,
            },
            diagnostics: Vec::new(),
        };
        render_report(&report, json)?;
        return Ok(());
    }

    let gh_value = std::env::var("GH_TOKEN")
        .map_err(|_| anyhow::anyhow!(ReleaseError::ReleaseCredentialMissing))?;
    let release_request = ReleaseNotesRequest {
        parity,
        release_api_credential: ReleaseApiCredential::new(gh_value)
            .map_err(|error| anyhow::anyhow!(error))?,
    };
    let notes_client = ci_dagger_session(&root, LockMode::Frozen).await?;
    let report = create_release_notes(&release_request, &notes_client)
        .await
        .map_err(|error| anyhow::anyhow!(error))?;
    notes_client
        .close()
        .await
        .context("close the release-note Dagger invocation")?;
    render_report(&report, json)
}

async fn run_verify(root: PathBuf, version: &str, output: Option<&Path>, json: bool) -> Result<()> {
    let repository = repository_identity(&root)?;
    let config = ReleaseConfig::load(&root, &repository).map_err(|error| anyhow::anyhow!(error))?;
    let request = ReleaseVerifyRequest {
        workspace_root: root.clone(),
        repository,
        version: version.to_owned(),
        tag: format!("v{version}"),
        release_branch: config.release_branch,
        expected_plan_digest: None,
    };
    let client = ci_dagger_session(&root, LockMode::Frozen).await?;
    let outcome = verify_release(&request, &client).await;
    client
        .close()
        .await
        .context("close the release verification Dagger invocation")?;
    let report = outcome.map_err(|error| anyhow::anyhow!(error))?;
    let bytes = serde_json::to_vec_pretty(&report)?;
    if let Some(path) = output {
        let mut terminated = bytes.clone();
        terminated.push(b'\n');
        write_atomic(path, &terminated).map_err(|source| {
            anyhow::anyhow!(ReleaseError::ReportOutput {
                path: path.to_path_buf(),
                reason: source.to_string(),
            })
        })?;
    }
    render_report(&report, json || output.is_none())
}

fn render_report(report: &ReleaseReport, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(report)?);
    } else {
        println!("Release {}: {:?}", report.tag.tag, report.state);
        for package in &report.packages {
            let marker = match package.outcome {
                PackageOutcome::Published => "published",
                PackageOutcome::ExistingVerified => "verified",
                PackageOutcome::Pending => "pending",
                PackageOutcome::Failed => "failed",
            };
            println!("  {} {}: {marker}", package.name, package.version);
        }
        println!("  release notes: {:?}", report.release_notes.outcome);
    }
    Ok(())
}

async fn run_plan(
    root: PathBuf,
    version: &str,
    base_ref: Option<&str>,
    output: Option<&Path>,
    json: bool,
) -> Result<()> {
    let repository = repository_identity(&root)?;
    let client = ci_dagger_session(&root, LockMode::Frozen).await?;
    let outcome = plan_release_with_dagger(&root, version, base_ref, repository, &client).await;
    client
        .close()
        .await
        .context("close the Dagger release planning session")?;
    let plan = outcome.context("release planning failed")?;
    if let Some(path) = output {
        write_atomic(
            path,
            &plan
                .canonical_json()
                .map_err(|error| anyhow::anyhow!(error))?,
        )
        .map_err(|source| {
            anyhow::anyhow!(ReleaseError::PlanOutput {
                path: path.to_path_buf(),
                reason: source.to_string(),
            })
        })?;
    }
    if json {
        print!(
            "{}",
            String::from_utf8(
                plan.canonical_json()
                    .map_err(|error| anyhow::anyhow!(error))?
            )?
        );
    } else if let Some(output) = output {
        render_plan(&plan);
        println!("Plan written to {}", output.display());
    } else {
        print!(
            "{}",
            String::from_utf8(
                plan.canonical_json()
                    .map_err(|error| anyhow::anyhow!(error))?
            )?
        );
    }
    Ok(())
}

fn render_plan(plan: &ReleasePlan) {
    let existing = plan
        .packages
        .iter()
        .filter(|package| matches!(package.registry, PlannedRegistryState::Existing { .. }))
        .count();
    println!("Release Plan {}", plan.tag);
    println!("  repository: {}", plan.repository.slug);
    println!("  base commit: {}", plan.base_commit);
    println!("  plan digest: sha256:{}", plan.digest);
    println!(
        "  packages: {} ({} already observed)",
        plan.packages.len(),
        existing
    );
    println!("  outward effects:");
    for effect in &plan.effects {
        println!("    - {:?}: {}", effect.kind, effect.summary);
    }
}

fn require_release_confirmation(
    plan: &ReleasePlan,
    yes: bool,
    interactive: bool,
) -> Result<(), ReleaseError> {
    render_plan(plan);
    if yes {
        return Ok(());
    }
    if !interactive {
        return Err(ReleaseError::Confirmation {
            reason: "--yes is required in a non-interactive session".to_owned(),
        });
    }
    print!("Apply this exact release Plan? [y/N] ");
    io::stdout()
        .flush()
        .map_err(|source| ReleaseError::Executor {
            reason: format!("could not flush confirmation prompt: {source}"),
        })?;
    let mut answer = String::new();
    io::stdin()
        .read_line(&mut answer)
        .map_err(|source| ReleaseError::Executor {
            reason: format!("could not read release confirmation: {source}"),
        })?;
    if matches!(answer.trim(), "y" | "Y" | "yes" | "YES") {
        Ok(())
    } else {
        Err(ReleaseError::ConfirmationDeclined)
    }
}

fn repository_identity(root: &Path) -> Result<RepositoryIdentity> {
    let output = Command::new("git")
        .args(["-C"])
        .arg(root)
        .args(["remote", "get-url", "origin"])
        .output()
        .map_err(|source| {
            anyhow::anyhow!(ReleaseError::Workspace {
                reason: format!("could not inspect the origin remote: {source}"),
            })
        })?;
    if !output.status.success() {
        return Err(anyhow::anyhow!(ReleaseError::Workspace {
            reason: format!(
                "could not read origin remote: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        }));
    }
    let raw = String::from_utf8(output.stdout)
        .map_err(|source| {
            anyhow::anyhow!(ReleaseError::Workspace {
                reason: format!("origin remote is not UTF-8: {source}"),
            })
        })?
        .trim()
        .to_owned();
    let normalized = normalize_remote(&raw)?;
    let path = normalized
        .strip_prefix("https://github.com/")
        .ok_or_else(|| {
            anyhow::anyhow!(ReleaseError::Workspace {
                reason: "origin is not a canonical GitHub repository".to_owned(),
            })
        })?
        .trim_end_matches('/');
    let slug = path.strip_suffix(".git").unwrap_or(path).to_owned();
    Ok(RepositoryIdentity {
        remote: format!("https://github.com/{slug}"),
        slug,
    })
}

fn normalize_remote(remote: &str) -> Result<String> {
    if let Some(path) = remote.strip_prefix("git@github.com:") {
        return Ok(format!("https://github.com/{path}"));
    }
    if let Some(rest) = remote.strip_prefix("https://") {
        let without_user = rest.rsplit_once('@').map_or(rest, |(_, value)| value);
        return Ok(format!("https://{without_user}"));
    }
    Err(anyhow::anyhow!(ReleaseError::Workspace {
        reason: "origin remote must use GitHub HTTPS or SSH syntax".to_owned(),
    }))
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("create Plan output directory {}", parent.display()))?;
    let temporary = parent.join(format!(
        ".{}-{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("release-plan"),
        Uuid::new_v4()
    ));
    let result = (|| {
        fs::write(&temporary, bytes)
            .with_context(|| format!("write temporary Plan {}", temporary.display()))?;
        fs::rename(&temporary, path)
            .with_context(|| format!("atomically publish Plan {}", path.display()))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn resolve_workspace(explicit: Option<PathBuf>) -> Result<PathBuf> {
    let exact = explicit.is_some();
    let start = explicit.unwrap_or(std::env::current_dir().context("read current directory")?);
    let canonical = start.canonicalize().map_err(|source| {
        anyhow::anyhow!(ReleaseError::Workspace {
            reason: format!(
                "could not resolve workspace path {}: {source}",
                start.display()
            ),
        })
    })?;
    if exact {
        return is_workspace_root(&canonical)
            .then_some(canonical)
            .ok_or_else(|| {
                anyhow::anyhow!(ReleaseError::Workspace {
                    reason: format!(
                        "--workspace-root must name the canonical Cargo workspace root: {}",
                        start.display()
                    ),
                })
            });
    }
    let matches = canonical
        .ancestors()
        .filter(|candidate| is_workspace_root(candidate))
        .map(Path::to_path_buf)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [root] => Ok(root.clone()),
        [] => Err(anyhow::anyhow!(ReleaseError::Workspace {
            reason: format!(
                "could not find a Cargo workspace containing {}",
                canonical.display()
            ),
        })),
        roots => Err(anyhow::anyhow!(ReleaseError::AmbiguousWorkspace {
            roots: roots.to_vec(),
        })),
    }
}

fn is_workspace_root(candidate: &Path) -> bool {
    let manifest = candidate.join("Cargo.toml");
    std::fs::read_to_string(manifest)
        .ok()
        .and_then(|text| text.parse::<toml::Value>().ok())
        .is_some_and(|manifest| manifest.get("workspace").is_some())
}

#[cfg(test)]
mod tests {
    use clap::Parser as _;

    use super::*;
    use crate::cli::{Cli, Command};

    #[test]
    fn exposes_exactly_the_four_release_subverbs() {
        for arguments in [
            vec!["tkr", "release", "fragment", "--kind", "internal"],
            vec!["tkr", "release", "plan", "--version", "1.2.3"],
            vec!["tkr", "release", "apply", "--plan", "plan.json", "--yes"],
            vec!["tkr", "release", "verify", "--version", "1.2.3"],
        ] {
            assert!(
                matches!(
                    Cli::try_parse_from(arguments)
                        .expect("valid release command")
                        .command,
                    Command::Release(_)
                ),
                "release command should parse"
            );
        }
        assert!(Cli::try_parse_from(["tkr", "release", "publish"]).is_err());
    }

    #[test]
    fn remote_identity_removes_transport_details() {
        assert_eq!(
            normalize_remote("git@github.com:tokeira/tokeira.git").expect("SSH remote"),
            "https://github.com/tokeira/tokeira.git"
        );
        assert_eq!(
            normalize_remote("https://operator@github.com/tokeira/tokeira.git")
                .expect("credential-bearing HTTPS remote"),
            "https://github.com/tokeira/tokeira.git"
        );
    }
}
