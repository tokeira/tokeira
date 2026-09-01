//! `tkr release` — generic Plan, confirmed apply, fragment, and verify commands.
//!
//! Stdout carries only the verb's answer (a Plan, a Report, or a fragment path), so
//! `--json` consumers can parse it. The Plan rendering that precedes a confirmation
//! prompt, and the prompt itself, go to stderr in JSON mode. A train that stops after
//! a public boundary still renders its Release Report before the refusal, so the
//! operator sees what is durable without reading logs.

mod changie;

use std::{
    fs,
    io::{self, BufRead, IsTerminal as _, Write},
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
    // The operator's answer is resolved first and handed to the fence as a fact, so
    // the fence, not this handler, is what stands between a decline and a mutation.
    let confirmed = require_release_confirmation(
        &recomputed,
        yes,
        io::stdin().is_terminal(),
        json,
        &mut io::stdin().lock(),
    )
    .map_err(|error| anyhow::anyhow!(error))?;
    require_apply_admission(&stored, &recomputed, confirmed)
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
    let parity = match publish_and_verify_release(&publish_request, &client).await {
        Ok(parity) => parity,
        Err(error) => {
            render_stopped_train(&error, json)?;
            return Err(anyhow::anyhow!(error));
        }
    };
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
            diagnostics: parity.diagnostics,
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
    let report = match outcome {
        Ok(report) => report,
        Err(error) => {
            render_stopped_train(&error, json)?;
            return Err(anyhow::anyhow!(error));
        }
    };
    if let Some(path) = output {
        let mut terminated = serde_json::to_vec_pretty(&report)?;
        terminated.push(b'\n');
        write_atomic(path, &terminated).map_err(|source| {
            anyhow::anyhow!(ReleaseError::ReportOutput {
                path: path.to_path_buf(),
                reason: source.to_string(),
            })
        })?;
    }
    render_report(&report, json)
}

/// Render the Release Report a stopped train carries, if it carries one.
fn render_stopped_train(error: &ReleaseError, json: bool) -> Result<()> {
    match error.report() {
        Some(report) => render_report(report, json),
        None => Ok(()),
    }
}

fn render_report(report: &ReleaseReport, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(report)?);
        return Ok(());
    }
    println!("Release {}: {:?}", report.tag.tag, report.state);
    if !report.tag.published {
        println!("  git: branch and tag not published");
    }
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
    for diagnostic in &report.diagnostics {
        println!("  {}: {}", diagnostic.code, diagnostic.summary);
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
    let canonical = plan
        .canonical_json()
        .map_err(|error| anyhow::anyhow!(error))?;
    if let Some(path) = output {
        write_atomic(path, &canonical).map_err(|source| {
            anyhow::anyhow!(ReleaseError::PlanOutput {
                path: path.to_path_buf(),
                reason: source.to_string(),
            })
        })?;
    }
    if json {
        print!("{}", String::from_utf8(canonical)?);
    } else if let Some(output) = output {
        let mut stdout = io::stdout().lock();
        render_plan(&plan, &mut stdout)?;
        writeln!(stdout, "Plan written to {}", output.display())?;
    } else {
        print!("{}", String::from_utf8(canonical)?);
    }
    Ok(())
}

fn render_plan(plan: &ReleasePlan, out: &mut dyn Write) -> io::Result<()> {
    let existing = plan
        .packages
        .iter()
        .filter(|package| matches!(package.registry, PlannedRegistryState::Existing { .. }))
        .count();
    writeln!(out, "Release Plan {}", plan.tag)?;
    writeln!(out, "  repository: {}", plan.repository.slug)?;
    writeln!(out, "  base commit: {}", plan.base_commit)?;
    writeln!(out, "  plan digest: sha256:{}", plan.digest)?;
    writeln!(
        out,
        "  packages: {} ({} already observed)",
        plan.packages.len(),
        existing
    )?;
    writeln!(out, "  outward effects:")?;
    for effect in &plan.effects {
        writeln!(out, "    - {:?}: {}", effect.kind, effect.summary)?;
    }
    Ok(())
}

/// Resolve the operator's answer to the exact recomputed Plan.
///
/// `Ok(true)` is an affirmative answer or `--yes`; `Ok(false)` is an explicit decline
/// that the fence turns into `ConfirmationDeclined`; a non-interactive session without
/// `--yes` cannot answer at all and is refused here. In JSON mode the rendering and
/// the prompt go to stderr so stdout stays parseable.
fn require_release_confirmation(
    plan: &ReleasePlan,
    yes: bool,
    interactive: bool,
    json: bool,
    input: &mut dyn BufRead,
) -> Result<bool, ReleaseError> {
    let mut prompt: Box<dyn Write> = if json {
        Box::new(io::stderr())
    } else {
        Box::new(io::stdout())
    };
    let io_error = |source: io::Error| ReleaseError::Executor {
        reason: format!("could not present the release confirmation: {source}"),
    };
    render_plan(plan, &mut *prompt).map_err(io_error)?;
    if yes {
        return Ok(true);
    }
    if !interactive {
        return Err(ReleaseError::Confirmation {
            reason: "--yes is required in a non-interactive session".to_owned(),
        });
    }
    write!(prompt, "Apply this exact release Plan? [y/N] ").map_err(io_error)?;
    prompt.flush().map_err(io_error)?;
    let mut answer = String::new();
    input.read_line(&mut answer).map_err(io_error)?;
    Ok(matches!(answer.trim(), "y" | "Y" | "yes" | "YES"))
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
    let raw = String::from_utf8(output.stdout).map_err(|source| {
        anyhow::anyhow!(ReleaseError::Workspace {
            reason: format!("origin remote is not UTF-8: {source}"),
        })
    })?;
    RepositoryIdentity::from_remote(&raw).map_err(|error| anyhow::anyhow!(error))
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
    use std::io::Cursor;

    use clap::Parser as _;
    use tokeira_build::{ChangieIdentity, PackagePlan, RELEASE_SCHEMA_VERSION, ToolchainIdentity};

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

    fn sample_plan() -> ReleasePlan {
        let mut plan = ReleasePlan {
            schema_version: RELEASE_SCHEMA_VERSION,
            repository: RepositoryIdentity {
                slug: "tokeira/tokeira".to_owned(),
                remote: "https://github.com/tokeira/tokeira".to_owned(),
            },
            workspace_root: PathBuf::from("/workspace"),
            base_commit: "a".repeat(40),
            target_version: "1.0.0".to_owned(),
            tag: "v1.0.0".to_owned(),
            packages: vec![PackagePlan {
                name: "crate-a".to_owned(),
                manifest_path: PathBuf::from("crate-a/Cargo.toml"),
                from_version: "0.1.0".to_owned(),
                target_version: "1.0.0".to_owned(),
                publishable_dependencies: Vec::new(),
                hermetic_sha256: "f".repeat(64),
                registry: PlannedRegistryState::Absent,
            }],
            fragments: Vec::new(),
            changelog_config_sha256: "b".repeat(64),
            changie_release: ChangieIdentity {
                version: "1.25.2".to_owned(),
                source_revision: "c".repeat(40),
                platform: "linux-x86_64".to_owned(),
                asset: "changie.tar.gz".to_owned(),
                asset_sha256: "d".repeat(64),
            },
            toolchain: ToolchainIdentity {
                rust: "1.97.1".to_owned(),
                dagger: "0.19.8".to_owned(),
            },
            release_notes_sha256: "e".repeat(64),
            effects: Vec::new(),
            digest: String::new(),
        };
        plan.seal().expect("sample Plan seals");
        plan
    }

    #[test]
    fn confirmation_is_answered_by_the_operator_not_the_handler() {
        let plan = sample_plan();
        let mut nothing = Cursor::new("");
        assert!(
            require_release_confirmation(&plan, true, false, true, &mut nothing).expect("--yes"),
            "--yes confirms without a prompt"
        );
        assert!(matches!(
            require_release_confirmation(&plan, false, false, true, &mut nothing),
            Err(ReleaseError::Confirmation { .. })
        ));
        let mut affirmative = Cursor::new("y\n");
        assert!(
            require_release_confirmation(&plan, false, true, true, &mut affirmative)
                .expect("interactive answer")
        );
        let mut decline = Cursor::new("n\n");
        assert!(
            !require_release_confirmation(&plan, false, true, true, &mut decline)
                .expect("interactive answer")
        );
        assert!(matches!(
            require_apply_admission(&plan, &plan, false),
            Err(ReleaseError::ConfirmationDeclined)
        ));
        assert!(require_apply_admission(&plan, &plan, true).is_ok());
    }
}
