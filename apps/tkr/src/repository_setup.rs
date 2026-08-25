//! The deployment-repository shell: create-side provisioning, the birth
//! publication, and the repository verbs (`fetch`, `list`, `publish`,
//! `refresh`, `inspect`) — all thin over `tokeira_deployment::repository`.
//!
//! Create's ordering is the design (Requirement 2.3/2.4): keys, publisher
//! configuration, pinned trust anchor, datastore, and the `metadata.json`
//! binding all land inside the *staged* deployment — so the atomic rename
//! commits a deployment already wired to its repository — while the upload
//! itself runs only after that local commit. A failed upload therefore
//! leaves a created deployment whose publication is pending, never a
//! half-created deployment.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use tokeira_deployment::repository::{
    assemble::{ClaimInputs, claim_from_dir, publication_input_from_dir},
    claim::Transition,
    config::{DATASTORE_DIR, RepositoryConfig, TRUST_ANCHOR},
    fetch::MaterializePlan,
    keys::RoleKeyConfig,
    list::{DeploymentEntry, list_local, list_remote},
    locator::RepositoryLocator,
    open::{Freshness, OpenRepository, open},
    publish::{PublicationReceipt, author_trust_anchor, publish_transition},
    refresh::refresh_freshness,
};

use crate::metadata::DeploymentRepositoryBinding;

/// Where a local deployment's repository lives under the deployments root.
pub(crate) fn local_repository_home(deployments_root: &Path, name: &str) -> PathBuf {
    deployments_root.join("repositories").join(name)
}

/// Where a local deployment's role keys live: under the deployments root,
/// outside the deployment dir, so paths recorded in `publisher.json` while
/// staged stay valid after the atomic rename.
pub(crate) fn local_keys_home(deployments_root: &Path, name: &str) -> PathBuf {
    deployments_root.join(".repository-keys").join(name)
}

/// The staged repository state create carries across the rename.
pub(crate) struct StagedRepository {
    pub(crate) config: RepositoryConfig,
    pub(crate) trusted_root: Vec<u8>,
}

/// Provision the staged deployment's repository state — everything except
/// the upload: local role keys, `publisher.json`, the pinned trust anchor,
/// the datastore dir, and the `metadata.json` binding.
pub(crate) async fn provision_staged(
    deployments_root: &Path,
    staging: &Path,
    name: &str,
) -> Result<StagedRepository> {
    let keys = RoleKeyConfig::generate_local(&local_keys_home(deployments_root, name))
        .context("failed to generate the repository role keys")?;
    let config = RepositoryConfig {
        locator: RepositoryLocator::Local {
            path: local_repository_home(deployments_root, name),
        },
        keys,
        lifetimes: Default::default(),
    };
    let trusted_root = author_trust_anchor(&config)
        .await
        .context("failed to author the trust anchor")?;

    let anchor_path = staging.join(TRUST_ANCHOR);
    if let Some(parent) = anchor_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(&anchor_path, &trusted_root)
        .with_context(|| format!("failed to pin {}", anchor_path.display()))?;
    std::fs::create_dir_all(staging.join(DATASTORE_DIR))
        .context("failed to initialize the repository datastore")?;
    config
        .store(staging)
        .context("failed to write publisher.json")?;

    let mut metadata = crate::metadata::read(staging)?;
    metadata.deployment_repository = Some(DeploymentRepositoryBinding {
        locator: config.locator.clone(),
        trusted_root_digest: tokeira_deployment::sha256_hex(&trusted_root),
    });
    crate::metadata::write(staging, &metadata)?;

    Ok(StagedRepository {
        config,
        trusted_root,
    })
}

/// Run the birth publication from the committed deployment dir. The caller
/// reports a failure as "created, publication pending" — the local commit
/// is never unwound.
pub(crate) async fn publish_birth(
    deployment_dir: &Path,
    staged: &StagedRepository,
    identity: tokeira_platform::definition::ConfigurationIdentity,
    companions: Vec<String>,
) -> Result<PublicationReceipt> {
    let claim = claim_from_dir(
        deployment_dir,
        &ClaimInputs {
            identity,
            companions,
            transition: Transition::Create,
            config_revision: 0,
        },
    )?;
    let input = publication_input_from_dir(deployment_dir, claim)?;
    Ok(publish_transition(&staged.config, input, 0, Some(&staged.trusted_root), None).await?)
}

/// Parse a CLI repository locator: an `s3://<bucket>/<prefix>` base or a
/// local filesystem path.
pub(crate) fn parse_locator(input: &str) -> Result<RepositoryLocator> {
    if let Some(rest) = input.strip_prefix("s3://") {
        let (bucket, prefix) = rest.split_once('/').ok_or_else(|| {
            anyhow!("an S3 locator needs a bucket and prefix: s3://<bucket>/<prefix>")
        })?;
        Ok(RepositoryLocator::S3 {
            bucket: bucket.to_string(),
            prefix: prefix.trim_end_matches('/').to_string(),
        })
    } else {
        Ok(RepositoryLocator::Local {
            path: PathBuf::from(input),
        })
    }
}

/// An S3 client from the ambient AWS configuration, only when a locator
/// needs one.
async fn s3_client_for(locator: &RepositoryLocator) -> Option<aws_sdk_s3::Client> {
    match locator {
        RepositoryLocator::Local { .. } => None,
        RepositoryLocator::S3 { .. } => {
            let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            Some(aws_sdk_s3::Client::new(&config))
        }
    }
}

/// Open a deployment's bound repository from its own pinned state, guarding
/// the anchor against replacement (its digest is compared to the
/// `metadata.json` record before use).
async fn open_bound(
    deployment_dir: &Path,
    freshness: Freshness,
) -> Result<(OpenRepository, RepositoryLocator, Vec<u8>)> {
    let metadata = crate::metadata::read(deployment_dir)?;
    let binding = metadata.deployment_repository.ok_or_else(|| {
        anyhow!(
            "this deployment carries no repository binding in metadata.json; \
             it predates the deployment repository"
        )
    })?;
    let anchor_path = deployment_dir.join(TRUST_ANCHOR);
    let anchor = std::fs::read(&anchor_path).with_context(|| {
        format!(
            "pinned trust anchor unreadable at {}",
            anchor_path.display()
        )
    })?;
    let digest = tokeira_deployment::sha256_hex(&anchor);
    if digest != binding.trusted_root_digest {
        bail!(
            "trust_anchor_digest_mismatch: {} hashes to {digest} but metadata.json records {}; \
             the pinned file was replaced outside the client",
            anchor_path.display(),
            binding.trusted_root_digest
        );
    }
    let s3 = s3_client_for(&binding.locator).await;
    let opened = open(
        &binding.locator,
        &anchor,
        Some(&deployment_dir.join(DATASTORE_DIR)),
        freshness,
        s3,
    )
    .await
    .map_err(|error| anyhow!("{error}"))?;
    Ok((opened, binding.locator, anchor))
}

/// `tkr deployment fetch`: materialize a published Deployment into a new
/// deployment dir — refusing before any byte lands (Requirement 5).
pub(crate) async fn fetch(
    deployments: &crate::deployment_dir::DeploymentResolver,
    name: &str,
    repository: &str,
    trust_anchor: &Path,
) -> Result<()> {
    let name = crate::deployment_dir::normalize_name(name);
    let final_path = deployments.path(&name);
    if final_path.exists() {
        bail!(
            "deployment '{name}' already exists at {}",
            final_path.display()
        );
    }
    let locator = parse_locator(repository)?;
    let anchor = std::fs::read(trust_anchor)
        .with_context(|| format!("trust anchor unreadable at {}", trust_anchor.display()))?;
    let s3 = s3_client_for(&locator).await;

    // Stage away from the final name; verification refusals leave nothing.
    std::fs::create_dir_all(deployments.root())?;
    let staging = deployments
        .root()
        .join(format!(".{name}.fetch-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(staging.join("state"))?;
    let result = async {
        let fetched = fetch_into(&staging, &locator, &anchor, s3, &name).await?;
        let facts = crate::launcher::validate_staged_definition(&staging).await?;
        crate::launcher::realize_staged_deployment(&staging, &facts, fetched.config_revision)
            .await?;
        Ok::<_, anyhow::Error>(fetched)
    }
    .await;
    match result {
        Ok(fetched) => {
            std::fs::rename(&staging, &final_path).with_context(|| {
                format!("failed to commit the fetch to {}", final_path.display())
            })?;
            println!(
                "fetched publication {version} of deployment '{name}' from {} into {}",
                locator.display(),
                final_path.display(),
                version = fetched.publication_version,
            );
            Ok(())
        }
        Err(error) => {
            let _ = std::fs::remove_dir_all(&staging);
            Err(error)
        }
    }
}

struct FetchedPublication {
    publication_version: u64,
    config_revision: u64,
}

async fn fetch_into(
    staging: &Path,
    locator: &RepositoryLocator,
    anchor: &[u8],
    s3: Option<aws_sdk_s3::Client>,
    name: &str,
) -> Result<FetchedPublication> {
    // The TUF client's datastore must exist before the load that fills it.
    std::fs::create_dir_all(staging.join(DATASTORE_DIR))?;
    let opened = open(
        locator,
        anchor,
        Some(&staging.join(DATASTORE_DIR)),
        Freshness::Enforced,
        s3,
    )
    .await
    .map_err(|error| anyhow!("{error}"))?;
    // Re-pin the anchor the chain accepted: the caller's own bytes when no
    // root walk happened (byte-exact — digests stay stable across seats),
    // the walked-forward root's re-serialization otherwise.
    let pinned_version = serde_json::from_slice::<serde_json::Value>(anchor)
        .ok()
        .and_then(|value| value["signed"]["version"].as_u64());
    let accepted_anchor = if pinned_version == Some(opened.root_version()) {
        anchor.to_vec()
    } else {
        opened.trust_anchor().map_err(|error| anyhow!("{error}"))?
    };
    let publication = opened
        .verified_publication()
        .await
        .map_err(|error| anyhow!("{error}"))?;
    let plan = MaterializePlan::new(&publication, env!("TKR_TARGET"))
        .map_err(|error| anyhow!("{error}"))?;
    plan.materialize_into(&publication, staging)
        .await
        .map_err(|error| anyhow!("{error}"))?;

    let anchor_path = staging.join(TRUST_ANCHOR);
    std::fs::create_dir_all(anchor_path.parent().expect("state/repository"))?;
    std::fs::write(&anchor_path, &accepted_anchor)?;

    // Synthesize the seat-local metadata from the verified claim; the
    // storage kind comes from the fetched server configuration.
    let claim = publication.claim();
    let storage = read_fetched_storage(staging);
    let now = chrono::Utc::now().to_rfc3339();
    let metadata = crate::metadata::DeploymentMetadata {
        name: name.to_string(),
        id: claim.deployment.id,
        platform: claim.platform.clone(),
        definition: Some(tokeira_deployment::RecordedDefinition {
            format: claim.format.clone(),
            path: tokeira_orchestrator::RelativeDefinitionPath::new(&claim.definition.root)
                .map_err(|error| anyhow!("claimed definition root: {error}"))?,
        }),
        deployment_repository: Some(DeploymentRepositoryBinding {
            locator: locator.clone(),
            trusted_root_digest: tokeira_deployment::sha256_hex(&accepted_anchor),
        }),
        storage,
        status: crate::metadata::DeploymentStatus::Created,
        created_at: now.clone(),
        updated_at: now,
    };
    crate::metadata::write(staging, &metadata)?;
    Ok(FetchedPublication {
        publication_version: publication.version(),
        config_revision: claim.config_revision,
    })
}

/// The storage kind recorded in the fetched `tokeirad.toml` (the CLI-side
/// display fact; the config itself is the authority).
fn read_fetched_storage(staging: &Path) -> tokeira_orchestrator::StorageKind {
    let path = staging.join(crate::deployment_dir::TOKEIRAD_TOML);
    match tokeira_config::TokeiraConfig::load(&path).map(|config| config.infrastructure.storage) {
        Ok(tokeira_config::ConfigStorageKind::Dsql) => tokeira_orchestrator::StorageKind::Dsql,
        _ => tokeira_orchestrator::StorageKind::InMemory,
    }
}

/// `tkr deployment list --repositories`: enumerate published repositories.
pub(crate) async fn list_repositories(
    deployments_root: &Path,
    selector: &str,
    json: bool,
) -> Result<()> {
    let entries: Vec<DeploymentEntry> = if selector == "local" {
        list_local(deployments_root).map_err(|error| anyhow!("{error}"))?
    } else if let RepositoryLocator::S3 { bucket, prefix } = parse_locator(selector)? {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = aws_sdk_s3::Client::new(&config);
        list_remote(&client, &bucket, &prefix)
            .await
            .map_err(|error| anyhow!("{error}"))?
    } else {
        bail!("--repositories takes `local` or an `s3://<bucket>/<prefix>` base");
    };
    let output = crate::output::OutputFormatter::new(json);
    if json {
        let rows: Vec<serde_json::Value> = entries
            .iter()
            .map(|entry| {
                serde_json::json!({
                    "name": entry.name,
                    "repository": entry.locator.display(),
                })
            })
            .collect();
        output.print_json(&rows)?;
    } else if entries.is_empty() {
        println!("no published deployment repositories found");
    } else {
        for entry in entries {
            println!("{}\t{}", entry.name, entry.locator.display());
        }
    }
    Ok(())
}

/// `tkr deployment publish`: complete a pending publication from the
/// committed deployment state (Requirements 2.4 / 4.2).
pub(crate) async fn publish_repair(deployment_dir: &Path, transition: Option<&str>) -> Result<()> {
    let config = RepositoryConfig::load(deployment_dir)
        .map_err(|error| anyhow!("{error}"))?
        .ok_or_else(|| {
            anyhow!(
                "this seat has no publisher configuration \
                 (state/repository/publisher.json); fetched seats are \
                 read-only until keys are supplied"
            )
        })?;
    let anchor_path = deployment_dir.join(TRUST_ANCHOR);
    let anchor = std::fs::read(&anchor_path).with_context(|| {
        format!(
            "pinned trust anchor unreadable at {}",
            anchor_path.display()
        )
    })?;
    let s3 = s3_client_for(&config.locator).await;

    // The lineage decides the default transition: an empty repository means
    // the pending publication is the birth.
    let existing = open(
        &config.locator,
        &anchor,
        Some(&deployment_dir.join(DATASTORE_DIR)),
        Freshness::Enforced,
        s3.clone(),
    )
    .await;
    let (expected_version, default_transition) = match &existing {
        Ok(opened) => (opened.version(), Transition::Apply),
        Err(_) if repository_absent(&config.locator) => (0, Transition::Create),
        Err(error) => bail!("repository load refused: {error}"),
    };
    let transition = match transition {
        Some("create") => Transition::Create,
        Some("apply") => Transition::Apply,
        Some("upgrade") => Transition::Upgrade,
        Some("revert") => Transition::Revert,
        Some(other) => bail!("unknown transition `{other}` (create|apply|upgrade|revert)"),
        None => default_transition,
    };

    // Identity facts from the engine's own check of the committed state.
    let facts = crate::launcher::validate_staged_definition(deployment_dir).await?;
    let identity = facts
        .identity
        .ok_or_else(|| anyhow!("the check verified but reported no configuration identity"))?;
    let config_revision = read_config_revision(deployment_dir).await?;

    let claim = claim_from_dir(
        deployment_dir,
        &ClaimInputs {
            identity,
            companions: facts.companions.unwrap_or_default(),
            transition,
            config_revision,
        },
    )
    .map_err(|error| anyhow!("{error}"))?;
    let input =
        publication_input_from_dir(deployment_dir, claim).map_err(|error| anyhow!("{error}"))?;
    let receipt = publish_transition(&config, input, expected_version, Some(&anchor), s3)
        .await
        .map_err(|error| anyhow!("{error}"))?;
    println!(
        "repository: publication {} written to {}",
        receipt.version,
        config.locator.display()
    );
    Ok(())
}

fn repository_absent(locator: &RepositoryLocator) -> bool {
    match locator {
        RepositoryLocator::Local { path } => !path.join("metadata/timestamp.json").exists(),
        // A remote head's absence is not probed here; the load error stands.
        RepositoryLocator::S3 { .. } => false,
    }
}

/// The committed configuration revision, from the deployment envelope.
async fn read_config_revision(deployment_dir: &Path) -> Result<u64> {
    let store: Box<
        dyn tokeira_state::DeploymentStore<tokeira_deployment::DeploymentStateEnvelope>,
    > = Box::new(tokeira_state::CasStore::new(
        Box::new(tokeira_state::LocalBackend::new(
            deployment_dir.join("state/envelope"),
        )),
        "envelope".to_string(),
    ));
    let (envelope, _) = store
        .load()
        .await
        .context("failed to load the deployment envelope")?;
    Ok(envelope.config_revision)
}

/// `tkr deployment refresh`: re-sign freshness for the current publication.
pub(crate) async fn refresh(deployment_dir: &Path) -> Result<()> {
    let config = RepositoryConfig::load(deployment_dir)
        .map_err(|error| anyhow!("{error}"))?
        .ok_or_else(|| {
            anyhow!(
                "this seat has no publisher configuration; refresh signs with \
                 the publisher's keys"
            )
        })?;
    let anchor = std::fs::read(deployment_dir.join(TRUST_ANCHOR))
        .context("pinned trust anchor unreadable")?;
    let s3 = s3_client_for(&config.locator).await;
    let refreshed = refresh_freshness(&config, &anchor, s3)
        .await
        .map_err(|error| anyhow!("{error}"))?;
    println!(
        "repository: freshness re-signed for publication {} — timestamp expires {}, snapshot {}{}",
        refreshed.publication_version,
        refreshed.timestamp_expires,
        refreshed.snapshot_expires,
        if refreshed.snapshot_resigned {
            " (re-signed)"
        } else {
            ""
        }
    );
    Ok(())
}

/// `tkr deployment inspect`: read-only verification + report
/// (Requirement 11.3) — refusing with the same classification a fetch
/// would.
pub(crate) async fn inspect(deployment_dir: &Path, json: bool) -> Result<()> {
    let (opened, locator, _anchor) = open_bound(deployment_dir, Freshness::Enforced).await?;
    let timestamp_expires = opened.timestamp_expires();
    let publication = opened
        .verified_publication()
        .await
        .map_err(|error| anyhow!("{error}"))?;
    let claim = publication.claim();
    if json {
        let value = serde_json::json!({
            "repository": locator.display(),
            "publication": publication.version(),
            "transition": claim.transition,
            "claim": claim,
            "timestamp_expires": timestamp_expires.to_string(),
            "targets": {
                "definition": claim.definition.root,
                "companions": claim.definition.companions,
                "config": publication.config_targets(),
                "engine": publication
                    .artifacts()
                    .iter()
                    .map(|artifact| artifact.target.clone())
                    .collect::<Vec<_>>(),
            },
        });
        crate::output::OutputFormatter::new(true).print_json(&value)?;
    } else {
        println!("repository   {}", locator.display());
        println!("publication  {}", publication.version());
        println!("transition   {:?}", claim.transition);
        println!(
            "deployment   {} ({})",
            claim.deployment.name, claim.deployment.id
        );
        println!(
            "definition   {} + {} companions — identity {}",
            claim.definition.root,
            claim.definition.companions.len(),
            claim.definition.identity.digest
        );
        println!(
            "engine       {} ({}, {})",
            &claim.engine.identity_digest[..12.min(claim.engine.identity_digest.len())],
            claim.engine.provisioner_version,
            claim.engine.build_authority
        );
        println!("freshness    expires {timestamp_expires}");
        println!(
            "targets      {} config, {} engine artifacts",
            publication.config_targets().len(),
            publication.artifacts().len()
        );
    }
    Ok(())
}
