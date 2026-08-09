//! Operator-poke CLI for the spike: generate keys, publish the fixture set,
//! verify/fetch it back — from a directory or straight from S3.
//!
//! ```text
//! tuf-spike keygen  --keys <dir>
//! tuf-spike publish --keys <dir> --set <dir> --root-doc deployment.tkd --out <dir>
//!                   [--version N] [--kms-key-id <id-or-arn> [--profile <p>]]
//! tuf-spike verify  --repo <dir>            # load + verify + print the set
//! tuf-spike upload  --repo <dir> --bucket <b> --prefix <p>
//! tuf-spike verify-s3 --trusted-root <file> --bucket <b> --prefix <p>
//! ```
// A CLI communicates on stdout/stderr by design; the library crates carry
// the print lints.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::path::PathBuf;

use anyhow::Context as _;
use spike_tuf_platform_definition::{
    consume::{fetch_definition_set, load_repository},
    keys::RoleKeyFiles,
    kms::kms_role_key,
    publish::{PublishOptions, RoleSources, SharedKeySource, publish_set},
    s3::{S3Transport, repo_urls, upload_repository},
    set::load_set_from_dir,
};
use tough::{FilesystemTransport, key_source::LocalKeySource};

fn arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn required(args: &[String], flag: &str) -> anyhow::Result<String> {
    arg(args, flag).with_context(|| format!("missing {flag}"))
}

fn local_role_sources(dir: &std::path::Path) -> RoleSources {
    let source = |name: &str| {
        SharedKeySource::new(LocalKeySource {
            path: dir.join(format!("{name}.ed25519.der")),
        })
    };
    RoleSources {
        root: source("root"),
        targets: source("targets"),
        snapshot: source("snapshot"),
        timestamp: source("timestamp"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let verb = args.first().map(String::as_str).unwrap_or("");
    match verb {
        "keygen" => {
            let dir = PathBuf::from(required(&args, "--keys")?);
            let files = RoleKeyFiles::generate(&dir)?;
            println!("generated role keys under {}", dir.display());
            for path in [
                &files.root,
                &files.targets,
                &files.snapshot,
                &files.timestamp,
            ] {
                println!("  {}", path.display());
            }
        }
        "publish" => {
            let keys_dir = PathBuf::from(required(&args, "--keys")?);
            let set_dir = PathBuf::from(required(&args, "--set")?);
            let root_doc = required(&args, "--root-doc")?;
            let out = PathBuf::from(required(&args, "--out")?);
            let format = root_doc
                .rsplit('.')
                .next()
                .context("root doc has no extension")?
                .to_owned();
            let version = arg(&args, "--version")
                .map(|v| v.parse::<u64>())
                .transpose()?
                .unwrap_or(1);

            let mut keys = local_role_sources(&keys_dir);
            if let Some(kms_key_id) = arg(&args, "--kms-key-id") {
                // KMS holds the online roles; root stays on the local file.
                let source = kms_role_key(arg(&args, "--profile"), kms_key_id);
                keys.targets = source.clone();
                keys.snapshot = source.clone();
                keys.timestamp = source;
            }

            let set = load_set_from_dir(&set_dir, &root_doc, &format)?;
            let identity = set.identity();
            let published = publish_set(
                &set,
                &keys,
                &out,
                &PublishOptions {
                    repo_version: version,
                    ..PublishOptions::default()
                },
            )
            .await?;
            println!("published {} v{version}", identity.digest);
            println!("  algorithm  {}", identity.algorithm);
            println!("  metadata   {}", published.metadata_dir.display());
            println!("  targets    {}", published.targets_dir.display());
            println!("  parts      {}", published.claim.parts.join(", "));
        }
        "verify" => {
            let repo_dir = PathBuf::from(required(&args, "--repo")?);
            let trusted_root = std::fs::read(repo_dir.join("metadata/root.json"))?;
            let base = url::Url::from_directory_path(repo_dir.canonicalize()?)
                .ok()
                .context("repo dir to URL")?;
            let repo = load_repository(
                &trusted_root,
                base.join("metadata/")?,
                base.join("targets/")?,
                FilesystemTransport,
                None,
            )
            .await?;
            report(&repo).await?;
        }
        "upload" => {
            let repo_dir = PathBuf::from(required(&args, "--repo")?);
            let bucket = required(&args, "--bucket")?;
            let prefix = required(&args, "--prefix")?;
            let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            let client = aws_sdk_s3::Client::new(&config);
            let outcomes = upload_repository(
                &client,
                &bucket,
                &prefix,
                &repo_dir.join("metadata"),
                &repo_dir.join("targets"),
            )
            .await?;
            for (key, outcome) in outcomes {
                println!("{outcome:?}  {key}");
            }
        }
        "verify-s3" => {
            let trusted_root = std::fs::read(required(&args, "--trusted-root")?)?;
            let bucket = required(&args, "--bucket")?;
            let prefix = required(&args, "--prefix")?;
            let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            let client = aws_sdk_s3::Client::new(&config);
            let (metadata, targets) = repo_urls(&bucket, &prefix)?;
            let repo = load_repository(
                &trusted_root,
                metadata,
                targets,
                S3Transport::new(client),
                None,
            )
            .await?;
            report(&repo).await?;
        }
        _ => {
            println!("verbs: keygen | publish | verify | upload | verify-s3 (see module docs)");
            std::process::exit(2);
        }
    }
    Ok(())
}

async fn report(repo: &tough::Repository) -> anyhow::Result<()> {
    let fetched = fetch_definition_set(repo).await?;
    println!(
        "verified definition set {}:{}",
        fetched.identity.algorithm, fetched.identity.digest
    );
    println!("  format  {}", fetched.claim.format);
    println!(
        "  root    {} ({} bytes)",
        fetched.claim.root,
        fetched.set.root.len()
    );
    for (name, bytes) in &fetched.set.parts {
        println!("  part    {name} ({} bytes)", bytes.len());
    }
    Ok(())
}
