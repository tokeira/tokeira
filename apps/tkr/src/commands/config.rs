//! `tkr config show` — dump the deployment's configuration sources.
//!
//! Intentionally trivial: the output format is the source text itself so
//! operators see exactly what `tokeirad` and the deployment engine will
//! load. Useful when chasing "why did my change not take effect" and for
//! pasting into bug reports.
//!
//! This command reads the deployment *directory* rather than a parsed
//! `DeploymentContext`, for two reasons. First, the sources are
//! platform-shaped: a forwarded deployment is defined by `definition.tkd`,
//! an in-process one by `deployment.toml`, and both carry a `tokeirad.toml`
//! — so parsing one fixed layout hard-fails on the other. Second, showing
//! configuration must never depend on that configuration being loadable: a
//! malformed or missing file is precisely when an operator reaches for
//! `config show`, and it is the one command that has to keep working then.

use std::{fs, path::Path};

use anyhow::{Context, Result, bail};

use crate::deployment_dir::{DEPLOYMENT_TOML, DeploymentResolver, METADATA_JSON, TOKEIRAD_TOML};

pub(crate) fn run_show(deployments: &DeploymentResolver, requested: Option<&str>) -> Result<()> {
    let name = deployments.resolve_name(requested)?;
    let path = deployments.path(&name);
    if !path.join(METADATA_JSON).exists() {
        bail!("{}", deployments.not_found_message(&name)?);
    }

    let mut present = fs::read_dir(&path)?
        .filter_map(std::result::Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| {
            name == DEPLOYMENT_TOML || name == TOKEIRAD_TOML || name.starts_with("definition.")
        })
        .collect::<Vec<_>>();
    present.sort_by_key(|name| {
        if name.starts_with("definition.") {
            (0, name.clone())
        } else if name == DEPLOYMENT_TOML {
            (1, name.clone())
        } else {
            (2, name.clone())
        }
    });
    if present.is_empty() {
        println!(
            "deployment '{name}' has no configuration sources under {}",
            path.display()
        );
        return Ok(());
    }
    for (index, source) in present.iter().enumerate() {
        if index > 0 {
            println!();
        }
        print_source(&path.join(source))?;
    }
    Ok(())
}

/// Print one source as `# <path>` followed by its verbatim text.
fn print_source(file: &Path) -> Result<()> {
    let text =
        fs::read_to_string(file).with_context(|| format!("failed to read {}", file.display()))?;
    println!("# {}\n{text}", file.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // The demon this fixes: a forwarded (`.tkd`) deployment has no
    // `deployment.toml`, and `config show` used to read that file
    // unconditionally — the one command an operator reaches for when
    // configuration is in question failed on the whole forwarded platform
    // family.
    #[test]
    fn shows_the_sources_a_forwarded_deployment_actually_has() {
        let tmp = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(tmp.path().to_path_buf());
        let path = deployments.path("fwd");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join(METADATA_JSON), "{}").unwrap();
        std::fs::write(path.join("definition.tkd"), "fn config() {}").unwrap();
        std::fs::write(path.join(TOKEIRAD_TOML), "[server]").unwrap();

        run_show(&deployments, Some("fwd")).expect("a `.tkd` deployment shows its sources");
    }

    #[test]
    fn shows_the_sources_a_legacy_deployment_actually_has() {
        let tmp = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(tmp.path().to_path_buf());
        let path = deployments.path("legacy");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join(METADATA_JSON), "{}").unwrap();
        std::fs::write(path.join(DEPLOYMENT_TOML), "platform = 'local'").unwrap();
        std::fs::write(path.join(TOKEIRAD_TOML), "[server]").unwrap();

        run_show(&deployments, Some("legacy")).expect("an in-process deployment shows its sources");
    }

    // Showing config never parses it: a deployment whose config is malformed
    // (or half-written) still dumps, because that is the moment `config show`
    // exists for.
    #[test]
    fn shows_malformed_config_verbatim() {
        let tmp = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(tmp.path().to_path_buf());
        let path = deployments.path("broken");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join(METADATA_JSON), "{}").unwrap();
        std::fs::write(path.join(TOKEIRAD_TOML), "this is not = valid = toml [[[").unwrap();

        run_show(&deployments, Some("broken")).expect("unparseable config still shows");
    }

    #[test]
    fn reports_a_deployment_that_does_not_exist() {
        let tmp = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(tmp.path().to_path_buf());
        let err = run_show(&deployments, Some("ghost")).expect_err("no such deployment");
        assert!(
            err.to_string().contains("does not exist"),
            "unexpected: {err}"
        );
    }
}
