// CLI: stdout/stderr are the user interface.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
};

use anyhow::{Context, Result, bail};
use compatibility_docs::{
    CONFIG_EXAMPLE_PATH, RenderedDocumentation, TEMPORAL_CONFIGURATION_PATH,
    TOKEIRA_CONFIGURATION_PATH, render_all,
};

const USAGE: &str = "usage: compatibility-docs <check|write|check-temporal|write-temporal>";

fn main() {
    if let Err(error) = run() {
        eprintln!("{error:#}");
        process::exit(1);
    }
}

fn run() -> Result<()> {
    let mode = parse_mode()?;
    let root = workspace_root();
    let rendered = render_all()?;
    match mode.as_str() {
        "check" => check(&root, &rendered, false),
        "write" => write(&root, &rendered, false),
        "check-temporal" => check(&root, &rendered, true),
        "write-temporal" => write(&root, &rendered, true),
        _ => bail!(USAGE),
    }
}

fn parse_mode() -> Result<String> {
    let mut args = env::args().skip(1);
    let Some(mode) = args.next() else {
        bail!(USAGE);
    };
    if args.next().is_some() {
        bail!(USAGE);
    }
    Ok(mode)
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("tool lives directly under the workspace tools directory")
        .to_path_buf()
}

// The target denominator advances before the advertised profile. A scoped
// render must not rewrite that profile's other independently maintained docs.
fn owned_documents(
    rendered: &RenderedDocumentation,
    temporal_only: bool,
) -> impl Iterator<Item = (&'static str, &str)> {
    [
        (
            TEMPORAL_CONFIGURATION_PATH,
            rendered.temporal_configuration.as_str(),
        ),
        (
            TOKEIRA_CONFIGURATION_PATH,
            rendered.tokeira_configuration.as_str(),
        ),
        (CONFIG_EXAMPLE_PATH, rendered.config_example.as_str()),
    ]
    .into_iter()
    .filter(move |(path, _)| !temporal_only || *path == TEMPORAL_CONFIGURATION_PATH)
}

fn check(root: &Path, rendered: &RenderedDocumentation, temporal_only: bool) -> Result<()> {
    let mut drifted = Vec::new();
    for (relative, expected) in owned_documents(rendered, temporal_only) {
        let path = root.join(relative);
        let actual = fs::read_to_string(&path)
            .with_context(|| format!("read generated artifact {}", path.display()))?;
        if actual != expected {
            drifted.push(relative);
        }
    }
    if !drifted.is_empty() {
        let mode = if temporal_only {
            "write-temporal"
        } else {
            "write"
        };
        bail!(
            "generated compatibility documentation drifted: {}; run `cargo run -p compatibility-docs --locked -- {mode}`",
            drifted.join(", ")
        );
    }
    println!("compatibility documentation is current");
    Ok(())
}

fn write(root: &Path, rendered: &RenderedDocumentation, temporal_only: bool) -> Result<()> {
    for (relative, contents) in owned_documents(rendered, temporal_only) {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        fs::write(&path, contents).with_context(|| format!("write {}", path.display()))?;
        println!("wrote {relative}");
    }
    Ok(())
}
