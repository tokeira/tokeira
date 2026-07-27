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
        "check" => check(&root, &rendered),
        "write" => write(&root, &rendered),
        _ => bail!("usage: compatibility-docs <check|write>"),
    }
}

fn parse_mode() -> Result<String> {
    let mut args = env::args().skip(1);
    let Some(mode) = args.next() else {
        bail!("usage: compatibility-docs <check|write>");
    };
    if args.next().is_some() {
        bail!("usage: compatibility-docs <check|write>");
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

fn owned_documents(rendered: &RenderedDocumentation) -> [(&'static str, &str); 3] {
    [
        (
            TEMPORAL_CONFIGURATION_PATH,
            &rendered.temporal_configuration,
        ),
        (TOKEIRA_CONFIGURATION_PATH, &rendered.tokeira_configuration),
        (CONFIG_EXAMPLE_PATH, &rendered.config_example),
    ]
}

fn check(root: &Path, rendered: &RenderedDocumentation) -> Result<()> {
    let mut drifted = Vec::new();
    for (relative, expected) in owned_documents(rendered) {
        let path = root.join(relative);
        let actual = fs::read_to_string(&path)
            .with_context(|| format!("read generated artifact {}", path.display()))?;
        if actual != expected {
            drifted.push(relative);
        }
    }
    if !drifted.is_empty() {
        bail!(
            "generated compatibility documentation drifted: {}; run `cargo run -p compatibility-docs -- write`",
            drifted.join(", ")
        );
    }
    println!("compatibility documentation is current");
    Ok(())
}

fn write(root: &Path, rendered: &RenderedDocumentation) -> Result<()> {
    for (relative, contents) in owned_documents(rendered) {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        fs::write(&path, contents).with_context(|| format!("write {}", path.display()))?;
        println!("wrote {relative}");
    }
    Ok(())
}
