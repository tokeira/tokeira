use std::path::{Path, PathBuf};

use crate::BuildError;

pub fn rust_toolchain_version(workspace_root: &Path) -> Result<String, BuildError> {
    let path = workspace_root.join("rust-toolchain.toml");
    let contents = std::fs::read_to_string(&path).map_err(|source| BuildError::ToolchainFile {
        path: path.clone(),
        source,
    })?;
    let value = contents
        .parse::<toml::Value>()
        .map_err(|source| BuildError::ToolchainParse(source.to_string()))?;
    extract_channel(&value)
        .map(ToOwned::to_owned)
        .ok_or_else(|| BuildError::ToolchainParse(missing_channel_message(path)))
}

fn extract_channel(value: &toml::Value) -> Option<&str> {
    let toolchain = value.get("toolchain")?;
    toolchain
        .get("channel")
        .or_else(|| toolchain.get("version"))?
        .as_str()
}

fn missing_channel_message(path: PathBuf) -> String {
    format!(
        "{} does not contain [toolchain].channel or [toolchain].version",
        path.display()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_channel_from_toolchain_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("rust-toolchain.toml"),
            "[toolchain]\nchannel = \"1.95\"\n",
        )
        .expect("write toolchain");

        let version = rust_toolchain_version(dir.path()).expect("toolchain version");

        assert_eq!(version, "1.95");
    }

    #[test]
    fn falls_back_to_version_field() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("rust-toolchain.toml"),
            "[toolchain]\nversion = \"1.95\"\n",
        )
        .expect("write toolchain");

        let version = rust_toolchain_version(dir.path()).expect("toolchain version");

        assert_eq!(version, "1.95");
    }
}
