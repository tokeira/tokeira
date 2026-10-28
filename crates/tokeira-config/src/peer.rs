//! Protected peer credential files and their non-secret path configuration.
//!
//! Startup mode decides whether to read a file. Nodes never create replacement
//! keys at startup; operators explicitly generate and install the shared file.

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{
    fs::File,
    io::{self, Read},
    path::{Component, Path},
};

use rustix::fs::{Mode, OFlags, open, openat};
use serde::{Deserialize, Serialize};
use tokeira_auth::peer::{MAX_DOCUMENT_BYTES, PeerError};

/// Only the path is configuration; the protected document contains its profile.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    /// Absolute path to a private regular file, with no symlink components.
    pub path: String,
}

/// Open every component without following symlinks. Checking metadata and then
/// opening by path would leave a substitution window in protected-key loading.
pub fn open_protected_file(path: &Path) -> Result<File, PeerError> {
    open_protected(path, OFlags::RDONLY, Mode::empty()).map_err(|error| {
        // Classify absence from the actual no-follow open, without a racing
        // existence check or exposing filesystem paths in startup diagnostics.
        if error.kind() == io::ErrorKind::NotFound {
            PeerError::MissingMaterial
        } else {
            PeerError::Material
        }
    })
}

/// Create a new owner-only file without overwriting or following any symlink.
/// The caller writes and syncs the document; an incomplete write fails closed.
pub fn create_protected_file(path: &Path) -> io::Result<File> {
    open_protected(
        path,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL,
        Mode::RUSR | Mode::WUSR,
    )
}

fn open_protected(path: &Path, access: OFlags, mode: Mode) -> io::Result<File> {
    let mut components = path.components();
    if components.next() != Some(Component::RootDir) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected an absolute path without parent components",
        ));
    }
    let mut directory = open(
        "/",
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    let mut components = components.peekable();
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "expected an absolute path without parent components",
            ));
        };
        let last = components.peek().is_none();
        let flags = OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK;
        let fd = openat(
            &directory,
            name,
            if last {
                flags | access
            } else {
                flags | OFlags::RDONLY | OFlags::DIRECTORY
            },
            mode,
        )?;
        if last {
            return Ok(File::from(fd));
        }
        directory = fd;
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "expected a file path",
    ))
}

/// Read a protected regular file with a hard allocation bound. Directory and
/// file ownership remain the operator's responsibility; group/world access is
/// rejected on Unix. The operator must also protect the containing directory.
pub fn load_peer_file(path: &Path) -> Result<Vec<u8>, PeerError> {
    let file = open_protected_file(path)?;
    let metadata = file.metadata().map_err(|_| PeerError::Material)?;
    if !metadata.is_file() {
        return Err(PeerError::Material);
    }
    #[cfg(unix)]
    {
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(PeerError::Material);
        }
    }
    let mut bytes = Vec::new();
    file.take(MAX_DOCUMENT_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| PeerError::Material)?;
    if bytes.len() > MAX_DOCUMENT_BYTES {
        return Err(PeerError::TooLarge);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // Feature: queue-home-delivery, Property 7
    // The real protected-file adapter accepts the exact bound and refuses one
    // byte over it; configuration accepts only the file path.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn generated_bounded_file_loading(over in any::<bool>(), byte in any::<u8>()) {
            let temporary = tempfile::tempdir().unwrap();
            let path = temporary.path().canonicalize().unwrap().join("credential.json");
            let bytes = vec![byte; MAX_DOCUMENT_BYTES + usize::from(over)];
            std::fs::write(&path, &bytes).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
            let loaded = load_peer_file(&path);
            if over { prop_assert_eq!(loaded.unwrap_err(), PeerError::TooLarge); }
            else { prop_assert_eq!(loaded.unwrap(), bytes); }
            let unknown = serde_json::json!({"path":path,"token":"untrusted"});
            prop_assert!(serde_json::from_value::<PeerConfig>(unknown).is_err());
            prop_assert!(load_peer_file(&temporary.path().join("missing")).is_err());
        }
    }
}
