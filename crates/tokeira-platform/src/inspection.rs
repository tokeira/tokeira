//! Safe atomic publication for deterministic, non-authoritative inspection bytes.

use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use tokeira_orchestrator::RelativeDefinitionPath;

use crate::{content::ContentIdentity, error::InspectionError};

/// Evidence for one atomically replaced inspection projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectionPublication {
    /// Validated deployment-relative target.
    pub path: RelativeDefinitionPath,
    /// Identity of the exact published bytes.
    pub identity: ContentIdentity,
}

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

/// Atomically replace one deterministic inspection projection.
pub fn publish_inspection(
    deployment_dir: &Path,
    path: RelativeDefinitionPath,
    bytes: &[u8],
) -> Result<InspectionPublication, InspectionError> {
    let root =
        std::fs::canonicalize(deployment_dir).map_err(|source| InspectionError::Prepare {
            path: PathBuf::from("."),
            source,
        })?;
    let parent = prepare_parent(&root, &path)?;
    let target = parent.join(
        path.as_path()
            .file_name()
            .expect("admitted relative path has a file name"),
    );
    let (temporary_path, mut temporary) = create_temp(&parent, &target, &path)?;
    if let Err(source) = temporary
        .write_all(bytes)
        .and_then(|()| temporary.flush())
        .and_then(|()| temporary.sync_all())
    {
        let _ = std::fs::remove_file(&temporary_path);
        return Err(InspectionError::Stage { path, source });
    }
    drop(temporary);
    if let Err(source) = std::fs::rename(&temporary_path, &target) {
        let _ = std::fs::remove_file(&temporary_path);
        return Err(InspectionError::Publish { path, source });
    }
    let identity = ContentIdentity::new(&format!("inspection-artifact/{}", path.as_str()), bytes);
    Ok(InspectionPublication { path, identity })
}

fn prepare_parent(
    root: &Path,
    relative: &RelativeDefinitionPath,
) -> Result<PathBuf, InspectionError> {
    let mut current = root.to_path_buf();
    let parent = relative
        .as_path()
        .parent()
        .expect("admitted relative path has a parent");
    for component in parent.components() {
        let Component::Normal(segment) = component else {
            unreachable!("relative-path admission permits only normal components");
        };
        current.push(segment);
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(InspectionError::EscapingTarget {
                    path: relative.as_path().to_path_buf(),
                });
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(InspectionError::Prepare {
                    path: relative.as_path().to_path_buf(),
                    source: std::io::Error::new(
                        std::io::ErrorKind::NotADirectory,
                        format!("{} is not a directory", current.display()),
                    ),
                });
            }
            Ok(_) => {}
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&current).map_err(|source| InspectionError::Prepare {
                    path: relative.as_path().to_path_buf(),
                    source,
                })?;
            }
            Err(source) => {
                return Err(InspectionError::Prepare {
                    path: relative.as_path().to_path_buf(),
                    source,
                });
            }
        }
        current = std::fs::canonicalize(&current).map_err(|source| InspectionError::Prepare {
            path: relative.as_path().to_path_buf(),
            source,
        })?;
        if !current.starts_with(root) {
            return Err(InspectionError::EscapingTarget {
                path: relative.as_path().to_path_buf(),
            });
        }
    }
    Ok(current)
}

fn create_temp(
    parent: &Path,
    target: &Path,
    relative: &RelativeDefinitionPath,
) -> Result<(PathBuf, File), InspectionError> {
    let file_name = target
        .file_name()
        .expect("admitted relative path has a file name")
        .to_string_lossy();
    for _ in 0..32 {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(
            ".{file_name}.tokeira-{}-{sequence}.tmp",
            std::process::id()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(source) => {
                return Err(InspectionError::Stage {
                    path: relative.clone(),
                    source,
                });
            }
        }
    }
    Err(InspectionError::Stage {
        path: relative.clone(),
        source: std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not reserve a unique same-directory temporary file",
        ),
    })
}
