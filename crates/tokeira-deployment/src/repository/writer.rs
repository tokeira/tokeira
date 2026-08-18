//! Object writes under the Repository Object Contract: one trait, two homes.
//!
//! Every object except the mutable heads (`timestamp.json` and the
//! convenience un-versioned `root.json`) is version- or digest-named and
//! therefore immutable: written create-only, byte-verified on collision,
//! never overwritten. The S3 home realizes create-only with
//! `If-None-Match: *`; the local home with `create_new`. Engine binaries
//! arrive as file paths and are read per object — a publication's memory
//! bound is one artifact at a time, never the whole bundle.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use aws_sdk_s3::{Client, error::SdkError, primitives::ByteStream};

use super::{error::WriteError, locator::RepositoryLocator};

/// What one create-only write did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadOutcome {
    /// Object created.
    Created,
    /// Immutable object already present with identical bytes (idempotent
    /// republish — how shared content across publications stays deduped).
    AlreadyPresent,
}

/// Bytes in memory (metadata, documents) or a file to stream (binaries).
#[derive(Debug)]
pub enum WriteSource<'a> {
    /// Small object held in memory.
    Bytes(&'a [u8]),
    /// Large object streamed from disk.
    File(&'a Path),
}

impl WriteSource<'_> {
    async fn to_bytes(&self) -> Result<Vec<u8>, String> {
        match self {
            Self::Bytes(bytes) => Ok(bytes.to_vec()),
            Self::File(path) => tokio::fs::read(path)
                .await
                .map_err(|error| format!("reading {}: {error}", path.display())),
        }
    }
}

/// Object writes for one repository home. Keys are relative to the
/// repository base (`metadata/1.root.json`, `targets/<sha>.name`).
#[async_trait]
pub trait RepositoryWriter: Send + Sync {
    /// Create-only write: refuse (with byte-verify) when the key exists.
    async fn put_create_only(
        &self,
        key: &str,
        source: WriteSource<'_>,
    ) -> Result<UploadOutcome, WriteError>;

    /// Mutable-head write: last writer wins (operator-serialized; the
    /// operation-lease spec owns making concurrent publishers impossible).
    async fn put_mutable_head(&self, key: &str, bytes: &[u8]) -> Result<(), WriteError>;
}

/// Select the home's writer from the locator.
pub fn writer_for(
    locator: &RepositoryLocator,
    s3: Option<Client>,
) -> Result<Box<dyn RepositoryWriter>, WriteError> {
    match locator {
        RepositoryLocator::Local { path } => Ok(Box::new(LocalWriter { base: path.clone() })),
        RepositoryLocator::S3 { bucket, prefix } => {
            let client = s3.ok_or_else(|| WriteError::Io {
                key: locator.display(),
                error: "an S3 locator needs a configured S3 client".to_string(),
            })?;
            Ok(Box::new(S3Writer {
                client,
                bucket: bucket.clone(),
                prefix: prefix.clone(),
            }))
        }
    }
}

/// Local filesystem home: `create_new` is the create-only primitive; the
/// mutable heads move by write-temp + rename so a reader never sees a torn
/// head.
#[derive(Debug)]
pub struct LocalWriter {
    base: PathBuf,
}

impl LocalWriter {
    /// A writer rooted at the repository directory.
    pub fn new(base: PathBuf) -> Self {
        Self { base }
    }
}

#[async_trait]
impl RepositoryWriter for LocalWriter {
    async fn put_create_only(
        &self,
        key: &str,
        source: WriteSource<'_>,
    ) -> Result<UploadOutcome, WriteError> {
        let path = self.base.join(key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| WriteError::Io {
                    key: key.to_string(),
                    error: error.to_string(),
                })?;
        }
        let bytes = source.to_bytes().await.map_err(|error| WriteError::Io {
            key: key.to_string(),
            error,
        })?;
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(mut file) => {
                use tokio::io::AsyncWriteExt as _;
                file.write_all(&bytes)
                    .await
                    .map_err(|error| WriteError::Io {
                        key: key.to_string(),
                        error: error.to_string(),
                    })?;
                Ok(UploadOutcome::Created)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = tokio::fs::read(&path)
                    .await
                    .map_err(|error| WriteError::Io {
                        key: key.to_string(),
                        error: error.to_string(),
                    })?;
                if existing == bytes {
                    Ok(UploadOutcome::AlreadyPresent)
                } else {
                    Err(WriteError::Conflict {
                        key: key.to_string(),
                    })
                }
            }
            Err(error) => Err(WriteError::Io {
                key: key.to_string(),
                error: error.to_string(),
            }),
        }
    }

    async fn put_mutable_head(&self, key: &str, bytes: &[u8]) -> Result<(), WriteError> {
        let path = self.base.join(key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| WriteError::Io {
                    key: key.to_string(),
                    error: error.to_string(),
                })?;
        }
        let tmp = path.with_extension("tmp-head");
        tokio::fs::write(&tmp, bytes)
            .await
            .map_err(|error| WriteError::Io {
                key: key.to_string(),
                error: error.to_string(),
            })?;
        tokio::fs::rename(&tmp, &path)
            .await
            .map_err(|error| WriteError::Io {
                key: key.to_string(),
                error: error.to_string(),
            })
    }
}

/// S3 home: `If-None-Match: *` is the create-only primitive.
#[derive(Debug)]
pub struct S3Writer {
    client: Client,
    bucket: String,
    prefix: String,
}

impl S3Writer {
    fn full_key(&self, key: &str) -> String {
        format!("{}/{key}", self.prefix)
    }
}

#[async_trait]
impl RepositoryWriter for S3Writer {
    async fn put_create_only(
        &self,
        key: &str,
        source: WriteSource<'_>,
    ) -> Result<UploadOutcome, WriteError> {
        let full = self.full_key(key);
        // One object in memory at a time: the collision path needs the bytes
        // for verification anyway, and an in-memory body stays replayable
        // and capturable in every environment.
        let bytes = source.to_bytes().await.map_err(|error| WriteError::Io {
            key: key.to_string(),
            error,
        })?;
        let body = ByteStream::from(bytes.clone());
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&full)
            .if_none_match("*")
            .body(body)
            .send()
            .await;
        match result {
            Ok(_) => Ok(UploadOutcome::Created),
            Err(err) if is_precondition_failed(&err) => {
                // "Verify exact bytes on collision" — an identical object is
                // an idempotent republish; different bytes are a conflicting
                // publication that must never be overwritten.
                let existing = self
                    .client
                    .get_object()
                    .bucket(&self.bucket)
                    .key(&full)
                    .send()
                    .await
                    .map_err(|error| WriteError::Io {
                        key: key.to_string(),
                        error: error.to_string(),
                    })?
                    .body
                    .collect()
                    .await
                    .map_err(|error| WriteError::Io {
                        key: key.to_string(),
                        error: error.to_string(),
                    })?
                    .into_bytes();
                if existing.as_ref() == bytes.as_slice() {
                    Ok(UploadOutcome::AlreadyPresent)
                } else {
                    Err(WriteError::Conflict {
                        key: key.to_string(),
                    })
                }
            }
            Err(err) => Err(WriteError::Io {
                key: key.to_string(),
                error: err.to_string(),
            }),
        }
    }

    async fn put_mutable_head(&self, key: &str, bytes: &[u8]) -> Result<(), WriteError> {
        let full = self.full_key(key);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&full)
            .body(ByteStream::from(bytes.to_vec()))
            .send()
            .await
            .map(|_| ())
            .map_err(|error| WriteError::Io {
                key: key.to_string(),
                error: error.to_string(),
            })
    }
}

fn is_precondition_failed<E>(err: &SdkError<E>) -> bool {
    match err {
        SdkError::ServiceError(service) => {
            let status = service.raw().status().as_u16();
            status == 412 || status == 409
        }
        _ => false,
    }
}
