use async_trait::async_trait;
use aws_sdk_s3::{
    error::{ProvideErrorMetadata, SdkError},
    operation::{
        get_object::GetObjectError, list_objects_v2::ListObjectsV2Error, put_object::PutObjectError,
    },
    primitives::ByteStream,
};

use crate::{StateError, backend::StateBackend};

/// S3-backed [`StateBackend`] adapter for the generic CAS state facade.
///
/// This backend stores each logical state document at
/// `{prefix}/{key}/manifest.json` and uses S3 conditional writes for CAS:
/// `If-None-Match: *` for first write and `If-Match: <etag>` for updates.
pub struct S3Backend {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}

impl S3Backend {
    pub fn new(client: aws_sdk_s3::Client, bucket: String, prefix: String) -> Self {
        Self {
            client,
            bucket,
            prefix: prefix.trim_matches('/').to_owned(),
        }
    }

    fn manifest_key(&self, key: &str) -> String {
        self.join_key(&format!("{}/manifest.json", key.trim_matches('/')))
    }

    fn snapshot_key(&self, key: &str) -> String {
        self.join_key(key.trim_matches('/'))
    }

    fn snapshot_prefix(&self, prefix: &str) -> String {
        self.join_key(prefix.trim_matches('/'))
    }

    fn join_key(&self, suffix: &str) -> String {
        if self.prefix.is_empty() {
            suffix.to_owned()
        } else if suffix.is_empty() {
            self.prefix.clone()
        } else {
            format!("{}/{}", self.prefix, suffix)
        }
    }

    fn s3_error<E>(operation: &str, err: &SdkError<E>) -> StateError
    where
        E: ProvideErrorMetadata,
    {
        let mut parts = vec![format!("{operation}: {err}")];
        if let Some(code) = err.code() {
            parts.push(format!("code={code}"));
        }
        if let Some(message) = err.message() {
            parts.push(format!("message={message}"));
        }
        StateError::S3(parts.join(", "))
    }

    fn is_code<E>(err: &SdkError<E>, code: &str) -> bool
    where
        E: ProvideErrorMetadata,
    {
        err.code() == Some(code)
    }

    async fn collect_body(
        operation: &str,
        body: aws_sdk_s3::primitives::ByteStream,
    ) -> Result<Vec<u8>, StateError> {
        let bytes = body
            .collect()
            .await
            .map_err(|error| StateError::S3(format!("{operation}: read body: {error}")))?;
        Ok(bytes.into_bytes().to_vec())
    }
}

#[async_trait]
impl StateBackend for S3Backend {
    async fn read_manifest(&self, key: &str) -> Result<Option<(Vec<u8>, String)>, StateError> {
        let object_key = self.manifest_key(key);
        let output = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&object_key)
            .send()
            .await
        {
            Ok(output) => output,
            // The remote-state module creates the bucket during the same apply
            // that first uses this backend. Treating absence as empty state lets
            // that bootstrap converge without a separate local-state phase.
            Err(err) if Self::is_code::<GetObjectError>(&err, "NoSuchKey") => return Ok(None),
            Err(err) if Self::is_code::<GetObjectError>(&err, "NotFound") => return Ok(None),
            Err(err) if Self::is_code::<GetObjectError>(&err, "NoSuchBucket") => return Ok(None),
            Err(err) => return Err(Self::s3_error("s3:GetObject", &err)),
        };
        let etag = output.e_tag().unwrap_or_default().to_owned();
        let data = Self::collect_body("s3:GetObject", output.body).await?;
        Ok(Some((data, etag)))
    }

    async fn write_manifest(
        &self,
        key: &str,
        data: &[u8],
        expected_version: &str,
    ) -> Result<(), StateError> {
        let object_key = self.manifest_key(key);
        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&object_key)
            .body(ByteStream::from(data.to_vec()));
        request = if expected_version.is_empty() {
            request.if_none_match("*")
        } else {
            request.if_match(expected_version)
        };

        match request.send().await {
            Ok(_) => Ok(()),
            Err(err) if Self::is_code::<PutObjectError>(&err, "PreconditionFailed") => {
                Err(StateError::Conflict(format!(
                    "state manifest changed before writing {object_key}"
                )))
            }
            Err(err) => Err(Self::s3_error("s3:PutObject", &err)),
        }
    }

    async fn read_snapshot(&self, key: &str) -> Result<Vec<u8>, StateError> {
        let object_key = self.snapshot_key(key);
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&object_key)
            .send()
            .await
            .map_err(|err| Self::s3_error("s3:GetObject", &err))?;
        Self::collect_body("s3:GetObject", output.body).await
    }

    async fn write_snapshot(&self, key: &str, data: &[u8]) -> Result<(), StateError> {
        let object_key = self.snapshot_key(key);
        let request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&object_key)
            .if_none_match("*")
            .body(ByteStream::from(data.to_vec()));

        match request.send().await {
            Ok(_) => Ok(()),
            Err(err) if Self::is_code::<PutObjectError>(&err, "PreconditionFailed") => {
                let existing = self.read_snapshot(key).await?;
                // Snapshots are content-addressed by the higher-level state
                // store. A duplicate write of identical bytes is a successful
                // retry, while different bytes for the same key indicate a real
                // state-history collision.
                if existing == data {
                    Ok(())
                } else {
                    Err(StateError::Conflict(format!(
                        "snapshot already exists with different content: {object_key}"
                    )))
                }
            }
            Err(err) => Err(Self::s3_error("s3:PutObject", &err)),
        }
    }

    async fn list_snapshots(&self, prefix: &str) -> Result<Vec<String>, StateError> {
        let object_prefix = self.snapshot_prefix(prefix);
        let mut keys = Vec::new();
        let mut continuation_token = None;
        loop {
            let output = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&object_prefix)
                .set_continuation_token(continuation_token)
                .send()
                .await
                .map_err(|err: SdkError<ListObjectsV2Error>| {
                    Self::s3_error("s3:ListObjectsV2", &err)
                })?;
            keys.extend(output.contents().iter().filter_map(|object| {
                object
                    .key()
                    .and_then(|key| key.strip_prefix(&self.prefix))
                    .map(|key| key.trim_start_matches('/').to_owned())
            }));
            if output.is_truncated().unwrap_or(false) {
                continuation_token = output.next_continuation_token().map(str::to_owned);
            } else {
                break;
            }
        }
        Ok(keys)
    }
}
