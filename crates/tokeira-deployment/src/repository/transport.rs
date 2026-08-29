//! Repository transports for `tough`.
//!
//! These transport properties matter for correctness:
//!
//! - `NoSuchKey` (and bare 404s from S3-compatible endpoints) MUST surface
//!   as `TransportErrorKind::FileNotFound` — TUF's root-version walk probes
//!   `N+1.root.json` until it is absent, and a transport that reports
//!   absence as a generic failure breaks every load (TUF v1.0.16 §5.2.2).
//! - Local `file:` URLs MUST decode ordinary URL escapes such as the `%20`
//!   in macOS's `Application Support`. Tough 0.24 deliberately retains every
//!   escape in its filesystem path, which makes valid repository homes with
//!   spaces unreadable.
//! - Decoding MUST NOT turn an escaped target name into path traversal. The
//!   local transport confines every decoded path to its repository root and
//!   rejects non-normal relative components before opening it.
//! - The body streams through unchanged; `tough` owns hash/length
//!   verification against signed metadata, so the transport adds no
//!   integrity duties — and no caching, truncation, or transformation.

use std::{
    io::ErrorKind,
    path::{Component, PathBuf},
};

use aws_sdk_s3::{Client, error::SdkError};
use futures::TryStreamExt as _;
use tokio_util::io::ReaderStream;
use tough::{Transport, TransportError, TransportErrorKind, TransportStream};
use url::Url;

/// A URL-decoding filesystem transport confined to one repository root.
///
/// Tough's built-in transport maps `Url::path()` directly to a filesystem
/// path. That preserves traversal escapes, but it also preserves `%20` and
/// therefore cannot read a normal macOS application-support path. This
/// transport decodes with `Url::to_file_path`, then restores the security
/// property by accepting only normal path components below `root`.
#[derive(Debug, Clone)]
pub(crate) struct LocalTransport {
    root: PathBuf,
}

impl LocalTransport {
    /// Confine reads to `root`.
    pub(crate) fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path(&self, url: &Url) -> Result<PathBuf, TransportError> {
        if url.scheme() != "file" {
            return Err(TransportError::new(
                TransportErrorKind::UnsupportedUrlScheme,
                url.as_str(),
            ));
        }
        let path = url
            .to_file_path()
            .map_err(|()| TransportError::new(TransportErrorKind::Other, url.as_str()))?;
        let relative = path.strip_prefix(&self.root).map_err(|error| {
            TransportError::new_with_cause(TransportErrorKind::Other, url.as_str(), error)
        })?;
        if relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(TransportError::new(TransportErrorKind::Other, url.as_str()));
        }
        Ok(path)
    }
}

#[async_trait::async_trait]
impl Transport for LocalTransport {
    async fn fetch(&self, url: Url) -> Result<TransportStream, TransportError> {
        let path = self.path(&url)?;
        let file = tokio::fs::File::open(&path)
            .await
            .map_err(|error| local_io_error(error, &url))?;
        let url_for_error = url.clone();
        let stream = ReaderStream::new(tokio::io::BufReader::new(file))
            .map_err(move |error| local_io_error(error, &url_for_error));
        Ok(Box::pin(stream))
    }
}

fn local_io_error(error: std::io::Error, url: &Url) -> TransportError {
    let kind = if error.kind() == ErrorKind::NotFound {
        TransportErrorKind::FileNotFound
    } else {
        TransportErrorKind::Other
    };
    TransportError::new_with_cause(kind, url.as_str(), error)
}

/// Serves `s3://<bucket>/<key>` URLs via `GetObject`.
#[derive(Debug, Clone)]
pub struct S3Transport {
    client: Client,
}

impl S3Transport {
    /// Wrap a configured S3 client (region, credentials, and any test
    /// endpoint already applied).
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
}

/// Split an `s3://bucket/key` URL.
fn bucket_and_key(url: &Url) -> Result<(String, String), TransportError> {
    if url.scheme() != "s3" {
        return Err(TransportError::new(
            TransportErrorKind::UnsupportedUrlScheme,
            url.as_str(),
        ));
    }
    let bucket = url
        .host_str()
        .ok_or_else(|| TransportError::new(TransportErrorKind::Other, url.as_str()))?;
    let key = url.path().trim_start_matches('/');
    if key.is_empty() {
        return Err(TransportError::new(TransportErrorKind::Other, url.as_str()));
    }
    Ok((bucket.to_owned(), key.to_owned()))
}

#[async_trait::async_trait]
impl Transport for S3Transport {
    async fn fetch(&self, url: Url) -> Result<TransportStream, TransportError> {
        let (bucket, key) = bucket_and_key(&url)?;
        let response = self
            .client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|err| classify_get_error(err, &url))?;

        let url_for_err = url.to_string();
        let stream =
            futures::stream::unfold((response.body, url_for_err), |(mut body, url)| async move {
                match body.next().await {
                    Some(Ok(bytes)) => Some((Ok(bytes), (body, url))),
                    Some(Err(err)) => Some((
                        Err(TransportError::new_with_cause(
                            TransportErrorKind::Other,
                            url.clone(),
                            err,
                        )),
                        (body, url),
                    )),
                    None => None,
                }
            });
        Ok(Box::pin(stream))
    }
}

/// `NoSuchKey` (and bare 404s) mean "absent"; everything else is a fault.
fn classify_get_error(
    err: SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
    url: &Url,
) -> TransportError {
    let not_found = match &err {
        SdkError::ServiceError(service) => {
            service.err().is_no_such_key() || service.raw().status().as_u16() == 404
        }
        _ => false,
    };
    let kind = if not_found {
        TransportErrorKind::FileNotFound
    } else {
        TransportErrorKind::Other
    };
    TransportError::new_with_cause(kind, url.as_str(), err)
}

#[cfg(test)]
mod tests {
    use tough::IntoVec as _;

    use super::*;

    #[tokio::test]
    async fn local_transport_decodes_spaces_without_losing_confinement() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repository = tmp.path().join("Application Support/repository");
        let metadata = repository.join("metadata");
        std::fs::create_dir_all(&metadata).expect("metadata dir");
        std::fs::write(metadata.join("timestamp.json"), b"timestamp").expect("timestamp");
        let url = Url::from_file_path(metadata.join("timestamp.json")).expect("file URL");
        assert!(url.as_str().contains("Application%20Support"));

        let bytes = LocalTransport::new(&repository)
            .fetch(url)
            .await
            .expect("fetch")
            .into_vec()
            .await
            .expect("body");

        assert_eq!(bytes, b"timestamp");
    }

    #[tokio::test]
    async fn local_transport_rejects_decoded_parent_components() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repository = tmp.path().join("repository");
        let base = Url::from_directory_path(&repository).expect("repository URL");
        let traversal =
            Url::parse(&format!("{}%2e%2e/secret", base.as_str())).expect("traversal URL");

        let Err(error) = LocalTransport::new(&repository).fetch(traversal).await else {
            panic!("decoded traversal must refuse");
        };

        assert_eq!(error.kind(), TransportErrorKind::Other);
    }
}
