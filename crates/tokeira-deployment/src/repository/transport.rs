//! S3 as a `tough::Transport`.
//!
//! Two mappings matter for correctness, both proven in the TUF spike this
//! module productionizes:
//!
//! - `NoSuchKey` (and bare 404s from S3-compatible endpoints) MUST surface
//!   as `TransportErrorKind::FileNotFound` — TUF's root-version walk probes
//!   `N+1.root.json` until it is absent, and a transport that reports
//!   absence as a generic failure breaks every load (TUF v1.0.16 §5.2.2).
//! - The body streams through unchanged; `tough` owns hash/length
//!   verification against signed metadata, so the transport adds no
//!   integrity duties — and no caching, truncation, or transformation.

use aws_sdk_s3::{Client, error::SdkError};
use tough::{Transport, TransportError, TransportErrorKind, TransportStream};
use url::Url;

/// Serves `s3://<bucket>/<key>` URLs via `GetObject`.
#[derive(Debug, Clone)]
pub struct S3Transport {
    client: Client,
}

impl S3Transport {
    /// Wrap a configured S3 client (region, credentials, and any test
    /// endpoint already applied).
    pub fn new(client: Client) -> Self {
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
