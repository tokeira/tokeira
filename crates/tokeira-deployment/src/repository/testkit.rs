//! Offline S3 for the repository tests: a closure-backed HTTP client stands
//! in for the endpoint, so the whole verification chain — auth, signing,
//! marshalling, error decoding, conditional writes — runs over the real
//! `aws-sdk-s3` request path with no credentials and no network. Promoted
//! from the TUF spike, where every property this crate now owns was first
//! proven against it.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_smithy_http_client::test_util::infallible_client_fn;
use aws_smithy_types::body::SdkBody;

/// `"<bucket>/<key>" → bytes`, shared with the closure serving the fake
/// endpoint (`force_path_style` puts the bucket on the URI path).
pub(crate) type Bucket = Arc<Mutex<HashMap<String, Vec<u8>>>>;

const NO_SUCH_KEY: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>"#;

/// One percent-decoded query parameter (`""` when absent). Decoding covers
/// what the SDK emits for prefixes and delimiters (`%2F`); the testkit is
/// not a general URL decoder.
fn query_param(query: &str, name: &str) -> String {
    query
        .split('&')
        .find_map(|pair| pair.strip_prefix(&format!("{name}=")))
        .unwrap_or_default()
        .replace("%2F", "/")
}

/// An S3 client whose endpoint is the in-memory bucket: GET serves stored
/// bytes or `NoSuchKey`; PUT honours `If-None-Match: *` with 412 on
/// collision.
pub(crate) fn s3_client(bucket: Bucket) -> aws_sdk_s3::Client {
    let http_client = infallible_client_fn(move |req: http::Request<SdkBody>| {
        let key = req.uri().path().trim_start_matches('/').to_owned();
        let mut store = bucket.lock().expect("bucket lock");
        let query = req.uri().query().unwrap_or_default();
        match req.method().as_str() {
            // ListObjectsV2: a GET on the bucket itself with `list-type=2`.
            // Answers only what the listing code consumes — common prefixes
            // under the requested prefix/delimiter, untruncated.
            "GET" if query.contains("list-type=2") => {
                let bucket_name = key.trim_end_matches('/');
                let prefix = query_param(query, "prefix");
                let delimiter = query_param(query, "delimiter");
                let scope = format!("{bucket_name}/{prefix}");
                let mut commons: Vec<String> = Vec::new();
                for stored in store.keys() {
                    if let Some(rest) = stored.strip_prefix(&scope)
                        && let Some(head) = rest.split(&delimiter).next()
                        && !head.is_empty()
                    {
                        let common = format!("{prefix}{head}{delimiter}");
                        if !commons.contains(&common) {
                            commons.push(common);
                        }
                    }
                }
                commons.sort();
                let prefixes: String = commons
                    .iter()
                    .map(|common| {
                        format!("<CommonPrefixes><Prefix>{common}</Prefix></CommonPrefixes>")
                    })
                    .collect();
                let body = format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>{bucket_name}</Name><Prefix>{prefix}</Prefix><Delimiter>{delimiter}</Delimiter>
<MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{prefixes}
</ListBucketResult>"#
                );
                http::Response::builder()
                    .status(200)
                    .header("content-type", "application/xml")
                    .body(SdkBody::from(body))
                    .expect("response")
            }
            "GET" => match store.get(&key) {
                Some(bytes) => http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(bytes.clone()))
                    .expect("response"),
                None => http::Response::builder()
                    .status(404)
                    .header("content-type", "application/xml")
                    .body(SdkBody::from(NO_SUCH_KEY))
                    .expect("response"),
            },
            "PUT" => {
                let create_only = req.headers().get("if-none-match").is_some();
                if create_only && store.contains_key(&key) {
                    http::Response::builder()
                        .status(412)
                        .body(SdkBody::empty())
                        .expect("response")
                } else {
                    let bytes = req.body().bytes().unwrap_or_default().to_vec();
                    store.insert(key, bytes);
                    http::Response::builder()
                        .status(200)
                        .header("etag", "\"testkit\"")
                        .body(SdkBody::empty())
                        .expect("response")
                }
            }
            other => http::Response::builder()
                .status(501)
                .body(SdkBody::from(format!("unhandled method {other}")))
                .expect("response"),
        }
    });
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("eu-west-2"))
        .credentials_provider(Credentials::for_tests())
        .http_client(http_client)
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(config)
}
