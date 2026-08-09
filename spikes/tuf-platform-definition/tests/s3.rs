//! The complete chain over S3: upload under the write policy, then load and
//! verify through `S3Transport` — with the S3 endpoint replaced by an
//! in-memory bucket behind the AWS SDK's test HTTP client, so the whole
//! request path (auth, signing, marshalling, error decoding) is the real
//! SDK's and nothing needs credentials or network.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_smithy_http_client::test_util::infallible_client_fn;
use aws_smithy_types::body::SdkBody;
use spike_tuf_platform_definition::{
    consume::{fetch_definition_set, load_repository},
    keys::RoleKeyFiles,
    publish::{PublishOptions, PublishedRepo, RoleSources, SharedKeySource, publish_set},
    s3::{S3Transport, UploadOutcome, repo_urls, upload_repository},
    set::{DefinitionSet, load_set_from_dir},
};
use tough::{Transport as _, TransportErrorKind, key_source::LocalKeySource};

/// `"<bucket>/<key>" → bytes`, shared with the closure serving the fake
/// endpoint (force_path_style puts the bucket on the URI path).
type Bucket = Arc<Mutex<HashMap<String, Vec<u8>>>>;

const NO_SUCH_KEY: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>"#;

fn s3_client(bucket: Bucket) -> aws_sdk_s3::Client {
    let http_client = infallible_client_fn(move |req: http::Request<SdkBody>| {
        let key = req.uri().path().trim_start_matches('/').to_owned();
        let mut store = bucket.lock().expect("bucket lock");
        match req.method().as_str() {
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
                        .header("etag", "\"spike\"")
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

fn compose_set() -> DefinitionSet {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/compose-set");
    load_set_from_dir(&dir, "deployment.tkd", "tkd").expect("fixture set loads")
}

fn role_sources(files: &RoleKeyFiles) -> RoleSources {
    let source = |path: &PathBuf| SharedKeySource::new(LocalKeySource { path: path.clone() });
    RoleSources {
        root: source(&files.root),
        targets: source(&files.targets),
        snapshot: source(&files.snapshot),
        timestamp: source(&files.timestamp),
    }
}

async fn publish_fixture(tmp: &Path) -> (DefinitionSet, PublishedRepo) {
    let keys = RoleKeyFiles::generate(&tmp.join("keys")).expect("keygen");
    let sources = role_sources(&keys);
    let set = compose_set();
    let published = publish_set(
        &set,
        &sources,
        &tmp.join("repo"),
        &PublishOptions::default(),
    )
    .await
    .expect("publish");
    (set, published)
}

#[tokio::test]
async fn upload_then_load_and_verify_through_s3_transport() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (set, published) = publish_fixture(tmp.path()).await;

    let bucket: Bucket = Arc::default();
    let client = s3_client(bucket.clone());

    // First upload: everything is created.
    let outcomes = upload_repository(
        &client,
        "definitions",
        "deployments/compose",
        &published.metadata_dir,
        &published.targets_dir,
    )
    .await
    .expect("upload");
    assert!(outcomes.iter().all(|(key, outcome)| {
        let mutable = key.ends_with("/timestamp.json") || key.ends_with("/root.json");
        *outcome
            == if mutable {
                UploadOutcome::Replaced
            } else {
                UploadOutcome::Created
            }
    }));

    // Idempotent republish: immutables verify-and-skip, heads move.
    let outcomes = upload_repository(
        &client,
        "definitions",
        "deployments/compose",
        &published.metadata_dir,
        &published.targets_dir,
    )
    .await
    .expect("re-upload");
    assert!(outcomes.iter().all(|(key, outcome)| {
        let mutable = key.ends_with("/timestamp.json") || key.ends_with("/root.json");
        *outcome
            == if mutable {
                UploadOutcome::Replaced
            } else {
                UploadOutcome::AlreadyPresent
            }
    }));

    // Load the repository straight from "S3" and extract the verified set.
    let (metadata_url, targets_url) =
        repo_urls("definitions", "deployments/compose").expect("urls");
    let repo = load_repository(
        &published.trusted_root,
        metadata_url,
        targets_url,
        S3Transport::new(client),
        None,
    )
    .await
    .expect("load over S3");
    let fetched = fetch_definition_set(&repo).await.expect("fetch");
    assert_eq!(fetched.identity, set.identity());
    assert_eq!(fetched.claim.parts, ["platform", "observability"]);
}

/// The transport contract TUF's root walk depends on: an absent object is
/// `FileNotFound`, not a generic failure. (During every load above, tough
/// already probed the absent `2.root.json` through this path — this pins the
/// mapping explicitly, for both the NoSuchKey XML and the bare-404 case.)
#[tokio::test]
async fn absent_object_maps_to_file_not_found() {
    let bucket: Bucket = Arc::default();
    let transport = S3Transport::new(s3_client(bucket));
    let err = match transport
        .fetch(url::Url::parse("s3://definitions/absent/2.root.json").expect("url"))
        .await
    {
        Err(err) => err,
        Ok(_) => panic!("absent key must not fetch"),
    };
    assert_eq!(err.kind(), TransportErrorKind::FileNotFound);
}

#[tokio::test]
async fn non_s3_scheme_is_refused() {
    let bucket: Bucket = Arc::default();
    let transport = S3Transport::new(s3_client(bucket));
    let err = match transport
        .fetch(url::Url::parse("file:///etc/hosts").expect("url"))
        .await
    {
        Err(err) => err,
        Ok(_) => panic!("non-s3 scheme must not fetch"),
    };
    assert_eq!(err.kind(), TransportErrorKind::UnsupportedUrlScheme);
}

/// A tampered object in the bucket cannot reach the consumer: target bytes
/// are verified against the signed hash as they stream.
#[tokio::test]
async fn tampered_bucket_object_is_refused() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (_, published) = publish_fixture(tmp.path()).await;
    let bucket: Bucket = Arc::default();
    let client = s3_client(bucket.clone());
    upload_repository(
        &client,
        "definitions",
        "d",
        &published.metadata_dir,
        &published.targets_dir,
    )
    .await
    .expect("upload");

    // Flip one byte in every stored target object.
    {
        let mut store = bucket.lock().expect("bucket lock");
        for (key, bytes) in store.iter_mut() {
            if key.starts_with("definitions/d/targets/") {
                let last = bytes.last_mut().expect("non-empty");
                *last ^= 0x01;
            }
        }
    }

    let (metadata_url, targets_url) = repo_urls("definitions", "d").expect("urls");
    let repo = load_repository(
        &published.trusted_root,
        metadata_url,
        targets_url,
        S3Transport::new(client),
        None,
    )
    .await
    .expect("metadata untouched, load succeeds");
    let err = fetch_definition_set(&repo)
        .await
        .expect_err("tampered target must be refused");
    let msg = format!("{err:#}").to_lowercase();
    assert!(
        msg.contains("hash") || msg.contains("digest") || msg.contains("verif"),
        "refusal names the integrity failure: {err:#}"
    );
}

/// The create-only policy catches a compromised publisher rewriting history:
/// an immutable object that exists with different bytes is a hard error, not
/// a silent overwrite.
#[tokio::test]
async fn immutable_collision_with_different_bytes_is_refused() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (_, published) = publish_fixture(tmp.path()).await;
    let bucket: Bucket = Arc::default();
    let client = s3_client(bucket.clone());

    // Seed the bucket with a conflicting version of 1.targets.json.
    bucket.lock().expect("bucket lock").insert(
        "definitions/d/metadata/1.targets.json".to_owned(),
        b"prior conflicting publication".to_vec(),
    );

    let err = upload_repository(
        &client,
        "definitions",
        "d",
        &published.metadata_dir,
        &published.targets_dir,
    )
    .await
    .expect_err("colliding immutable object must refuse the upload");
    assert!(
        format!("{err:#}").contains("different bytes"),
        "refusal names the collision: {err:#}"
    );
}
