//! The repository property suite (deployment-repository spec, P1–P11).
//!
//! Everything runs offline: the local home on tempdirs, the S3 home over
//! the testkit's in-memory endpoint (real `aws-sdk-s3` request path).
//! Signing-heavy properties keep proptest case counts small — each case
//! authors, signs, and verifies a whole repository.

use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

use proptest::prelude::*;

use super::{
    claim::{DefinitionSection, DeploymentClaim, DeploymentRef, EngineSection, Transition},
    config::{RepositoryConfig, RoleLifetimes},
    error::{PublishError, WriteError},
    fetch::MaterializePlan,
    keys::RoleKeyConfig,
    list::list_remote,
    locator::RepositoryLocator,
    open::{Freshness, open},
    publish::{PublicationInput, PublicationReceipt, publish_transition},
    testkit,
    transport::S3Transport,
    writer::{LocalWriter, RepositoryWriter, UploadOutcome, WriteSource},
};
use crate::{
    BinaryArtifactDescriptor, BuildAuthority, ProvisionerBundle, Target as TripleTarget,
    bundle::{BuildManifest, TestEvidence},
    identity::{BuildProfile, EngineIdentity},
    integrity::sha256_hex,
};
use tokeira_orchestrator::{DefinitionFormatId, PlatformId};
use tokeira_platform::definition::ConfigurationIdentity;

const HOST: &str = "aarch64-apple-darwin";
const OTHER: &str = "x86_64-unknown-linux-musl";

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
}

struct Fixture {
    config: RepositoryConfig,
    input_dir: tempfile::TempDir,
    _keys_dir: tempfile::TempDir,
    repo_dir: Option<tempfile::TempDir>,
}

impl Fixture {
    fn local() -> Self {
        let keys_dir = tempfile::tempdir().expect("keys dir");
        let repo_dir = tempfile::tempdir().expect("repo dir");
        let config = RepositoryConfig {
            locator: RepositoryLocator::Local {
                path: repo_dir.path().to_path_buf(),
            },
            keys: RoleKeyConfig::generate_local(keys_dir.path()).expect("keygen"),
            lifetimes: RoleLifetimes::default(),
        };
        Self {
            config,
            input_dir: tempfile::tempdir().expect("input dir"),
            _keys_dir: keys_dir,
            repo_dir: Some(repo_dir),
        }
    }

    fn s3(bucket_name: &str) -> (Self, testkit::Bucket) {
        let keys_dir = tempfile::tempdir().expect("keys dir");
        let bucket: testkit::Bucket = Default::default();
        let config = RepositoryConfig {
            locator: RepositoryLocator::S3 {
                bucket: bucket_name.to_string(),
                prefix: "deployments/dev".to_string(),
            },
            keys: RoleKeyConfig::generate_local(keys_dir.path()).expect("keygen"),
            lifetimes: RoleLifetimes::default(),
        };
        (
            Self {
                config,
                input_dir: tempfile::tempdir().expect("input dir"),
                _keys_dir: keys_dir,
                repo_dir: None,
            },
            bucket,
        )
    }

    fn repo_path(&self) -> &std::path::Path {
        self.repo_dir.as_ref().expect("local fixture").path()
    }

    /// A publication input over the given documents; the claim's identity
    /// is honestly computed over (root, served order).
    fn input(
        &self,
        root_bytes: &[u8],
        companions: &[(String, Vec<u8>)],
        config_files: &[(String, Vec<u8>)],
        transition: Transition,
        config_revision: u64,
    ) -> PublicationInput {
        let format = DefinitionFormatId::new("tkd").expect("format");
        let served: Vec<(String, Arc<[u8]>)> = companions
            .iter()
            .map(|(name, bytes)| (name.clone(), Arc::from(bytes.as_slice())))
            .collect();
        let identity = if served.is_empty() {
            ConfigurationIdentity::compute(&format, root_bytes)
        } else {
            ConfigurationIdentity::compute_set(&format, root_bytes, &served)
        };

        // Two engine artifacts so host selection is meaningful.
        let mut artifacts = Vec::new();
        let mut artifact_paths = Vec::new();
        for (triple, content) in [(HOST, b"host-binary".as_slice()), (OTHER, b"other-binary")] {
            let path = self.input_dir.path().join(format!("bin-{triple}"));
            std::fs::write(&path, content).expect("artifact");
            artifacts.push(BinaryArtifactDescriptor {
                target: TripleTarget(triple.to_string()),
                sha256: sha256_hex(content),
                retrieval_ref: None,
                size_bytes: content.len() as u64,
            });
            artifact_paths.push((triple.to_string(), path));
        }
        let manifest = ProvisionerBundle {
            identity: engine_identity(),
            bound: None,
            authority: BuildAuthority::LocalDeveloper,
            provisioner_version: "0.1.0".to_string(),
            artifacts,
            tests: TestEvidence {
                command: "cargo nextest run".to_string(),
                passed: true,
            },
            build: BuildManifest {
                request_id: "fixture".to_string(),
                source_tree_oid: "tree".to_string(),
                snapshot_commit_oid: "commit".to_string(),
                toolchain: "rustc test".to_string(),
                builder: "testkit".to_string(),
            },
        };

        let mut documents = vec![("deployment.tkd".to_string(), root_bytes.to_vec())];
        for (name, bytes) in companions {
            documents.push((format!("{name}.tkd"), bytes.clone()));
        }
        let mut config_tree = Vec::new();
        for (name, bytes) in config_files {
            documents.push((name.clone(), bytes.clone()));
            config_tree.push(name.clone());
        }

        PublicationInput {
            claim: DeploymentClaim {
                deployment: DeploymentRef {
                    name: "dev".to_string(),
                    id: uuid::Uuid::nil(),
                },
                platform: PlatformId::new("compose").expect("platform"),
                format,
                definition: DefinitionSection {
                    root: "deployment.tkd".to_string(),
                    companions: companions.iter().map(|(name, _)| name.clone()).collect(),
                    identity,
                },
                engine: EngineSection {
                    identity_digest: manifest.identity_digest().to_hex(),
                    provisioner_version: "0.1.0".to_string(),
                    manifest: "tkp.manifest.json".to_string(),
                    build_authority: "local-developer".to_string(),
                },
                transition,
                config_revision,
            },
            documents,
            config_tree,
            bundle_manifest: manifest,
            bundle_artifacts: artifact_paths,
        }
    }
}

fn engine_identity() -> EngineIdentity {
    EngineIdentity {
        source_closure: crate::Sha256Digest::from_bytes(b"source"),
        lock_closure: crate::Sha256Digest::from_bytes(b"lock"),
        toolchain: "rustc test".to_string(),
        build_container: None,
        features: BTreeSet::new(),
        profile: BuildProfile::Dist,
    }
}

async fn publish_local(
    fixture: &Fixture,
    input: PublicationInput,
    expected: u64,
    root: Option<&[u8]>,
) -> Result<PublicationReceipt, PublishError> {
    publish_transition(&fixture.config, input, expected, root, None).await
}

// Property P1 — round-trip materialization: publish then fetch materializes
// every published file byte-identically, and tkp is the host artifact.
// Feature: deployment-repository, Property P1
#[test]
fn p1_roundtrip_materializes_byte_identically() {
    let config = ProptestConfig {
        cases: 8,
        ..ProptestConfig::default()
    };
    proptest!(config, |(
        root in proptest::collection::vec(any::<u8>(), 1..64),
        companions in proptest::collection::btree_map("[a-z]{1,6}", proptest::collection::vec(any::<u8>(), 0..32), 0..3),
        configs in proptest::collection::btree_map("[a-z]{1,6}\\.yaml", proptest::collection::vec(any::<u8>(), 0..32), 0..3),
    )| {
        runtime().block_on(async {
            let fixture = Fixture::local();
            let companions: Vec<(String, Vec<u8>)> = companions.into_iter().collect();
            let configs: Vec<(String, Vec<u8>)> = configs.into_iter().collect();
            let input = fixture.input(&root, &companions, &configs, Transition::Create, 0);
            let expected_files: Vec<(String, Vec<u8>)> = input.documents.to_vec();
            let receipt = publish_local(&fixture, input, 0, None).await.expect("publish");
            prop_assert_eq!(receipt.version, 1);

            let opened = open(
                &fixture.config.locator,
                &receipt.trusted_root,
                None,
                Freshness::Enforced,
                None,
            )
            .await
            .expect("open");
            let publication = opened.verified_publication().await.expect("verify");
            prop_assert_eq!(publication.version(), 1);

            let plan = MaterializePlan::new(&publication, HOST).expect("plan");
            let out = tempfile::tempdir().expect("out");
            plan.materialize_into(&publication, out.path()).await.expect("materialize");

            for (name, bytes) in &expected_files {
                let materialized = std::fs::read(out.path().join(name)).expect("placed file");
                prop_assert_eq!(&materialized, bytes, "{} differs", name);
            }
            let tkp = std::fs::read(out.path().join("tkp")).expect("tkp placed");
            prop_assert_eq!(tkp.as_slice(), b"host-binary".as_slice());
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                let mode = std::fs::metadata(out.path().join("tkp")).expect("meta").permissions().mode();
                prop_assert_eq!(mode & 0o111, 0o111, "tkp must be executable");
            }
            prop_assert!(out.path().join(crate::BUNDLE_MANIFEST_BASENAME).is_file());
            Ok(())
        })?;
    });
}

// Property P2 — identity agreement: a claim whose identity was not computed
// over the claimed order is refused whole.
// Feature: deployment-repository, Property P2
#[test]
fn p2_identity_disagreement_refuses_the_publication() {
    runtime().block_on(async {
        let fixture = Fixture::local();
        let companions = vec![
            ("alpha".to_string(), b"a".to_vec()),
            ("beta".to_string(), b"b".to_vec()),
        ];
        let mut input = fixture.input(b"root", &companions, &[], Transition::Create, 0);
        // Claim the reversed order without recomputing the identity: the
        // fetched bytes recompute differently and the publication refuses.
        input.claim.definition.companions.reverse();
        let receipt = publish_local(&fixture, input, 0, None)
            .await
            .expect("publish");
        let opened = open(
            &fixture.config.locator,
            &receipt.trusted_root,
            None,
            Freshness::Enforced,
            None,
        )
        .await
        .expect("open");
        let refusal = opened
            .verified_publication()
            .await
            .expect_err("must refuse");
        assert_eq!(refusal.name(), "identity_mismatch", "{refusal}");
    });
}

// Property P3 — monotonic lineage: versions advance by one; revert is a new
// higher version with older content; a stale expected_version conflicts.
// Feature: deployment-repository, Property P3
#[test]
fn p3_versions_advance_and_stale_publishers_conflict() {
    runtime().block_on(async {
        let fixture = Fixture::local();
        let v1_input = fixture.input(b"v1", &[], &[], Transition::Create, 0);
        let receipt = publish_local(&fixture, v1_input, 0, None)
            .await
            .expect("v1");
        let root = receipt.trusted_root.clone();

        let v2_input = fixture.input(b"v2", &[], &[], Transition::Apply, 1);
        let receipt2 = publish_local(&fixture, v2_input, 1, Some(&root))
            .await
            .expect("v2");
        assert_eq!(receipt2.version, 2);

        // Revert: new version, v1 content — content targets dedupe.
        let v3_input = fixture.input(b"v1", &[], &[], Transition::Revert, 2);
        let receipt3 = publish_local(&fixture, v3_input, 2, Some(&root))
            .await
            .expect("v3");
        assert_eq!(receipt3.version, 3);
        let deduped = receipt3
            .outcomes
            .iter()
            .filter(|(key, outcome)| {
                key.starts_with("targets/") && *outcome == UploadOutcome::AlreadyPresent
            })
            .count();
        assert!(deduped > 0, "reverted content re-publishes idempotently");

        // A stale publisher (expected=1 again) conflicts loudly.
        let stale_input = fixture.input(b"stale", &[], &[], Transition::Apply, 3);
        let error = publish_local(&fixture, stale_input, 1, Some(&root))
            .await
            .expect_err("stale publisher must conflict");
        assert!(
            matches!(error, PublishError::Conflict { .. }),
            "got: {error}"
        );
    });
}

// Property P4 — create-only immutability at the writer: identical bytes are
// AlreadyPresent, differing bytes refuse, in both homes.
// Feature: deployment-repository, Property P4
#[test]
fn p4_create_only_writer_verifies_bytes_in_both_homes() {
    runtime().block_on(async {
        let dir = tempfile::tempdir().expect("dir");
        let local = LocalWriter::new(dir.path().to_path_buf());
        let bucket: testkit::Bucket = Default::default();
        let s3 = super::writer::writer_for(
            &RepositoryLocator::S3 {
                bucket: "b".to_string(),
                prefix: "p".to_string(),
            },
            Some(testkit::s3_client(bucket)),
        )
        .expect("writer");

        for writer in [&local as &dyn RepositoryWriter, s3.as_ref()] {
            assert_eq!(
                writer
                    .put_create_only("targets/x", WriteSource::Bytes(b"one"))
                    .await
                    .expect("create"),
                UploadOutcome::Created
            );
            assert_eq!(
                writer
                    .put_create_only("targets/x", WriteSource::Bytes(b"one"))
                    .await
                    .expect("idempotent"),
                UploadOutcome::AlreadyPresent
            );
            let error = writer
                .put_create_only("targets/x", WriteSource::Bytes(b"two"))
                .await
                .expect_err("divergence refuses");
            assert!(matches!(error, WriteError::Conflict { .. }), "{error}");
            // Mutable heads move freely.
            writer
                .put_mutable_head("metadata/timestamp.json", b"t1")
                .await
                .expect("head");
            writer
                .put_mutable_head("metadata/timestamp.json", b"t2")
                .await
                .expect("head again");
        }
    });
}

// Property P5 — absence signal: an absent object is FileNotFound (the
// root-walk dependency); other schemes refuse.
// Feature: deployment-repository, Property P5
#[test]
fn p5_transport_reports_absence_and_refuses_foreign_schemes() {
    runtime().block_on(async {
        use tough::Transport as _;
        let bucket: testkit::Bucket = Default::default();
        let transport = S3Transport::new(testkit::s3_client(bucket));
        let err = match transport
            .fetch(url::Url::parse("s3://b/absent/2.root.json").expect("url"))
            .await
        {
            Err(err) => err,
            Ok(_) => panic!("absent key must not fetch"),
        };
        assert_eq!(err.kind(), tough::TransportErrorKind::FileNotFound);

        let err = match transport
            .fetch(url::Url::parse("file:///etc/hosts").expect("url"))
            .await
        {
            Err(err) => err,
            Ok(_) => panic!("foreign scheme must not fetch"),
        };
        assert_eq!(err.kind(), tough::TransportErrorKind::UnsupportedUrlScheme);
    });
}

// Property P6 — tamper refusal: mutated metadata refuses the load; mutated
// target bytes refuse the read; nothing materializes.
// Feature: deployment-repository, Property P6
#[test]
fn p6_tampered_objects_refuse() {
    runtime().block_on(async {
        let fixture = Fixture::local();
        let input = fixture.input(
            b"root",
            &[("alpha".to_string(), b"a".to_vec())],
            &[],
            Transition::Create,
            0,
        );
        let receipt = publish_local(&fixture, input, 0, None)
            .await
            .expect("publish");

        // Target tamper: flip bytes in every stored target object.
        for entry in std::fs::read_dir(fixture.repo_path().join("targets")).expect("targets") {
            let path = entry.expect("entry").path();
            let mut bytes = std::fs::read(&path).expect("read");
            if let Some(last) = bytes.last_mut() {
                *last ^= 0x01;
            }
            std::fs::write(&path, bytes).expect("write");
        }
        let opened = open(
            &fixture.config.locator,
            &receipt.trusted_root,
            None,
            Freshness::Enforced,
            None,
        )
        .await
        .expect("metadata untouched, load succeeds");
        let refusal = opened
            .verified_publication()
            .await
            .expect_err("tampered targets must refuse");
        assert_eq!(refusal.name(), "target_unreadable", "{refusal}");

        // Metadata tamper: structural mutation without re-signing.
        let fixture2 = Fixture::local();
        let input2 = fixture2.input(b"root", &[], &[], Transition::Create, 0);
        let receipt2 = publish_local(&fixture2, input2, 0, None)
            .await
            .expect("publish");
        let targets_path = fixture2.repo_path().join("metadata/1.targets.json");
        let mut doc: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&targets_path).expect("read")).expect("parse");
        doc["signed"]["version"] = serde_json::json!(9);
        std::fs::write(&targets_path, serde_json::to_vec(&doc).expect("ser")).expect("write");
        let result = open(
            &fixture2.config.locator,
            &receipt2.trusted_root,
            None,
            Freshness::Enforced,
            None,
        )
        .await;
        assert!(result.is_err(), "tampered metadata must refuse the load");
    });
}

// Property P7 — freshness fails closed (break-glass loads); rollback with a
// datastore refuses an older publication.
// Feature: deployment-repository, Property P7
#[test]
fn p7_freshness_and_rollback_refuse() {
    runtime().block_on(async {
        // Freshness: a zero-hour window is expired by load time.
        let mut fixture = Fixture::local();
        fixture.config.lifetimes = RoleLifetimes {
            timestamp_hours: 0,
            ..RoleLifetimes::default()
        };
        let input = fixture.input(b"root", &[], &[], Transition::Create, 0);
        let receipt = publish_local(&fixture, input, 0, None)
            .await
            .expect("publish");
        let refused = open(
            &fixture.config.locator,
            &receipt.trusted_root,
            None,
            Freshness::Enforced,
            None,
        )
        .await;
        assert!(refused.is_err(), "expired freshness must refuse");
        let opened = open(
            &fixture.config.locator,
            &receipt.trusted_root,
            None,
            Freshness::BreakGlass,
            None,
        )
        .await
        .expect("break-glass loads");
        opened.verified_publication().await.expect("and verifies");

        // Rollback: a datastore that has trusted v2 refuses a v1 host.
        let fx = Fixture::local();
        let v1 = fx.input(b"v1", &[], &[], Transition::Create, 0);
        let receipt1 = publish_local(&fx, v1, 0, None).await.expect("v1");
        let root = receipt1.trusted_root.clone();
        // Copy the v1 repository aside before it advances.
        let v1_copy = tempfile::tempdir().expect("copy dir");
        copy_dir(fx.repo_path(), v1_copy.path());
        let v2 = fx.input(b"v2", &[], &[], Transition::Apply, 1);
        publish_local(&fx, v2, 1, Some(&root)).await.expect("v2");

        let datastore = tempfile::tempdir().expect("datastore");
        open(
            &fx.config.locator,
            &root,
            Some(datastore.path()),
            Freshness::Enforced,
            None,
        )
        .await
        .expect("v2 loads");
        let rolled_back = open(
            &RepositoryLocator::Local {
                path: v1_copy.path().to_path_buf(),
            },
            &root,
            Some(datastore.path()),
            Freshness::Enforced,
            None,
        )
        .await;
        assert!(
            rolled_back.is_err(),
            "older publication must refuse under the datastore"
        );
    });
}

// Property P8 — engine agreement: a claim whose engine identity digest
// disagrees with the manifest refuses.
// Feature: deployment-repository, Property P8
#[test]
fn p8_engine_identity_disagreement_refuses() {
    runtime().block_on(async {
        let fixture = Fixture::local();
        let mut input = fixture.input(b"root", &[], &[], Transition::Create, 0);
        input.claim.engine.identity_digest = "ff".repeat(32);
        let receipt = publish_local(&fixture, input, 0, None)
            .await
            .expect("publish");
        let opened = open(
            &fixture.config.locator,
            &receipt.trusted_root,
            None,
            Freshness::Enforced,
            None,
        )
        .await
        .expect("open");
        let refusal = opened
            .verified_publication()
            .await
            .expect_err("must refuse");
        assert_eq!(refusal.name(), "engine_identity_mismatch", "{refusal}");
    });
}

// Property P8 (host selection) — fetching for an unsupported host refuses
// with the available triples named.
#[test]
fn p8_host_target_unsupported_refuses() {
    runtime().block_on(async {
        let fixture = Fixture::local();
        let input = fixture.input(b"root", &[], &[], Transition::Create, 0);
        let receipt = publish_local(&fixture, input, 0, None)
            .await
            .expect("publish");
        let publication = open(
            &fixture.config.locator,
            &receipt.trusted_root,
            None,
            Freshness::Enforced,
            None,
        )
        .await
        .expect("open")
        .verified_publication()
        .await
        .expect("verify");
        let refusal = MaterializePlan::new(&publication, "riscv64gc-unknown-none")
            .expect_err("unsupported host must refuse");
        assert_eq!(refusal.name(), "host_target_unsupported", "{refusal}");
    });
}

// Property P9 — rotation in place: a v2 root signed under the v1 chain is
// walked and re-pinned by a consumer anchored at v1.
// Feature: deployment-repository, Property P9
#[test]
fn p9_root_walk_repins_the_accepted_root() {
    runtime().block_on(async {
        let fixture = Fixture::local();
        let input = fixture.input(b"root", &[], &[], Transition::Create, 0);
        let receipt = publish_local(&fixture, input, 0, None)
            .await
            .expect("publish");

        // Author root v2 with the SAME keys (the online-rotation shape: the
        // root key carries; only the version advances) and place it in the
        // repository for the walk.
        let v2 = super::publish::author_root_for_tests(&fixture.config, 2)
            .await
            .expect("author v2");
        std::fs::write(
            fixture.repo_path().join("metadata/2.root.json"),
            v2.buffer(),
        )
        .expect("place 2.root.json");

        let opened = open(
            &fixture.config.locator,
            &receipt.trusted_root,
            None,
            Freshness::Enforced,
            None,
        )
        .await
        .expect("open walks");
        let anchor = opened.trust_anchor().expect("anchor");
        let root: serde_json::Value = serde_json::from_slice(&anchor).expect("root json");
        assert_eq!(
            root["signed"]["version"], 2,
            "consumer re-pins the walked root"
        );
    });
}

// Property P10 — commit authority: a torn publication (heads unwritten) is
// completed by re-running the same publish; nothing is lost or unwound.
// Feature: deployment-repository, Property P10
#[test]
fn p10_torn_publication_is_republishable() {
    runtime().block_on(async {
        let fixture = Fixture::local();
        let v1 = fixture.input(b"v1", &[], &[], Transition::Create, 0);
        let receipt = publish_local(&fixture, v1, 0, None).await.expect("v1");
        let root = receipt.trusted_root.clone();
        let v2 = fixture.input(b"v2", &[], &[], Transition::Apply, 1);
        publish_local(&fixture, v2, 1, Some(&root))
            .await
            .expect("v2");

        // Tear: the heads vanish after the create-only writes (the crash
        // window). The torn version's metadata is immutable — a re-publish
        // at the stale expected version collides with it and the conflict
        // names the version already written. Retrying with THAT version as
        // the expected version is the repair contract: the publication
        // lands at the next version and its content dedupes.
        std::fs::remove_file(fixture.repo_path().join("metadata/timestamp.json"))
            .expect("tear the head");
        let v2_again = fixture.input(b"v2", &[], &[], Transition::Apply, 1);
        let error = publish_local(&fixture, v2_again, 1, Some(&root))
            .await
            .expect_err("the torn version's metadata is immutable");
        let PublishError::Conflict { attempted, .. } = error else {
            panic!("expected a conflict naming the torn version, got {error}");
        };
        assert_eq!(
            attempted, 2,
            "the conflict names the version already written"
        );
        let v2_retry = fixture.input(b"v2", &[], &[], Transition::Apply, 1);
        let receipt2 = publish_local(&fixture, v2_retry, attempted, Some(&root))
            .await
            .expect("retrying at the conflicting version completes the publication");
        assert_eq!(receipt2.version, 3);
        assert!(
            receipt2
                .outcomes
                .iter()
                .filter(|(key, _)| key.starts_with("targets/"))
                .all(|(_, outcome)| *outcome == UploadOutcome::AlreadyPresent),
            "content was already admitted"
        );
        open(
            &fixture.config.locator,
            &root,
            None,
            Freshness::Enforced,
            None,
        )
        .await
        .expect("healed repository loads")
        .verified_publication()
        .await
        .expect("and verifies");
    });
}

// Property P11 — home equivalence: the same input publishes the same
// inventory and claims to both homes (metadata bytes differ only by signing
// instants).
// Feature: deployment-repository, Property P11
#[test]
fn p11_local_and_s3_homes_publish_equivalent_publications() {
    runtime().block_on(async {
        let local = Fixture::local();
        let companions = vec![("alpha".to_string(), b"a".to_vec())];
        let configs = vec![(
            "observability/alerts/critical.yaml".to_string(),
            b"alert".to_vec(),
        )];
        let input = local.input(b"root", &companions, &configs, Transition::Create, 0);
        let local_receipt = publish_local(&local, input, 0, None).await.expect("local");

        let (s3_fixture, bucket) = Fixture::s3("b");
        let client = testkit::s3_client(bucket.clone());
        let input = s3_fixture.input(b"root", &companions, &configs, Transition::Create, 0);
        let s3_receipt =
            publish_transition(&s3_fixture.config, input, 0, None, Some(client.clone()))
                .await
                .expect("s3");

        let local_keys: BTreeSet<String> = local_receipt
            .outcomes
            .iter()
            .map(|(key, _)| key.clone())
            .collect();
        let s3_keys: BTreeSet<String> = s3_receipt
            .outcomes
            .iter()
            .map(|(key, _)| key.clone())
            .collect();
        assert_eq!(local_keys, s3_keys, "identical object inventories");

        // Both verify end-to-end in their homes, with equal claims.
        let local_pub = open(
            &local.config.locator,
            &local_receipt.trusted_root,
            None,
            Freshness::Enforced,
            None,
        )
        .await
        .expect("open local")
        .verified_publication()
        .await
        .expect("verify local");
        let s3_pub = open(
            &s3_fixture.config.locator,
            &s3_receipt.trusted_root,
            None,
            Freshness::Enforced,
            Some(client.clone()),
        )
        .await
        .expect("open s3")
        .verified_publication()
        .await
        .expect("verify s3");
        assert_eq!(local_pub.claim(), s3_pub.claim());

        // And the remote listing names the deployment under the base.
        let listed = list_remote(&client, "b", "deployments")
            .await
            .expect("list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "dev");
    });
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    for entry in walk(from) {
        let relative = entry.strip_prefix(from).expect("under from");
        let dest = to.join(relative);
        if entry.is_dir() {
            std::fs::create_dir_all(&dest).expect("mkdir");
        } else {
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent).expect("mkdir");
            }
            std::fs::copy(&entry, &dest).expect("copy");
        }
    }
}

fn walk(dir: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        for entry in std::fs::read_dir(&current).expect("read_dir") {
            let path = entry.expect("entry").path();
            out.push(path.clone());
            if path.is_dir() {
                stack.push(path);
            }
        }
    }
    out
}
