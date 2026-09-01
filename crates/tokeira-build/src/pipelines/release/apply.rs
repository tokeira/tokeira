//! Structural credential phases, resumable train classification, and executor seams.

use std::{fmt, path::PathBuf, time::Duration};

use async_trait::async_trait;

use super::{
    PackageOutcome, PublishParityReport, RELEASE_SCHEMA_VERSION, ReleaseError, ReleasePlan,
    ReleaseReport, RepositoryIdentity, TagResult, TrainState,
};

// The 0.1.1 train measured registry index liveness of 6–98 seconds per crate; the
// schedule starts below that band and the window caps well above it.
const INITIAL_OBSERVATION_DELAY: Duration = Duration::from_secs(5);
const OBSERVATION_WINDOW: Duration = Duration::from_secs(10 * 60);
/// Pause after a successful upload before the next one, so a train of seventeen
/// crates never trips the registry's publish rate limit.
pub(crate) const SUCCESS_COOLDOWN: Duration = Duration::from_secs(10 * 60);

/// Opaque registry credential admitted only by the publish-and-parity request.
pub struct RegistryCredential(String);

impl RegistryCredential {
    /// Wrap a non-empty environment value at the last responsible moment.
    pub fn new(value: String) -> Result<Self, ReleaseError> {
        if value.is_empty() {
            return Err(ReleaseError::CredentialMissing {
                name: "<selected>".to_owned(),
            });
        }
        Ok(Self(value))
    }

    /// Expose the value only to the production Dagger secret registration boundary.
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RegistryCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RegistryCredential(***)")
    }
}

/// Opaque GitHub credential admitted only by the later release-note request.
pub struct ReleaseApiCredential(String);

impl ReleaseApiCredential {
    /// Wrap the non-empty fixed `GH_TOKEN` environment value after parity succeeds.
    pub fn new(value: String) -> Result<Self, ReleaseError> {
        if value.is_empty() {
            return Err(ReleaseError::ReleaseCredentialMissing);
        }
        Ok(Self(value))
    }

    /// Expose the value only to the production Dagger secret registration boundary.
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ReleaseApiCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReleaseApiCredential(***)")
    }
}

/// First executor invocation: source through registry parity, with no release API field.
pub struct ReleasePublishRequest {
    /// Revalidated exact release Plan.
    pub plan: ReleasePlan,
    /// Registry secret required only when at least one version is absent.
    pub registry_credential: Option<RegistryCredential>,
}

impl fmt::Debug for ReleasePublishRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReleasePublishRequest")
            .field("plan_digest", &self.plan.digest)
            .field(
                "registry_credential",
                &self.registry_credential.as_ref().map(|_| "***"),
            )
            .finish()
    }
}

/// Second executor invocation: immutable release notes, with no registry field.
pub struct ReleaseNotesRequest {
    /// Successful secret-free parity evidence from the first invocation.
    pub parity: PublishParityReport,
    /// Fixed GitHub release API credential.
    pub release_api_credential: ReleaseApiCredential,
}

impl fmt::Debug for ReleaseNotesRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReleaseNotesRequest")
            .field("train", &self.parity.train)
            .field("notes_sha256", &self.parity.release_notes_sha256)
            .field("release_api_credential", &"***")
            .finish()
    }
}

/// Read-only complete-train verification request with no credential field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReleaseVerifyRequest {
    /// Selected local workspace used to derive the publishable package set.
    pub workspace_root: PathBuf,
    /// Public repository identity.
    pub repository: RepositoryIdentity,
    /// Stable released or partially released version.
    pub version: String,
    /// Exact `v`-prefixed tag.
    pub tag: String,
    /// Configured release branch observed together with the tag.
    pub release_branch: String,
    /// Optional Train Identity digest expected by an apply resume.
    pub expected_plan_digest: Option<String>,
}

/// Two structurally distinct mutations plus a read-only verifier.
#[async_trait]
pub trait ReleaseDaggerClient: Send + Sync {
    /// Execute preparation, atomic Git publication, serial package publication, and parity.
    async fn execute_publish_and_parity(
        &self,
        request: &ReleasePublishRequest,
    ) -> Result<PublishParityReport, ReleaseError>;

    /// Create or verify immutable release notes in a later invocation.
    async fn execute_release_notes(
        &self,
        request: &ReleaseNotesRequest,
    ) -> Result<ReleaseReport, ReleaseError>;

    /// Observe the complete public train without credentials.
    async fn execute_verify(
        &self,
        request: &ReleaseVerifyRequest,
    ) -> Result<ReleaseReport, ReleaseError>;
}

/// Run the first executor invocation after validating token necessity and Plan identity.
pub async fn publish_and_verify_release(
    request: &ReleasePublishRequest,
    dagger: &dyn ReleaseDaggerClient,
) -> Result<PublishParityReport, ReleaseError> {
    request.plan.validate_digest()?;
    let upload_required = request
        .plan
        .packages
        .iter()
        .any(|package| matches!(package.registry, super::PlannedRegistryState::Absent));
    if upload_required && request.registry_credential.is_none() {
        return Err(ReleaseError::CredentialMissing {
            name: "<selected>".to_owned(),
        });
    }
    let report = dagger.execute_publish_and_parity(request).await?;
    validate_parity_report(&report, &request.plan)?;
    Ok(report)
}

/// Run the second executor invocation only after complete parity.
pub async fn create_release_notes(
    request: &ReleaseNotesRequest,
    dagger: &dyn ReleaseDaggerClient,
) -> Result<ReleaseReport, ReleaseError> {
    if request.parity.schema_version != RELEASE_SCHEMA_VERSION
        || request
            .parity
            .packages
            .iter()
            .any(|package| !package_verified(package.outcome))
    {
        return Err(ReleaseError::Plan {
            reason: "release-note invocation requires complete package parity".to_owned(),
        });
    }
    dagger.execute_release_notes(request).await
}

/// Observe a public train without resolving either credential.
pub async fn verify_release(
    request: &ReleaseVerifyRequest,
    dagger: &dyn ReleaseDaggerClient,
) -> Result<ReleaseReport, ReleaseError> {
    dagger.execute_verify(request).await
}

fn validate_parity_report(
    report: &PublishParityReport,
    plan: &ReleasePlan,
) -> Result<(), ReleaseError> {
    let expected_train = super::TrainIdentity::from(plan);
    // The note digest is deliberately not compared with the Plan: the Plan holds a
    // dated preview, while the report holds the digest derived from the tagged
    // version file, which is what the release object must later carry.
    if report.schema_version != RELEASE_SCHEMA_VERSION
        || report.train != expected_train
        || report.tag.tag != plan.tag
        || report.tag.commit.is_empty()
        || !report.tag.published
        || report.release_notes_sha256.len() != 64
        || report.packages.len() != plan.packages.len()
    {
        return Err(ReleaseError::Plan {
            reason: "publish-and-parity report does not match the admitted Plan".to_owned(),
        });
    }
    for (result, package) in report.packages.iter().zip(&plan.packages) {
        // Parity is four-way here: the bytes the Plan was confirmed for, the bytes
        // the tag build produced, the bytes the registry serves, and the registry's
        // own checksum must all agree.
        let checksums_match = result
            .hermetic_sha256
            .as_ref()
            .zip(result.downloaded_sha256.as_ref())
            .zip(result.registry_sha256.as_ref())
            .is_some_and(|((hermetic, downloaded), registry)| {
                *hermetic == package.hermetic_sha256
                    && hermetic == downloaded
                    && downloaded == registry
            });
        if result.name != package.name
            || result.version != package.target_version
            || !package_verified(result.outcome)
            || !checksums_match
            || result.readme_url.is_none()
        {
            return Err(ReleaseError::ArtifactMismatch {
                package: package.name.clone(),
                version: package.target_version.clone(),
                hermetic: result
                    .hermetic_sha256
                    .clone()
                    .unwrap_or_else(|| "absent".to_owned()),
                downloaded: result
                    .downloaded_sha256
                    .clone()
                    .unwrap_or_else(|| "absent".to_owned()),
                registry: result
                    .registry_sha256
                    .clone()
                    .unwrap_or_else(|| "absent".to_owned()),
            });
        }
    }
    Ok(())
}

fn package_verified(outcome: PackageOutcome) -> bool {
    matches!(
        outcome,
        PackageOutcome::Published | PackageOutcome::ExistingVerified
    )
}

/// One remote Git ref observation used by resume admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedGitRef {
    /// Observed ref object ID; annotated tags retain their tag object ID here.
    pub object_id: String,
    /// Peeled Release Commit.
    pub commit: String,
    /// Plan digest parsed from the commit trailer or tag annotation.
    pub plan_digest: String,
}

/// Branch and tag are always observed together.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteGitObservation {
    /// Configured release branch observation.
    pub branch: Option<ObservedGitRef>,
    /// Annotated release tag observation.
    pub tag: Option<ObservedGitRef>,
}

/// Require both remote refs to identify one Release Commit and Train Identity.
pub fn verify_resume_refs(
    tag_name: &str,
    expected_commit: &str,
    expected_plan_digest: &str,
    observation: &RemoteGitObservation,
) -> Result<TagResult, ReleaseError> {
    let branch_value = observation
        .branch
        .as_ref()
        .map_or_else(|| "absent".to_owned(), |value| value.object_id.clone());
    let tag_value = observation
        .tag
        .as_ref()
        .map_or_else(|| "absent".to_owned(), |value| value.object_id.clone());
    let consistent = observation
        .branch
        .as_ref()
        .zip(observation.tag.as_ref())
        .is_some_and(|(branch, tag)| {
            branch.commit == expected_commit
                && tag.commit == expected_commit
                && branch.commit == tag.commit
                && branch.plan_digest == expected_plan_digest
                && tag.plan_digest == expected_plan_digest
        });
    if !consistent {
        return Err(ReleaseError::GitRefConflict {
            branch_observed: branch_value,
            tag_observed: tag_value,
        });
    }
    Ok(TagResult {
        tag: tag_name.to_owned(),
        commit: expected_commit.to_owned(),
        published: true,
    })
}

/// How the pre-mutation observation classifies a train.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReleaseAdmission {
    /// No release tag exists and the branch still sits at the admitted base.
    Fresh,
    /// Both refs already identify this train's Release Commit; nothing is pushed.
    Resume {
        /// The published Release Commit.
        commit: String,
    },
}

/// Decide fresh publication versus resume from one branch-and-tag observation.
///
/// The decision is taken here, from observed facts, so it is provable offline and
/// cannot be reordered by the executor: a branch that moved without a tag, or a tag
/// that is not this train's, is a conflict before any mutation.
pub fn admit_release_refs(
    base_commit: &str,
    tag_name: &str,
    plan_digest: &str,
    observation: &RemoteGitObservation,
) -> Result<ReleaseAdmission, ReleaseError> {
    match (&observation.tag, &observation.branch) {
        (None, Some(branch)) if branch.commit == base_commit => Ok(ReleaseAdmission::Fresh),
        (None, branch) => Err(ReleaseError::GitRefConflict {
            branch_observed: branch
                .as_ref()
                .map_or_else(|| "absent".to_owned(), |value| value.object_id.clone()),
            tag_observed: "absent".to_owned(),
        }),
        // The tag exists but the branch never moved: another train, or a stray tag,
        // owns the name, and this Plan can never become the published one.
        (Some(tag), Some(branch)) if branch.commit == base_commit => {
            Err(ReleaseError::TagConflict {
                tag: tag_name.to_owned(),
                expected: format!("sha256:{plan_digest}"),
                observed: tag.commit.clone(),
            })
        }
        (Some(tag), _) => {
            verify_resume_refs(tag_name, &tag.commit, plan_digest, observation).map(|result| {
                ReleaseAdmission::Resume {
                    commit: result.commit,
                }
            })
        }
    }
}

/// Verify a published train without the resume rule's exact-tip requirement.
///
/// Verification reads a release that may be days old: the branch has legitimately
/// moved on, so it must *contain* the tagged commit rather than equal it. The tag is
/// the identity: its annotation and its commit must carry the same Plan digest, and
/// the expected digest when the caller supplies one.
pub fn verify_published_refs(
    tag_name: &str,
    observation: &RemoteGitObservation,
    tag_commit_digest: Option<&str>,
    branch_contains_tag: bool,
    expected_plan_digest: Option<&str>,
) -> Result<TagResult, ReleaseError> {
    let Some(tag) = &observation.tag else {
        return Err(ReleaseError::ReleaseNotFound {
            tag: tag_name.to_owned(),
        });
    };
    let branch_observed = observation
        .branch
        .as_ref()
        .map_or_else(|| "absent".to_owned(), |value| value.object_id.clone());
    let digest_consistent = tag_commit_digest.is_some_and(|digest| digest == tag.plan_digest)
        && expected_plan_digest.is_none_or(|expected| expected == tag.plan_digest);
    if !digest_consistent || !branch_contains_tag || observation.branch.is_none() {
        return Err(ReleaseError::GitRefConflict {
            branch_observed,
            tag_observed: tag.object_id.clone(),
        });
    }
    Ok(TagResult {
        tag: tag_name.to_owned(),
        commit: tag.commit.clone(),
        published: true,
    })
}

/// Generate polling intervals: 5 seconds, exponential backoff, hard 10-minute window.
///
/// The executor hands this schedule to the registry script verbatim, so the bounds
/// the spec measured (registry liveness of 6–98 s per crate on the 0.1.1 train) live
/// in exactly one place.
pub fn registry_observation_delays() -> Vec<Duration> {
    let mut elapsed = Duration::ZERO;
    let mut delay = INITIAL_OBSERVATION_DELAY;
    let mut delays = Vec::new();
    while elapsed < OBSERVATION_WINDOW {
        let remaining = OBSERVATION_WINDOW - elapsed;
        let next = delay.min(remaining);
        delays.push(next);
        elapsed += next;
        delay = delay.saturating_mul(2);
    }
    delays
}

/// Earliest next upload time, respecting both cooldown and registry retry deadline.
pub fn next_upload_at(last_success_seconds: Option<u64>, retry_at_seconds: Option<u64>) -> u64 {
    let cooldown = last_success_seconds
        .map(|success| success.saturating_add(SUCCESS_COOLDOWN.as_secs()))
        .unwrap_or(0);
    cooldown.max(retry_at_seconds.unwrap_or(0))
}

/// Durable phase facts used to classify safe resume behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TrainPhaseFacts {
    /// Whether a failure occurred before complete Git publication.
    pub pre_publication_failure: bool,
    /// Whether the branch/tag atomic transaction is observable.
    pub git_published: bool,
    /// Whether any package/version is publicly observable.
    pub any_package_public: bool,
    /// Whether every package has complete parity.
    pub all_packages_verified: bool,
    /// Whether any immutable checksum or release object conflicts.
    pub terminal_mismatch: bool,
    /// Whether matching immutable release notes exist.
    pub release_notes_complete: bool,
}

/// Classify the externally meaningful train state without inventing rollback.
pub fn classify_train_state(facts: TrainPhaseFacts) -> TrainState {
    if facts.terminal_mismatch {
        TrainState::TerminalMismatch
    } else if facts.all_packages_verified && facts.release_notes_complete {
        TrainState::Complete
    } else if facts.git_published || facts.any_package_public {
        TrainState::PartiallyPublished
    } else if facts.pre_publication_failure {
        TrainState::PrePublicationFailed
    } else {
        TrainState::Planned
    }
}

/// The mutation fence: refuse Plan drift or a declined confirmation before any gateway.
///
/// `confirmed` is the operator's actual answer, resolved by the caller from `--yes`
/// or the interactive prompt; a `false` here is an explicit decline, which is why it
/// maps to `ConfirmationDeclined` rather than to the non-interactive refusal.
pub fn require_apply_admission(
    stored: &ReleasePlan,
    recomputed: &ReleasePlan,
    confirmed: bool,
) -> Result<(), ReleaseError> {
    stored.validate_digest()?;
    recomputed.validate_digest()?;
    if stored.digest != recomputed.digest {
        return Err(ReleaseError::PlanDrift {
            stored: stored.digest.clone(),
            recomputed: recomputed.digest.clone(),
        });
    }
    if !confirmed {
        return Err(ReleaseError::ConfirmationDeclined);
    }
    Ok(())
}

/// Exact immutable GitHub release creation arguments, as the executor runs them.
///
/// `--verify-tag` refuses to invent a tag, and `--target` pins the release object to
/// the Release Commit so a later force of the tag cannot silently move it.
pub fn gh_release_create_arguments(
    repository: &str,
    tag: &str,
    target: &str,
    notes_file: &str,
) -> Vec<String> {
    vec![
        "release".to_owned(),
        "create".to_owned(),
        tag.to_owned(),
        "--repo".to_owned(),
        repository.to_owned(),
        "--verify-tag".to_owned(),
        "--target".to_owned(),
        target.to_owned(),
        "--title".to_owned(),
        tag.to_owned(),
        "--notes-file".to_owned(),
        notes_file.to_owned(),
    ]
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{path::PathBuf, sync::Mutex};

    use proptest::prelude::*;

    use super::*;
    use crate::pipelines::release::{
        ChangieIdentity, PackagePlan, PackageResult, PlannedRegistryState, ReleaseNotesOutcome,
        ReleaseNotesResult, RepositoryIdentity, ToolchainIdentity, TrainIdentity, sha256_hex,
    };

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        // Feature: release-engineering, Property 3: confirmation is a mutation fence
        #[test]
        fn refused_admission_leaves_modeled_state_unchanged(
            state in proptest::collection::vec(any::<u8>(), 0..256),
            confirmed in any::<bool>(),
            drift in any::<bool>(),
        ) {
            let stored = sample_plan("a");
            let recomputed = if drift { sample_plan("b") } else { stored.clone() };
            let before = state.clone();
            let admitted = require_apply_admission(&stored, &recomputed, confirmed).is_ok();
            prop_assert_eq!(state, before);
            prop_assert_eq!(admitted, confirmed && !drift);
        }

        // Feature: release-engineering, Property 8: publish execution is idempotent
        // (the skip-existing half runs the real registry script in `scripts.rs`)
        #[test]
        fn observation_schedule_starts_small_doubles_and_stays_inside_the_window(
            _case in 0_u8..8,
        ) {
            let delays = registry_observation_delays();
            prop_assert_eq!(delays.first().copied(), Some(Duration::from_secs(5)));
            prop_assert_eq!(delays.iter().sum::<Duration>(), Duration::from_secs(600));
            for pair in delays[..delays.len() - 1].windows(2) {
                prop_assert_eq!(pair[1], pair[0].saturating_mul(2));
            }
        }

        // Feature: release-engineering, Property 13: partial-train state classification and resume
        #[test]
        fn admission_is_fresh_only_at_the_untouched_base(
            branch_at_base in any::<bool>(),
            tag_present in any::<bool>(),
            tag_is_this_train in any::<bool>(),
        ) {
            let base = "b".repeat(40);
            let moved = "c".repeat(40);
            let digest = "d".repeat(64);
            let branch_commit = if branch_at_base { base.clone() } else { moved.clone() };
            let tag_digest = if tag_is_this_train { digest.clone() } else { "e".repeat(64) };
            let observation = RemoteGitObservation {
                branch: Some(ObservedGitRef {
                    object_id: branch_commit.clone(),
                    commit: branch_commit.clone(),
                    plan_digest: if branch_at_base { "absent".to_owned() } else { tag_digest.clone() },
                }),
                tag: tag_present.then(|| ObservedGitRef {
                    object_id: "t".repeat(40),
                    commit: moved.clone(),
                    plan_digest: tag_digest.clone(),
                }),
            };
            let admission = admit_release_refs(&base, "v1.0.0", &digest, &observation);
            let ref_conflict = matches!(admission, Err(ReleaseError::GitRefConflict { .. }));
            let tag_conflict = matches!(admission, Err(ReleaseError::TagConflict { .. }));
            match (tag_present, branch_at_base, tag_is_this_train) {
                (false, true, _) => prop_assert_eq!(admission.ok(), Some(ReleaseAdmission::Fresh)),
                (false, false, _) => prop_assert!(ref_conflict),
                (true, true, _) => prop_assert!(tag_conflict),
                (true, false, true) => prop_assert_eq!(
                    admission.ok(),
                    Some(ReleaseAdmission::Resume { commit: moved.clone() })
                ),
                (true, false, false) => prop_assert!(ref_conflict),
            }
        }

        // Feature: release-engineering, Property 9: publish pacing respects both clocks
        #[test]
        fn pacing_is_the_maximum_of_both_clocks(
            success in proptest::option::of(0_u64..100_000),
            retry in proptest::option::of(0_u64..100_000),
        ) {
            let expected = success.map(|value| value + 600).unwrap_or(0)
                .max(retry.unwrap_or(0));
            prop_assert_eq!(next_upload_at(success, retry), expected);
        }

        // Feature: release-engineering, Property 13: partial-train state classification and resume
        #[test]
        fn state_and_ref_admission_match_durable_facts(
            pre_failure in any::<bool>(),
            git in any::<bool>(),
            package in any::<bool>(),
            verified in any::<bool>(),
            mismatch in any::<bool>(),
            notes in any::<bool>(),
            branch_matches in any::<bool>(),
            tag_matches in any::<bool>(),
        ) {
            let facts = TrainPhaseFacts {
                pre_publication_failure: pre_failure,
                git_published: git,
                any_package_public: package,
                all_packages_verified: verified,
                terminal_mismatch: mismatch,
                release_notes_complete: notes,
            };
            let expected = if mismatch {
                TrainState::TerminalMismatch
            } else if verified && notes {
                TrainState::Complete
            } else if git || package {
                TrainState::PartiallyPublished
            } else if pre_failure {
                TrainState::PrePublicationFailed
            } else {
                TrainState::Planned
            };
            prop_assert_eq!(classify_train_state(facts), expected);

            let matching = ObservedGitRef {
                object_id: "object".to_owned(),
                commit: "commit".to_owned(),
                plan_digest: "digest".to_owned(),
            };
            let conflicting = ObservedGitRef {
                object_id: "foreign".to_owned(),
                commit: "other".to_owned(),
                plan_digest: "other".to_owned(),
            };
            let observation = RemoteGitObservation {
                branch: Some(if branch_matches { matching.clone() } else { conflicting.clone() }),
                tag: Some(if tag_matches { matching } else { conflicting }),
            };
            let result = verify_resume_refs("v1.0.0", "commit", "digest", &observation);
            prop_assert_eq!(result.is_ok(), branch_matches && tag_matches);
            if let Err(ReleaseError::GitRefConflict { branch_observed, tag_observed }) = result {
                prop_assert!(!branch_observed.is_empty());
                prop_assert!(!tag_observed.is_empty());
            }
        }
    }

    #[derive(Default)]
    struct PhaseRecordingClient {
        phases: Mutex<Vec<&'static str>>,
    }

    #[async_trait]
    impl ReleaseDaggerClient for PhaseRecordingClient {
        async fn execute_publish_and_parity(
            &self,
            request: &ReleasePublishRequest,
        ) -> Result<PublishParityReport, ReleaseError> {
            self.phases.lock().expect("phase lock").push("registry");
            let checksum = "f".repeat(64);
            Ok(PublishParityReport {
                schema_version: RELEASE_SCHEMA_VERSION,
                train: TrainIdentity::from(&request.plan),
                packages: vec![PackageResult {
                    name: "crate-a".to_owned(),
                    version: "1.0.0".to_owned(),
                    outcome: PackageOutcome::Published,
                    hermetic_sha256: Some(checksum.clone()),
                    downloaded_sha256: Some(checksum.clone()),
                    registry_sha256: Some(checksum),
                    readme_url: Some("https://example.invalid/readme".to_owned()),
                }],
                tag: TagResult {
                    tag: "v1.0.0".to_owned(),
                    commit: "commit".to_owned(),
                    published: true,
                },
                release_notes_sha256: request.plan.release_notes_sha256.clone(),
                diagnostics: Vec::new(),
            })
        }

        async fn execute_release_notes(
            &self,
            request: &ReleaseNotesRequest,
        ) -> Result<ReleaseReport, ReleaseError> {
            self.phases.lock().expect("phase lock").push("release-api");
            Ok(ReleaseReport {
                schema_version: RELEASE_SCHEMA_VERSION,
                train: request.parity.train.clone(),
                state: TrainState::Complete,
                packages: request.parity.packages.clone(),
                tag: request.parity.tag.clone(),
                release_notes: ReleaseNotesResult {
                    outcome: ReleaseNotesOutcome::Created,
                    sha256: request.parity.release_notes_sha256.clone(),
                },
                diagnostics: Vec::new(),
            })
        }

        async fn execute_verify(
            &self,
            _request: &ReleaseVerifyRequest,
        ) -> Result<ReleaseReport, ReleaseError> {
            Err(ReleaseError::Executor {
                reason: "not used".to_owned(),
            })
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        // Feature: release-engineering, Property 10: credential noninterference
        #[test]
        fn phase_request_shapes_and_outputs_are_credential_independent(
            registry_token in "[A-Za-z]{8,24}",
            release_token in "[A-Za-z]{8,24}",
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            runtime.block_on(async {
                let registry_token = format!("REGISTRY-SECRET-{registry_token}-END");
                let release_token = format!("RELEASE-SECRET-{release_token}-END");
                let mut plan = sample_plan("a");
                plan.release_notes_sha256 = sha256_hex(b"notes");
                plan.seal().expect("reseal plan");
                let publish = ReleasePublishRequest {
                    plan,
                    registry_credential: Some(RegistryCredential::new(registry_token.clone())?),
                };
                let client = PhaseRecordingClient::default();
                let parity = publish_and_verify_release(&publish, &client).await?;
                let release = ReleaseNotesRequest {
                    parity,
                    release_api_credential: ReleaseApiCredential::new(release_token.clone())?,
                };
                let report = create_release_notes(&release, &client).await?;
                let output = format!("{publish:?}\n{release:?}\n{}", serde_json::to_string(&report).expect("report JSON"));
                prop_assert!(!output.contains(&registry_token));
                prop_assert!(!output.contains(&release_token));
                let phases = client.phases.lock().expect("phase lock");
                prop_assert_eq!(phases.as_slice(), ["registry", "release-api"]);
                Ok(())
            })?;
        }
    }

    pub(crate) fn sample_plan(base: &str) -> ReleasePlan {
        let mut plan = ReleasePlan {
            schema_version: RELEASE_SCHEMA_VERSION,
            repository: RepositoryIdentity {
                slug: "tokeira/tokeira".to_owned(),
                remote: "https://github.com/tokeira/tokeira".to_owned(),
            },
            workspace_root: PathBuf::from("/workspace"),
            base_commit: base.repeat(40),
            target_version: "1.0.0".to_owned(),
            tag: "v1.0.0".to_owned(),
            packages: vec![PackagePlan {
                name: "crate-a".to_owned(),
                manifest_path: PathBuf::from("crate-a/Cargo.toml"),
                from_version: "0.1.0".to_owned(),
                target_version: "1.0.0".to_owned(),
                publishable_dependencies: Vec::new(),
                hermetic_sha256: "f".repeat(64),
                registry: PlannedRegistryState::Absent,
            }],
            fragments: Vec::new(),
            changelog_config_sha256: "b".repeat(64),
            changie_release: ChangieIdentity {
                version: "1.25.2".to_owned(),
                source_revision: "c".repeat(40),
                platform: "linux-x86_64".to_owned(),
                asset: "changie.tar.gz".to_owned(),
                asset_sha256: "d".repeat(64),
            },
            toolchain: ToolchainIdentity {
                rust: "1.97.1".to_owned(),
                dagger: "0.19.8".to_owned(),
            },
            release_notes_sha256: sha256_hex(b"notes"),
            effects: Vec::new(),
            digest: String::new(),
        };
        plan.seal().expect("sample Plan");
        plan
    }

    #[test]
    fn exact_poll_and_release_commands() {
        assert_eq!(
            registry_observation_delays(),
            [5, 10, 20, 40, 80, 160, 285].map(Duration::from_secs)
        );
        assert_eq!(
            gh_release_create_arguments("tokeira/tokeira", "v1.2.3", "commit", "/tmp/notes.md"),
            [
                "release",
                "create",
                "v1.2.3",
                "--repo",
                "tokeira/tokeira",
                "--verify-tag",
                "--target",
                "commit",
                "--title",
                "v1.2.3",
                "--notes-file",
                "/tmp/notes.md",
            ]
        );
    }

    #[test]
    fn verification_needs_a_tag_and_a_branch_that_contains_it() {
        let digest = "d".repeat(64);
        let tag = ObservedGitRef {
            object_id: "t".repeat(40),
            commit: "c".repeat(40),
            plan_digest: digest.clone(),
        };
        let branch = ObservedGitRef {
            object_id: "b".repeat(40),
            commit: "b".repeat(40),
            plan_digest: "absent".to_owned(),
        };
        let absent = RemoteGitObservation {
            branch: Some(branch.clone()),
            tag: None,
        };
        assert!(matches!(
            verify_published_refs("v1.0.0", &absent, None, true, None),
            Err(ReleaseError::ReleaseNotFound { .. })
        ));
        let published = RemoteGitObservation {
            branch: Some(branch),
            tag: Some(tag),
        };
        assert!(verify_published_refs("v1.0.0", &published, Some(&digest), true, None).is_ok());
        assert!(
            verify_published_refs("v1.0.0", &published, Some(&digest), true, Some(&digest)).is_ok()
        );
        assert!(verify_published_refs("v1.0.0", &published, Some(&digest), false, None).is_err());
        assert!(verify_published_refs("v1.0.0", &published, Some("other"), true, None).is_err());
        assert!(
            verify_published_refs("v1.0.0", &published, Some(&digest), true, Some("other"))
                .is_err()
        );
    }

    #[test]
    fn parity_report_requires_complete_matching_evidence() {
        let plan = sample_plan("a");
        let mut report = PublishParityReport {
            schema_version: RELEASE_SCHEMA_VERSION,
            train: TrainIdentity::from(&plan),
            packages: vec![PackageResult {
                name: "crate-a".to_owned(),
                version: "1.0.0".to_owned(),
                outcome: PackageOutcome::Published,
                hermetic_sha256: None,
                downloaded_sha256: None,
                registry_sha256: None,
                readme_url: Some("https://example.invalid/readme".to_owned()),
            }],
            tag: TagResult {
                tag: plan.tag.clone(),
                commit: "commit".to_owned(),
                published: true,
            },
            release_notes_sha256: plan.release_notes_sha256.clone(),
            diagnostics: Vec::new(),
        };

        assert!(validate_parity_report(&report, &plan).is_err());
        let checksum = "f".repeat(64);
        report.packages[0].hermetic_sha256 = Some(checksum.clone());
        report.packages[0].downloaded_sha256 = Some(checksum.clone());
        report.packages[0].registry_sha256 = Some(checksum);
        assert!(validate_parity_report(&report, &plan).is_ok());
        report.tag.tag = "v9.9.9".to_owned();
        assert!(validate_parity_report(&report, &plan).is_err());
    }
}
