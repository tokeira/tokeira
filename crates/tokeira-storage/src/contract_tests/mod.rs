//! Backend-independent storage contracts. Inputs author the expected history and
//! write set; adapters only supply real stores, inspection and owned-row cleanup.

use std::{collections::BTreeMap, fmt::Debug, future::Future, path::PathBuf};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use proptest::{
    prelude::*,
    test_runner::{RngSeed, TestRunner},
};
use serde::Serialize;
use time::OffsetDateTime;
use tokeira_kernel::{HistoryEvent, LoadedRun, Transition, WorkflowState};
use tokeira_types::{NamespaceId, RunKey, ShardId, WorkflowId};

use crate::{
    AttributedHistoryEvent, CommitResult, LeaseRepository, ProjectionLog, RunRepository,
    TransitionAuditRecord, WriteFence, memory::projection_accumulator_tests::fresh_transition,
};

pub(crate) mod commits;
pub(crate) mod history;
pub(crate) mod projection;
pub(crate) mod recovery;

pub(crate) const HOMES: [ShardId; 2] = [ShardId(4094), ShardId(4095)];
pub(crate) const SHARD_COUNT: u32 = 4096;
pub(crate) const CASES: u32 = 100;
const SEED: u64 = 0x5354_4f52_4147_4500;

/// Identities for one trial, including rows an unsuccessful attempt must not create.
#[derive(Debug)]
pub(crate) struct FixtureScope {
    pub(crate) states: Vec<WorkflowState>,
}

impl FixtureScope {
    pub(crate) fn new(count: usize) -> Self {
        let prefix = uuid::Uuid::new_v4().as_u128() & !0xffff;
        let namespace = NamespaceId::new();
        let states = (0..count)
            .map(|index| {
                // Keep all images in partition 3 and preserve logical key order on
                // both backends even though each shrink attempt has fresh identities.
                let key = RunKey(uuid::Uuid::from_u128(prefix + (index as u128 + 1) * 16 + 3));
                let mut state = fresh_transition(key).next_state;
                state.namespace_id = namespace;
                state.workflow_id = WorkflowId(format!("storage-contract-{index}"));
                state.started_at = now();
                state.used_worker_deployment_versions = Some(Vec::new());
                if let Some(task) = &mut state.pending_workflow_task {
                    task.scheduled_at = now();
                }
                state
            })
            .collect();
        Self { states }
    }

    pub(crate) fn key(&self, index: usize) -> RunKey {
        self.states[index].run_key
    }

    pub(crate) fn contains(&self, key: RunKey) -> bool {
        self.states.iter().any(|state| state.run_key == key)
    }

    pub(crate) fn place(&mut self, index: usize, home: ShardId) {
        let state = &mut self.states[index];
        state.workflow_id = (0..)
            .map(|attempt| WorkflowId(format!("contract-{index}-{attempt}")))
            .find(|id| {
                tokeira_types::execution_home_bundle(
                    state.namespace_id.0.as_bytes(),
                    id.0.as_bytes(),
                    SHARD_COUNT,
                ) == home
            })
            .expect("a business identity maps to the fixture home");
    }
}

pub(crate) fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap()
}

pub(crate) fn start(state: &WorkflowState) -> Transition {
    let mut transition = fresh_transition(state.run_key);
    transition.next_state = state.clone();
    transition
}

/// Physical rows are compared only within one backend, with complete values.
pub(crate) type Rows = BTreeMap<String, Vec<String>>;

#[async_trait]
pub(crate) trait ContractBackend: Sync {
    fn repo(&self) -> &dyn RunRepository;
    fn log(&self) -> &dyn ProjectionLog;
    fn leases(&self) -> &dyn LeaseRepository;
    async fn rows(&self, scope: &FixtureScope) -> Result<Rows>;
    async fn cleanup(&self, scope: &FixtureScope) -> Result<()>;
    async fn remove_hot(&self, key: RunKey) -> Result<()>;
    async fn expire_lease(&self, home: ShardId) -> Result<()>;
    async fn clear_lease(&self, home: ShardId, original_owner: &str) -> Result<()>;
    async fn seed_image(&self, image: &crate::ProjectionRecord) -> Result<()>;
    fn seeded_image_time(&self) -> Option<OffsetDateTime>;
    async fn workflow_row(&self, key: RunKey) -> Result<Option<crate::WorkflowDispatchRow>>;
}

/// API observations retain full values; diagnostic audits need not share a codec.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Observation {
    states: Vec<LoadedRun>,
    histories: Vec<Vec<HistoryEvent>>,
    attributed: Vec<Vec<AttributedHistoryEvent>>,
    audits: Vec<Vec<TransitionAuditRecord>>,
    rows: Rows,
}

pub(crate) async fn observe(
    backend: &impl ContractBackend,
    scope: &FixtureScope,
) -> Result<Observation> {
    let mut result = Observation {
        states: Vec::new(),
        histories: Vec::new(),
        attributed: Vec::new(),
        audits: Vec::new(),
        rows: backend.rows(scope).await?,
    };
    for state in &scope.states {
        result
            .states
            .push(backend.repo().load_run(state.run_key).await?);
        result
            .histories
            .push(backend.repo().read_history_to_end(state.run_key, 0).await?);
        result.attributed.push(
            backend
                .repo()
                .read_attributed_history_to_end(state.run_key, 0)
                .await?,
        );
        result
            .audits
            .push(backend.repo().read_transition_audit(state.run_key).await?);
    }
    Ok(result)
}

pub(crate) async fn scoped(
    backend: &impl ContractBackend,
    scope: &FixtureScope,
    action: impl Future<Output = Result<()>>,
) -> Result<()> {
    let result = action.await;
    let cleanup = backend.cleanup(scope).await;
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error.context("fixture cleanup")),
        (Err(error), Err(cleanup)) => {
            Err(error.context(format!("cleanup also failed: {cleanup:#}")))
        }
    }
}

pub(crate) async fn commit(
    backend: &impl ContractBackend,
    transition: Transition,
    bundle: bool,
    home: ShardId,
    fence: WriteFence,
) -> Result<CommitResult> {
    let key = transition.next_state.run_key;
    if bundle {
        backend
            .repo()
            .commit_transition_for_bundle(key, home, transition, fence)
            .await
    } else {
        backend
            .repo()
            .commit_transition(key, transition, fence)
            .await
    }
}

pub(crate) async fn apply(
    backend: &impl ContractBackend,
    transition: Transition,
) -> Result<WorkflowState> {
    let expected = transition.next_state.clone();
    let result = backend
        .repo()
        .commit_transition(expected.run_key, transition, WriteFence::Unfenced)
        .await?;
    let CommitResult::Applied { new_state } = result else {
        anyhow::bail!("fixture rejected: {result:?}");
    };
    equal(&new_state, &expected, "returned post-state")?;
    equal(
        &backend.repo().load_run(expected.run_key).await?,
        &LoadedRun::Existing(expected.clone()),
        "reloaded post-state",
    )?;
    Ok(expected)
}

pub(crate) fn equal<T: Debug + PartialEq + ?Sized>(
    actual: &T,
    expected: &T,
    context: &str,
) -> Result<()> {
    ensure!(
        actual == expected,
        "{context}: expected {expected:?}, got {actual:?}"
    );
    Ok(())
}

#[derive(Debug, Serialize)]
struct Receipt<'a> {
    test: &'a str,
    backend: &'a str,
    seed: String,
    successful_cases: u32,
}

/// Each backend draws the same traces; fresh scopes make replay/shrinking isolated.
pub(crate) fn run_cases<S: Strategy>(
    name: &str,
    backend: &str,
    property: u64,
    strategy: S,
    case: impl Fn(S::Value) -> Result<()>,
) -> Result<()> {
    let seed = SEED + property;
    let successes = std::cell::Cell::new(0);
    let mut runner = TestRunner::new(ProptestConfig {
        cases: CASES,
        rng_seed: RngSeed::Fixed(seed),
        failure_persistence: None,
        ..ProptestConfig::default()
    });
    runner
        .run(&strategy, |input| {
            case(input).map_err(|error| TestCaseError::fail(format!("{error:#}")))?;
            successes.set(successes.get() + 1);
            Ok(())
        })
        .map_err(|error| anyhow::anyhow!("{name}, backend={backend}, seed={seed}: {error}"))?;
    ensure!(
        successes.get() >= CASES,
        "insufficient executed property cases"
    );
    let receipt = toml::to_string(&Receipt {
        test: name,
        backend,
        seed: seed.to_string(),
        successful_cases: successes.get(),
    })?;
    // The test harness captures stderr without weakening production stdout lints.
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(receipt, "storage contract completed")
    });
    if let Some(directory) = std::env::var_os("TOKEIRA_STORAGE_CONTRACT_OUTPUT") {
        let path = PathBuf::from(directory);
        std::fs::create_dir_all(&path)?;
        std::fs::write(path.join(format!("{name}.toml")), receipt)?;
    }
    Ok(())
}

pub(crate) fn required_url(value: Option<String>) -> Result<String> {
    value.filter(|value| !value.trim().is_empty()).context(
        "explicit live test requires TOKEIRA_DSQL_TEST_DATABASE_URL for an ephemeral database",
    )
}

#[test]
fn explicitly_selected_live_test_requires_configuration() {
    assert!(required_url(None).is_err());
    assert!(required_url(Some(String::new())).is_err());
}

// Feature: storage-contract-tests, Property 9: The oracle rejects the failures it claims to detect
#[test]
fn storage_contract_oracle_rejects_corruption() -> Result<()> {
    run_cases(
        "storage_contract_oracle_rejects_corruption",
        "pure",
        9,
        prop::collection::vec(any::<u16>(), 3..12),
        |values| {
            let authored: Vec<_> = values
                .iter()
                .enumerate()
                .map(|(index, value)| (index, *value))
                .collect();
            equal(&authored, &authored.clone(), "valid ordered observation")?;
            let mut truncated = authored.clone();
            truncated.pop();
            ensure!(
                equal(&truncated, &authored, "truncated page").is_err(),
                "oracle accepted truncation"
            );
            let mut reordered = authored.clone();
            reordered.swap(0, 1);
            ensure!(
                equal(&reordered, &authored, "reordered page").is_err(),
                "oracle accepted wrong ordering"
            );
            let skipped_resume = authored[..1].to_vec();
            ensure!(
                equal(&skipped_resume, &authored, "skipped continuation").is_err(),
                "oracle accepted skipped resumption"
            );
            let before = Rows::from([(
                "durable-values".into(),
                authored.iter().map(|row| format!("{row:?}")).collect(),
            )]);
            let mut partial = before.clone();
            partial.get_mut("durable-values").unwrap()[0] =
                "overwritten without changing count".into();
            equal(
                &partial["durable-values"].len(),
                &before["durable-values"].len(),
                "control row count",
            )?;
            ensure!(
                equal(&partial, &before, "partial write").is_err(),
                "oracle accepted partial overwrite"
            );
            Ok(())
        },
    )
}
