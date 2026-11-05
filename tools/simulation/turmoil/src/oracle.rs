//! What must hold in a run.
//!
//! Two kinds of check, with the TLA+ models in `spec/tla/` as the source of
//! the properties, so the code is checked against the same oracle as the
//! design:
//!
//! - **Every tick.** [`one_owner_per_epoch`] is host software: registered with
//!   [`topology::Run::observe`](crate::topology::Run::observe), it reads the
//!   shared store on every tick and fails the run at the first violation. The
//!   store is shared memory, not network, so no fault cuts the observer off.
//! - **After the run.** The other functions check the histories a workload
//!   recorded in the [`Trace`].

use std::{
    collections::BTreeMap,
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{anyhow, ensure};
use tokeira_proto::enums::EventType;
use tokeira_storage::{InMemoryStore, LeaseRepository as _};
use tokeira_types::ShardId;

use crate::{Event, Trace, observe};

/// How often the observer reads the store: turmoil's default tick, so it
/// sees every state the store passes through between two steps' worth of
/// host activity.
const OBSERVE_EVERY: Duration = Duration::from_millis(1);

/// Check, on every tick, that no bundle has two owners at one epoch and that
/// no bundle's epoch goes backwards (`OneOwnerPerEpoch`,
/// `spec/tla/30_bundle_lease`).
///
/// It records each change of a bundle's owner in the trace, naming owners by
/// the host their endpoint names rather than by incarnation id, and returns
/// an error, which fails the run, at the first violation.
pub async fn one_owner_per_epoch(
    store: InMemoryStore,
    trace: Arc<Mutex<Trace>>,
) -> Result<(), Box<dyn Error>> {
    // The owner first seen at each bundle and epoch. A relinquished lease
    // keeps its epoch with no owner, which is not a second owner, so only
    // owned rows are kept.
    let mut owners: BTreeMap<(ShardId, u64), String> = BTreeMap::new();
    let mut latest: BTreeMap<ShardId, (u64, Option<String>)> = BTreeMap::new();
    loop {
        let mut leases = store.list_bundle_leases().await?;
        // The store lists leases in hash-map order, which differs between
        // runs; sorted, the trace records same-tick changes in one order.
        leases.sort_by_key(|lease| lease.bundle_id.0);
        for lease in leases {
            let bundle = lease.bundle_id;
            let owner = lease.owner_node_id.clone();
            let epoch = lease.epoch.0;
            if let Some((latest_epoch, _)) = latest.get(&bundle)
                && epoch < *latest_epoch
            {
                return Err(format!(
                    "bundle {} went back from epoch {latest_epoch} to epoch {epoch}",
                    bundle.0
                )
                .into());
            }
            if let Some(owner) = &owner {
                let first = owners
                    .entry((bundle, epoch))
                    .or_insert_with(|| owner.clone());
                if first != owner {
                    return Err(format!(
                        "bundle {} has two owners at epoch {epoch}: {first} and {owner}",
                        bundle.0
                    )
                    .into());
                }
            }
            let state = (epoch, owner);
            if latest.get(&bundle) != Some(&state) {
                let holder = match (&state.1, &lease.node_endpoint) {
                    (None, _) => "nobody".to_owned(),
                    (Some(_), Some(endpoint)) => endpoint
                        .split(':')
                        .next()
                        .unwrap_or(endpoint.as_str())
                        .to_owned(),
                    (Some(_), None) => "an owner with no endpoint".to_owned(),
                };
                observe(
                    &trace,
                    format!("bundle {} at epoch {epoch} is owned by {holder}", bundle.0),
                );
                latest.insert(bundle, state);
            }
        }
        tokio::time::sleep(OBSERVE_EVERY).await;
    }
}

/// `kind` appears exactly once in `history`.
pub fn exactly_once(history: &[Event], kind: EventType) -> anyhow::Result<()> {
    let count = history.iter().filter(|event| event.kind == kind).count();
    ensure!(
        count == 1,
        "expected one {} event, found {count}",
        kind.as_str_name()
    );
    Ok(())
}

/// The first `later` event was stamped at least `gap` after the first
/// `earlier` event.
pub fn at_least_apart(
    history: &[Event],
    earlier: EventType,
    later: EventType,
    gap: Duration,
) -> anyhow::Result<()> {
    let stamp = |kind: EventType| {
        history
            .iter()
            .find(|event| event.kind == kind)
            .map(|event| event.stamped_ms)
            .ok_or_else(|| anyhow!("the history has no {} event", kind.as_str_name()))
    };
    let (from, to) = (stamp(earlier)?, stamp(later)?);
    let gap_ms = i64::try_from(gap.as_millis()).expect("a gap fits an i64");
    ensure!(
        to - from >= gap_ms,
        "{} came {} ms after {}, sooner than {gap_ms} ms",
        later.as_str_name(),
        to - from,
        earlier.as_str_name()
    );
    Ok(())
}
