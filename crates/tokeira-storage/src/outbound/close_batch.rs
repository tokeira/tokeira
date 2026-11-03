//! Shared physical write planning for close obligations and operation cleanup.
//! A batch owns every unexpanded obligation; advancing its key and inserting a
//! page are atomic, so a lost reply cannot insert that page twice.

use std::collections::BTreeMap;

use anyhow::{Result, ensure};
use tokeira_kernel::OperationKey;
use tokeira_types::RunKey;

use crate::{
    IntentKind, IntentPayload, IntentRow, OperationRow, RecoveryError, value_parts,
    write_budget::{WriteBudget, WriteCost, pages},
};

/// Combine physical mutation costs without ignoring deleted rows.
pub(crate) fn add(left: WriteCost, right: WriteCost) -> WriteCost {
    WriteCost {
        rows: left.rows + right.rows,
        bytes: left.bytes + right.bytes,
    }
}

/// Check both dimensions; an oversized singleton page must still be refused.
pub(crate) fn fits(cost: WriteCost, budget: WriteBudget) -> bool {
    cost.rows <= budget.rows && cost.bytes <= budget.bytes
}

/// Price the SQL payload envelope, overflow rows and the separately stored target.
pub(crate) fn intent_cost(row: &IntentRow) -> Result<WriteCost> {
    let bytes = postcard::to_allocvec(&(1_u32, &row.payload))?.len();
    Ok(WriteCost {
        rows: value_parts::parts_of(bytes),
        bytes: bytes
            + row
                .target
                .as_ref()
                .map_or(0, |(_, workflow)| workflow.0.len()),
    })
}

/// Price all direct obligations together before reserving room for cleanup.
pub(crate) fn intents_cost(rows: &[IntentRow]) -> Result<WriteCost> {
    rows.iter().try_fold(WriteCost::default(), |cost, row| {
        Ok(add(cost, intent_cost(row)?))
    })
}

/// Preserve fitting closes, otherwise replace only per-child rows with one batch.
pub(crate) fn batch_close(rows: Vec<IntentRow>, budget: WriteBudget) -> Result<Vec<IntentRow>> {
    if fits(intents_cost(&rows)?, budget) {
        return Ok(rows);
    }
    let (children, mut direct): (Vec<_>, Vec<_>) = rows.into_iter().partition(|row| {
        matches!(
            row.key.kind,
            IntentKind::ParentClose | IntentKind::ChildRelease
        )
    });
    if let Some(first) = children.first() {
        let mut batch = first.clone();
        batch.key.kind = IntentKind::CloseBatch;
        batch.key.ordinal = 0;
        batch.target = None;
        batch.payload = IntentPayload::CloseBatch {
            obligations: children
                .into_iter()
                .map(|row| (row.key.ordinal, row.payload))
                .collect(),
        };
        // Child results and frozen successors keep their own payload limits.
        // Only the child-count-dependent batch must fit here; direct intents
        // still consume rows when operation_changes caps obsolete deletions.
        ensure!(
            fits(intent_cost(&batch)?, budget),
            "close batch exceeds the outbound write budget; workflow IDs beyond v1.31.0's \
             1,000-byte limit can prevent batch expansion from making progress"
        );
        direct.push(batch);
    }
    Ok(direct)
}

fn expand_row(batch: &IntentRow, ordinal: u32, payload: &IntentPayload) -> Result<IntentRow> {
    let (kind, target) = match payload {
        IntentPayload::ParentClose {
            namespace_id,
            workflow_id,
            ..
        } => (
            IntentKind::ParentClose,
            (*namespace_id, workflow_id.clone()),
        ),
        IntentPayload::ChildRelease { release } => (
            IntentKind::ChildRelease,
            (release.namespace_id, release.workflow_id.clone()),
        ),
        _ => {
            return Err(RecoveryError::InvalidIdentity {
                run: batch.key.source_run,
                detail: "close batch contains a nested batch or non-child obligation",
            }
            .into());
        }
    };
    Ok(IntentRow {
        key: crate::IntentKey {
            kind,
            ordinal,
            ..batch.key
        },
        source_home: batch.source_home,
        created_at: batch.created_at,
        target: Some(target),
        payload: payload.clone(),
    })
}

/// Plan one atomic replacement, rejecting malformed identities before any writes.
pub(crate) fn expansion(batch: &IntentRow, budget: WriteBudget) -> Result<Vec<IntentRow>> {
    let IntentPayload::CloseBatch { obligations } = &batch.payload else {
        return Err(RecoveryError::InvalidIdentity {
            run: batch.key.source_run,
            detail: "expansion requires a close batch",
        }
        .into());
    };
    ensure!(
        batch.key.kind == IntentKind::CloseBatch && batch.target.is_none(),
        RecoveryError::InvalidIdentity {
            run: batch.key.source_run,
            detail: "invalid close batch identity",
        }
    );
    // Validate the whole remainder before any writes, including a nested batch
    // outside this page. Original kind/ordinal identities must remain unique.
    let mut rows = obligations
        .iter()
        .map(|(ordinal, payload)| expand_row(batch, *ordinal, payload))
        .collect::<Result<Vec<_>>>()?;
    let keys: std::collections::BTreeSet<_> = rows.iter().map(|row| row.key).collect();
    ensure!(
        keys.len() == rows.len(),
        RecoveryError::InvalidIdentity {
            run: batch.key.source_run,
            detail: "duplicate close batch obligation",
        }
    );
    let old = intent_cost(batch)?;
    let deletion = WriteCost {
        rows: old.rows,
        bytes: 0,
    };
    if fits(add(intents_cost(&rows)?, deletion), budget) {
        return Ok(rows);
    }
    // Reserve the old batch's size for its smaller replacement. This also
    // counts deletion of every old part; deleted bytes cost nothing on DSQL.
    let reserved = add(old, deletion);
    let available = WriteBudget {
        rows: budget.rows.saturating_sub(reserved.rows),
        bytes: budget.bytes.saturating_sub(reserved.bytes),
    };
    let costs = rows.iter().map(intent_cost).collect::<Result<Vec<_>>>()?;
    let end = pages(&costs, available).first().map_or(0, |page| page.end);
    ensure!(
        end > 0 && fits(add(costs[0], reserved), budget),
        "close batch cannot make progress within the outbound write budget"
    );
    rows.truncate(end);
    let mut next = batch.clone();
    next.key.ordinal = next
        .key
        .ordinal
        .checked_add(1)
        .ok_or(RecoveryError::InvalidIdentity {
            run: batch.key.source_run,
            detail: "close batch ordinal overflow",
        })?;
    next.payload = IntentPayload::CloseBatch {
        obligations: obligations[end..].to_vec(),
    };
    rows.push(next);
    Ok(rows)
}

/// Compare persisted operation values at SQL timestamp precision in both stores.
pub(crate) fn same_index_entry(left: &OperationRow, right: &OperationRow) -> bool {
    // PostgreSQL stores whole microseconds since 2000. Comparing nanoseconds
    // would rewrite unchanged operations on every WFT and consume its row budget.
    let micros =
        |at: time::OffsetDateTime| (at.unix_timestamp_nanos() - 946_684_800_000_000_000) / 1_000;
    left.incarnation == right.incarnation
        && left.source_home == right.source_home
        && left.target == right.target
        && micros(left.due_at) == micros(right.due_at)
}

/// Keep every changed current cause, deferring only obsolete rows beyond the budget.
pub(crate) fn operation_changes(
    mut prior: BTreeMap<OperationKey, OperationRow>,
    desired: Vec<OperationRow>,
    used: WriteCost,
    budget: WriteBudget,
) -> (Vec<OperationRow>, Vec<OperationKey>) {
    let mut upserts = Vec::new();
    for row in desired {
        if !prior
            .remove(&row.incarnation.key)
            .is_some_and(|old| same_index_entry(&old, &row))
        {
            upserts.push(row);
        }
    }
    // Upserts establish current causes and cannot be deferred. Only obsolete
    // rows may survive; delivery re-derives operations under its source guard.
    let remaining = budget.rows.saturating_sub(used.rows + upserts.len());
    let costs = vec![WriteCost::DELETE; prior.len()];
    let end = if remaining == 0 {
        0
    } else {
        pages(
            &costs,
            WriteBudget {
                rows: remaining,
                bytes: budget.bytes,
            },
        )
        .first()
        .map_or(0, |page| page.end)
    };
    (upserts, prior.into_keys().take(end).collect())
}

/// Select one source deterministically so both stores defer the same obsolete rows.
pub(crate) fn prior_operations(
    rows: &BTreeMap<OperationKey, OperationRow>,
    run: RunKey,
) -> BTreeMap<OperationKey, OperationRow> {
    rows.iter()
        .filter(|(key, _)| key.source_run == run)
        .map(|(key, row)| (*key, row.clone()))
        .collect()
}
