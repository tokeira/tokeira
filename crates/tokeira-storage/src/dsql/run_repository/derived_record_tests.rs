//! Properties of the derived-row record (`dsql-derived-row-record`), checked
//! on a model of one run's hot row and derived rows. Every writer in the model
//! decides through the real record functions and the real operation planner,
//! so the model differs from the repository only in its storage.

use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;
use time::OffsetDateTime;
use tokeira_kernel::{OperationIncarnation, OperationKey, OperationKind};
use tokeira_types::{RunKey, ShardId};
use uuid::Uuid;

use super::{
    derived_record::{DerivedKinds, may_hold, must_forget, read_record},
    outbound::plan_operations,
};
use crate::{
    OperationRow,
    write_budget::{WriteBudget, WriteCost},
};

const RUN: RunKey = RunKey(Uuid::from_u128(7));
const KINDS: [OperationKind; 3] = [
    OperationKind::ChildStart,
    OperationKind::ExternalSignal,
    OperationKind::NexusStart,
];

/// The hot row's sequence and record columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Hot {
    seq: i64,
    kinds: Option<i16>,
    stamp: Option<i64>,
}

impl Hot {
    fn record(self) -> Option<DerivedKinds> {
        read_record(self.seq, self.kinds, self.stamp)
    }
}

/// One run's hot row and the three kinds of derived row.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Run {
    hot: Option<Hot>,
    workflow: bool,
    activities: BTreeSet<u8>,
    operations: BTreeMap<OperationKey, OperationRow>,
}

/// The rows a state wants.
#[derive(Clone, Debug)]
struct Wanted {
    workflow: bool,
    activities: BTreeSet<u8>,
    operations: Vec<OperationRow>,
}

#[derive(Clone, Debug)]
enum Step {
    /// A commit of this release; `used` and `budget` rows can defer
    /// obsolete operation deletes.
    Commit {
        wanted: Wanted,
        used: usize,
        budget: usize,
    },
    /// A commit of an older release: it maintains every kind fully and moves
    /// the sequence without naming the record's columns.
    OlderCommit(Wanted),
    /// Workflow dispatch repair under a derivation that may differ from the
    /// committing one.
    WorkflowRepair { wants: bool },
    /// One activity repair page: upserts and deletes chosen freely.
    ActivityRepair {
        upserts: BTreeSet<u8>,
        deletes: BTreeSet<u8>,
    },
    /// Outbound reconciliation under a derivation that may differ.
    OutboundReconcile {
        desired: Vec<OperationRow>,
        budget: usize,
    },
    /// One stopped-cluster upgrade page of operation rows.
    UpgradePage(Vec<OperationRow>),
    /// A writer that only deletes.
    Delete {
        workflow: bool,
        activities: BTreeSet<u8>,
        operations: usize,
    },
    /// Staged preparation or reset copy pages, then the hot row a reset
    /// materializes or a publication creates. Only for an absent run.
    Materialize {
        activities: BTreeSet<u8>,
        workflow: bool,
    },
    /// Run deletion.
    DeleteRun,
}

fn operation(kind: usize, initiating_event: i64, attempt: u32) -> OperationRow {
    OperationRow {
        incarnation: OperationIncarnation {
            key: OperationKey {
                source_run: RUN,
                kind: KINDS[kind],
                initiating_event,
            },
            requested_event: initiating_event + 1,
            attempt,
        },
        source_home: ShardId(0),
        due_at: OffsetDateTime::UNIX_EPOCH,
        target: None,
    }
}

fn operations() -> impl Strategy<Value = Vec<OperationRow>> {
    prop::collection::btree_map((0..KINDS.len(), 0_i64..4), 0_u32..2, 0..5).prop_map(|rows| {
        rows.into_iter()
            .map(|((kind, event), attempt)| operation(kind, event, attempt))
            .collect()
    })
}

fn activities() -> impl Strategy<Value = BTreeSet<u8>> {
    prop::collection::btree_set(0_u8..6, 0..4)
}

fn wanted() -> impl Strategy<Value = Wanted> {
    (any::<bool>(), activities(), operations()).prop_map(|(workflow, activities, operations)| {
        Wanted {
            workflow,
            activities,
            operations,
        }
    })
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        4 => (wanted(), 0_usize..4, 0_usize..8)
            .prop_map(|(wanted, used, budget)| Step::Commit { wanted, used, budget }),
        1 => wanted().prop_map(Step::OlderCommit),
        1 => any::<bool>().prop_map(|wants| Step::WorkflowRepair { wants }),
        1 => (activities(), activities())
            .prop_map(|(upserts, deletes)| Step::ActivityRepair { upserts, deletes }),
        1 => (operations(), 0_usize..8)
            .prop_map(|(desired, budget)| Step::OutboundReconcile { desired, budget }),
        1 => operations().prop_map(Step::UpgradePage),
        1 => (any::<bool>(), activities(), 0_usize..4).prop_map(|(workflow, activities, operations)| {
            Step::Delete { workflow, activities, operations }
        }),
        1 => (activities(), any::<bool>())
            .prop_map(|(activities, workflow)| Step::Materialize { activities, workflow }),
        1 => Just(Step::DeleteRun),
    ]
}

fn budget(rows: usize) -> WriteBudget {
    WriteBudget {
        rows,
        bytes: WriteBudget::TRANSACTION.bytes,
    }
}

impl Run {
    fn forget(&mut self) {
        if let Some(hot) = self.hot.as_mut() {
            hot.stamp = None;
        }
    }

    fn next_seq(&self) -> i64 {
        self.hot.map_or(1, |hot| hot.seq + 1)
    }

    fn apply_operations(&mut self, upserts: Vec<OperationRow>, deletes: Vec<OperationKey>) {
        for row in upserts {
            self.operations.insert(row.incarnation.key, row);
        }
        for key in deletes {
            self.operations.remove(&key);
        }
    }

    /// `write_transition_mode` under `record`, which a real commit reads in
    /// its hot-row lock. Returns the record it writes.
    fn commit_with(
        &mut self,
        record: Option<DerivedKinds>,
        wanted: &Wanted,
        used: usize,
        rows: usize,
    ) -> DerivedKinds {
        let prior = if may_hold(record, DerivedKinds::OPERATIONS) {
            self.operations.clone()
        } else {
            BTreeMap::new()
        };
        let (upserts, deletes, remain) = plan_operations(
            prior,
            wanted.operations.clone(),
            WriteCost {
                rows: used,
                bytes: 0,
            },
            budget(rows),
        );
        self.apply_operations(upserts, deletes);
        if wanted.workflow {
            self.workflow = true;
        } else if may_hold(record, DerivedKinds::WORKFLOW_DISPATCH) {
            self.workflow = false;
        }
        if may_hold(record, DerivedKinds::ACTIVITY_DISPATCH) {
            self.activities = wanted.activities.clone();
        } else {
            self.activities.extend(wanted.activities.iter().copied());
        }
        let derived =
            DerivedKinds::after_commit(wanted.workflow, !wanted.activities.is_empty(), remain);
        let seq = self.next_seq();
        self.hot = Some(Hot {
            seq,
            kinds: Some(derived.to_column()),
            stamp: Some(seq),
        });
        derived
    }

    fn apply(&mut self, step: &Step) {
        match step {
            Step::Commit {
                wanted,
                used,
                budget,
            } => {
                let record = self.hot.and_then(Hot::record);
                self.commit_with(record, wanted, *used, *budget);
            }
            Step::OlderCommit(wanted) => {
                self.workflow = wanted.workflow;
                self.activities = wanted.activities.clone();
                self.operations = wanted
                    .operations
                    .iter()
                    .map(|row| (row.incarnation.key, row.clone()))
                    .collect();
                // Its insert names neither column; its update leaves both.
                let seq = self.next_seq();
                let (kinds, stamp) = self.hot.map_or((None, None), |hot| (hot.kinds, hot.stamp));
                self.hot = Some(Hot { seq, kinds, stamp });
            }
            Step::WorkflowRepair { wants } => {
                let Some(hot) = self.hot else {
                    self.workflow = false;
                    return;
                };
                if *wants {
                    self.workflow = true;
                    if must_forget(hot.record(), DerivedKinds::WORKFLOW_DISPATCH) {
                        self.forget();
                    }
                } else {
                    self.workflow = false;
                }
            }
            Step::ActivityRepair { upserts, deletes } => {
                let record = self.hot.and_then(Hot::record);
                for key in deletes {
                    self.activities.remove(key);
                }
                self.activities.extend(upserts.iter().copied());
                if !upserts.is_empty() && must_forget(record, DerivedKinds::ACTIVITY_DISPATCH) {
                    self.forget();
                }
            }
            Step::OutboundReconcile {
                desired,
                budget: rows,
            } => {
                if self.hot.is_none() {
                    return;
                }
                let (upserts, deletes, _) = plan_operations(
                    self.operations.clone(),
                    desired.clone(),
                    WriteCost::default(),
                    budget(*rows),
                );
                let upserted = !upserts.is_empty();
                self.apply_operations(upserts, deletes);
                if upserted {
                    self.forget();
                }
            }
            Step::UpgradePage(rows) => {
                self.apply_operations(rows.clone(), Vec::new());
                self.forget();
            }
            Step::Delete {
                workflow,
                activities,
                operations,
            } => {
                self.workflow &= !*workflow;
                for key in activities {
                    self.activities.remove(key);
                }
                let doomed = self
                    .operations
                    .keys()
                    .take(*operations)
                    .copied()
                    .collect::<Vec<_>>();
                for key in doomed {
                    self.operations.remove(&key);
                }
            }
            Step::Materialize {
                activities,
                workflow,
            } => {
                if self.hot.is_some() {
                    return;
                }
                self.activities.extend(activities.iter().copied());
                self.workflow = *workflow;
                self.hot = Some(Hot {
                    seq: 1,
                    kinds: None,
                    stamp: None,
                });
            }
            Step::DeleteRun => *self = Self::default(),
        }
    }

    /// A valid record that rules a kind out means the run holds none of it.
    fn sound(&self) -> bool {
        let Some(record) = self.hot.and_then(Hot::record) else {
            return true;
        };
        (record.contains(DerivedKinds::WORKFLOW_DISPATCH) || !self.workflow)
            && (record.contains(DerivedKinds::ACTIVITY_DISPATCH) || self.activities.is_empty())
            && (record.contains(DerivedKinds::OPERATIONS) || self.operations.is_empty())
    }

    fn rows(&self) -> (bool, &BTreeSet<u8>, &BTreeMap<OperationKey, OperationRow>) {
        (self.workflow, &self.activities, &self.operations)
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    // Feature: dsql-derived-row-record, Property 1: The record is sound under every writer
    #[test]
    fn the_record_is_sound_under_every_writer(steps in prop::collection::vec(step(), 1..24)) {
        let mut run = Run::default();
        for step in &steps {
            run.apply(step);
            prop_assert!(run.sound(), "unsound after {step:?}: {run:?}");
        }
    }

    // Feature: dsql-derived-row-record, Property 2: A commit with a record leaves the same rows as one without
    #[test]
    fn a_commit_with_a_record_leaves_the_same_rows_as_one_without(
        steps in prop::collection::vec(step(), 0..16),
        wanted in wanted(),
        used in 0_usize..4,
        rows in 0_usize..8,
    ) {
        let mut run = Run::default();
        for step in &steps {
            run.apply(step);
        }
        let record = run.hot.and_then(Hot::record);
        let mut with_record = run.clone();
        let mut without = run;
        let written = with_record.commit_with(record, &wanted, used, rows);
        let unknown = without.commit_with(None, &wanted, used, rows);
        prop_assert_eq!(with_record.rows(), without.rows());
        prop_assert_eq!(written, unknown);
    }

    // Feature: dsql-derived-row-record, Property 3: Every commit writes an exact, valid record
    #[test]
    fn every_commit_writes_an_exact_valid_record(
        steps in prop::collection::vec(step(), 0..16),
        wanted in wanted(),
        used in 0_usize..4,
        rows in 0_usize..8,
    ) {
        let mut run = Run::default();
        for step in &steps {
            run.apply(step);
        }
        run.apply(&Step::Commit { wanted, used, budget: rows });
        let hot = run.hot.expect("a commit writes the hot row");
        prop_assert_eq!(hot.stamp, Some(hot.seq));
        let record = hot.record().expect("a commit's record is valid");
        prop_assert_eq!(record.contains(DerivedKinds::WORKFLOW_DISPATCH), run.workflow);
        prop_assert_eq!(
            record.contains(DerivedKinds::ACTIVITY_DISPATCH),
            !run.activities.is_empty()
        );
        prop_assert_eq!(record.contains(DerivedKinds::OPERATIONS), !run.operations.is_empty());
    }

    // Feature: dsql-derived-row-record, Property 4: The record reads valid only under a matching stamp
    #[test]
    fn the_record_reads_valid_only_under_a_matching_stamp(
        seq in 0_i64..4,
        kinds in prop::option::of(any::<i16>()),
        stamp in prop::option::of(0_i64..4),
        held in (any::<bool>(), any::<bool>(), any::<bool>()),
        extra in any::<i16>(),
    ) {
        prop_assert_eq!(
            read_record(seq, kinds, stamp).is_some(),
            kinds.is_some() && stamp == Some(seq)
        );
        let record = DerivedKinds::after_commit(held.0, held.1, held.2);
        prop_assert_eq!(read_record(seq, Some(record.to_column()), Some(seq)), Some(record));
        // Bits this release doesn't name never change the named bits' meaning.
        let unnamed = extra & !7;
        prop_assert_eq!(
            read_record(seq, Some(record.to_column() | unnamed), Some(seq)),
            Some(record)
        );
    }
}

#[test]
fn may_hold_and_must_forget_follow_the_record() {
    let none = DerivedKinds::after_commit(false, false, false);
    let all = DerivedKinds::after_commit(true, true, true);
    for kind in [
        DerivedKinds::WORKFLOW_DISPATCH,
        DerivedKinds::ACTIVITY_DISPATCH,
        DerivedKinds::OPERATIONS,
    ] {
        assert!(may_hold(None, kind));
        assert!(may_hold(Some(all), kind));
        assert!(!may_hold(Some(none), kind));
        assert!(!must_forget(None, kind));
        assert!(must_forget(Some(none), kind));
    }
}

#[test]
fn a_deferred_obsolete_delete_keeps_operations_in_the_record() {
    let prior = (0..3)
        .map(|event| operation(0, event, 0))
        .map(|row| (row.incarnation.key, row))
        .collect::<BTreeMap<_, _>>();
    let (upserts, deletes, remain) =
        plan_operations(prior.clone(), Vec::new(), WriteCost::default(), budget(2));
    assert!(upserts.is_empty());
    assert_eq!(deletes.len(), 2);
    assert!(remain);
    let (_, deletes, remain) = plan_operations(prior, Vec::new(), WriteCost::default(), budget(3));
    assert_eq!(deletes.len(), 3);
    assert!(!remain);
}
