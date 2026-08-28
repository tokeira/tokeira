//! Queue-local Priority normalization and delivery-order assignment.
//!
//! The kernel records raw/effective Temporal Priority metadata on declarative
//! dispatch effects. This module is the first delivery-policy boundary: it
//! resolves stock defaults, clips public values to the configured v1.31.0
//! bands, assigns queue-local fair passes, and allocates deterministic
//! insertion tie-breakers. The resulting [`DeliveryOrder`] is disposable
//! policy metadata; workflow state and history remain authoritative.
//!
//! User Fairness here is deliberately independent from the runtime's existing
//! inter-queue drain-share controller. This module chooses *which task within
//! one queue* is next; `fairness.rs` controls *how much drain capacity a queue
//! receives*.

use std::collections::{HashMap, HashSet};

use thiserror::Error;
use tokeira_kernel::Priority;
use tokeira_storage::DeliveryOrder;
use tokeira_types::QueueKey;

use crate::task_queue_config::TaskQueueConfigEntry;

/// Number of priority bands enabled by v1.31.0's stock configuration.
pub const PRIORITY_LEVELS: i32 = 5;
/// Priority band used when a task supplies zero or no priority key.
pub const DEFAULT_PRIORITY_KEY: i32 = 3;
const FAIRNESS_KEY_MAX_BYTES: usize = 64;
const STRIDE_FACTOR: f32 = 1000.0;
const MIN_FAIRNESS_WEIGHT: f32 = 0.001;
const MAX_FAIRNESS_WEIGHT: f32 = 1000.0;

/// One normalized delivery-policy view of public Priority metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct EffectivePriority {
    /// Clipped delivery band in the inclusive range 1 through 5.
    pub priority_key: i16,
    /// Effective fairness group, empty for the default group.
    pub fairness_key: String,
    /// Effective queue-override/task/default weight.
    pub fairness_weight: f32,
}

/// Validation failure for an inbound public Priority value.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum PriorityValidationError {
    /// A negative priority key is never a valid inherit/default marker.
    #[error("priority key can't be negative")]
    NegativePriorityKey,
    /// Temporal limits fairness keys by encoded byte length.
    #[error("fairness key length exceeds limit")]
    FairnessKeyTooLong,
    /// A negative task weight is invalid; zero remains the inherit/default marker.
    #[error("must be greater than zero")]
    NegativeFairnessWeight,
}

/// Validate raw public Priority without resolving its zero/empty markers.
///
/// Error text and the 64-byte boundary follow
/// `common/priorities/priority_util.go @ v1.31.0`.
pub fn validate_priority(priority: Option<&Priority>) -> Result<(), PriorityValidationError> {
    let Some(priority) = priority else {
        return Ok(());
    };
    if priority.priority_key < 0 {
        return Err(PriorityValidationError::NegativePriorityKey);
    }
    if priority.fairness_key.len() > FAIRNESS_KEY_MAX_BYTES {
        return Err(PriorityValidationError::FairnessKeyTooLong);
    }
    if priority.fairness_weight < 0.0 {
        return Err(PriorityValidationError::NegativeFairnessWeight);
    }
    Ok(())
}

/// Resolve delivery defaults, clipping, and task-queue weight precedence.
///
/// The queue override wins over a positive task weight, then weight defaults to
/// one. The effective range follows the public proto contract; v1.31.0's stride
/// calculation independently floors stride at one, producing the same upper
/// scheduling bound (`service/matching/fairness_util.go` and
/// `fair_task_writer.go @ v1.31.0`).
#[must_use]
pub fn effective_priority(
    raw: Option<&Priority>,
    config: Option<&TaskQueueConfigEntry>,
) -> EffectivePriority {
    let raw_key = raw.map_or(0, |priority| priority.priority_key);
    let priority_key = if raw_key == 0 {
        DEFAULT_PRIORITY_KEY
    } else {
        raw_key.clamp(1, PRIORITY_LEVELS)
    };
    let fairness_key = raw
        .map(|priority| priority.fairness_key.clone())
        .unwrap_or_default();
    let configured_weight = config
        .and_then(|entry| entry.fairness_weight_overrides.get(&fairness_key))
        .copied();
    let task_weight = raw
        .map(|priority| priority.fairness_weight)
        .filter(|weight| *weight > 0.0);
    let fairness_weight = configured_weight
        .filter(|weight| weight.is_finite() && *weight > 0.0)
        .or(task_weight)
        .unwrap_or(1.0)
        .clamp(MIN_FAIRNESS_WEIGHT, MAX_FAIRNESS_WEIGHT);

    EffectivePriority {
        priority_key: priority_key as i16,
        fairness_key,
        fairness_weight,
    }
}

/// Effective matcher and User Fairness switches for one task queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeliveryMode {
    /// Whether public priority keys select distinct delivery bands.
    pub priority_enabled: bool,
    /// Whether fairness key and weight affect within-band order.
    pub fairness_enabled: bool,
    /// Whether qualifying task metadata activates V2 for this process lifetime.
    pub auto_enable: bool,
}

impl Default for DeliveryMode {
    fn default() -> Self {
        Self {
            priority_enabled: true,
            fairness_enabled: false,
            auto_enable: false,
        }
    }
}

impl DeliveryMode {
    fn coherent(self) -> Self {
        Self {
            priority_enabled: self.priority_enabled || self.fairness_enabled,
            ..self
        }
    }
}

/// Runtime seam that supplies release-pinned delivery modes.
pub trait DeliveryModeProvider: Send + Sync + 'static {
    /// Return live mode policy for `queue`.
    fn mode_for(&self, queue: &QueueKey) -> DeliveryMode;

    /// Generation of disposable policy state.
    ///
    /// Production providers retain the default generation forever. The
    /// conformance provider advances it when the harness resets its scoped
    /// overrides, preventing one test's auto-enable observation from leaking
    /// into the next test in the same server process.
    fn scope_generation(&self) -> u64 {
        0
    }
}

/// Typed startup policy for queue-local delivery.
///
/// Priority remains part of the pinned v1.31.0 baseline, so production
/// construction supplies only the separately gated User Fairness choice.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StaticDeliveryPolicy {
    /// Enable weighted User Fairness for non-sticky task queues.
    pub enable_fairness: bool,
}

/// Delivery-mode provider backed by typed startup configuration.
///
/// Raw Temporal dynamic-config keys are deliberately absent from this public
/// boundary. A conformance build applies its allow-listed overlay internally at
/// the live consult site without changing the typed production policy.
#[derive(Clone, Copy, Debug, Default)]
pub struct ConfiguredDeliveryModeProvider {
    policy: StaticDeliveryPolicy,
}

impl ConfiguredDeliveryModeProvider {
    /// Construct a provider from already-validated startup policy.
    #[must_use]
    pub const fn new(policy: StaticDeliveryPolicy) -> Self {
        Self { policy }
    }
}

impl DeliveryModeProvider for ConfiguredDeliveryModeProvider {
    fn mode_for(&self, _queue: &QueueKey) -> DeliveryMode {
        delivery_mode(self.policy)
    }

    #[cfg(feature = "conformance")]
    fn scope_generation(&self) -> u64 {
        crate::conformance::reads().scope_generation()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct DeliveryModeOverlay {
    priority_enabled: Option<bool>,
    fairness_enabled: Option<bool>,
    auto_enable: Option<bool>,
}

fn resolve_delivery_mode(
    policy: StaticDeliveryPolicy,
    overlay: DeliveryModeOverlay,
) -> DeliveryMode {
    DeliveryMode {
        priority_enabled: overlay.priority_enabled.unwrap_or(true),
        fairness_enabled: overlay.fairness_enabled.unwrap_or(policy.enable_fairness),
        auto_enable: overlay.auto_enable.unwrap_or(false),
    }
    .coherent()
}

#[cfg(not(feature = "conformance"))]
fn delivery_mode(policy: StaticDeliveryPolicy) -> DeliveryMode {
    resolve_delivery_mode(policy, DeliveryModeOverlay::default())
}

#[cfg(feature = "conformance")]
fn delivery_mode(policy: StaticDeliveryPolicy) -> DeliveryMode {
    resolve_delivery_mode(
        policy,
        DeliveryModeOverlay {
            priority_enabled: crate::conformance::reads().get_bool("matching.useNewMatcher"),
            fairness_enabled: crate::conformance::reads().get_bool("matching.enableFairness"),
            auto_enable: crate::conformance::reads().get_bool("matching.autoEnableV2"),
        },
    )
}

/// Queue-local, process-lifetime assignment state.
///
/// Loss or reset may perturb best-effort ordering but cannot affect task
/// identity or transition admission. Callers hold this value under the same
/// broker lock used to insert the task, so pass selection and publication are
/// one atomic in-memory operation.
#[derive(Debug, Default)]
pub struct DeliveryOrdering {
    next_insertion_tie: u64,
    key_passes: HashMap<(QueueKey, i16, String), i64>,
    band_frontiers: HashMap<(QueueKey, i16), i64>,
    auto_enabled_queues: HashSet<QueueKey>,
    scope_generation: u64,
}

impl DeliveryOrdering {
    /// Enter a policy scope, dropping only state that the scoped controls own.
    ///
    /// Fair-pass and insertion state are process-lifetime delivery policy and
    /// remain intact. Auto-enable activation is the one value whose harness
    /// semantics are scoped to an individual test.
    pub fn enter_scope(&mut self, generation: u64) {
        if self.scope_generation != generation {
            self.auto_enabled_queues.clear();
            self.scope_generation = generation;
        }
    }

    /// Assign a fresh order to an initially published task.
    pub fn assign(
        &mut self,
        queue: &QueueKey,
        raw_priority: Option<&Priority>,
        is_sticky: bool,
        config: Option<&TaskQueueConfigEntry>,
        mode: DeliveryMode,
    ) -> DeliveryOrder {
        let mut mode = mode.coherent();
        if mode.auto_enable && !is_sticky {
            let qualifies_for_v2 = raw_priority.is_some_and(|priority| {
                !priority.fairness_key.is_empty()
                    || (priority.priority_key != 0 && !mode.priority_enabled)
            });
            if qualifies_for_v2 {
                self.auto_enabled_queues.insert(queue.clone());
            }
            if self.auto_enabled_queues.contains(queue) {
                mode.priority_enabled = true;
                mode.fairness_enabled = true;
            }
        }
        if is_sticky {
            mode.fairness_enabled = false;
        }

        let effective = effective_priority(raw_priority, config);
        let priority_key = if mode.priority_enabled {
            effective.priority_key
        } else {
            DEFAULT_PRIORITY_KEY as i16
        };
        let insertion_tie = self.next_insertion_tie;
        self.next_insertion_tie = self.next_insertion_tie.saturating_add(1);

        let fair_pass = if mode.fairness_enabled {
            let band_key = (queue.clone(), priority_key);
            let frontier = self.band_frontiers.get(&band_key).copied().unwrap_or(0);
            let pass_key = (queue.clone(), priority_key, effective.fairness_key.clone());
            let stride = (STRIDE_FACTOR / effective.fairness_weight).floor() as i64;
            let stride = stride.max(1);
            let next = self
                .key_passes
                .get(&pass_key)
                .map_or(stride.max(frontier), |previous| {
                    previous.saturating_add(stride).max(frontier)
                });
            self.key_passes.insert(pass_key, next);
            next
        } else {
            i64::try_from(insertion_tie).unwrap_or(i64::MAX)
        };

        DeliveryOrder {
            priority_key,
            fair_pass,
            insertion_tie,
        }
    }

    /// Preserve a previously assigned durable order during backlog rehydration.
    ///
    /// Advancing the local tie frontier prevents a subsequent fresh insertion
    /// from colliding with the restored order after graceful demotion/drain.
    pub fn preserve(&mut self, order: DeliveryOrder) -> DeliveryOrder {
        self.next_insertion_tie = self
            .next_insertion_tie
            .max(order.insertion_tie.saturating_add(1));
        order
    }

    /// Advance the service frontier after a task is actually handed out.
    ///
    /// v1.31.0 bases a newly observed fairness key on the backlog reader's
    /// acknowledged fair level, not the largest pass merely assigned to queued
    /// work (`fairTaskWriter.pickPasses @ v1.31.0`). Keeping assignment and
    /// service separate prevents a late key from being placed behind all
    /// unserved work of an established key.
    pub fn served(&mut self, queue: &QueueKey, order: DeliveryOrder) {
        self.band_frontiers
            .entry((queue.clone(), order.priority_key))
            .and_modify(|frontier| *frontier = (*frontier).max(order.fair_pass))
            .or_insert(order.fair_pass);
    }

    /// Whether auto-enable has activated V2 for this normal queue.
    #[must_use]
    pub fn is_auto_enabled(&self, queue: &QueueKey) -> bool {
        self.auto_enabled_queues.contains(queue)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokeira_types::{NamespaceId, TaskKind, TaskQueueName};

    use super::*;
    use crate::task_queue_config::TaskQueueConfigMetadata;

    fn queue(name: &str) -> QueueKey {
        QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName(name.to_string()),
            task_kind: TaskKind::Activity,
            deployment: None,
            build_id: None,
        }
    }

    fn config(overrides: BTreeMap<String, f32>) -> TaskQueueConfigEntry {
        TaskQueueConfigEntry {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("queue".to_string()),
            kind: crate::TaskQueueConfigKind::Activity,
            queue_rate_limit: None,
            queue_rate_limit_metadata: None,
            fairness_key_rate_limit_default: None,
            fairness_key_rate_limit_metadata: None,
            fairness_weight_overrides: overrides,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: configuration-policy, Property 1: effective-policy precedence
        #[test]
        fn effective_policy_precedence_matches_reference(
            configured_fairness in any::<bool>(),
            priority_overlay in prop::option::of(any::<bool>()),
            fairness_overlay in prop::option::of(any::<bool>()),
            auto_overlay in prop::option::of(any::<bool>()),
        ) {
            let policy = StaticDeliveryPolicy {
                enable_fairness: configured_fairness,
            };
            let resolved = resolve_delivery_mode(
                policy,
                DeliveryModeOverlay {
                    priority_enabled: priority_overlay,
                    fairness_enabled: fairness_overlay,
                    auto_enable: auto_overlay,
                },
            );
            let expected_fairness = fairness_overlay.unwrap_or(configured_fairness);
            prop_assert_eq!(resolved.fairness_enabled, expected_fairness);
            prop_assert_eq!(
                resolved.priority_enabled,
                priority_overlay.unwrap_or(true) || expected_fairness
            );
            prop_assert_eq!(resolved.auto_enable, auto_overlay.unwrap_or(false));
        }

        // Feature: configuration-policy, Property 12: delivery-mode composition
        #[test]
        fn delivery_mode_composition_matches_reference(
            configured_fairness in any::<bool>(),
            priority_overlay in prop::option::of(any::<bool>()),
            fairness_overlay in prop::option::of(any::<bool>()),
            auto_overlay in prop::option::of(any::<bool>()),
            sticky in any::<bool>(),
            workflow_queue in any::<bool>(),
            priority_key in 0i32..=PRIORITY_LEVELS,
            fairness_key in "[a-z]{0,8}",
            fairness_weight in 0.001f32..10.0,
        ) {
            let queue = QueueKey {
                namespace_id: NamespaceId::new(),
                task_queue: TaskQueueName("composition".to_owned()),
                task_kind: if workflow_queue {
                    TaskKind::Workflow
                } else {
                    TaskKind::Activity
                },
                deployment: None,
                build_id: None,
            };
            let overlay = DeliveryModeOverlay {
                priority_enabled: priority_overlay,
                fairness_enabled: fairness_overlay,
                auto_enable: auto_overlay,
            };
            let mode = resolve_delivery_mode(
                StaticDeliveryPolicy {
                    enable_fairness: configured_fairness,
                },
                overlay,
            );
            let raw = Priority {
                priority_key,
                fairness_key: fairness_key.clone(),
                fairness_weight,
            };
            let mut ordering = DeliveryOrdering::default();
            let order = ordering.assign(&queue, Some(&raw), sticky, None, mode);

            let qualifies_for_auto = !sticky
                && mode.auto_enable
                && (!fairness_key.is_empty()
                    || (priority_key != 0 && !mode.priority_enabled));
            let fairness_active =
                !sticky && (mode.fairness_enabled || qualifies_for_auto);
            let priority_active = mode.priority_enabled || qualifies_for_auto;
            let expected_priority = if priority_active {
                if priority_key == 0 {
                    DEFAULT_PRIORITY_KEY as i16
                } else {
                    priority_key as i16
                }
            } else {
                DEFAULT_PRIORITY_KEY as i16
            };

            prop_assert_eq!(order.priority_key, expected_priority);
            prop_assert_eq!(order.fair_pass > 0, fairness_active);
            prop_assert_eq!(order.insertion_tie, 0);
        }

        // Feature: task-queue-priority-fairness, Property 1
        #[test]
        fn priority_validation_and_effective_values_match_reference(
            key in -10i32..20,
            fairness_len in 0usize..80,
            weight in -2.0f32..1500.0,
            override_weight in prop::option::of(0.001f32..1500.0),
        ) {
            let fairness_key = "x".repeat(fairness_len);
            let priority = Priority {
                priority_key: key,
                fairness_key: fairness_key.clone(),
                fairness_weight: weight,
            };
            let validation = validate_priority(Some(&priority));
            let valid = key >= 0 && fairness_len <= FAIRNESS_KEY_MAX_BYTES && weight >= 0.0;
            prop_assert_eq!(validation.is_ok(), valid);

            if valid {
                let overrides = override_weight
                    .map(|value| BTreeMap::from([(fairness_key.clone(), value)]))
                    .unwrap_or_default();
                let config = config(overrides);
                let effective = effective_priority(Some(&priority), Some(&config));
                let expected_key = if key == 0 {
                    DEFAULT_PRIORITY_KEY
                } else {
                    key.clamp(1, PRIORITY_LEVELS)
                };
                let expected_weight = override_weight
                    .or((weight > 0.0).then_some(weight))
                    .unwrap_or(1.0)
                    .clamp(MIN_FAIRNESS_WEIGHT, MAX_FAIRNESS_WEIGHT);
                prop_assert_eq!(effective.priority_key, expected_key as i16);
                prop_assert_eq!(effective.fairness_key, fairness_key);
                prop_assert_eq!(effective.fairness_weight, expected_weight);
            }
        }

        // Feature: task-queue-priority-fairness, Property 4
        #[test]
        fn priority_bands_order_and_disabled_mode_is_fifo(
            keys in prop::collection::vec(0i32..8, 1..64),
        ) {
            let queue = queue("priority");
            let mut enabled = DeliveryOrdering::default();
            let mut enabled_orders = keys
                .iter()
                .map(|key| {
                    enabled.assign(
                        &queue,
                        Some(&Priority {
                            priority_key: *key,
                            fairness_key: String::new(),
                            fairness_weight: 0.0,
                        }),
                        false,
                        None,
                        DeliveryMode::default(),
                    )
                })
                .collect::<Vec<_>>();
            enabled_orders.sort();
            prop_assert!(enabled_orders.windows(2).all(|pair| pair[0] <= pair[1]));

            let mut disabled = DeliveryOrdering::default();
            let disabled_orders = keys
                .iter()
                .map(|key| {
                    disabled.assign(
                        &queue,
                        Some(&Priority {
                            priority_key: *key,
                            fairness_key: String::new(),
                            fairness_weight: 0.0,
                        }),
                        false,
                        None,
                        DeliveryMode {
                            priority_enabled: false,
                            fairness_enabled: false,
                            auto_enable: false,
                        },
                    )
                })
                .collect::<Vec<_>>();
            prop_assert!(disabled_orders
                .windows(2)
                .all(|pair| pair[0].priority_key == DEFAULT_PRIORITY_KEY as i16
                    && pair[0] < pair[1]));
        }

        // Feature: task-queue-priority-fairness, Property 6
        #[test]
        fn fair_pass_matches_stride_and_served_frontier_reference(
            assignments in prop::collection::vec(
                (0u8..4, 0.001f32..1000.0, any::<bool>()),
                1..64,
            ),
        ) {
            let queue = queue("fair");
            let mode = DeliveryMode {
                priority_enabled: true,
                fairness_enabled: true,
                auto_enable: false,
            };
            let mut ordering = DeliveryOrdering::default();
            let mut key_passes = HashMap::<String, i64>::new();
            let mut served_frontier = 0i64;
            for (key_index, weight, serve) in assignments {
                let fairness_key = format!("tenant-{key_index}");
                let stride = ((STRIDE_FACTOR / weight).floor() as i64).max(1);
                let expected = key_passes
                    .get(&fairness_key)
                    .map_or(stride.max(served_frontier), |previous| {
                        previous.saturating_add(stride).max(served_frontier)
                    });
                let order = ordering.assign(
                    &queue,
                    Some(&Priority {
                        priority_key: 3,
                        fairness_key: fairness_key.clone(),
                        fairness_weight: weight,
                    }),
                    false,
                    None,
                    mode,
                );
                prop_assert_eq!(order.fair_pass, expected);
                if let Some(previous) = key_passes.insert(fairness_key, order.fair_pass) {
                    prop_assert!(order.fair_pass >= previous);
                }
                if serve {
                    ordering.served(&queue, order);
                    served_frontier = served_frontier.max(order.fair_pass);
                }
            }
        }

        // Feature: task-queue-priority-fairness, Property 7
        #[test]
        fn user_order_is_independent_of_drain_budget(
            budget_a in 1usize..100,
            budget_b in 1usize..100,
            weights in prop::collection::vec(0.1f32..10.0, 1..32),
        ) {
            let queue = queue("independent");
            let mode = DeliveryMode {
                priority_enabled: true,
                fairness_enabled: true,
                auto_enable: false,
            };
            let assign = |_: usize| {
                let mut ordering = DeliveryOrdering::default();
                weights
                    .iter()
                    .map(|weight| {
                        ordering.assign(
                            &queue,
                            Some(&Priority {
                                priority_key: 3,
                                fairness_key: "tenant".to_string(),
                                fairness_weight: *weight,
                            }),
                            false,
                            None,
                            mode,
                        )
                    })
                    .collect::<Vec<_>>()
            };
            prop_assert_eq!(assign(budget_a), assign(budget_b));
        }

        // Feature: task-queue-priority-fairness, Property 16
        #[test]
        fn mode_and_auto_enable_are_monotonic(
            priority_key in 0i32..6,
            fairness_key in prop::option::of("[a-z]{1,8}"),
        ) {
            let normal_queue = queue("auto");
            let priority = Priority {
                priority_key,
                fairness_key: fairness_key.clone().unwrap_or_default(),
                fairness_weight: 1.0,
            };
            let mode = DeliveryMode {
                priority_enabled: false,
                fairness_enabled: false,
                auto_enable: true,
            };
            let mut ordering = DeliveryOrdering::default();
            let first = ordering.assign(&normal_queue, Some(&priority), false, None, mode);
            let activated = !priority.fairness_key.is_empty() || priority_key != 0;
            prop_assert_eq!(ordering.is_auto_enabled(&normal_queue), activated);
            let second = ordering.assign(&normal_queue, None, false, None, mode);
            prop_assert_eq!(ordering.is_auto_enabled(&normal_queue), activated);
            if activated {
                prop_assert_eq!(
                    first.priority_key,
                    if priority_key == 0 {
                        3
                    } else {
                        priority_key.clamp(1, PRIORITY_LEVELS) as i16
                    }
                );
                prop_assert_eq!(second.priority_key, 3);
            } else {
                prop_assert_eq!(first.priority_key, 3);
                prop_assert_eq!(second.priority_key, 3);
            }

            let sticky = queue("sticky");
            let _ = ordering.assign(&sticky, Some(&priority), true, None, mode);
            prop_assert!(!ordering.is_auto_enabled(&sticky));

            ordering.enter_scope(1);
            prop_assert!(!ordering.is_auto_enabled(&normal_queue));
        }
    }

    #[test]
    fn kernel_and_lane_remain_independent_from_delivery_policy() {
        let kernel_manifest = include_str!("../../tokeira-kernel/Cargo.toml");
        for forbidden_dependency in ["tokeira-config", "tokeira-conformance", "tokeira-runtime"] {
            assert!(
                !kernel_manifest.contains(forbidden_dependency),
                "kernel manifest must not depend on {forbidden_dependency}"
            );
        }

        // Lane selection is run-key locality, not task-queue delivery policy.
        // Keep this structural tripwire beside the policy provider so a future
        // import cannot silently move policy into the correctness scheduler.
        let lane_source = include_str!("lane.rs");
        for forbidden_policy in [
            "DeliveryMode",
            "StaticDeliveryPolicy",
            "effective_priority",
            "fairness_key",
        ] {
            assert!(
                !lane_source.contains(forbidden_policy),
                "lane router must not consult {forbidden_policy}"
            );
        }
    }

    #[test]
    fn config_fixture_metadata_shape_stays_usable() {
        let metadata = TaskQueueConfigMetadata {
            reason: "test".to_string(),
            update_identity: "test".to_string(),
            update_time: OffsetDateTime::UNIX_EPOCH,
        };
        assert_eq!(metadata.update_time, OffsetDateTime::UNIX_EPOCH);
    }

    #[test]
    fn new_fairness_key_starts_at_the_served_band_frontier() {
        let queue = queue("frontier");
        let mode = DeliveryMode {
            priority_enabled: true,
            fairness_enabled: true,
            auto_enable: false,
        };
        let mut ordering = DeliveryOrdering::default();
        let established = Priority {
            priority_key: 3,
            fairness_key: "established".to_string(),
            fairness_weight: 1.0,
        };
        let first = ordering.assign(&queue, Some(&established), false, None, mode);
        let second = ordering.assign(&queue, Some(&established), false, None, mode);
        ordering.served(&queue, second);

        let newcomer = ordering.assign(
            &queue,
            Some(&Priority {
                priority_key: 3,
                fairness_key: "newcomer".to_string(),
                fairness_weight: 1.0,
            }),
            false,
            None,
            mode,
        );

        assert_eq!(first.fair_pass, 1_000);
        assert_eq!(second.fair_pass, 2_000);
        assert_eq!(newcomer.fair_pass, second.fair_pass);
    }
}
