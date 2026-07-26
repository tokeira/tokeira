//! Pure queue and fairness-key handout pacing.
//!
//! Task-queue configuration shapes delivery, not ingress or workflow
//! correctness. This module therefore owns only reconstructible monotonic
//! deadlines. Callers inspect eligibility while choosing a candidate, then
//! consume exactly once when that candidate is actually removed from a broker.

use std::{collections::HashMap, time::Duration};

use crate::{EffectivePriority, TaskQueueConfigEntry, TaskQueueConfigKey};

/// Result of inspecting one candidate without consuming capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DispatchEligibility {
    /// Both applicable limits permit immediate handout.
    Ready,
    /// Handout becomes eligible at this monotonic offset.
    At(Duration),
    /// At least one configured rate is zero.
    Blocked,
}

#[derive(Clone, Copy, Debug)]
struct RateDeadline {
    rate: f64,
    next: Duration,
}

/// Volatile handout deadlines for one broker.
#[derive(Debug, Default)]
pub struct DispatchRateLimits {
    queue: HashMap<TaskQueueConfigKey, RateDeadline>,
    fairness_key: HashMap<(TaskQueueConfigKey, String), RateDeadline>,
}

impl DispatchRateLimits {
    /// Inspect queue-wide and per-key eligibility without consuming either.
    pub fn inspect(
        &mut self,
        key: &TaskQueueConfigKey,
        effective: &EffectivePriority,
        config: Option<&TaskQueueConfigEntry>,
        now: Duration,
    ) -> DispatchEligibility {
        let queue_rate = config
            .and_then(|config| config.queue_rate_limit)
            .map(f64::from);
        let key_rate = config
            .and_then(|config| config.fairness_key_rate_limit_default)
            .map(|rate| f64::from(rate) * f64::from(effective.fairness_weight));

        let queue = inspect_rate(&mut self.queue, key.clone(), queue_rate, now);
        let fairness = inspect_rate(
            &mut self.fairness_key,
            (key.clone(), effective.fairness_key.clone()),
            key_rate,
            now,
        );
        combine(queue, fairness)
    }

    /// Consume capacity after a candidate inspected as ready is handed out.
    pub fn consume(
        &mut self,
        key: &TaskQueueConfigKey,
        effective: &EffectivePriority,
        config: Option<&TaskQueueConfigEntry>,
        now: Duration,
    ) {
        consume_rate(
            &mut self.queue,
            key,
            config
                .and_then(|config| config.queue_rate_limit)
                .map(f64::from),
            now,
        );
        consume_rate(
            &mut self.fairness_key,
            &(key.clone(), effective.fairness_key.clone()),
            config
                .and_then(|config| config.fairness_key_rate_limit_default)
                .map(|rate| f64::from(rate) * f64::from(effective.fairness_weight)),
            now,
        );
    }
}

fn inspect_rate<K>(
    state: &mut HashMap<K, RateDeadline>,
    key: K,
    rate: Option<f64>,
    now: Duration,
) -> DispatchEligibility
where
    K: Eq + std::hash::Hash,
{
    let Some(rate) = rate else {
        state.remove(&key);
        return DispatchEligibility::Ready;
    };
    if rate == 0.0 {
        return DispatchEligibility::Blocked;
    }
    let deadline = state.entry(key).or_insert(RateDeadline { rate, next: now });
    if deadline.rate.to_bits() != rate.to_bits() {
        // v1.31.0 swaps the live limiter when config changes. Resetting the
        // disposable deadline applies the new value immediately rather than
        // retaining debt from a superseded limiter.
        *deadline = RateDeadline { rate, next: now };
    }
    if deadline.next <= now {
        DispatchEligibility::Ready
    } else {
        DispatchEligibility::At(deadline.next)
    }
}

fn consume_rate<K>(state: &mut HashMap<K, RateDeadline>, key: &K, rate: Option<f64>, now: Duration)
where
    K: Clone + Eq + std::hash::Hash,
{
    let Some(rate) = rate.filter(|rate| *rate > 0.0) else {
        return;
    };
    let interval = Duration::from_secs_f64(1.0 / rate);
    let deadline = state
        .entry(key.clone())
        .or_insert(RateDeadline { rate, next: now });
    deadline.rate = rate;
    deadline.next = deadline.next.max(now).saturating_add(interval);
}

fn combine(left: DispatchEligibility, right: DispatchEligibility) -> DispatchEligibility {
    match (left, right) {
        (DispatchEligibility::Blocked, _) | (_, DispatchEligibility::Blocked) => {
            DispatchEligibility::Blocked
        }
        (DispatchEligibility::At(left), DispatchEligibility::At(right)) => {
            DispatchEligibility::At(left.max(right))
        }
        (DispatchEligibility::At(deadline), DispatchEligibility::Ready)
        | (DispatchEligibility::Ready, DispatchEligibility::At(deadline)) => {
            DispatchEligibility::At(deadline)
        }
        (DispatchEligibility::Ready, DispatchEligibility::Ready) => DispatchEligibility::Ready,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokeira_types::{NamespaceId, TaskQueueName};

    use super::*;
    use crate::{TaskQueueConfigKind, TaskQueueConfigMetadata};

    fn key() -> TaskQueueConfigKey {
        TaskQueueConfigKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("rate".to_string()),
            kind: TaskQueueConfigKind::Activity,
        }
    }

    fn config(queue_rate: Option<f32>, key_rate: Option<f32>) -> TaskQueueConfigEntry {
        TaskQueueConfigEntry {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("rate".to_string()),
            kind: TaskQueueConfigKind::Activity,
            queue_rate_limit: queue_rate,
            queue_rate_limit_metadata: Some(TaskQueueConfigMetadata {
                reason: String::new(),
                update_identity: String::new(),
                update_time: OffsetDateTime::UNIX_EPOCH,
            }),
            fairness_key_rate_limit_default: key_rate,
            fairness_key_rate_limit_metadata: None,
            fairness_weight_overrides: BTreeMap::new(),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: task-queue-priority-fairness, Property 14
        #[test]
        fn queue_and_fairness_key_rate_model(
            queue_rate in prop::option::of(0.0f32..100.0),
            key_rate in prop::option::of(0.0f32..100.0),
            weight in 0.001f32..1000.0,
        ) {
            let key = key();
            let config = config(queue_rate, key_rate);
            let priority = EffectivePriority {
                priority_key: 3,
                fairness_key: "tenant".to_string(),
                fairness_weight: weight,
            };
            let now = Duration::from_secs(10);
            let mut limits = DispatchRateLimits::default();
            let first = limits.inspect(&key, &priority, Some(&config), now);
            let blocked = queue_rate == Some(0.0) || key_rate == Some(0.0);
            prop_assert_eq!(first == DispatchEligibility::Blocked, blocked);
            if !blocked {
                prop_assert_eq!(first, DispatchEligibility::Ready);
                limits.consume(&key, &priority, Some(&config), now);
                let second = limits.inspect(&key, &priority, Some(&config), now);
                let queue_deadline = queue_rate
                    .map(|rate| now + Duration::from_secs_f64(1.0 / f64::from(rate)))
                    .unwrap_or(now);
                let key_deadline = key_rate
                    .map(|rate| {
                        now + Duration::from_secs_f64(
                            1.0 / (f64::from(rate) * f64::from(weight)),
                        )
                    })
                    .unwrap_or(now);
                let expected = queue_deadline.max(key_deadline);
                if queue_rate.is_none() && key_rate.is_none() {
                    prop_assert_eq!(second, DispatchEligibility::Ready);
                } else {
                    prop_assert_eq!(second, DispatchEligibility::At(expected));
                }
            }
        }
    }
}
