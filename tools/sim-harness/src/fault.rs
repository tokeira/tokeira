//! Reusable adversarial fault-injection framework.
//!
//! Faults are model-defined: a model registers named faults whose `schedule`
//! closure enqueues adversarial events through the [`SimCtx`] (and thus the
//! seeded RNG, keeping fault timing reproducible). The harness only owns the
//! enable/disable configuration and the per-fault injection counts — it knows
//! nothing about what any fault does.

use std::collections::BTreeMap;

use crate::event::SimCtx;

/// A named adversarial fault for model events `E`.
///
/// `schedule` is invoked (when enabled) to enqueue the fault's events. It takes
/// the same `SimCtx` a model handler does, so faults schedule through the seeded
/// RNG and simulated clock — never wall time.
pub struct Fault<E> {
    /// Stable fault name, used for config and injection counting.
    pub name: &'static str,
    /// Enqueues the fault's adversarial events relative to the current time.
    pub schedule: fn(&mut SimCtx<'_, E>),
}

/// Per-run enable/disable state for named faults. Absent names default to
/// enabled, so a model that registers faults gets them all unless a config
/// explicitly disables some.
#[derive(Clone, Debug, Default)]
pub struct FaultConfig {
    overrides: BTreeMap<&'static str, bool>,
}

impl FaultConfig {
    /// All faults enabled (the default).
    pub fn all_enabled() -> Self {
        Self::default()
    }

    /// Explicitly enable a fault by name.
    pub fn enable(&mut self, name: &'static str) {
        self.overrides.insert(name, true);
    }

    /// Explicitly disable a fault by name.
    pub fn disable(&mut self, name: &'static str) {
        self.overrides.insert(name, false);
    }

    /// Whether `name` is enabled (default true when unset).
    pub fn is_enabled(&self, name: &str) -> bool {
        self.overrides.get(name).copied().unwrap_or(true)
    }
}

/// Holds the model's registered faults plus the active config and injection
/// counts. Generic over the event type so fault definitions live in the model.
pub struct FaultInjector<E> {
    faults: Vec<Fault<E>>,
    config: FaultConfig,
    counts: BTreeMap<&'static str, u64>,
}

impl<E> FaultInjector<E> {
    /// Create an injector with the given config.
    pub fn new(config: FaultConfig) -> Self {
        Self {
            faults: Vec::new(),
            config,
            counts: BTreeMap::new(),
        }
    }

    /// Register a fault definition.
    pub fn register(&mut self, fault: Fault<E>) {
        self.faults.push(fault);
    }

    /// Fire `name` if it is registered and enabled, recording the injection.
    ///
    /// Returns `true` if the fault was scheduled. Models call this from their
    /// own fault-scheduling logic (typically chosen via `ctx.rng()`), so the
    /// harness imposes no policy on *when* faults fire — only that firing is
    /// counted and gated by config.
    pub fn fire(&mut self, name: &'static str, ctx: &mut SimCtx<'_, E>) -> bool {
        if !self.config.is_enabled(name) {
            return false;
        }
        let Some(fault) = self.faults.iter().find(|f| f.name == name) else {
            return false;
        };
        (fault.schedule)(ctx);
        *self.counts.entry(name).or_insert(0) += 1;
        true
    }

    /// The names of every registered fault.
    pub fn names(&self) -> Vec<&'static str> {
        self.faults.iter().map(|f| f.name).collect()
    }

    /// How many times `name` has been injected this run.
    pub fn injection_count(&self, name: &str) -> u64 {
        self.counts.get(name).copied().unwrap_or(0)
    }

    /// Iterate `(name, injection_count)` for reporting.
    pub fn counts(&self) -> impl Iterator<Item = (&'static str, u64)> + '_ {
        self.faults
            .iter()
            .map(move |f| (f.name, self.injection_count(f.name)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{event::EventQueue, rng::Rng};

    fn inject_one(ctx: &mut SimCtx<'_, u32>) {
        ctx.schedule(1, 7);
    }

    #[test]
    fn disabled_fault_does_not_fire() {
        let mut cfg = FaultConfig::all_enabled();
        cfg.disable("crash");
        let mut injector = FaultInjector::new(cfg);
        injector.register(Fault {
            name: "crash",
            schedule: inject_one,
        });

        let mut rng = Rng::new(1);
        let mut q: EventQueue<u32> = EventQueue::new();
        let mut ctx = SimCtx::new(0, &mut rng, &mut q);
        assert!(!injector.fire("crash", &mut ctx));
        assert_eq!(injector.injection_count("crash"), 0);
    }

    #[test]
    fn enabled_fault_fires_and_counts() {
        let mut injector = FaultInjector::new(FaultConfig::all_enabled());
        injector.register(Fault {
            name: "crash",
            schedule: inject_one,
        });

        let mut rng = Rng::new(1);
        let mut q: EventQueue<u32> = EventQueue::new();
        {
            let mut ctx = SimCtx::new(0, &mut rng, &mut q);
            assert!(injector.fire("crash", &mut ctx));
            assert!(injector.fire("crash", &mut ctx));
        }
        assert_eq!(injector.injection_count("crash"), 2);
        assert_eq!(q.len(), 2);
    }

    #[test]
    fn unregistered_fault_is_a_noop() {
        let mut injector: FaultInjector<u32> = FaultInjector::new(FaultConfig::all_enabled());
        let mut rng = Rng::new(1);
        let mut q: EventQueue<u32> = EventQueue::new();
        let mut ctx = SimCtx::new(0, &mut rng, &mut q);
        assert!(!injector.fire("nope", &mut ctx));
    }
}
