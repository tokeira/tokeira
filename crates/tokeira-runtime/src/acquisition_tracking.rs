//! Acquisition-scoped volatile indexes shared by recovery and timer scanners.
//!
//! Ownership is locked before entries so replacement cannot interleave validation
//! with installation. Every authoritative tracking mutation advances a revision: an awaited scan may
//! retire only the exact value it submitted, even when its logical key is reused.
//! Scanner-only cache refreshes preserve that revision and never replace anchors.

use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, Mutex, OnceLock, RwLock},
};

use crate::deadline_wake::DeadlineWake;
use time::OffsetDateTime;
use tokeira_types::ShardId;

use crate::shard::{ShardAcquisition, ShardOwner};

#[derive(Clone, Debug)]
pub(crate) struct Tracked<V> {
    pub(crate) value: V,
    home: ShardId,
    revision: u64,
    acquisition: Option<ShardAcquisition>,
    // Unattempted work survives a bounded pass; failed attempts wait for the next full pass.
    pub(crate) deadline_pending: bool,
    pub(crate) refresh_pending: bool,
}

impl<V> Tracked<V> {
    /// Capture the original generation for a related volatile index; never
    /// adopt the owner observed after an awaited scanner operation.
    pub(crate) fn acquisition(&self) -> Option<ShardAcquisition> {
        self.acquisition.clone()
    }
}

#[derive(Debug)]
struct Entries<K, V> {
    values: HashMap<K, Tracked<V>>,
    revision: u64,
}

#[derive(Clone, Debug)]
struct DeadlineIndex<V> {
    wake: DeadlineWake,
    due: fn(&V) -> Option<OffsetDateTime>,
}

#[derive(Clone, Debug)]
pub(crate) struct AcquisitionEntries<K, V> {
    entries: Arc<Mutex<Entries<K, V>>>,
    owner: Arc<OnceLock<Arc<RwLock<ShardOwner>>>>,
    acquisition: Option<ShardAcquisition>,
    submitted_revision: Option<u64>,
    deadlines: Option<DeadlineIndex<V>>,
}

impl<K, V> Default for AcquisitionEntries<K, V> {
    fn default() -> Self {
        Self {
            entries: Arc::new(Mutex::new(Entries {
                values: HashMap::new(),
                revision: 0,
            })),
            owner: Arc::new(OnceLock::new()),
            acquisition: None,
            submitted_revision: None,
            deadlines: None,
        }
    }
}

impl<K: Clone + Eq + Hash, V: Clone> AcquisitionEntries<K, V> {
    pub(crate) fn with_deadlines(
        wake: DeadlineWake,
        due: fn(&V) -> Option<OffsetDateTime>,
    ) -> Self {
        Self {
            deadlines: Some(DeadlineIndex { wake, due }),
            ..Self::default()
        }
    }

    fn changed_deadline(
        &self,
        value: &V,
        owner: Option<&ShardOwner>,
        acquisition: Option<&ShardAcquisition>,
    ) {
        if let Some(index) = &self.deadlines {
            let active = owner.is_none_or(|owner| {
                acquisition.is_some_and(|acquisition| owner.acquisition_active(acquisition))
            });
            index
                .wake
                .changed(if active { (index.due)(value) } else { None });
        }
    }

    /// Start a periodic reconciliation without rearming failed entries on every deadline wake.
    pub(crate) fn begin_full_pass(&self, refresh: bool) {
        let mut entries = self.entries.lock().expect("tracking lock poisoned");
        for entry in entries.values.values_mut() {
            entry.deadline_pending = true;
            entry.refresh_pending = refresh;
        }
    }

    /// Record an examination before awaiting I/O. Never consume a replacement's work.
    pub(crate) fn mark_attempted(&self, key: &K, submitted: &Tracked<V>) -> bool {
        let mut entries = self.entries.lock().expect("tracking lock poisoned");
        let Some(entry) = entries
            .values
            .get_mut(key)
            .filter(|entry| entry.revision == submitted.revision)
        else {
            return false;
        };
        entry.deadline_pending = false;
        entry.refresh_pending = false;
        true
    }

    /// Refresh only derived timing data after a scan, preserving the submission's
    /// revision so its completion cannot retire a concurrent start or heartbeat.
    pub(crate) fn refresh_scanned(
        &self,
        key: &K,
        submitted: &Tracked<V>,
        now: OffsetDateTime,
        update: impl FnOnce(&mut V),
    ) {
        let mut entries = self.entries.lock().expect("tracking lock poisoned");
        let Some(entry) = entries
            .values
            .get_mut(key)
            .filter(|entry| entry.revision == submitted.revision)
        else {
            return;
        };
        update(&mut entry.value);
        if let Some(index) = &self.deadlines {
            entry.deadline_pending = (index.due)(&entry.value).is_some_and(|due| due > now);
        }
    }

    pub(crate) fn deadline_delay(
        &self,
        now: OffsetDateTime,
        maximum: std::time::Duration,
    ) -> std::time::Duration {
        let Some(index) = &self.deadlines else {
            return maximum;
        };
        // Match installation's lock order; planning borrows values so every
        // deadline wake need not copy the entire activity/eligibility index.
        let owner = self
            .owner
            .get()
            .map(|owner| owner.read().expect("shard owner lock poisoned"));
        let entries = self.entries.lock().expect("tracking lock poisoned");
        entries
            .values
            .values()
            .filter(|entry| Self::active_with_owner(owner.as_deref(), entry))
            .filter_map(|entry| {
                if entry.refresh_pending {
                    return Some(std::time::Duration::ZERO);
                }
                let due = (index.due)(&entry.value)?;
                if due > now {
                    std::time::Duration::try_from(due - now).ok()
                } else {
                    entry.deadline_pending.then_some(std::time::Duration::ZERO)
                }
            })
            .fold(maximum, std::cmp::min)
    }

    pub(crate) fn set_owner(&self, owner: Arc<RwLock<ShardOwner>>) {
        let _ = self.owner.set(owner);
    }

    pub(crate) fn for_acquisition(&self, acquisition: ShardAcquisition) -> Self {
        Self {
            acquisition: self.acquisition.clone().or(Some(acquisition)),
            ..self.clone()
        }
    }

    pub(crate) fn captured_acquisition(&self) -> Option<ShardAcquisition> {
        self.acquisition.clone()
    }

    /// Restrict post-await mutations to the exact snapshot a scanner submitted.
    /// Revisions are unique across the shared map, including replacement keys.
    pub(crate) fn for_entry(&self, entry: &Tracked<V>) -> Self {
        Self {
            acquisition: entry.acquisition.clone(),
            submitted_revision: Some(entry.revision),
            ..self.clone()
        }
    }

    /// Bind a callback to the value present before its await, including absence.
    /// Revision zero is never allocated, so an absent entry cannot later mutate
    /// a replacement installed under the same logical key.
    pub(crate) fn for_key(&self, key: &K) -> Self {
        let entries = self.entries.lock().expect("tracking lock poisoned");
        Self {
            submitted_revision: Some(entries.values.get(key).map_or(0, |entry| entry.revision)),
            ..self.clone()
        }
    }

    fn acquisition(&self, owner: Option<&ShardOwner>, home: ShardId) -> Option<ShardAcquisition> {
        self.acquisition
            .clone()
            .or_else(|| owner.and_then(|owner| owner.acquisition(home)))
    }

    fn current(
        owner: Option<&ShardOwner>,
        acquisition: Option<&ShardAcquisition>,
        home: ShardId,
    ) -> bool {
        match (owner, acquisition) {
            (Some(owner), Some(acquisition)) => {
                acquisition.shard_id == home
                    && acquisition.valid()
                    && owner.matches_acquisition(acquisition)
            }
            (Some(_), None) => false,
            (None, _) => true,
        }
    }

    pub(crate) fn insert(&self, key: K, home: ShardId, value: V) {
        self.upsert(key, home, |_| value);
    }

    pub(crate) fn upsert(&self, key: K, home: ShardId, make: impl FnOnce(Option<V>) -> V) {
        self.upsert_if(key, home, |previous| Some(make(previous)));
    }

    /// Preserve a completed obligation's revision when its authoritative
    /// incarnation is unchanged; unrelated transitions must not rearm it.
    pub(crate) fn upsert_if(
        &self,
        key: K,
        home: ShardId,
        make: impl FnOnce(Option<V>) -> Option<V>,
    ) {
        let owner = self
            .owner
            .get()
            .map(|owner| owner.read().expect("shard owner lock poisoned"));
        let acquisition = self.acquisition(owner.as_deref(), home);
        if !Self::current(owner.as_deref(), acquisition.as_ref(), home) {
            return;
        }
        let mut inner = self.entries.lock().expect("tracking lock poisoned");
        if self.submitted_revision.is_some_and(|revision| {
            inner
                .values
                .get(&key)
                .is_none_or(|entry| entry.revision != revision)
        }) {
            return;
        }
        let previous = inner
            .values
            .get(&key)
            .filter(|entry| match (&entry.acquisition, &acquisition) {
                (Some(a), Some(b)) => a.generation == b.generation,
                (None, None) => true,
                _ => false,
            })
            .map(|entry| entry.value.clone());
        let Some(value) = make(previous) else {
            return;
        };
        inner.revision = inner
            .revision
            .checked_add(1)
            .expect("tracking revision exhausted");
        let revision = inner.revision;
        self.changed_deadline(&value, owner.as_deref(), acquisition.as_ref());
        inner.values.insert(
            key,
            Tracked {
                value,
                home,
                revision,
                acquisition,
                deadline_pending: self.deadlines.is_some(),
                refresh_pending: false,
            },
        );
    }

    pub(crate) fn update<T>(&self, key: &K, update: impl FnOnce(&mut V) -> T) -> Option<T> {
        let owner = self
            .owner
            .get()
            .map(|owner| owner.read().expect("shard owner lock poisoned"));
        let mut inner = self.entries.lock().expect("tracking lock poisoned");
        let entry = inner.values.get(key)?;
        if self
            .submitted_revision
            .is_some_and(|revision| entry.revision != revision)
            || self.acquisition.as_ref().is_some_and(|scope| {
                entry
                    .acquisition
                    .as_ref()
                    .is_none_or(|a| a.generation != scope.generation)
            })
            || !Self::current(owner.as_deref(), entry.acquisition.as_ref(), entry.home)
        {
            return None;
        }
        inner.revision = inner
            .revision
            .checked_add(1)
            .expect("tracking revision exhausted");
        let revision = inner.revision;
        let entry = inner.values.get_mut(key).expect("entry remains under lock");
        entry.revision = revision;
        let result = update(&mut entry.value);
        entry.deadline_pending = self.deadlines.is_some();
        entry.refresh_pending = false;
        self.changed_deadline(&entry.value, owner.as_deref(), entry.acquisition.as_ref());
        Some(result)
    }

    pub(crate) fn get(&self, key: &K) -> Option<V> {
        self.entries
            .lock()
            .expect("tracking lock poisoned")
            .values
            .get(key)
            .map(|entry| entry.value.clone())
    }

    /// Fetch one descriptor with its acquisition, without scanning other homes.
    pub(crate) fn tracked(&self, key: &K) -> Option<Tracked<V>> {
        self.entries
            .lock()
            .expect("tracking lock poisoned")
            .values
            .get(key)
            .cloned()
    }

    #[cfg(all(test, feature = "dsql-integration"))]
    pub(crate) fn generation(&self, key: &K) -> Option<u64> {
        self.tracked(key)
            .and_then(|entry| entry.acquisition.map(|acquisition| acquisition.generation))
    }

    pub(crate) fn remove(&self, key: &K) {
        let mut inner = self.entries.lock().expect("tracking lock poisoned");
        if self.submitted_revision.is_none_or(|revision| {
            inner
                .values
                .get(key)
                .is_some_and(|entry| entry.revision == revision)
        }) && self.acquisition.as_ref().is_none_or(|scope| {
            inner.values.get(key).is_some_and(|entry| {
                entry
                    .acquisition
                    .as_ref()
                    .is_some_and(|a| a.generation == scope.generation)
            })
        }) {
            inner.values.remove(key);
        }
    }

    pub(crate) fn retain(&self, mut retain: impl FnMut(&K, &V) -> bool) {
        self.entries
            .lock()
            .expect("tracking lock poisoned")
            .values
            .retain(|key, entry| {
                // A late acquisition-scoped cleanup has no authority over a
                // replacement's entries, even if its logical keys are reused.
                self.acquisition.as_ref().is_some_and(|scope| {
                    entry
                        .acquisition
                        .as_ref()
                        .is_none_or(|a| a.generation != scope.generation)
                }) || self
                    .submitted_revision
                    .is_some_and(|revision| revision != entry.revision)
                    || retain(key, &entry.value)
            });
    }

    pub(crate) fn snapshot(&self, home: Option<ShardId>) -> Vec<Tracked<V>> {
        self.entries
            .lock()
            .expect("tracking lock poisoned")
            .values
            .values()
            .filter(|entry| home.is_none_or(|home| entry.home == home))
            .cloned()
            .collect()
    }

    /// Clone only selected active descriptors before a scanner releases its
    /// locks for I/O. The predicate must not reacquire ownership or entries.
    pub(crate) fn select_active(
        &self,
        home: Option<ShardId>,
        mut select: impl FnMut(&Tracked<V>) -> bool,
    ) -> Vec<Tracked<V>> {
        let owner = self
            .owner
            .get()
            .map(|owner| owner.read().expect("shard owner lock poisoned"));
        let entries = self.entries.lock().expect("tracking lock poisoned");
        entries
            .values
            .values()
            .filter(|entry| home.is_none_or(|home| entry.home == home))
            .filter(|entry| Self::active_with_owner(owner.as_deref(), entry))
            .filter(|entry| select(entry))
            .cloned()
            .collect()
    }

    fn active_with_owner(owner: Option<&ShardOwner>, entry: &Tracked<V>) -> bool {
        match (owner, &entry.acquisition) {
            (Some(owner), Some(acquisition)) => owner.acquisition_active(acquisition),
            (None, _) => true,
            _ => false,
        }
    }

    pub(crate) fn active(&self, entry: &Tracked<V>) -> bool {
        let owner = self
            .owner
            .get()
            .map(|owner| owner.read().expect("shard owner lock poisoned"));
        Self::active_with_owner(owner.as_deref(), entry)
    }

    /// Capture the entry's original acquisition, never a replacement observed
    /// after a scanner await. Callback outcomes use this through lane commit.
    pub(crate) fn outbound_attempt(
        &self,
        entry: &Tracked<V>,
    ) -> Option<crate::outbound::driver::Attempt> {
        let owner = self.owner.get()?.read().expect("shard owner lock poisoned");
        let acquisition = entry.acquisition.clone()?;
        if !owner.acquisition_active(&acquisition) {
            return None;
        }
        Some(crate::outbound::driver::Attempt {
            fence: owner.fence_at(entry.home, acquisition.epoch),
            acquisition,
        })
    }

    pub(crate) fn remove_submitted(&self, key: &K, submitted: &Tracked<V>) {
        let mut inner = self.entries.lock().expect("tracking lock poisoned");
        if inner
            .values
            .get(key)
            .is_some_and(|entry| entry.revision == submitted.revision)
        {
            inner.values.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tokeira_types::ShardEpoch;

    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct CloneCountedDeadline {
        due: OffsetDateTime,
        clones: Arc<AtomicUsize>,
    }

    impl Clone for CloneCountedDeadline {
        fn clone(&self) -> Self {
            self.clones.fetch_add(1, Ordering::SeqCst);
            Self {
                due: self.due,
                clones: self.clones.clone(),
            }
        }
    }

    #[test]
    fn planning_borrows_and_passes_clone_only_selected_entries() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let clones = Arc::new(AtomicUsize::new(0));
        let owner = Arc::new(RwLock::new(ShardOwner::new(2)));
        owner
            .write()
            .unwrap()
            .record_acquired(ShardId(0), ShardEpoch(1));
        owner.write().unwrap().mark_active(ShardId(0));
        owner
            .write()
            .unwrap()
            .record_acquired(ShardId(1), ShardEpoch(1));
        let entries = AcquisitionEntries::with_deadlines(
            DeadlineWake::default(),
            |entry: &CloneCountedDeadline| Some(entry.due),
        );
        entries.set_owner(owner);
        for id in 0..259 {
            entries.insert(
                id,
                ShardId(u32::from(id == 258)),
                CloneCountedDeadline {
                    due: if id < 2 || id == 258 {
                        now
                    } else {
                        now + time::Duration::hours(1)
                    },
                    clones: clones.clone(),
                },
            );
        }
        assert_eq!(clones.load(Ordering::SeqCst), 0);
        assert_eq!(
            entries.deadline_delay(now, std::time::Duration::from_secs(1)),
            std::time::Duration::ZERO
        );
        assert_eq!(
            clones.load(Ordering::SeqCst),
            0,
            "planning must borrow every value"
        );
        let selected = entries.select_active(None, |entry| {
            entry.deadline_pending && entry.value.due <= now
        });
        assert_eq!(selected.len(), 2);
        assert_eq!(
            clones.load(Ordering::SeqCst),
            2,
            "future and inactive values must not be cloned"
        );
        entries.begin_full_pass(true);
        clones.store(0, Ordering::SeqCst);
        assert_eq!(
            entries
                .select_active(Some(ShardId(0)), |entry| entry.refresh_pending)
                .len(),
            258
        );
        assert_eq!(
            clones.load(Ordering::SeqCst),
            258,
            "full passes still select every active entry"
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        // Feature: activity-dispatch, Property 6: a late start callback cannot update a replacement or fill a formerly absent slot.
        #[test]
        fn captured_activity_key_preserves_replacement(original in proptest::option::of(any::<u64>()), replacement in any::<u64>()) {
            let entries = AcquisitionEntries::default();
            if let Some(value) = original { entries.insert(1, ShardId(0), value); }
            let callback = entries.for_key(&1);
            entries.insert(1, ShardId(0), replacement);
            prop_assert_eq!(callback.update(&1, |value| *value = 0), None);
            callback.remove(&1);
            prop_assert_eq!(entries.get(&1), Some(replacement));
        }

        // Feature: workflow-dispatch, Property 8: cancelled acquisition cannot install or retire a replacement.
        #[test]
        fn replacement_survives_old_install_and_scan(value in any::<u64>(), replacement in any::<u64>()) {
            let home = ShardId(1);
            let owner = Arc::new(RwLock::new(ShardOwner::new(8)));
            owner.write().unwrap().record_acquired(home, ShardEpoch(1));
            let first = owner.read().unwrap().acquisition(home).unwrap();
            let entries = AcquisitionEntries::default();
            entries.set_owner(owner.clone());
            let old = entries.for_acquisition(first.clone());
            old.insert(7, home, value);
            let submitted = entries.snapshot(None).pop().unwrap();
            prop_assert!(!entries.active(&submitted));
            owner.write().unwrap().activate_acquisition(&first);
            prop_assert!(entries.active(&submitted));
            let scan = entries.for_entry(&submitted);
            entries.update(&7, |entry| *entry = replacement);
            scan.update(&7, |entry| *entry = value);
            scan.remove(&7);
            scan.insert(7, home, value);
            entries.remove_submitted(&7, &submitted);
            prop_assert_eq!(entries.get(&7), Some(replacement));
            owner.write().unwrap().record_acquired(home, ShardEpoch(1));
            let second = owner.read().unwrap().acquisition(home).unwrap();
            entries.for_acquisition(second.clone()).insert(7, home, replacement);
            old.insert(7, home, value);
            old.update(&7, |entry| *entry = value);
            old.remove(&7);
            scan.update(&7, |entry| *entry = value);
            scan.remove(&7);
            entries.remove_submitted(&7, &submitted);
            prop_assert_eq!(entries.get(&7), Some(replacement));
            prop_assert!(!entries.active(&submitted));
            owner.write().unwrap().activate_acquisition(&second);
            prop_assert!(entries.active(&entries.snapshot(None)[0]));
        }
    }
}
