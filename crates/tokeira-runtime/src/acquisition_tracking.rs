//! Acquisition-scoped volatile indexes shared by recovery and timer scanners.
//!
//! Ownership is locked before entries so replacement cannot interleave validation
//! with installation. Every mutation advances a revision: an awaited scan may
//! retire only the exact value it submitted, even when its logical key is reused.

use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, Mutex, OnceLock, RwLock},
};

use tokeira_types::ShardId;

use crate::shard::{ShardAcquisition, ShardOwner};

#[derive(Clone, Debug)]
pub(crate) struct Tracked<V> {
    pub(crate) value: V,
    home: ShardId,
    revision: u64,
    acquisition: Option<ShardAcquisition>,
}

#[derive(Debug)]
struct Entries<K, V> {
    values: HashMap<K, Tracked<V>>,
    revision: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct AcquisitionEntries<K, V> {
    entries: Arc<Mutex<Entries<K, V>>>,
    owner: Arc<OnceLock<Arc<RwLock<ShardOwner>>>>,
    acquisition: Option<ShardAcquisition>,
    submitted_revision: Option<u64>,
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
        }
    }
}

impl<K: Clone + Eq + Hash, V: Clone> AcquisitionEntries<K, V> {
    pub(crate) fn set_owner(&self, owner: Arc<RwLock<ShardOwner>>) {
        let _ = self.owner.set(owner);
    }

    pub(crate) fn for_acquisition(&self, acquisition: ShardAcquisition) -> Self {
        Self {
            acquisition: Some(acquisition),
            submitted_revision: None,
            ..self.clone()
        }
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
        let value = make(previous);
        inner.revision = inner
            .revision
            .checked_add(1)
            .expect("tracking revision exhausted");
        let revision = inner.revision;
        inner.values.insert(
            key,
            Tracked {
                value,
                home,
                revision,
                acquisition,
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
        Some(update(&mut entry.value))
    }

    pub(crate) fn get(&self, key: &K) -> Option<V> {
        self.entries
            .lock()
            .expect("tracking lock poisoned")
            .values
            .get(key)
            .map(|entry| entry.value.clone())
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
            .retain(|key, entry| retain(key, &entry.value));
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

    pub(crate) fn active(&self, entry: &Tracked<V>) -> bool {
        match (self.owner.get(), &entry.acquisition) {
            (Some(owner), Some(acquisition)) => owner
                .read()
                .expect("shard owner lock poisoned")
                .acquisition_active(acquisition),
            (None, _) => true,
            _ => false,
        }
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

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
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
