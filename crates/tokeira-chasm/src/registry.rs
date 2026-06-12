//! The component registry and library index.
//!
//! A [`Library`] groups the components a domain registers; the [`Registry`] indexes
//! every registered component three ways — by fully-qualified name (FQN), by a
//! `u32` archetype/type id derived from the FQN, and by Rust [`TypeId`]
//! (Requirement 8.1). An **archetype** is the FQN of a root component and its
//! **archetype id** is the type id of that FQN.
//!
//! Two invariants this module upholds:
//! - Archetype id `0` ([`LEGACY_WORKFLOW_ARCHETYPE_ID`]) is **reserved for legacy
//!   Workflow** and is never assigned to a CHASM archetype, so the workflow engine
//!   and CHASM never collide on identity (Requirement 8.2).
//! - The registry is built **once** via a [`RegistryBuilder`] and is **immutable**
//!   thereafter — no runtime mutation, consistent with the no-reflection rule
//!   (Requirement 8.3).
//!
//! ## Type-id derivation (tokeira-owned)
//!
//! Upstream derives the id from `farm.Fingerprint32(fqn)` (`registry.go @
//! v1.31.0`). tokeira reproduces the *contract* — a deterministic `u32` from the
//! FQN with `0` reserved — but owns the hash: the pure crate's dependency set is
//! confined to value/wire types (Requirement 1.1), so it cannot pull in a
//! FarmHash crate, and these ids are internal to tokeira (never on the wire to an
//! SDK), so byte-compatibility with Temporal's fingerprint is not required (same
//! reasoning as `ComponentRef`'s tokeira-owned encoding). [`archetype_id_for_fqn`]
//! therefore uses FNV-1a/32, which is deterministic and dependency-free. The id `0`
//! is remapped to a fixed non-zero sentinel so the reservation always holds; any
//! resulting collision is caught by [`RegistryBuilder`], which rejects two FQNs
//! that map to the same id.

use std::{any::TypeId, collections::HashMap};

use crate::{component::Component, error::ChasmError};

/// The archetype id reserved for the legacy Workflow engine. CHASM never assigns
/// it to one of its archetypes (Requirement 8.2).
pub const LEGACY_WORKFLOW_ARCHETYPE_ID: u32 = 0;

/// FNV-1a/32 offset basis.
const FNV_OFFSET_BASIS: u32 = 0x811c_9dc5;
/// FNV-1a/32 prime.
const FNV_PRIME: u32 = 0x0100_0193;
/// Sentinel the reserved id `0` is remapped to, so a real FQN never receives the
/// legacy-workflow id. Chosen at the top of the `u32` range to keep it clear of
/// the dense low-id space typical FQN hashes land in.
const ZERO_REMAP_SENTINEL: u32 = u32::MAX;

/// Derive a CHASM archetype/type id from a fully-qualified name.
///
/// Deterministic FNV-1a/32 over the FQN's bytes, with the reserved
/// [`LEGACY_WORKFLOW_ARCHETYPE_ID`] (`0`) remapped to a fixed non-zero sentinel so
/// no CHASM component can ever be assigned the legacy-workflow id (Requirement
/// 8.2). See the module doc for why the hash is tokeira-owned rather than FarmHash.
pub fn archetype_id_for_fqn(fqn: &str) -> u32 {
    let mut hash = FNV_OFFSET_BASIS;
    for byte in fqn.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    if hash == LEGACY_WORKFLOW_ARCHETYPE_ID {
        ZERO_REMAP_SENTINEL
    } else {
        hash
    }
}

/// A registered component's index entry: its FQN, derived archetype id, Rust
/// [`TypeId`], and the name of the [`Library`] that registered it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentEntry {
    /// The component's fully-qualified name (its [`Component::FQN`]).
    pub fqn: &'static str,
    /// The derived archetype/type id (never [`LEGACY_WORKFLOW_ARCHETYPE_ID`]).
    pub archetype_id: u32,
    /// The Rust [`TypeId`] of the component type.
    pub type_id: TypeId,
    /// The name of the library that registered the component.
    pub library: &'static str,
}

/// A group of components a domain registers into the [`Registry`].
///
/// A library implementation names itself and registers its components against a
/// [`RegistryBuilder`]. This is the unit `tokeira-chasm-activity` (and every future
/// archetype crate) implements to declare "one ASM among many" (foundation §4).
pub trait Library {
    /// The library's stable name (e.g. `"activity"`).
    const NAME: &'static str;

    /// Register the library's components into `builder`.
    ///
    /// # Errors
    ///
    /// Propagates [`RegistryBuilder::register`] errors (FQN/id/type collisions).
    fn register(builder: &mut RegistryBuilder) -> Result<(), ChasmError>;
}

/// Builder for the immutable [`Registry`]. Components are registered once at
/// startup; [`build`](RegistryBuilder::build) freezes the index (Requirement 8.3).
#[derive(Debug, Default)]
pub struct RegistryBuilder {
    entries: Vec<ComponentEntry>,
}

impl RegistryBuilder {
    /// Construct an empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register component `C` as belonging to `library`.
    ///
    /// Derives the archetype id from [`Component::FQN`] and records the Rust
    /// [`TypeId`].
    ///
    /// # Errors
    ///
    /// [`ChasmError::Internal`] if the FQN, the derived archetype id, or the Rust
    /// type is already registered — duplicate identities would make lookup
    /// ambiguous, and an archetype-id collision (including against the
    /// zero-remap sentinel) is rejected here rather than silently shadowing.
    pub fn register<C: Component>(
        &mut self,
        library: &'static str,
    ) -> Result<&mut Self, ChasmError> {
        let fqn = C::FQN;
        let archetype_id = archetype_id_for_fqn(fqn);
        let type_id = TypeId::of::<C>();

        for entry in &self.entries {
            if entry.fqn == fqn {
                return Err(ChasmError::Internal(format!(
                    "registry: FQN `{fqn}` is already registered"
                )));
            }
            if entry.archetype_id == archetype_id {
                return Err(ChasmError::Internal(format!(
                    "registry: archetype id {archetype_id} collides between `{}` and `{fqn}`",
                    entry.fqn
                )));
            }
            if entry.type_id == type_id {
                return Err(ChasmError::Internal(format!(
                    "registry: Rust type for `{fqn}` is already registered as `{}`",
                    entry.fqn
                )));
            }
        }

        self.entries.push(ComponentEntry {
            fqn,
            archetype_id,
            type_id,
            library,
        });
        Ok(self)
    }

    /// Register all of library `L`'s components.
    ///
    /// # Errors
    ///
    /// Propagates [`register`](RegistryBuilder::register) collision errors.
    pub fn register_library<L: Library>(&mut self) -> Result<&mut Self, ChasmError> {
        L::register(self)?;
        Ok(self)
    }

    /// Freeze the builder into an immutable [`Registry`].
    pub fn build(self) -> Registry {
        let mut by_fqn = HashMap::with_capacity(self.entries.len());
        let mut by_archetype = HashMap::with_capacity(self.entries.len());
        let mut by_type = HashMap::with_capacity(self.entries.len());
        for (index, entry) in self.entries.iter().enumerate() {
            by_fqn.insert(entry.fqn, index);
            by_archetype.insert(entry.archetype_id, index);
            by_type.insert(entry.type_id, index);
        }
        Registry {
            entries: self.entries,
            by_fqn,
            by_archetype,
            by_type,
        }
    }
}

/// The immutable component index. Built once via [`RegistryBuilder`] and never
/// mutated thereafter (Requirement 8.3). Lookups by FQN, archetype id, and Rust
/// [`TypeId`] all resolve to the same [`ComponentEntry`].
#[derive(Debug)]
pub struct Registry {
    entries: Vec<ComponentEntry>,
    by_fqn: HashMap<&'static str, usize>,
    by_archetype: HashMap<u32, usize>,
    by_type: HashMap<TypeId, usize>,
}

impl Registry {
    /// Start building a registry.
    pub fn builder() -> RegistryBuilder {
        RegistryBuilder::new()
    }

    /// Number of registered components.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True iff no components are registered.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The archetype id for `fqn`, if a component with that FQN is registered.
    pub fn archetype_id(&self, fqn: &str) -> Option<u32> {
        self.by_fqn.get(fqn).map(|&i| self.entries[i].archetype_id)
    }

    /// The entry for an archetype id, if registered. Always `None` for
    /// [`LEGACY_WORKFLOW_ARCHETYPE_ID`] (it is never a CHASM archetype).
    pub fn component_for_archetype(&self, id: u32) -> Option<&ComponentEntry> {
        self.by_archetype.get(&id).map(|&i| &self.entries[i])
    }

    /// The entry for a fully-qualified name, if registered.
    pub fn component_for_fqn(&self, fqn: &str) -> Option<&ComponentEntry> {
        self.by_fqn.get(fqn).map(|&i| &self.entries[i])
    }

    /// The entry for component type `C`, if registered.
    pub fn component_for_type<C: Component>(&self) -> Option<&ComponentEntry> {
        self.by_type
            .get(&TypeId::of::<C>())
            .map(|&i| &self.entries[i])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        component::{Component, Lifecycle, LifecycleState},
        context::Context,
        field::FieldRegistry,
    };

    // Two minimal components for index tests. They implement the trait by hand
    // (the derive macro is exercised by its own crate's consumers); only FQN/Data
    // and the field registry matter here.
    struct CompA;
    struct CompB;

    impl Lifecycle for CompA {
        fn lifecycle_state(&self, _ctx: &dyn Context) -> LifecycleState {
            LifecycleState::Running
        }
    }
    impl Component for CompA {
        type Data = ();
        const FQN: &'static str = "test.alpha";
        fn fields(&self) -> FieldRegistry<'_> {
            FieldRegistry::new(&[])
        }
    }

    impl Lifecycle for CompB {
        fn lifecycle_state(&self, _ctx: &dyn Context) -> LifecycleState {
            LifecycleState::Running
        }
    }
    impl Component for CompB {
        type Data = ();
        const FQN: &'static str = "test.beta";
        fn fields(&self) -> FieldRegistry<'_> {
            FieldRegistry::new(&[])
        }
    }

    #[test]
    fn archetype_id_is_deterministic_and_never_zero() {
        let id = archetype_id_for_fqn("activity.activity");
        assert_eq!(id, archetype_id_for_fqn("activity.activity"));
        assert_ne!(id, LEGACY_WORKFLOW_ARCHETYPE_ID);
    }

    #[test]
    fn registry_indexes_three_ways() {
        let mut builder = Registry::builder();
        builder.register::<CompA>("test").expect("register A");
        builder.register::<CompB>("test").expect("register B");
        let registry = builder.build();

        assert_eq!(registry.len(), 2);
        let a_id = registry.archetype_id("test.alpha").expect("A id");
        assert_eq!(
            registry.component_for_archetype(a_id).map(|e| e.fqn),
            Some("test.alpha")
        );
        assert_eq!(
            registry
                .component_for_fqn("test.beta")
                .map(|e| e.archetype_id),
            registry.archetype_id("test.beta")
        );
        assert_eq!(
            registry.component_for_type::<CompA>().map(|e| e.fqn),
            Some("test.alpha")
        );
    }

    #[test]
    fn legacy_workflow_archetype_is_never_registered() {
        let registry = Registry::builder().build();
        assert!(
            registry
                .component_for_archetype(LEGACY_WORKFLOW_ARCHETYPE_ID)
                .is_none()
        );
    }

    #[test]
    fn duplicate_fqn_registration_is_rejected() {
        let mut builder = Registry::builder();
        builder.register::<CompA>("test").expect("first");
        // Re-registering the same type/FQN must fail.
        assert!(matches!(
            builder.register::<CompA>("test"),
            Err(ChasmError::Internal(_))
        ));
    }

    #[test]
    fn library_registration_path() {
        struct TestLib;
        impl Library for TestLib {
            const NAME: &'static str = "test";
            fn register(builder: &mut RegistryBuilder) -> Result<(), ChasmError> {
                builder.register::<CompA>(Self::NAME)?;
                builder.register::<CompB>(Self::NAME)?;
                Ok(())
            }
        }
        let mut builder = Registry::builder();
        builder.register_library::<TestLib>().expect("register lib");
        let registry = builder.build();
        assert_eq!(registry.len(), 2);
        assert_eq!(
            registry.component_for_fqn("test.alpha").map(|e| e.library),
            Some("test")
        );
    }
}
