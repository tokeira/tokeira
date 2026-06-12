//! The persistent field types and the field-registry contract.
//!
//! A component's persistent children are declared as typed fields:
//!
//! - [`Field<T>`] — a single persistent child whose `T` is either a proto message
//!   (a **data** leaf) or a child `Component` (a subtree root).
//! - [`Map<K, T>`] — a keyed collection; each entry persists as its own node.
//! - [`ParentPtr<T>`] — upward access to the parent component whose ancestry walk
//!   **skips map nodes** (Requirement 2.8), plus transient plain fields that are
//!   not persisted.
//!
//! The companion [`FieldDescriptor`]/[`FieldKind`]/[`FieldRegistry`] types form the
//! contract the `#[derive(Component)]` macro targets: the macro emits a static
//! registry describing each declared field so the node tree can walk and persist
//! children without runtime reflection (Requirement 2.5–2.7, 3.1).
//!
//! ## Lazy resolution and the two-state field
//!
//! A field does **not** own its value inline. Mirroring upstream CHASM's
//! `fieldInternal` (`field.go`, `field_internal.go @ v1.31.0`), a [`Field`] is in
//! one of two non-empty states: it either holds an **in-memory value** set during
//! the current transition but not yet attached to a tree node, or it holds a
//! **[`NodeHandle`]** linking it to the persisted child node, whose value is
//! deserialized **lazily** against the live tree on access. This two-state shape is
//! the structural reason loading a component does not eagerly materialize its whole
//! subtree — only touched fields are resolved (Requirement 2.7; foundation §1).
//!
//! The lazy read path (resolving a node-backed value through the active
//! [`context`](crate::context)) is owned by the node tree and engine and lands with
//! task 5.1 / task 2.1; this module models the field *shape* and the node linkage
//! so component authors and the derive macro have a stable surface to target. Until
//! the node tree exists, [`NodeHandle`] is a minimal placeholder carrying only the
//! path segment that names the field's node — see its docs.
//!
//! Purity: these are plain value types. No I/O, no async, no storage
//! (Requirement 1.1).

use std::{collections::BTreeMap, marker::PhantomData};

use serde::{Deserialize, Serialize};

/// A minimal stand-in for a field's linkage to its persisted tree node.
///
/// Upstream this is the `*Node` pointer inside `fieldInternal`
/// (`field_internal.go @ v1.31.0`): an *attached* field does not store its value
/// inline, it points at the child node and the value is deserialized lazily on
/// access. The node tree itself is implemented by task 5.1 of the
/// `chasm-foundation` spec; until then this handle carries only the **path
/// segment** that names the field's node under its owning component — the stable
/// identity the tree will key on. It is deliberately opaque so that fleshing out
/// the tree does not change this module's public surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeHandle {
    /// Path segment naming this field's node relative to its owning component.
    segment: String,
}

impl NodeHandle {
    /// Construct a handle for the node named by `segment` under its owning
    /// component. The segment is the field's name in the parent's
    /// [`FieldRegistry`] (the `$`/`#` separator is applied by the path encoder,
    /// not stored here).
    pub fn new(segment: impl Into<String>) -> Self {
        Self {
            segment: segment.into(),
        }
    }

    /// The path segment naming this field's node under its owning component.
    pub fn segment(&self) -> &str {
        &self.segment
    }
}

/// The two-state interior of a [`Field`], mirroring upstream `fieldInternal`
/// (`field_internal.go @ v1.31.0`). A field is empty iff it holds neither an
/// in-memory value nor a node link.
#[derive(Debug, Serialize, Deserialize)]
enum FieldState<T> {
    /// An in-memory value set during the current transition, not yet attached to a
    /// node. This is what `NewDataField`/`NewComponentField` produce upstream
    /// before `CloseTransaction` attaches them to the tree.
    Value(T),
    /// A link to the persisted child node; the value is resolved lazily against
    /// the tree on access (the read path lands with the node tree, task 5.1).
    Node(NodeHandle),
    /// Neither a value nor a node — the field is unset (`fieldInternal.isEmpty`).
    Empty,
}

/// A single persistent child of a component.
///
/// `T` is either a proto message (a **data** field — a leaf carrying serialized
/// bytes) or a child `Component` (a **component** field — a subtree root); which
/// one is a static fact recorded in the owning component's [`FieldRegistry`] as
/// [`FieldKind::Data`] or [`FieldKind::Component`], decided at derive time, not at
/// runtime. Each `Field` persists as its **own node** in the tree, which is what
/// makes write-only-dirty-nodes possible (Requirement 2.7; foundation §1).
///
/// Contract: a field holds *either* an in-memory value (set this transition) *or* a
/// link to its persisted node, *or* nothing. [`value`](Field::value) returns the
/// in-memory value when one is present; a node-backed value is resolved lazily
/// through the tree (task 5.1) and is therefore `None` here. `T: Serialize` /
/// `T: Deserialize` are required only to serialize a field that currently holds an
/// in-memory value; node-backed and empty fields round-trip regardless of `T`.
#[derive(Debug, Serialize, Deserialize)]
pub struct Field<T> {
    state: FieldState<T>,
}

impl<T> Field<T> {
    /// Construct a field holding an in-memory `value` not yet attached to a node
    /// (the state produced inside a transition before close). Mirrors upstream
    /// `NewDataField`/`NewComponentField` (`field.go @ v1.31.0`).
    pub fn with_value(value: T) -> Self {
        Self {
            state: FieldState::Value(value),
        }
    }

    /// Construct a field linked to its persisted node. The value is resolved
    /// lazily against the tree on access. Mirrors `newFieldInternalWithNode`
    /// (`field_internal.go @ v1.31.0`).
    pub fn attached(handle: NodeHandle) -> Self {
        Self {
            state: FieldState::Node(handle),
        }
    }

    /// Construct an empty (unset) field.
    pub fn empty() -> Self {
        Self {
            state: FieldState::Empty,
        }
    }

    /// True iff the field holds neither an in-memory value nor a node link
    /// (`fieldInternal.isEmpty @ v1.31.0`). A node-backed field is **not** empty:
    /// its value lives in the tree.
    pub fn is_empty(&self) -> bool {
        matches!(self.state, FieldState::Empty)
    }

    /// True iff the field is linked to a persisted node (its value lives in the
    /// tree and must be resolved lazily rather than read via [`value`](Field::value)).
    pub fn is_attached(&self) -> bool {
        matches!(self.state, FieldState::Node(_))
    }

    /// The in-memory value if the field currently holds one.
    ///
    /// Returns `None` when the field is node-backed — that value lives in the tree
    /// and is resolved lazily through the active [`context`](crate::context) once
    /// the node tree lands (task 5.1) — or when the field is empty. It never
    /// performs I/O.
    pub fn value(&self) -> Option<&T> {
        match &self.state {
            FieldState::Value(value) => Some(value),
            FieldState::Node(_) | FieldState::Empty => None,
        }
    }

    /// Mutable access to the in-memory value if the field currently holds one;
    /// `None` for node-backed or empty fields (see [`value`](Field::value)).
    pub fn value_mut(&mut self) -> Option<&mut T> {
        match &mut self.state {
            FieldState::Value(value) => Some(value),
            FieldState::Node(_) | FieldState::Empty => None,
        }
    }

    /// The node link if the field is attached to one; `None` otherwise.
    pub fn node_handle(&self) -> Option<&NodeHandle> {
        match &self.state {
            FieldState::Node(handle) => Some(handle),
            FieldState::Value(_) | FieldState::Empty => None,
        }
    }

    /// Replace the field's contents with an in-memory `value`. Used when a
    /// transition writes a field (the node-attach happens at close, task 5.2).
    pub fn set(&mut self, value: T) {
        self.state = FieldState::Value(value);
    }

    /// Consume the field, yielding the in-memory value if it held one.
    pub fn into_value(self) -> Option<T> {
        match self.state {
            FieldState::Value(value) => Some(value),
            FieldState::Node(_) | FieldState::Empty => None,
        }
    }
}

impl<T> Default for Field<T> {
    /// An empty field.
    fn default() -> Self {
        Self::empty()
    }
}

/// A keyed collection field; each entry persists as its **own node** (an `#`-keyed
/// child in the encoded path), exactly as a standalone [`Field`] does
/// (Requirement 2.7). Iteration order is the key order (`BTreeMap`), which keeps
/// the substrate deterministic — a purity property the kernel and this crate share.
///
/// Each value is held as a [`Field<T>`], so a map entry has the same two-state
/// (in-memory value vs. lazily-resolved node) semantics as any other field.
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize, T: Serialize",
    deserialize = "K: Deserialize<'de> + Ord, T: Deserialize<'de>"
))]
pub struct Map<K, T> {
    entries: BTreeMap<K, Field<T>>,
}

impl<K, T> Default for Map<K, T> {
    /// An empty map.
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }
}

impl<K: Ord, T> Map<K, T> {
    /// Construct an empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of entries (each its own node).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True iff the map has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Insert an in-memory `value` under `key`, returning the previous entry if
    /// one existed. The value is wrapped in a fresh [`Field`]; it is attached to
    /// its own node at transition close (task 5.2).
    pub fn insert(&mut self, key: K, value: T) -> Option<Field<T>> {
        self.entries.insert(key, Field::with_value(value))
    }

    /// Insert a prepared [`Field`] (e.g. a node-backed entry loaded from the tree)
    /// under `key`, returning the previous entry if one existed.
    pub fn insert_field(&mut self, key: K, field: Field<T>) -> Option<Field<T>> {
        self.entries.insert(key, field)
    }

    /// The entry [`Field`] for `key`, if present.
    pub fn get(&self, key: &K) -> Option<&Field<T>> {
        self.entries.get(key)
    }

    /// The in-memory value for `key`, if the entry is present and holds one (see
    /// [`Field::value`] for the node-backed caveat).
    pub fn get_value(&self, key: &K) -> Option<&T> {
        self.entries.get(key).and_then(Field::value)
    }

    /// Remove and return the entry for `key`, if present.
    pub fn remove(&mut self, key: &K) -> Option<Field<T>> {
        self.entries.remove(key)
    }

    /// True iff an entry exists for `key`.
    pub fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }

    /// Iterate the keys in key order.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.entries.keys()
    }

    /// Iterate `(key, entry)` pairs in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &Field<T>)> {
        self.entries.iter()
    }
}

/// Upward access to a component's parent component.
///
/// A CHASM map is **not** a component, so when a component lives inside a
/// [`Map`], its parent pointer must skip past the intervening map node(s) to the
/// nearest ancestor that is an actual component. This skip-map-nodes ancestry walk
/// is the one non-obvious behaviour of the parent pointer and is reproduced from
/// `parent_pointer.go @ v1.31.0` (the upstream `Get` loops while `parent.isMap()`).
///
/// A `ParentPtr` is **never serialized as a value** — it is resolved against the
/// live tree, and is only initialized **after** the transition that creates the
/// owning component completes (`parent_pointer.go @ v1.31.0`). Hence it derives
/// only `Debug`, not `Serialize`/`Deserialize`. The walk itself is performed by the
/// node tree (task 5.1); this type models the linkage (the "current node" anchor)
/// the walk starts from.
#[derive(Debug)]
pub struct ParentPtr<T> {
    /// The owning component's own node, the anchor the upward walk starts from.
    /// `None` until the creating transition completes (uninitialized).
    current: Option<NodeHandle>,
    /// `T` is the parent component type the resolved walk yields; it carries no
    /// data here, so a covariant, always-`Send`/`Sync` marker is used.
    _marker: PhantomData<fn() -> T>,
}

impl<T> ParentPtr<T> {
    /// An uninitialized parent pointer (before the creating transition completes).
    pub fn uninitialized() -> Self {
        Self {
            current: None,
            _marker: PhantomData,
        }
    }

    /// A parent pointer anchored at the owning component's node `handle`. The
    /// upward, map-skipping resolution to the parent component is performed by the
    /// tree on access (task 5.1).
    pub fn at_node(handle: NodeHandle) -> Self {
        Self {
            current: Some(handle),
            _marker: PhantomData,
        }
    }

    /// True iff the pointer has been initialized (its owning component exists in
    /// the tree).
    pub fn is_initialized(&self) -> bool {
        self.current.is_some()
    }

    /// The owning component's node anchor, if initialized. The tree walks upward
    /// from here, skipping map nodes, to resolve the parent component.
    pub fn current_node(&self) -> Option<&NodeHandle> {
        self.current.as_ref()
    }
}

impl<T> Default for ParentPtr<T> {
    /// An uninitialized parent pointer.
    fn default() -> Self {
        Self::uninitialized()
    }
}

/// The classification of a declared component field, recorded statically by
/// `#[derive(Component)]` (Requirement 3.1). Classification is from the syntactic
/// field type at expansion time, never from runtime inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    /// The single proto-message data field. A component has exactly one — the
    /// derive macro enforces this at compile time (Requirement 2.6, 3.2).
    Data,
    /// A child-component field (a [`Field<T>`] whose `T` is a `Component`).
    Component,
    /// A keyed collection of child fields (a [`Map<K, T>`]).
    Map,
    /// An upward [`ParentPtr<T>`]; resolved against the tree, not persisted as data.
    Parent,
    /// A transient field, explicitly marked `#[chasm(transient)]` and not persisted
    /// (Requirement 3.5).
    Transient,
}

impl FieldKind {
    /// True iff a field of this kind materializes as a persisted node carrying
    /// data: [`Data`](FieldKind::Data), [`Component`](FieldKind::Component), and
    /// [`Map`](FieldKind::Map) do; [`Parent`](FieldKind::Parent) (resolved against
    /// the tree) and [`Transient`](FieldKind::Transient) (not persisted) do not.
    pub fn is_persistent(self) -> bool {
        matches!(
            self,
            FieldKind::Data | FieldKind::Component | FieldKind::Map
        )
    }
}

/// Static description of one declared field, emitted by `#[derive(Component)]`.
///
/// `name` is the field's path segment under its owning component (the `$`/`#`
/// separator is applied by the path encoder, not stored here); `kind` is its
/// [`FieldKind`] classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldDescriptor {
    /// Path segment naming this field under its owning component.
    pub name: &'static str,
    /// The field's static classification.
    pub kind: FieldKind,
}

impl FieldDescriptor {
    /// Construct a descriptor. `const` so the derive macro can build the registry
    /// in a `const`/`static` context with no runtime work.
    pub const fn new(name: &'static str, kind: FieldKind) -> Self {
        Self { name, kind }
    }
}

/// The ordered set of a component's fields — the static contract the node tree uses
/// to walk and persist a component's children, and the bridge between
/// `#[derive(Component)]` and the framework.
///
/// The derive macro emits a `&'static [FieldDescriptor]` and hands it back through
/// `Component::fields(&self) -> FieldRegistry<'_>`; the `'a` lifetime ties the
/// registry to the borrow of the component so future accessors can read field
/// values from `&'a self` without copying. This carries the descriptor metadata
/// (names, kinds); value access is performed against the live tree.
#[derive(Debug, Clone, Copy)]
pub struct FieldRegistry<'a> {
    descriptors: &'a [FieldDescriptor],
}

impl<'a> FieldRegistry<'a> {
    /// Build a registry over a slice of descriptors (emitted by the derive macro).
    pub const fn new(descriptors: &'a [FieldDescriptor]) -> Self {
        Self { descriptors }
    }

    /// The full descriptor slice in declaration order.
    pub fn descriptors(&self) -> &'a [FieldDescriptor] {
        self.descriptors
    }

    /// Number of declared fields.
    pub fn len(&self) -> usize {
        self.descriptors.len()
    }

    /// True iff the component declares no fields.
    pub fn is_empty(&self) -> bool {
        self.descriptors.is_empty()
    }

    /// Iterate the descriptors in declaration order.
    pub fn iter(&self) -> impl Iterator<Item = &'a FieldDescriptor> {
        self.descriptors.iter()
    }

    /// The descriptor named `name`, if declared.
    pub fn get(&self, name: &str) -> Option<&'a FieldDescriptor> {
        self.descriptors
            .iter()
            .find(|descriptor| descriptor.name == name)
    }

    /// The single [`FieldKind::Data`] descriptor, if present. The derive macro
    /// guarantees exactly one (Requirement 2.6, 3.2); this returns the first.
    pub fn data_field(&self) -> Option<&'a FieldDescriptor> {
        self.descriptors
            .iter()
            .find(|descriptor| descriptor.kind == FieldKind::Data)
    }

    /// Count of [`FieldKind::Data`] descriptors. The derive macro enforces that
    /// this is exactly `1`; exposed so the invariant can be asserted in tests and
    /// by the tree.
    pub fn data_field_count(&self) -> usize {
        self.descriptors
            .iter()
            .filter(|descriptor| descriptor.kind == FieldKind::Data)
            .count()
    }

    /// Iterate the descriptors of a given [`FieldKind`] in declaration order.
    pub fn fields_of_kind(&self, kind: FieldKind) -> impl Iterator<Item = &'a FieldDescriptor> {
        self.descriptors
            .iter()
            .filter(move |descriptor| descriptor.kind == kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_value_state_holds_in_memory_value() {
        let field = Field::with_value(42u32);
        assert!(!field.is_empty());
        assert!(!field.is_attached());
        assert_eq!(field.value(), Some(&42));
        assert!(field.node_handle().is_none());
        assert_eq!(field.into_value(), Some(42));
    }

    #[test]
    fn field_attached_state_defers_value_to_tree() {
        // A node-backed field has no in-memory value: it must be resolved lazily
        // against the tree, which this layer models as `value() == None`.
        let field: Field<u32> = Field::attached(NodeHandle::new("state"));
        assert!(!field.is_empty());
        assert!(field.is_attached());
        assert_eq!(field.value(), None);
        assert_eq!(field.node_handle().map(NodeHandle::segment), Some("state"));
    }

    #[test]
    fn field_empty_is_neither_value_nor_node() {
        let field: Field<u32> = Field::empty();
        assert!(field.is_empty());
        assert!(!field.is_attached());
        assert_eq!(field.value(), None);
        assert_eq!(Field::<u32>::default().value(), None);
    }

    #[test]
    fn field_set_replaces_contents_with_value() {
        let mut field: Field<u32> = Field::attached(NodeHandle::new("state"));
        field.set(7);
        assert!(!field.is_attached());
        assert_eq!(field.value(), Some(&7));
        *field.value_mut().expect("value present") += 1;
        assert_eq!(field.value(), Some(&8));
    }

    #[test]
    fn field_serde_round_trips_each_state() {
        for field in [
            Field::with_value(9u32),
            Field::attached(NodeHandle::new("x")),
            Field::empty(),
        ] {
            let json = serde_json::to_string(&field).expect("serialize");
            let back: Field<u32> = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(field.value(), back.value());
            assert_eq!(field.is_empty(), back.is_empty());
            assert_eq!(field.node_handle(), back.node_handle());
        }
    }

    #[test]
    fn map_entries_are_keyed_and_ordered() {
        let mut map: Map<u32, String> = Map::new();
        assert!(map.is_empty());
        map.insert(2, "two".to_owned());
        map.insert(1, "one".to_owned());
        assert_eq!(map.len(), 2);
        assert!(map.contains_key(&1));
        assert_eq!(map.get_value(&1), Some(&"one".to_owned()));
        // BTreeMap key order keeps iteration deterministic.
        let keys: Vec<u32> = map.keys().copied().collect();
        assert_eq!(keys, vec![1, 2]);
        let removed = map.remove(&1).expect("entry present");
        assert_eq!(removed.value(), Some(&"one".to_owned()));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn map_serde_round_trips() {
        let mut map: Map<u32, u64> = Map::new();
        map.insert(1, 100);
        map.insert(2, 200);
        let json = serde_json::to_string(&map).expect("serialize");
        let back: Map<u32, u64> = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.get_value(&1), Some(&100));
        assert_eq!(back.get_value(&2), Some(&200));
        assert_eq!(back.len(), 2);
    }

    #[test]
    fn parent_ptr_initialization_state() {
        let unset: ParentPtr<u32> = ParentPtr::uninitialized();
        assert!(!unset.is_initialized());
        assert!(unset.current_node().is_none());
        assert!(!ParentPtr::<u32>::default().is_initialized());

        let set: ParentPtr<u32> = ParentPtr::at_node(NodeHandle::new("child"));
        assert!(set.is_initialized());
        assert_eq!(set.current_node().map(NodeHandle::segment), Some("child"));
    }

    #[test]
    fn field_kind_persistence_classification() {
        assert!(FieldKind::Data.is_persistent());
        assert!(FieldKind::Component.is_persistent());
        assert!(FieldKind::Map.is_persistent());
        // Parent pointers resolve against the tree and transients are not stored.
        assert!(!FieldKind::Parent.is_persistent());
        assert!(!FieldKind::Transient.is_persistent());
    }

    #[test]
    fn registry_exposes_descriptors_and_data_field() {
        static FIELDS: &[FieldDescriptor] = &[
            FieldDescriptor::new("state", FieldKind::Data),
            FieldDescriptor::new("input", FieldKind::Component),
            FieldDescriptor::new("attempts", FieldKind::Map),
            FieldDescriptor::new("parent", FieldKind::Parent),
            FieldDescriptor::new("cached", FieldKind::Transient),
        ];
        let registry = FieldRegistry::new(FIELDS);

        assert_eq!(registry.len(), 5);
        assert!(!registry.is_empty());
        assert_eq!(registry.iter().count(), 5);

        assert_eq!(
            registry.get("input").map(|d| d.kind),
            Some(FieldKind::Component)
        );
        assert!(registry.get("missing").is_none());

        // Exactly one data field — the macro-enforced invariant (Requirement 2.6).
        assert_eq!(registry.data_field_count(), 1);
        assert_eq!(registry.data_field().map(|d| d.name), Some("state"));

        let maps: Vec<&str> = registry
            .fields_of_kind(FieldKind::Map)
            .map(|d| d.name)
            .collect();
        assert_eq!(maps, vec!["attempts"]);
    }

    #[test]
    fn empty_registry_has_no_data_field() {
        let registry = FieldRegistry::new(&[]);
        assert!(registry.is_empty());
        assert_eq!(registry.data_field_count(), 0);
        assert!(registry.data_field().is_none());
    }

    #[test]
    fn node_handle_round_trips() {
        let handle = NodeHandle::new("attempts");
        let json = serde_json::to_string(&handle).expect("serialize");
        let back: NodeHandle = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(handle, back);
        assert_eq!(back.segment(), "attempts");
    }
}
