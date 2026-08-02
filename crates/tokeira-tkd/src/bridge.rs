//! The interpreter's host-object seam, implemented by the standard `.tkd`
//! frontend adapter. The interpreter core holds host values opaquely (`Value<Self::Host>`)
//! and routes *every* host operation through this trait: construct a kind,
//! dispatch a builder verb, read a context field, unwrap the final deployment.
//! This keeps the parser/evaluator independent of framework handle types and
//! needs no `Box<dyn Any>`.

use crate::value::{EvalError, FieldMap, Value};

/// Opaque host operations required by the sandboxed interpreter.
pub trait HostBridge {
    /// The platform's opaque host handle (its closed `HostObj` enum). Must be
    /// `Clone`/`Debug` because config `Value`s are cloned (retarget diff) and
    /// formatted in error messages.
    type Host: Clone + std::fmt::Debug;
    /// The engine-injected context the operator's `deployment(cfg, cx)` reads.
    type Cx;
    /// The realized deployment the interpreter returns (the platform's builder).
    type Output;

    /// Is `name` an author kind (vs a `.tkd`-defined config struct)?
    fn is_kind(&self, name: &str) -> bool;
    /// Is `name` a method some host type exposes? (check-time subset validation).
    fn knows_method(&self, name: &str) -> bool;
    /// Is `path` a recognized associated fn (e.g. `Deployment::new`)?
    fn knows_assoc(&self, path: &str) -> bool;

    /// The interpreter image of `<Kind>::EMPTY` for `..EMPTY` spread, if any.
    fn kind_defaults(&self, name: &str) -> Option<FieldMap<Self::Host>>;
    /// Build a kind from its evaluated field map (consumes it; unknown keys error).
    fn construct_kind(
        &self,
        name: &str,
        fields: FieldMap<Self::Host>,
        cx: &Self::Cx,
    ) -> Result<Self::Host, EvalError>;
    /// Call an associated fn (`Deployment::new(..)`).
    fn assoc(
        &self,
        path: &str,
        args: Vec<Value<Self::Host>>,
        cx: &Self::Cx,
    ) -> Result<Self::Host, EvalError>;
    /// Dispatch a method on a host receiver (`d.module`, `r.output`, `cx.state`).
    /// Returns an `EvalError` if the receiver has no such method.
    fn call_method(
        &self,
        recv: &Self::Host,
        method: &str,
        args: Vec<Value<Self::Host>>,
        cx: &Self::Cx,
    ) -> Result<Value<Self::Host>, EvalError>;
    /// Read a whitelisted field of a host value (`cx.project_name`). The host
    /// carries whatever it needs (e.g. the injected context).
    fn host_field(&self, host: &Self::Host, field: &str) -> Result<Value<Self::Host>, EvalError>;
    /// Seed the top-level `cx` binding as a host value for `deployment(cfg, cx)`.
    fn cx_host(&self, cx: &Self::Cx) -> Self::Host;
    /// Unwrap the `deployment()` return host into the realized deployment.
    fn finish(&self, ret: Self::Host) -> Result<Self::Output, EvalError>;
}
