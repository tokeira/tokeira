//! Immutable platform runtime-fact construction and typed context dispatch.

use std::{fmt::Debug, marker::PhantomData, path::PathBuf};

use serde::Serialize;

use crate::{author::AuthorNode, error::ContextError};

/// Shell-owned facts from which a platform constructs one immutable evaluation context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvocationContext {
    /// Stable operator-visible deployment identity.
    pub deployment_id: String,
    /// Stable UUID recorded for the deployment.
    pub deployment_uuid: uuid::Uuid,
    /// Optional admitted environment identity.
    pub environment: Option<String>,
    /// Optional provider-resolved or recorded region fact.
    pub region: Option<String>,
    /// Optional provider-resolved account identity fact.
    pub account_id: Option<String>,
    /// Host deployment root; platform contexts keep it private from author code.
    pub deployment_dir: PathBuf,
}

/// One argument to a platform context method after token resolution.
#[derive(Debug, Clone)]
pub enum ContextArgument<T> {
    /// Ordinary host-free author value.
    Value(AuthorNode),
    /// Typed value previously produced by this context.
    Token(T),
}

/// One platform context result before the author session interns typed values.
#[derive(Debug, Clone)]
pub enum ContextProjection<T> {
    /// Ordinary host-free author value.
    Value(AuthorNode),
    /// Typed value that must remain opaque to the definition frontend.
    Token(T),
}

/// Immutable, platform-specific facts available during one definition evaluation.
pub trait PlatformContext: Clone + Debug + Send + Sync + 'static {
    /// Serializable opaque values produced by specialized context operations.
    type Value: Clone + Debug + Serialize + Send + Sync + 'static;

    /// Complete context field inventory for frontend schema discovery and errors.
    fn fields() -> &'static [&'static str];

    /// Complete context method inventory for frontend schema discovery and errors.
    fn methods() -> &'static [&'static str];

    /// Read an allow-listed immutable field.
    fn field(&self, name: &str) -> Result<ContextProjection<Self::Value>, ContextError>;

    /// Invoke an allow-listed pure context projection.
    fn call(
        &self,
        method: &str,
        args: &[ContextArgument<Self::Value>],
    ) -> Result<ContextProjection<Self::Value>, ContextError>;
}

/// Construction contract kept separate from author-visible context access.
#[derive(Clone, Copy)]
pub struct ContextContract<C: PlatformContext> {
    construct: fn(&InvocationContext) -> Result<C, ContextError>,
    authoring: fn() -> Result<C, ContextError>,
    marker: PhantomData<fn() -> C>,
}

impl<C: PlatformContext> std::fmt::Debug for ContextContract<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContextContract").finish_non_exhaustive()
    }
}

impl<C: PlatformContext> ContextContract<C> {
    /// Bind live and deterministic authoring-mode constructors.
    pub const fn new(
        construct: fn(&InvocationContext) -> Result<C, ContextError>,
        authoring: fn() -> Result<C, ContextError>,
    ) -> Self {
        Self {
            construct,
            authoring,
            marker: PhantomData,
        }
    }

    /// Construct a context from shell-admitted invocation facts.
    pub fn construct(&self, input: &InvocationContext) -> Result<C, ContextError> {
        (self.construct)(input)
    }

    /// Construct deterministic placeholder facts for provider-free authoring checks.
    pub fn authoring(&self) -> Result<C, ContextError> {
        (self.authoring)()
    }
}
