//! Rust module-authoring values and the version-locked generated bridge boundary.
//!
//! Public application errors and call context are ordinary owned Rust values. The
//! hidden exact-version bridge carries typed wire values, codecs, static descriptors,
//! and call-scoped dispatch access needed by generated code. It cannot create or close
//! a connection, adopt process-global state, or erase values into a fallback codec.

#[cfg(feature = "gen")]
mod adapter;
mod codec;
mod context;
mod dispatch;
mod error;
mod view;
mod wire;

pub use context::{CurrentCall, ModuleCancellation, TelemetryContext};
pub use error::{ModuleError, ModuleErrorBuildError, ModuleErrorDetail};

#[doc(hidden)]
pub mod __private {
    pub use serde_json;

    #[cfg(feature = "gen")]
    pub use super::adapter::{ModuleEntrypointError, run_module_entrypoint};
    pub use super::{
        codec::__private::*,
        context::ModuleContextBase,
        dispatch::{
            CallCoordinate, CallOutcome, CallPreparationError, CallReceipt, DispatchError,
            DispatchRegistry, ErrorOutcomeKind, InvocationError, PreparedCall, PublishedOutcome,
            RegistrationError, RegistrationSink, ResultPublishError, ResultSink,
            apply_close_precedence, dispatch, handle_call, prepare_call,
        },
        error::ModuleError,
        view::{
            ArgumentView, FunctionView, ModuleDescriptorView, ModuleTypeKindView, ModuleTypeView,
            RegistrationView,
        },
        wire::{
            CallEnvelope, CallIdentity, CallSelector, ModuleFunctionName, ModuleJson,
            ModuleWireName, NamedModuleArgument,
        },
    };
}
