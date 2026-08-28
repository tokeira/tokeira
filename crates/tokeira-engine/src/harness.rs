//! Process-global hooks a test harness installs before serving.
//!
//! The functional-conformance harness (spec
//! `.kiro/specs/conformance-config-override/`) needs to reshape a served
//! engine in ways production never does: substitute an environment-driven
//! authenticator, wrap the Nexus HTTP authorizer, keep the standalone-activity
//! timer sweeper armed for live enablement, and mount an out-of-band control
//! listener. That machinery is never published, so this crate cannot name it;
//! the application assembling a conformance server installs the behaviour here
//! as plain trait objects before calling [`run_from_cli`](crate::run_from_cli).
//!
//! Installation is process-global and write-once, mirroring the environment
//! variables the hooks are derived from: every engine stack built in the
//! process after `install` observes the same hooks, exactly as every stack
//! observes the same environment. A production process installs nothing and
//! every hook read falls back to the default behaviour, so this module is
//! inert dead weight outside a harness.

use std::sync::{Arc, OnceLock};

use tokio_util::sync::CancellationToken;

use tokeira_edge::Authenticator;

/// Wraps the production Nexus HTTP authorizer with harness behaviour, or
/// returns it unchanged when the harness environment does not ask for a wrap.
pub type NexusAuthorizerWrap = Box<
    dyn Fn(
            Arc<dyn tokeira_edge::nexus_http::NexusHttpAuthorizer>,
        ) -> Arc<dyn tokeira_edge::nexus_http::NexusHttpAuthorizer>
        + Send
        + Sync,
>;

/// Spawns one harness-owned background task per served stack, tied to that
/// stack's background cancellation token. Invoked at the same point the stack
/// spawns its own background work, once per engine boot in the process.
pub type BackgroundTask = Box<dyn Fn(CancellationToken) + Send + Sync>;

/// Hooks a harness installs to reshape every subsequently served stack.
///
/// Every field defaults to "no change from production". The hooks are read at
/// stack-construction time only; installing after a stack is built does not
/// retrofit it.
#[derive(Default)]
pub struct HarnessHooks {
    /// Replaces the allow-all fallback authenticator used when the
    /// configuration names no identity source. Ignored when the configuration
    /// carries real authorization: the harness must not silently weaken a
    /// configured identity stack.
    pub fallback_grpc_authenticator: Option<Arc<dyn Authenticator>>,
    /// Wraps the Nexus HTTP authorizer the configuration produced.
    pub wrap_nexus_http_authorizer: Option<NexusAuthorizerWrap>,
    /// Keeps the CHASM standalone-activity timer sweeper armed even when the
    /// boot-time default is off, because the corpus enables
    /// `activity.enableStandalone` live after server startup.
    pub force_chasm_timer_sweeper: bool,
    /// Harness-owned background work spawned alongside each stack's own
    /// background tasks (the dynamic-config control listener lives here).
    pub background_task: Option<BackgroundTask>,
}

impl std::fmt::Debug for HarnessHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HarnessHooks")
            .field(
                "fallback_grpc_authenticator",
                &self.fallback_grpc_authenticator.is_some(),
            )
            .field(
                "wrap_nexus_http_authorizer",
                &self.wrap_nexus_http_authorizer.is_some(),
            )
            .field("force_chasm_timer_sweeper", &self.force_chasm_timer_sweeper)
            .field("background_task", &self.background_task.is_some())
            .finish()
    }
}

static HOOKS: OnceLock<HarnessHooks> = OnceLock::new();

/// Install the harness hooks. The first install wins; later calls are ignored
/// so repeated harness setup in one process stays idempotent.
pub fn install(hooks: HarnessHooks) {
    let _ = HOOKS.set(hooks);
}

/// The installed hooks, or `None` in a production process.
pub(crate) fn installed() -> Option<&'static HarnessHooks> {
    HOOKS.get()
}
