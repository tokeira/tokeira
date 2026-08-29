//! Compile-verified authored surface for a Tokeira CI Dagger module.
//!
//! This crate is the engine-free half of the dagger-module-ci spike (see
//! `../README.md`): it proves, against the pinned rust.3 SDK and macros, what the
//! authored source of a module-mode `tkr ci` interior would look like — the root
//! object, bar-shaped check functions carrying `role = "check"` so `dagger check`
//! discovers them, the report object that crosses the module wire, and the probe
//! function for the documented ~30s long-call engine regression (finding F3).
//!
//! Nothing here executes against an engine. Function bodies are shape-only stubs:
//! the container-driving interiors are already proven in client-mode by
//! `tokeira-build`'s pipelines against the same GraphQL surface, and the module
//! build path is blocked at rust.3 (findings F1/F2). What compiling this crate
//! establishes is the authoring contract: which signatures, attributes, and types
//! the fork SDK accepts for a CI-shaped module.

// Outside the test profile nothing can consume the authored items: dispatch
// registration lives in the generated bridge, which is stubbed here (findings
// F1/F2 make generation unreachable at rust.3). The unit test is the only caller.
#![cfg_attr(not(test), allow(dead_code))]
// Macro-emitted, not authored: at rust.3, `#[sdk::object]` on a fieldless struct
// expands an expression clippy flags as `unused_unit`, and an item-site allow does
// not reach the emitted items (SDK macro-hygiene feedback, ../README.md).
#![allow(clippy::unused_unit)]

use dagger_sdk as sdk;

// Stand-in for the checked generated bridge. A real module project receives a
// `dagger_generated` module from `dagger generate` (committed, digest-verified);
// generation is unreachable while finding F2 stands, and the SDK's own authoring
// compile fixture (`module_authoring/pass/foundations.rs` @ rust.3) sanctions this
// same shim for engine-free compile verification.
mod dagger_generated {
    pub mod __private {
        pub use crate::sdk::__private::*;
    }
}

/// One finished check's outcome — the wire projection of the report row that
/// client-mode `run_ci_checks` returns in-process (`CiCheckResult` in
/// `tokeira-build`). Fields cross the module boundary as a typed Dagger object,
/// not JSON-in-a-string; this is the seam finding F2's fix unblocks for real
/// round-trip measurement.
#[sdk::object]
pub(crate) struct CiCheckOutcome {
    /// Bar-check name exactly as the finishing bar spells it (e.g. `fmt`,
    /// `nextest`).
    #[dagger(field)]
    check: String,
    /// Verdict. `dagger check` reads the function's success/failure; this field
    /// preserves the verdict when outcomes are aggregated into a report instead.
    #[dagger(field)]
    passed: bool,
    /// One-line operator-facing summary.
    #[dagger(field)]
    summary: String,
}

/// Root object of the Tokeira CI module: the module-mode counterpart of
/// `run_ci_checks`. Each bar check is one function; `role = "check"` is what makes
/// `dagger check` (and check patterns like `ci:fmt`) enumerate it.
#[sdk::object(root, rename = "tokeiraCi")]
pub(crate) struct TokeiraCi {}

#[sdk::methods]
impl TokeiraCi {
    /// Modules are constructed per call; the CI module carries no configuration —
    /// the workspace source arrives per-function as context.
    #[dagger(constructor)]
    fn new() -> Self {
        Self {}
    }

    /// The cheap bar check: rustfmt under the pinned nightly, `--check`.
    ///
    /// The contextual-default parameter is the load-bearing authoring feature for
    /// CI: when the caller passes nothing, the engine supplies the workspace source
    /// with `target/` excluded at the mount — the same `target/`-excluding filter
    /// the client-mode pipeline applies by hand (release-process Req 7.4). The
    /// macros enforce that `context` (injected call context) and value metadata
    /// like `default_path` are mutually exclusive; a contextual directory is an
    /// ordinary parameter with a contextual default, not injected context.
    #[dagger(function, role = "check")]
    async fn fmt(
        &self,
        #[dagger(default_path = "/", ignore = ["target", ".git"])] source: sdk::Directory,
    ) -> CiCheckOutcome {
        // Shape-only: the real body mounts `source` into the builder toolchain
        // container and execs `cargo +nightly fmt --all --check`.
        let _ = source;
        CiCheckOutcome {
            check: "fmt".into(),
            passed: true,
            summary: "shape-only stub".into(),
        }
    }

    /// The expensive bar check: `cargo nextest run --workspace --locked`.
    ///
    /// This is the function that cannot be trusted until finding F3 is probed: a
    /// workspace nextest run holds one module→engine query open for many minutes,
    /// and rust.3 documents an unverified ~30s `unexpected EOF` regression on
    /// exactly that boundary.
    #[dagger(function, role = "check")]
    async fn nextest(
        &self,
        #[dagger(default_path = "/", ignore = ["target", ".git"])] source: sdk::Directory,
    ) -> CiCheckOutcome {
        let _ = source;
        CiCheckOutcome {
            check: "nextest".into(),
            passed: true,
            summary: "shape-only stub".into(),
        }
    }

    /// Finding-F3 probe, ready to run the day findings F1/F2 clear: holds a single
    /// module call open for `seconds` by awaiting one long engine operation (the
    /// real body execs `sleep <seconds>` in a container and returns its output).
    /// Bisecting `seconds` across the documented ~30s boundary answers whether
    /// module-mode can carry real CI checks at all.
    #[dagger(function)]
    async fn probe_long_call(&self, #[dagger(default = 45)] seconds: i64) -> String {
        format!("shape-only stub: would hold one engine query open for {seconds}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // With the generated bridge stubbed (see `dagger_generated` above), no dispatch
    // registry consumes the authored items, so this test is what proves them
    // reachable: the macros emit ordinary Rust the crate can call directly.
    #[test]
    fn authored_surface_is_callable_rust() {
        let ci = TokeiraCi::new();
        let probe = ci.probe_long_call(5);
        drop(probe);
        // Directory-taking checks need a live session to construct arguments;
        // referencing the fn items keeps the shape honest without an engine.
        let _ = TokeiraCi::fmt;
        let _ = TokeiraCi::nextest;
        let outcome = CiCheckOutcome {
            check: "fmt".into(),
            passed: true,
            summary: "constructed in a plain unit test".into(),
        };
        assert!(outcome.passed);
        assert_eq!(outcome.check, "fmt");
        assert!(!outcome.summary.is_empty());
    }
}
