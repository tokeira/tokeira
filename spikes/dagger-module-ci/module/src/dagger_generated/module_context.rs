//! PRE-GENERATION STUB — reduced faithful shape of the generated
//! `module_context.rs` (same names, same Deref-to-`Query` contract) so the
//! authored check bodies compile before `dagger generate` writes the real one.

/// Call-scoped access to the active module session (stub).
pub struct ModuleContext {
    query: ModuleQuery,
}

/// Query root bound to the active session (stub).
pub struct ModuleQuery {
    root: dagger_sdk::Query,
}

impl ModuleContext {
    /// Borrows the query root for the active session.
    pub fn query(&self) -> &ModuleQuery {
        &self.query
    }
}

impl ::core::ops::Deref for ModuleQuery {
    type Target = dagger_sdk::Query;

    fn deref(&self) -> &Self::Target {
        &self.root
    }
}
