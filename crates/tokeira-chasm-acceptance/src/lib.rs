//! A test-only, long-lived resource archetype exercising CHASM without activity
//! implementation dependencies. Commands and handlers are pure; integration tests
//! supply the typed engine, real executors, scoped workers and recovery scans.
//! The root owns one active reconciliation and at most eight historical outcomes.

pub mod commands;
pub mod component;
pub mod reconcile;
pub mod state;
pub mod tasks;

pub use component::{AcceptanceLibrary, Resource};
pub use state::{Operation, OperationOutcome, ReconcileInput, ResourceState};
