//! Behavioural evidence outside the production dependency graph.
//!
//! The ledger keeps executed decision comparisons separate from scenario and
//! corpus evidence. Passing comparisons cannot calibrate the reference model.

pub mod ledger;
