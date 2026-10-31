//! Recovery of committed outbound obligations after volatile delivery is lost.
//!
//! Source transitions own operation lifecycle; destination commits and verified
//! outcomes settle obligations. Notifications and local attempts carry no authority.

pub(crate) mod delivery;
pub(crate) mod driver;
pub(crate) mod successor;

#[cfg(test)]
mod bug_condition_exploration_tests;
#[cfg(test)]
mod test_repository;
