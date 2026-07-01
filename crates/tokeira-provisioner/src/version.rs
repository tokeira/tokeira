//! Monotonic version ordering for the binding gate and the upgrade boundary.
//!
//! Ordering is by numeric `major.minor.patch[…]` components; any pre-release or
//! build suffix (after `-` or `+`) is ignored. A version that cannot be parsed
//! yields `None` — callers must not infer an ordering (never a spurious
//! downgrade / advance) from an unparseable version.

use std::cmp::Ordering;

fn parse_version(version: &str) -> Option<Vec<u64>> {
    let core = version.split(['-', '+']).next().unwrap_or(version);
    core.split('.').map(|part| part.parse::<u64>().ok()).collect()
}

/// Compare two versions numerically. `None` when either is unparseable.
pub(crate) fn compare_versions(a: &str, b: &str) -> Option<Ordering> {
    match (parse_version(a), parse_version(b)) {
        (Some(a), Some(b)) => Some(a.cmp(&b)),
        _ => None,
    }
}

/// Whether `a` is strictly older than `b`. `false` when order is undeterminable.
pub(crate) fn is_older(a: &str, b: &str) -> bool {
    matches!(compare_versions(a, b), Some(Ordering::Less))
}
