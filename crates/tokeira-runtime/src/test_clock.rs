//! Test wall clock anchored away from host time and advanced by Tokio time.
//!
//! Deadline tests share this clock with their scanners so pausing or advancing
//! Tokio affects both the wait and the wall-clock deadline check.

use time::{Duration, OffsetDateTime};
use tokeira_clock::Clock;

/// A fixed-epoch clock that follows Tokio's elapsed time, including paused time.
#[derive(Debug)]
pub(crate) struct TestClock(tokio::time::Instant);

impl TestClock {
    /// Begin at the test epoch independently of the host's current wall time.
    pub(crate) fn new() -> Self {
        Self(tokio::time::Instant::now())
    }
}

impl Clock for TestClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
            + Duration::try_from(self.0.elapsed()).unwrap()
    }
}
