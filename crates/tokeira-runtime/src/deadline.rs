//! Waiting for durable wall-clock deadlines without overflowing monotonic clocks.
//!
//! Stored durations can predate admission limits. Date arithmetic saturates at
//! the latest representable date; bounded waits preserve that deadline even where
//! `Instant` has a smaller range. A truncated wait must never fire a task early.

use time::{Duration, OffsetDateTime};

/// Translate a stored deadline once, then wait in bounded monotonic segments.
/// This preserves the existing relative-wait semantics and never truncates a
/// distant deadline to the range of a nanosecond counter or `Instant`.
pub async fn sleep_until(deadline: OffsetDateTime) {
    let mut remaining = deadline - OffsetDateTime::now_utc();
    while remaining > Duration::ZERO {
        let segment = remaining.min(Duration::hours(24));
        let wait = std::time::Duration::try_from(segment)
            .expect("positive bounded wait fits std duration");
        tokio::time::sleep(wait).await;
        remaining -= segment;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_kernel::deadline_after;

    #[tokio::test(start_paused = true)]
    async fn distant_deadline_does_not_fire_after_a_bounded_segment() {
        let deadline = deadline_after(OffsetDateTime::now_utc(), Duration::MAX);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_secs(2 * 86_400),
                sleep_until(deadline)
            )
            .await
            .is_err()
        );
    }
}
