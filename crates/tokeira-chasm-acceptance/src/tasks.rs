//! Resource retry timers use derived identities and carry their own codec.
//! The component owns backoff; each retry starts a fresh one-attempt activity.

use serde::{Deserialize, Serialize};
use tokeira_chasm::{ChasmError, Task, TaskKind};

/// A timer fenced by both the desired generation and consecutive-failure count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryTimer {
    /// Desired generation when the failed outcome was applied.
    pub generation: u64,
    /// Failure count when this timer was staged.
    pub attempt: u32,
    /// Absolute CHASM-clock deadline.
    pub fire_at_nanos: i64,
}

impl Task for RetryTimer {
    const KIND: TaskKind = TaskKind::Pure;
    const FQN: &'static str = "acceptance.retry";

    fn fire_at(&self) -> Option<i64> {
        Some(self.fire_at_nanos)
    }

    fn encode(&self) -> Result<Vec<u8>, ChasmError> {
        postcard::to_allocvec(self)
            .map_err(|error| ChasmError::Internal(format!("encode {}: {error}", Self::FQN)))
    }

    fn decode(bytes: &[u8]) -> Result<Self, ChasmError> {
        postcard::from_bytes(bytes)
            .map_err(|error| ChasmError::Validation(format!("decode {}: {error}", Self::FQN)))
    }
}

/// Nanosecond delay after a failure: one second, doubling to a sixty-second cap.
/// Zero is treated as the first failure; large counts cannot overflow.
pub fn backoff(attempt: u32) -> i64 {
    (1_i64 << attempt.saturating_sub(1).min(6)).min(60) * 1_000_000_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_codec_and_backoff_boundaries() {
        for attempt in [0, 1, 2, 6, 7, u32::MAX] {
            let timer = RetryTimer {
                generation: 7,
                attempt,
                fire_at_nanos: backoff(attempt),
            };
            assert_eq!(RetryTimer::decode(&timer.encode().unwrap()).unwrap(), timer);
            assert_eq!(timer.fire_at(), Some(timer.fire_at_nanos));
        }
        assert_eq!(
            (0..=8).map(backoff).collect::<Vec<_>>(),
            vec![1, 1, 2, 4, 8, 16, 32, 60, 60]
                .into_iter()
                .map(|s| s * 1_000_000_000)
                .collect::<Vec<_>>()
        );
        assert_eq!(backoff(u32::MAX), 60_000_000_000);
    }
}
