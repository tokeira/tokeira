//! Transaction startup cancellation and preservation of permit health. Live
//! coverage exercises SQLx and the reservoir; generated traces vary poll counts
//! and outcomes without requiring a database.

use std::{
    future::{Future, poll_fn},
    task::{Context, Poll, Waker},
};

use anyhow::{Result, ensure};
use proptest::prelude::*;

use super::guard_transaction_start;
use crate::contract_tests::run_cases;

#[cfg(feature = "dsql-integration")]
mod live;

// Feature: storage-contract-tests, Property 10: Interrupted starts never restore reusable health.
#[test]
fn storage_contract_transaction_start_health() -> Result<()> {
    run_cases(
        "storage_contract_transaction_start_health",
        "pure",
        10,
        (any::<bool>(), 1_usize..16, any::<u64>()),
        |(initially_bad, pending_polls, value)| {
            // Every generated case covers every outcome; an outcome cannot go
            // untested just because a fixed seed did not select it.
            for outcome in 0..4 {
                let mut marked_bad = initially_bad;
                let mut polls = 0;
                let start = poll_fn(|_| {
                    polls += 1;
                    if polls <= pending_polls {
                        Poll::Pending
                    } else if outcome == 2 {
                        Poll::Ready(Err(value))
                    } else {
                        Poll::Ready(Ok(value))
                    }
                });
                let mut guarded = Box::pin(guard_transaction_start(&mut marked_bad, start));
                let mut cx = Context::from_waker(Waker::noop());
                if outcome != 0 {
                    for _ in 0..pending_polls {
                        ensure!(guarded.as_mut().poll(&mut cx).is_pending());
                    }
                    if outcome >= 2 {
                        let expected = if outcome == 2 { Err(value) } else { Ok(value) };
                        ensure!(guarded.as_mut().poll(&mut cx) == Poll::Ready(expected));
                    }
                }
                drop(guarded);
                let expected_bad = initially_bad || outcome == 1 || outcome == 2;
                ensure!(
                    marked_bad == expected_bad,
                    "incorrect health after outcome {outcome}"
                );
                let expected_polls = match outcome {
                    0 => 0,
                    1 => pending_polls,
                    _ => pending_polls + 1,
                };
                ensure!(polls == expected_polls, "startup was polled unexpectedly");
            }
            Ok(())
        },
    )
}
