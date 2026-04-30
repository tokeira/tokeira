//! State-machine model for query consistency verification.
//!
//! This module models the server-side state transitions that affect
//! query delivery correctness. It does NOT model the full Tokeira
//! runtime — only the subset relevant to the invariant:
//!
//! **No query with `required_barrier = B` is ever evaluated by a
//! worker whose visible state is `< B`.**
//!
//! The model uses proptest to explore random interleavings of:
//! - Signal arrival
//! - Query arrival
//! - Poll (WFT dispatch to worker)
//! - WFT completion
//! - Eager WFT return
//!
//! Each interleaving is checked against the invariant.

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use std::collections::VecDeque;

    // ── Server-side state ──

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum WftState {
        /// No WFT exists.
        None,
        /// WFT is scheduled but no worker has polled it yet.
        Scheduled,
        /// WFT has been dispatched to a worker. The worker's
        /// visible history goes up to `worker_visible_event_id`.
        Started { worker_visible_event_id: i64 },
    }

    #[derive(Clone, Debug)]
    struct BufferedQuery {
        id: u32,
        required_barrier: i64,
    }

    #[derive(Clone, Debug)]
    struct DeliveredQuery {
        id: u32,
        required_barrier: i64,
        /// The event ID the worker will have replayed when
        /// it evaluates this query.
        worker_eval_event_id: i64,
    }

    #[derive(Clone, Debug)]
    struct ServerState {
        /// Monotonically increasing event counter.
        last_event_id: i64,
        /// Current WFT lifecycle state.
        wft: WftState,
        /// Queries waiting for delivery.
        buffered: VecDeque<BufferedQuery>,
        /// Queries that have been delivered to a worker.
        /// We track these to check the invariant when the
        /// worker "evaluates" them.
        delivered: Vec<DeliveredQuery>,
        /// All invariant violations found.
        violations: Vec<String>,
        /// Counter for generating unique query IDs.
        next_query_id: u32,
        /// Whether the workflow has completed at least one WFT
        /// (needed for direct dispatch eligibility).
        has_completed_wft: bool,
    }

    impl ServerState {
        fn new() -> Self {
            // Start with a workflow that has just been created.
            // Event 1: WorkflowExecutionStarted
            // Event 2: WorkflowTaskScheduled
            Self {
                last_event_id: 2,
                wft: WftState::Scheduled,
                buffered: VecDeque::new(),
                delivered: Vec::new(),
                violations: Vec::new(),
                next_query_id: 0,
                has_completed_wft: false,
            }
        }

        /// A signal arrives. Appends a signal event to history.
        /// If no WFT is pending, schedules one.
        fn signal(&mut self) {
            // SignalReceived event
            self.last_event_id += 1;

            if self.wft == WftState::None {
                // WFT-Scheduled event
                self.last_event_id += 1;
                self.wft = WftState::Scheduled;
            }
        }

        /// A query arrives. Captures required_barrier and either
        /// buffers or direct-dispatches.
        fn query(&mut self) {
            let id = self.next_query_id;
            self.next_query_id += 1;
            let required_barrier = self.last_event_id;

            match &self.wft {
                WftState::None if self.has_completed_wft => {
                    // Direct dispatch: no WFT in flight, run is
                    // quiescent. The worker will evaluate against
                    // the current state (last_event_id).
                    self.delivered.push(DeliveredQuery {
                        id,
                        required_barrier,
                        worker_eval_event_id: self.last_event_id,
                    });
                }
                _ => {
                    // Buffer: there is a pending/started WFT, or
                    // no WFT has ever completed (can't direct-dispatch).
                    self.buffered.push_back(BufferedQuery {
                        id,
                        required_barrier,
                    });
                }
            }
        }

        /// A worker polls for a WFT. Transitions Scheduled → Started.
        /// Attaches buffered queries whose barrier is satisfied.
        fn poll(&mut self) {
            if self.wft != WftState::Scheduled {
                return; // Nothing to poll.
            }

            // WFT-Started event
            self.last_event_id += 1;
            let worker_visible = self.last_event_id;
            self.wft = WftState::Started {
                worker_visible_event_id: worker_visible,
            };

            // Attach buffered queries whose barrier is satisfied
            // by this task's history AND no other started WFT exists
            // (there can only be one, so this is always safe here).
            let mut remaining = VecDeque::new();
            for q in self.buffered.drain(..) {
                if q.required_barrier <= worker_visible {
                    self.delivered.push(DeliveredQuery {
                        id: q.id,
                        required_barrier: q.required_barrier,
                        worker_eval_event_id: worker_visible,
                    });
                } else {
                    remaining.push_back(q);
                }
            }
            self.buffered = remaining;
        }

        /// The worker completes the current WFT.
        /// Checks for buffered events and schedules a new WFT if needed.
        /// Optionally does eager return of buffered queries.
        fn complete(&mut self, eager_return: bool) {
            let started_event_id = match &self.wft {
                WftState::Started {
                    worker_visible_event_id,
                } => *worker_visible_event_id,
                _ => return, // Nothing to complete.
            };

            // WFT-Completed event
            self.last_event_id += 1;
            let pre_completion_last = self.last_event_id;
            self.wft = WftState::None;
            self.has_completed_wft = true;

            // Check for buffered events: events that arrived between
            // WFT-Started and now (excluding the completion event itself).
            // pre_completion_last includes the completion event, so we
            // compare against started_event_id + 1 (the completion event).
            // If there are events between started and completion, those
            // are buffered events (e.g., signals).
            let has_buffered_events = (pre_completion_last - 1) > started_event_id;

            if has_buffered_events {
                // Schedule a new WFT for the buffered events.
                self.last_event_id += 1;
                self.wft = WftState::Scheduled;
            }

            // Eager return: if quiescent and queries are buffered,
            // deliver them inline. The worker's cached state is at
            // started_event_id (what it replayed during this WFT).
            if eager_return && self.wft == WftState::None && !self.buffered.is_empty() {
                // The worker's cached state after this completion
                // includes everything up to started_event_id (what
                // it replayed). The eager return delivers a query-only
                // WFT with empty history — the worker evaluates against
                // its cached state.
                let worker_cached = started_event_id;
                let mut remaining = VecDeque::new();
                for q in self.buffered.drain(..) {
                    if q.required_barrier <= worker_cached {
                        self.delivered.push(DeliveredQuery {
                            id: q.id,
                            required_barrier: q.required_barrier,
                            worker_eval_event_id: worker_cached,
                        });
                    } else {
                        remaining.push_back(q);
                    }
                }
                self.buffered = remaining;
            }
        }

        /// Check the invariant on all delivered queries.
        fn check_invariant(&mut self) {
            for dq in &self.delivered {
                if dq.worker_eval_event_id < dq.required_barrier {
                    self.violations.push(format!(
                        "VIOLATION: query {} required barrier {} but worker \
                         evaluated at event {} (delta: {})",
                        dq.id,
                        dq.required_barrier,
                        dq.worker_eval_event_id,
                        dq.required_barrier - dq.worker_eval_event_id,
                    ));
                }
            }
        }
    }

    // ── Actions that proptest can generate ──

    #[derive(Clone, Debug)]
    enum Action {
        Signal,
        Query,
        Poll,
        Complete { eager_return: bool },
    }

    fn action_strategy() -> impl Strategy<Value = Action> {
        prop_oneof![
            2 => Just(Action::Signal),
            3 => Just(Action::Query),
            3 => Just(Action::Poll),
            2 => any::<bool>().prop_map(|eager_return| Action::Complete { eager_return }),
        ]
    }

    // ── The property test ──

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        #[test]
        fn query_barrier_invariant_holds(
            actions in prop::collection::vec(action_strategy(), 1..30)
        ) {
            let mut state = ServerState::new();

            for action in &actions {
                match action {
                    Action::Signal => state.signal(),
                    Action::Query => state.query(),
                    Action::Poll => state.poll(),
                    Action::Complete { eager_return } => state.complete(*eager_return),
                }
            }

            state.check_invariant();

            if !state.violations.is_empty() {
                let trace: Vec<String> = actions.iter().map(|a| format!("{a:?}")).collect();
                panic!(
                    "Invariant violations found!\n\
                     Actions: {}\n\
                     Violations:\n  {}\n\
                     Final state: {:?}",
                    trace.join(" → "),
                    state.violations.join("\n  "),
                    state,
                );
            }
        }
    }

    // ── Targeted scenario tests ──

    /// The exact message_passing scenario: start → signal → query.
    /// The query must see the signal.
    #[test]
    fn signal_then_query_scenario() {
        let mut state = ServerState::new();

        // Workflow started: events 1-2, WFT scheduled.
        assert_eq!(state.last_event_id, 2);
        assert_eq!(state.wft, WftState::Scheduled);

        // Worker polls WFT₁.
        state.poll();
        // Events: 1=Started, 2=Scheduled, 3=WFT-Started
        assert_eq!(state.last_event_id, 3);
        assert!(matches!(
            state.wft,
            WftState::Started {
                worker_visible_event_id: 3
            }
        ));

        // Signal arrives while WFT₁ is started.
        state.signal();
        // Event 4: SignalReceived. No new WFT (one is started).
        assert_eq!(state.last_event_id, 4);
        assert!(matches!(state.wft, WftState::Started { .. }));

        // Query arrives. required_barrier = 4 (includes signal).
        state.query();
        assert_eq!(state.buffered.len(), 1);
        assert_eq!(state.buffered[0].required_barrier, 4);

        // Worker completes WFT₁ with eager return.
        state.complete(true);
        // Event 5: WFT-Completed.
        // pre_completion_last=5, started_event_id=3.
        // has_buffered_events: (5-1) > 3 → 4 > 3 → true.
        // New WFT scheduled: event 6.
        // Eager return: wft != None (Scheduled), so NO eager return.
        assert_eq!(state.wft, WftState::Scheduled);
        // Query should still be buffered (not eagerly returned).
        assert_eq!(state.buffered.len(), 1);
        assert!(state.delivered.is_empty());

        // Worker polls WFT₂ (the signal WFT).
        state.poll();
        // Event 7: WFT-Started. worker_visible = 7.
        // Query barrier = 4 ≤ 7 → attached.
        assert!(state.buffered.is_empty());
        assert_eq!(state.delivered.len(), 1);
        assert_eq!(state.delivered[0].worker_eval_event_id, 7);
        assert!(state.delivered[0].worker_eval_event_id >= state.delivered[0].required_barrier);

        // Check invariant.
        state.check_invariant();
        assert!(state.violations.is_empty());
    }

    /// Query on a fully idle workflow (no pending WFT).
    #[test]
    fn query_on_idle_workflow() {
        let mut state = ServerState::new();

        // Poll and complete the initial WFT.
        state.poll();
        state.complete(false);
        assert_eq!(state.wft, WftState::None);
        assert!(state.has_completed_wft);

        // Query arrives on idle workflow. Direct dispatch.
        state.query();
        assert!(state.buffered.is_empty());
        assert_eq!(state.delivered.len(), 1);

        state.check_invariant();
        assert!(state.violations.is_empty());
    }

    /// Two signals then a query. The query must see both signals.
    #[test]
    fn two_signals_then_query() {
        let mut state = ServerState::new();

        state.poll(); // WFT₁ started
        state.signal(); // Signal 1 while WFT₁ in progress
        state.signal(); // Signal 2 while WFT₁ in progress
        state.query(); // Query: barrier includes both signals

        let barrier = state.buffered[0].required_barrier;

        state.complete(false); // WFT₁ done, new WFT scheduled (buffered events)
        state.poll(); // WFT₂ started, query attached

        assert_eq!(state.delivered.len(), 1);
        assert!(state.delivered[0].worker_eval_event_id >= barrier);

        state.check_invariant();
        assert!(state.violations.is_empty());
    }

    /// Query arrives before any WFT has completed (no direct dispatch).
    #[test]
    fn query_before_first_wft_completion() {
        let mut state = ServerState::new();

        // Query arrives while initial WFT is scheduled.
        state.query();
        assert_eq!(state.buffered.len(), 1);
        assert!(state.delivered.is_empty());

        // Poll and complete.
        state.poll();
        // Query should be attached to this WFT.
        assert!(state.buffered.is_empty());
        assert_eq!(state.delivered.len(), 1);

        state.check_invariant();
        assert!(state.violations.is_empty());
    }

    /// Eager return only fires when quiescent.
    #[test]
    fn eager_return_blocked_by_pending_wft() {
        let mut state = ServerState::new();

        state.poll(); // WFT₁ started
        state.signal(); // Signal while WFT₁ in progress
        state.query(); // Query buffered

        // Complete with eager return requested.
        state.complete(true);
        // Buffered events → new WFT scheduled → NOT quiescent → no eager return.
        assert_eq!(state.wft, WftState::Scheduled);
        assert_eq!(state.buffered.len(), 1); // Query still buffered.
        assert!(state.delivered.is_empty());

        // Poll WFT₂ → query attached.
        state.poll();
        assert!(state.buffered.is_empty());
        assert_eq!(state.delivered.len(), 1);

        state.check_invariant();
        assert!(state.violations.is_empty());
    }

    /// Eager return fires when truly quiescent (no buffered events).
    #[test]
    fn eager_return_fires_when_quiescent() {
        let mut state = ServerState::new();

        state.poll(); // WFT₁ started (event 3)
        state.complete(false); // WFT₁ done, no buffered events, quiescent

        // Now idle. Signal arrives → WFT scheduled.
        state.signal();
        state.poll(); // WFT₂ started
        state.complete(false); // WFT₂ done, quiescent

        // Query arrives on idle workflow.
        // has_completed_wft=true, wft=None → direct dispatch.
        state.query();
        assert!(state.buffered.is_empty());
        assert_eq!(state.delivered.len(), 1);

        state.check_invariant();
        assert!(state.violations.is_empty());
    }

    /// Query arrives exactly when WFT is scheduled but not yet polled.
    #[test]
    fn query_while_wft_scheduled() {
        let mut state = ServerState::new();

        state.poll();
        state.complete(false);
        // Quiescent.

        state.signal(); // WFT scheduled
        state.query(); // WFT is Scheduled → buffer

        assert_eq!(state.buffered.len(), 1);

        state.poll(); // WFT started, query attached
        assert!(state.buffered.is_empty());
        assert_eq!(state.delivered.len(), 1);

        state.check_invariant();
        assert!(state.violations.is_empty());
    }
}
