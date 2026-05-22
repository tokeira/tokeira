# Controller Generation CAS Failures

Placement generation writes are conflicting or failing.

Likely causes: multiple active controllers, stale state, DSQL/storage errors, or deployment churn.

First dashboard: Placement Controller.

First queries: `tokeira_controller_generation_cas_total`, `tokeira_controller_placement_loop_duration_seconds`.

Safe remediation: confirm only one active controller is reconciling, inspect controller logs, and verify storage reachability.

Escalate if routing snapshots stop advancing.

Related alerts: ControllerOwnershipChurnSpike.
