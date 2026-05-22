# Controller Ownership Churn Spike

Bundle ownership is changing unusually quickly.

Likely causes: runtime restarts, lease instability, bad drain loop behavior, or unstable membership.

First dashboard: Placement Controller.

First queries: `tokeira_controller_bundle_ownership_churn_total`, `tokeira_controller_membership_nodes_total`, `tokeira_controller_drain_active_nodes`.

Safe remediation: inspect recent deploys and node health, pause voluntary drains, and verify leases are being renewed.

Escalate if churn causes NotShardOwner spikes.

Related alerts: ControllerGenerationCasFailures.
