# Outbound recovery cutover

The outbound recovery writer requires schema V127 and the current workflow-state,
history and prepared-state envelopes. It cannot be mixed with older writers. The
cutover preserves pending work whose original intent can be verified; it does not
claim to recover previously lost parent-close actions or child results.

## Review and apply

Apply the release's normal forward migrations first. Take a compatible database
backup and stop every workflow writer, including embedded processes and old unfenced
development servers. Absence of live leases alone does not establish that condition.
Do not restart writers until preparation completes. These commands create no runtime,
node claim or listener, and use the normal DSQL connection director.

Use an explicit DSQL configuration and the usual credential provider; do not place
credentials in a command, report or repair file. Planning is read-only and may be
used for inspection before stopping writers, but that plan must be regenerated once
the cluster is stopped because its digest covers the observed maintenance status.

```bash
tokeirad storage outbound-recovery --config file:maintenance.toml plan > outbound-plan.json
```

Review `examined`, `legacy`, `pending`, `oldest_pending_at`, `historical_unknown`,
`unresolved_count`, `unresolved`, and `status.live_leases`. Diagnostics include at
most 100 unresolved entries; the count includes omitted entries. They identify the
source, sequence, fingerprint, operation and missing fields without exposing workflow
payloads. The plan digest includes the schema contract, writer algorithm, inventory,
revision and supplied repair evidence.

```bash
tokeirad storage outbound-recovery --config file:maintenance.toml apply \
  --plan-digest REVIEWED_DIGEST --confirm-stopped-writers
```

Copy the exact digest from the reviewed plan. Apply refuses a changed digest, live
writer leases, unresolved work or missing stopped-writer confirmation. It commits a
revision-guarded continuation with each source metadata update. Large states and
operation indexes are staged in bounded transactions while activation remains blocked;
the final source update publishes a complete state and index together. Public history
is not rewritten.

After interruption, repeat the same apply command with the same repair file. It resumes
the committed continuation. If the inventory, repair evidence or revision changes,
generate and review a new plan; a revised interrupted plan preserves its completed
prefix. A completed marker permits runtime activation. Startup of a nonempty database
without that marker fails with a maintenance instruction; an empty database can
establish the marker without a legacy scan.

## Missing legacy child evidence

A pre-cutover child initiation can omit its original reuse policy. Preparation first
tries to verify an existing first child run using its namespace, workflow ID, parent
run, parent namespace/workflow and initiation. A later unrelated execution with the
same workflow ID is not evidence. Adoption grants permission only to confirm that
existing child; it cannot create a replacement if that child's evidence disappears.

For unresolved legacy initiations, an operator may supply the original accepted reuse
policy from an independent authoritative record, or the identity of an existing child
whose lineage the tool verifies. Never choose a convenient default. A JSON repair file
is an array of objects with these fields:

| Field | Value |
|---|---|
| `run` | Source UUID from the inspection entry |
| `source_seq` | Exact inspected transition sequence |
| `source_digest` | Exact inspected source fingerprint |
| `initiation` | Inspected child initiation event ID |
| `original_policy` | Original `AllowDuplicate`, `AllowDuplicateFailedOnly`, `RejectDuplicate` or `TerminateIfRunning`, or `null` |
| `existing_child` | Verified first child run UUID, or `null` |

At least one evidence field must be present. Unknown fields, duplicate entries, absent
sources, stale fingerprints and policies contradicting accepted history are refused.
The file is limited to 1 MiB and 1,000 repairs. Generate a new plan using it, then apply
that exact plan using the same file:

```bash
tokeirad storage outbound-recovery --config file:maintenance.toml plan \
  --repairs outbound-repairs.json > outbound-repaired-plan.json
tokeirad storage outbound-recovery --config file:maintenance.toml apply \
  --plan-digest REVIEWED_REPAIRED_DIGEST --confirm-stopped-writers \
  --repairs outbound-repairs.json
```

Missing history, corrupt value parts or unsupported required envelopes need verified
data restoration or a reviewed repair implementation. They are not permission to mark
work delivered. A current-format unresolved operation remains discoverable, and
inspection plus the `blocked_by_data` attempt counter exposes it while other work
continues. Historical unknowns remain recorded across subsequent preparations.

## Runtime and retention diagnostics

Operation rows are derived from committed source state. Close intents are immutable
receipts of owed work, retired only after a definitive outcome. A notification or an
in-memory attempt claim is never completion evidence. Metrics distinguish observed
pending work, advanced attempts, retries, obsolete references and blocked data using
bounded labels; the maintenance report provides the oldest pending timestamp.

A pending successor reservation makes competing current-run mutations retryable. Do
not delete its row to unblock a workflow: its frozen successor, predecessor and intent
must be settled together by the owning execution home. Likewise, do not truncate
operation, intent, preparation or dedupe tables independently.

Logical deletion removes public access immediately. Physical reclamation can wait for
an owed close intent, target dedupe evidence, an open child's future result or a reset
redirect. The retained state is available only to internal recovery readers. Bounded
purging resumes after the last dependency settles. Retention delays therefore do not
mean that a deleted execution has become publicly visible again.

## Rollback boundary

Older binaries reject the new required envelopes. Rolling back code in place after
new-format writes is unsupported: use compatible data restoration and review any
effects delivered after that backup. Never mix old and new writers or remove the
completed marker to pretend the cutover did not happen.

The [approved design](../../.kiro/specs/outbound-dispatch-recovery/design.md) defines
the storage and ownership contracts; its [requirements](../../.kiro/specs/outbound-dispatch-recovery/bugfix.md)
define the public behaviour and historical recovery boundary.
