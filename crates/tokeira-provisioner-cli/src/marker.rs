//! The operation-marker gate (task 19.4, Req 9.7/11).
//!
//! An interrupted `upgrade`/`rollback` leaves its marker open in the
//! envelope. Recovery is **by re-running that same verb** — its steps are
//! idempotent and read the marker's `phase` to skip completed work; there is
//! no separate `resume` verb (dropped by the spec — recovery re-enters
//! through the front door, under the same gate and lock as the original
//! run).
//!
//! While a marker is open, exactly three things are permitted:
//!
//! - the **in-flight verb** re-run (resumes from the recorded phase),
//! - **`rollback`** (aborts an interrupted upgrade forward to `A` — the
//!   checkpoint the transfer captured is exactly what rollback consumes),
//! - **`describe`** (read-only; never gates — diagnostics must work
//!   precisely when everything else refuses).
//!
//! Every other mutating verb refuses with the marker named: mutating a
//! half-transferred deployment through an unrelated verb would interleave
//! with the recovery the marker exists to make possible.

use anyhow::Result;
use tokeira_provisioner::{DeploymentStateEnvelope, Operation, OperationKind};

/// What the gate decided for a mutating verb.
#[derive(Debug)]
pub(crate) enum MarkerDisposition {
    /// No marker is open — the verb proceeds normally.
    Proceed,
    /// This verb IS the in-flight operation — proceed in resume mode,
    /// skipping the phases the marker records as done.
    Resume(Operation),
}

/// The gate for verbs that never resume: refuse on any open marker.
pub(crate) fn refuse_if_marked(envelope: &DeploymentStateEnvelope, verb: &str) -> Result<()> {
    check_marker(envelope, verb, None).map(|_| ())
}

/// Gate a mutating verb against the envelope's operation marker.
/// `verb` names the caller; `resumes` is the marker kind that verb recovers
/// (`None` for verbs that never resume — they refuse on any open marker).
pub(crate) fn check_marker(
    envelope: &DeploymentStateEnvelope,
    verb: &str,
    resumes: Option<OperationKind>,
) -> Result<MarkerDisposition> {
    let Some(operation) = &envelope.operation else {
        return Ok(MarkerDisposition::Proceed);
    };
    // `rollback` is additionally the abort path for an interrupted upgrade:
    // it consumes the checkpoint the transfer captured, superseding the
    // upgrade marker with its own.
    if verb == "rollback" && operation.kind == OperationKind::UpgradeInFlight {
        return Ok(MarkerDisposition::Proceed);
    }
    if resumes == Some(operation.kind) {
        return Ok(MarkerDisposition::Resume(operation.clone()));
    }
    anyhow::bail!(
        "a {} operation is in flight (id {}, phase '{}') — `{verb}` refuses while its marker \
         is open. Recover by re-running the interrupted verb ({}), or `rollback` to abort an \
         interrupted upgrade; `describe` always works.",
        match operation.kind {
            OperationKind::UpgradeInFlight => "upgrade",
            OperationKind::RollbackInFlight => "rollback",
        },
        operation.operation_id,
        operation.phase,
        match operation.kind {
            OperationKind::UpgradeInFlight => "`upgrade`",
            OperationKind::RollbackInFlight => "`rollback`",
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope_with(kind: OperationKind) -> DeploymentStateEnvelope {
        DeploymentStateEnvelope {
            operation: Some(Operation {
                operation_id: "op-1".into(),
                kind,
                phase: "ownership-transferred".into(),
                audit_log: None,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn no_marker_proceeds() {
        let env = DeploymentStateEnvelope::default();
        assert!(matches!(
            check_marker(&env, "apply", None).unwrap(),
            MarkerDisposition::Proceed
        ));
    }

    #[test]
    fn an_unrelated_verb_refuses_while_a_marker_is_open() {
        let env = envelope_with(OperationKind::UpgradeInFlight);
        let err = check_marker(&env, "apply", None).expect_err("refuses");
        assert!(err.to_string().contains("in flight"), "unexpected: {err}");
        assert!(
            err.to_string().contains("re-running"),
            "recovery guidance named: {err}"
        );
    }

    #[test]
    fn the_in_flight_verb_resumes_with_its_phase() {
        let env = envelope_with(OperationKind::UpgradeInFlight);
        match check_marker(&env, "upgrade", Some(OperationKind::UpgradeInFlight)).unwrap() {
            MarkerDisposition::Resume(op) => {
                assert_eq!(op.phase, "ownership-transferred");
            }
            other => panic!("expected Resume, got {other:?}"),
        }
    }

    #[test]
    fn rollback_aborts_an_interrupted_upgrade() {
        let env = envelope_with(OperationKind::UpgradeInFlight);
        assert!(matches!(
            check_marker(&env, "rollback", Some(OperationKind::RollbackInFlight)).unwrap(),
            MarkerDisposition::Proceed
        ));
    }

    #[test]
    fn rollback_resumes_its_own_marker() {
        let env = envelope_with(OperationKind::RollbackInFlight);
        assert!(matches!(
            check_marker(&env, "rollback", Some(OperationKind::RollbackInFlight)).unwrap(),
            MarkerDisposition::Resume(_)
        ));
    }

    #[test]
    fn upgrade_refuses_an_open_rollback_marker() {
        // The abort path is one-directional: an interrupted ROLLBACK is
        // finished by rollback, never escaped by a new upgrade.
        let env = envelope_with(OperationKind::RollbackInFlight);
        assert!(check_marker(&env, "upgrade", Some(OperationKind::UpgradeInFlight)).is_err());
    }
}
