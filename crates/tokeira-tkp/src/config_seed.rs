//! `tkp config seed` — create-time server-config rendering.
//!
//! Invoked by `tkr deployment create` against the staging directory, after
//! the staged definition check and before the deployment is published.
//! Evaluation only: no provider access, no engine state, no lock — nothing
//! else can reach a staging directory. No bound platform renders a seed
//! today, and absence is success here: the generic seeded document stays,
//! and create proceeds — unlike scale, where the refusal is the answer.

use anyhow::Result;

use crate::platform::Admitted;

pub(crate) fn seed(_admitted: &Admitted) -> Result<()> {
    println!("config seed skipped: this platform does not seed server configuration");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn absence_is_success_so_create_never_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let (_engine, admitted) = crate::testkit::engine(tmp.path());
        seed(&admitted).expect("an unseeded platform keeps the generic seed");
    }
}
