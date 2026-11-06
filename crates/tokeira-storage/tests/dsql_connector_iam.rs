#![cfg(feature = "dsql-integration")]

//! Opt-in IAM and TLS round trip through the production DSQL connection factory,
//! and the `application_name` it gives the cluster.
//!
//! Both endpoint and region must be supplied explicitly. An unset gate returns
//! without connecting; this test neither provisions a cluster nor changes schema.

use anyhow::Result;
use tokeira_storage::dsql::{APPLICATION_NAME_PREFIX, ConnectionFactory};

#[tokio::test]
async fn connector_iam_round_trips_query() -> Result<()> {
    let (Ok(endpoint), Ok(region)) = (
        std::env::var("TOKEIRA_DSQL_TEST_ENDPOINT"),
        std::env::var("TOKEIRA_DSQL_TEST_REGION"),
    ) else {
        return Ok(());
    };
    let mut connection = ConnectionFactory::new(&endpoint, &region)?
        .create_connection()
        .await?;
    let value: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&mut connection)
        .await?;
    assert_eq!(value, 1);
    // The name the cluster attributes this connection's statements to.
    let application_name: String = sqlx::query_scalar("SELECT current_setting('application_name')")
        .fetch_one(&mut connection)
        .await?;
    assert!(
        application_name.starts_with(&format!("{APPLICATION_NAME_PREFIX}:aurora-dsql-rust-sqlx/")),
        "application_name {application_name}"
    );
    Ok(())
}
