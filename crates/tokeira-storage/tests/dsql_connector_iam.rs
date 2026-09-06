#![cfg(feature = "dsql-integration")]

//! Opt-in IAM and TLS round trip through the production DSQL connection factory.
//!
//! Both endpoint and region must be supplied explicitly. An unset gate returns
//! without connecting; this test neither provisions a cluster nor changes schema.

use anyhow::Result;
use tokeira_storage::dsql::ConnectionFactory;

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
    Ok(())
}
