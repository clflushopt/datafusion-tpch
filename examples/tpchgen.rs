//! Example of using the datafusion-tpch extension to generate TPCH datasets
//! on the the fly in datafusion.

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_tpch::register_tpch_udtf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
    register_tpch_udtf(&ctx);

    let sql_df = ctx.sql(&format!("SELECT * FROM tpch(1.0);")).await?;
    sql_df.show().await?;

    let sql_df = ctx.sql(&format!("SHOW TABLES;")).await?;
    sql_df.show().await?;

    let sql_df = ctx.sql(&format!("SELECT * FROM nation LIMIT 5;")).await?;
    sql_df.show().await?;

    let sql_df = ctx.sql(&format!("SELECT * FROM partsupp LIMIT 5;")).await?;
    sql_df.show().await?;

    let sql_df = ctx.sql(&format!("SELECT * FROM region LIMIT 5;")).await?;
    sql_df.show().await?;

    let sql_df = ctx.sql(&format!("SELECT * FROM customer LIMIT 5;")).await?;
    sql_df.show().await?;

    let sql_df = ctx.sql(&format!("SELECT * FROM orders LIMIT 5;")).await?;
    sql_df.show().await?;

    let sql_df = ctx.sql(&format!("SELECT * FROM lineitem LIMIT 5;")).await?;
    sql_df.show().await?;

    let sql_df = ctx.sql(&format!("SELECT * FROM part LIMIT 5;")).await?;
    sql_df.show().await?;
    Ok(())
}
