//! Example of using the datafusion-tpch extension to generate TPCH tables
//! and writing them to disk via `COPY`.
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_tpch::{register_tpch_udtf, register_tpch_udtfs};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
    register_tpch_udtf(&ctx);

    let sql_df = ctx.sql(&format!("SELECT * FROM tpch(1.0);")).await?;
    sql_df.show().await?;

    let sql_df = ctx.sql(&format!("SHOW TABLES;")).await?;
    sql_df.show().await?;

    let sql_df = ctx
        .sql(&format!(
            "COPY nation TO './tpch_nation.parquet' STORED AS PARQUET"
        ))
        .await?;
    sql_df.show().await?;

    register_tpch_udtfs(&ctx)?;

    let sql_df = ctx
        .sql(&format!(
            "COPY (SELECT * FROM tpch_lineitem(1.0)) TO './tpch_lineitem_sf_10.parquet' STORED AS PARQUET"
        ))
        .await?;
    sql_df.show().await?;

    Ok(())
}
