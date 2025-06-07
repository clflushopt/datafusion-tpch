# datafusion-tpch

[![Apache licensed][license-badge]][license-url]
[![Build Status][actions-badge]][actions-url]

[license-badge]: https://img.shields.io/badge/license-Apache%20v2-blue.svg
[license-url]: https://github.com/clflushopt/datafusion-tpch/blob/main/LICENSE
[actions-badge]: https://github.com/clflushopt/datafusion-tpch/actions/workflows/rust.yml/badge.svg
[actions-url]: https://github.com/clflushopt/datafusion-tpch/actions?query=branch%3Amain

Note: This is not an official Apache Software Foundation release.

This crate provides functions to generate the TPCH benchmark dataset for Datafusion
using the [tpchgen](https://github.com/clflushopt/tpchgen-rs) crates.

## Usage

The `datafusion-tpch` crate offers two possible ways to register the TPCH table
functions.

You can register the individual udtfs separately.

```rust
use datafusion_tpch::register_tpch_udtfs;

#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    // Register all the UDTFs.
    register_tpch_udtfs(&ctx);

    // Generate the nation table with a scale factor of 1.
    let df = ctx
        .sql(format!("SELECT * FROM tpch_nation(1.0);").as_str())
        .await?;
    df.show().await?;
    Ok(())
}
```

Or you can register a single UDTF which generates all tables at once.

```rust
use datafusion_tpch::register_tpch_udtfs;

#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    // Register all the UDTFs.
    register_tpch_udtf(&ctx);

    // Generate the nation table with a scale factor of 1.
    let df = ctx
        .sql(format!("SELECT * FROM tpch(1.0);").as_str())
        .await?;
    df.show().await?;
    Ok(())
}
```

## Examples

To keep things simple we don't bundle writing to parquet in the table provider
but instead defer that to the user who can use the `COPY` command.


```rust
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
```

You can find other examples in the [examples](examples/) directory.

### Running Examples

To quickly see the Parquet example in action, you can run the provided example directly from your terminal:

```bash
cargo run --example parquet
```

## License

The project is licensed under the [APACHE 2.0](LICENSE) license.
