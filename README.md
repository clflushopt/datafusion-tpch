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

The `datafusion-tpch` crate offers two possible ways to register the TPCH individual
table functions.

You can register functions individually.

```rust
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    // Register all the UDTFs.
    ctx.register_udtf(TpchNation::name(), Arc::new(TpchNation {}));
    ctx.register_udtf(TpchCustomer::name(), Arc::new(TpchCustomer {}));
    ctx.register_udtf(TpchOrders::name(), Arc::new(TpchOrders {}));
    ctx.register_udtf(TpchLineitem::name(), Arc::new(TpchLineitem {}));
    ctx.register_udtf(TpchPart::name(), Arc::new(TpchPart {}));
    ctx.register_udtf(TpchPartsupp::name(), Arc::new(TpchPartsupp {}));
    ctx.register_udtf(TpchSupplier::name(), Arc::new(TpchSupplier {}));
    ctx.register_udtf(TpchRegion::name(), Arc::new(TpchRegion {}));
 
    // Generate the nation table with a scale factor of 1.
    let df = ctx
        .sql(format!("SELECT * FROM tpch_nation(1.0);").as_str())
        .await?;
    df.show().await?;
    Ok(())
}
```

Or use the helper function `register_tpch_udtfs` to register all of them
at once (which is the preferred approach).

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

## License

The project is licensed under the [APACHE 2.0](LICENSE) license.