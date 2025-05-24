use datafusion::arrow::compute::concat_batches;
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_expr::Expr;
use std::sync::Arc;
use tpchgen_arrow::RecordBatchIterator;

/// Defines a table function provider and its implementation using [`tpchgen`]
/// as the data source.
macro_rules! define_tpch_udtf_provider {
    ($TABLE_FUNCTION_NAME:ident, $TABLE_FUNCTION_SQL_NAME:ident, $GENERATOR:ty, $ARROW_GENERATOR:ty) => {
        #[doc = concat!(
                                            "A table function that generates the `",
                                            stringify!($TABLE_FUNCTION_SQL_NAME),
                                            "` table using the `tpchgen` library."
                                        )]
        ///
        /// The expected arguments are a float literal for the scale factor,
        /// an i64 literal for the part, and an i64 literal for the number of parts.
        /// The second and third arguments are optional and will default to 1
        /// for both values which tells the generator to generate all parts.
        ///
        /// # Examples
        /// ```
        /// #[tokio::main]
        /// async fn main() -> Result<()> {
        ///     // create local execution context
        ///     let ctx = SessionContext::new();

        ///     // Register all the UDTFs.
        ///     ctx.register_udtf(TpchNation::name(), Arc::new(TpchNation {}));
        ///     ctx.register_udtf(TpchCustomer::name(), Arc::new(TpchCustomer {}));
        ///     ctx.register_udtf(TpchOrders::name(), Arc::new(TpchOrders {}));
        ///     ctx.register_udtf(TpchLineitem::name(), Arc::new(TpchLineitem {}));
        ///     ctx.register_udtf(TpchPart::name(), Arc::new(TpchPart {}));
        ///     ctx.register_udtf(TpchPartsupp::name(), Arc::new(TpchPartsupp {}));
        ///     ctx.register_udtf(TpchSupplier::name(), Arc::new(TpchSupplier {}));
        ///     ctx.register_udtf(TpchRegion::name(), Arc::new(TpchRegion {}));
        ///     // Generate the nation table with a scale factor of 1.
        ///     let df = ctx
        ///         .sql(format!("SELECT * FROM tpch_nation(1.0);").as_str())
        ///         .await?;
        ///     df.show().await?;
        ///     Ok(())
        /// }
        /// ```
        #[derive(Debug)]
        pub struct $TABLE_FUNCTION_NAME {}

        impl $TABLE_FUNCTION_NAME {
            /// Returns the name of the table function.
            pub fn name() -> &'static str {
                stringify!($TABLE_FUNCTION_SQL_NAME)
            }
        }

        impl TableFunctionImpl for $TABLE_FUNCTION_NAME {
            /// Implementation of the UDTF invocation for TPCH table generation
            /// using the [`tpchgen`] library.
            ///
            /// The first argument is a float literal that specifies the scale factor.
            /// The second argument is the part to generate.
            /// The third argument is the number of parts to generate.
            ///
            /// The second and third argument are optional and will default to 1
            /// for both values which tells the generator to generate all parts.
            fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
                let Some(Expr::Literal(ScalarValue::Float64(Some(value)))) = args.get(0) else {
                    return plan_err!("First argument must be a float literal.");
                };

                // Default values for part and num_parts.
                let part = 1;
                let num_parts = 1;

                // Check if we have more arguments `part` and `num_parts` respectively
                // and if they are i64 literals.
                if args.len() > 1 {
                    // Check if the second argument and third arguments are i64 literals and
                    // greater than 0.
                    let Some(Expr::Literal(ScalarValue::Int64(Some(part)))) = args.get(1) else {
                        return plan_err!("Second argument must be an i64 literal.");
                    };
                    let Some(Expr::Literal(ScalarValue::Int64(Some(num_parts)))) = args.get(2)
                    else {
                        return plan_err!("Third argument must be an i64 literal.");
                    };
                    if *part < 0 || *num_parts < 0 {
                        return plan_err!("Second and third arguments must be greater than 0.");
                    }
                }

                // Init the table generator.
                let tablegen = <$GENERATOR>::new(*value, part, num_parts);

                // Init the arrow provider.
                let mut arrow_tablegen = <$ARROW_GENERATOR>::new(tablegen);

                // The arrow provider is a batched generator with a default batch size of 8000
                // so to build the full table we need to drain it completely.
                let mut batches = Vec::new();
                while let Some(batch) = arrow_tablegen.next() {
                    batches.push(batch);
                }
                // Use `concat_batches` to create a single batch from the vector of batches.
                // This is needed because the `MemTable` provider requires a single batch.
                // This is a bit of a hack, but it works.
                let batch = concat_batches(arrow_tablegen.schema(), &batches)?;

                // Build the memtable plan.
                let provider =
                    MemTable::try_new(arrow_tablegen.schema().clone(), vec![vec![batch]])?;

                Ok(Arc::new(provider))
            }
        }
    };
}

define_tpch_udtf_provider!(
    TpchNation,
    tpch_nation,
    tpchgen::generators::NationGenerator,
    tpchgen_arrow::NationArrow
);

define_tpch_udtf_provider!(
    TpchCustomer,
    tpch_customer,
    tpchgen::generators::CustomerGenerator,
    tpchgen_arrow::CustomerArrow
);

define_tpch_udtf_provider!(
    TpchOrders,
    tpch_orders,
    tpchgen::generators::OrderGenerator,
    tpchgen_arrow::OrderArrow
);

define_tpch_udtf_provider!(
    TpchLineitem,
    tpch_lineitem,
    tpchgen::generators::LineItemGenerator,
    tpchgen_arrow::LineItemArrow
);

define_tpch_udtf_provider!(
    TpchPart,
    tpch_part,
    tpchgen::generators::PartGenerator,
    tpchgen_arrow::PartArrow
);

define_tpch_udtf_provider!(
    TpchPartsupp,
    tpch_partsupp,
    tpchgen::generators::PartSuppGenerator,
    tpchgen_arrow::PartSuppArrow
);

define_tpch_udtf_provider!(
    TpchSupplier,
    tpch_supplier,
    tpchgen::generators::SupplierGenerator,
    tpchgen_arrow::SupplierArrow
);

define_tpch_udtf_provider!(
    TpchRegion,
    tpch_region,
    tpchgen::generators::RegionGenerator,
    tpchgen_arrow::RegionArrow
);

/// Registers all the TPCH UDTFs in the given session context.
pub fn register_tpch_udtfs(ctx: &SessionContext) -> Result<()> {
    ctx.register_udtf(TpchNation::name(), Arc::new(TpchNation {}));
    ctx.register_udtf(TpchCustomer::name(), Arc::new(TpchCustomer {}));
    ctx.register_udtf(TpchOrders::name(), Arc::new(TpchOrders {}));
    ctx.register_udtf(TpchLineitem::name(), Arc::new(TpchLineitem {}));
    ctx.register_udtf(TpchPart::name(), Arc::new(TpchPart {}));
    ctx.register_udtf(TpchPartsupp::name(), Arc::new(TpchPartsupp {}));
    ctx.register_udtf(TpchSupplier::name(), Arc::new(TpchSupplier {}));
    ctx.register_udtf(TpchRegion::name(), Arc::new(TpchRegion {}));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::SessionContext;

    #[tokio::test]
    async fn test_register_all_tpch_functions() -> Result<()> {
        let ctx = SessionContext::new();

        // Register all the UDTFs.
        register_tpch_udtfs(&ctx)?;

        // Test all the UDTFs, the constants were computed using the tpchgen library
        // and the expected values are the number of rows and columns for each table.
        let test_cases = vec![
            (TpchNation::name(), 25, 4),
            (TpchCustomer::name(), 150000, 8),
            (TpchOrders::name(), 1500000, 9),
            (TpchLineitem::name(), 6001215, 16),
            (TpchPart::name(), 200000, 9),
            (TpchPartsupp::name(), 800000, 5),
            (TpchSupplier::name(), 10000, 7),
            (TpchRegion::name(), 5, 3),
        ];

        for (function, expected_rows, expected_columns) in test_cases {
            let df = ctx
                .sql(&format!("SELECT * FROM {}(1.0)", function))
                .await?
                .collect()
                .await?;

            assert_eq!(df.len(), 1);
            assert_eq!(
                df[0].num_rows(),
                expected_rows,
                "{}: {}",
                function,
                expected_rows
            );
            assert_eq!(
                df[0].num_columns(),
                expected_columns,
                "{}: {}",
                function,
                expected_columns
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_register_individual_tpch_functions() -> Result<()> {
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

        // Test all the UDTFs, the constants were computed using the tpchgen library
        // and the expected values are the number of rows and columns for each table.
        let test_cases = vec![
            (TpchNation::name(), 25, 4),
            (TpchCustomer::name(), 150000, 8),
            (TpchOrders::name(), 1500000, 9),
            (TpchLineitem::name(), 6001215, 16),
            (TpchPart::name(), 200000, 9),
            (TpchPartsupp::name(), 800000, 5),
            (TpchSupplier::name(), 10000, 7),
            (TpchRegion::name(), 5, 3),
        ];

        for (function, expected_rows, expected_columns) in test_cases {
            let df = ctx
                .sql(&format!("SELECT * FROM {}(1.0)", function))
                .await?
                .collect()
                .await?;

            assert_eq!(df.len(), 1);
            assert_eq!(
                df[0].num_rows(),
                expected_rows,
                "{}: {}",
                function,
                expected_rows
            );
            assert_eq!(
                df[0].num_columns(),
                expected_columns,
                "{}: {}",
                function,
                expected_columns
            );
        }
        Ok(())
    }
}
