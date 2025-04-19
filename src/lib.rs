use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::datasource::memory::MemTable;
use datafusion_expr::Expr;
use std::sync::Arc;
use tpchgen_arrow::{NationArrow, RecordBatchIterator};

/// A Table function that returns a table provider exposing the TPCH
/// dataset.
/// The function takes a single argument, which is an integer value
/// representing the dataset scale factor.
#[derive(Debug)]
pub struct TpchgenFunction {}

impl TableFunctionImpl for TpchgenFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let Some(Expr::Literal(ScalarValue::Float64(Some(value)))) = exprs.get(0) else {
            return plan_err!("First argument must be a float literal.");
        };

        // Create a list of tuples mapping the TPCH table names to their corresponding
        // schemas.
        let tables = vec![(
            "nation",
            tpchgen::generators::NationGenerator::new(*value, 0, 0),
        )];

        // Create a single RecordBatch with the value as a single column
        let mut table_arrow_gen = NationArrow::new(tables[0].1.clone());
        let batch = table_arrow_gen.next().unwrap();

        // Create a MemTable plan that returns the RecordBatch
        let provider = MemTable::try_new(table_arrow_gen.schema().clone(), vec![vec![batch]])?;

        Ok(Arc::new(provider))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::SessionContext;

    #[tokio::test]
    async fn test_tpchgen_function() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_udtf("tpchgen", Arc::new(TpchgenFunction {}));

        let df = ctx
            .sql("SELECT * FROM tpchgen(1.0)")
            .await?
            .collect()
            .await?;

        assert_eq!(df.len(), 1);
        assert_eq!(df[0].num_rows(), 25);
        assert_eq!(df[0].num_columns(), 4);
        Ok(())
    }
}
