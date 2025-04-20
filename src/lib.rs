use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::datasource::memory::MemTable;
use datafusion_expr::Expr;
use std::sync::Arc;
use tpchgen_arrow::{NationArrow, RecordBatchIterator};

/// Table function that returns the TPCH nation table.
#[derive(Debug)]
pub struct TpchNationFunction {}

impl TableFunctionImpl for TpchNationFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let Some(Expr::Literal(ScalarValue::Float64(Some(value)))) = args.get(0) else {
            return plan_err!("First argument must be a float literal.");
        };

        // Init the table generator.
        let tablegen = tpchgen::generators::NationGenerator::new(*value, 0, 0);

        // Init the arrow provider.
        let mut arrow_tablegen = NationArrow::new(tablegen);

        let batch = arrow_tablegen.next().unwrap();

        // Build the memtable plan.
        let provider = MemTable::try_new(arrow_tablegen.schema().clone(), vec![vec![batch]])?;

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
        ctx.register_udtf("tpchgen_nation", Arc::new(TpchNationFunction {}));

        let df = ctx
            .sql("SELECT * FROM tpchgen_nation(1.0)")
            .await?
            .collect()
            .await?;

        assert_eq!(df.len(), 1);
        assert_eq!(df[0].num_rows(), 25);
        assert_eq!(df[0].num_columns(), 4);
        Ok(())
    }
}
