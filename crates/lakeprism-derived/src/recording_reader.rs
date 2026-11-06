use std::fmt;
use std::fs::File;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

#[derive(Debug)]
pub(crate) struct SnapshotTable {
    inner: Arc<dyn TableProvider>,
    lease: Arc<File>,
}

impl SnapshotTable {
    pub(crate) fn new(inner: Arc<dyn TableProvider>, lease: Arc<File>) -> Self {
        Self { inner, lease }
    }
}

#[async_trait::async_trait]
impl TableProvider for SnapshotTable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SnapshotExecution {
            inner: self.inner.scan(state, projection, filters, limit).await?,
            lease: Arc::clone(&self.lease),
        }))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
}

#[derive(Debug)]
struct SnapshotExecution {
    inner: Arc<dyn ExecutionPlan>,
    lease: Arc<File>,
}

impl DisplayAs for SnapshotExecution {
    fn fmt_as(&self, _: DisplayFormatType, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "RecordingSnapshot")
    }
}

impl ExecutionPlan for SnapshotExecution {
    fn name(&self) -> &str {
        "RecordingSnapshot"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [inner]: [Arc<dyn ExecutionPlan>; 1] = children.try_into().map_err(|_| {
            DataFusionError::Internal("recording snapshot requires exactly one child".into())
        })?;
        Ok(Arc::new(Self {
            inner,
            lease: Arc::clone(&self.lease),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let lease = Arc::clone(&self.lease);
        let stream = self.inner.execute(partition, context)?.map(move |batch| {
            let _retained_until_stream_drop = &lease;
            batch
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
