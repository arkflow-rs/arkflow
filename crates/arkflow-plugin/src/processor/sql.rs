/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! SQL processor component
//!
//! DataFusion is used to process data with SQL queries.

use crate::{context_pool::SessionContextPool, expr};
use arkflow_core::component::{register_processor_metadata, ComponentMetadata};
use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::temporary::Temporary;
use arkflow_core::{Error, MessageBatch, MessageBatchRef, ProcessResult, Resource};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableType;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::{Expr as LogicalExpr, LogicalPlan};
use datafusion::optimizer::OptimizerConfig;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use datafusion::sql::parser::Statement;
use expr::Expr;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

const DEFAULT_TABLE_NAME: &str = "flow";
/// SQL processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SqlProcessorConfig {
    /// SQL query statement
    query: String,

    /// Table name (used in SQL queries)
    table_name: Option<String>,

    temporary_list: Option<Vec<TemporaryConfig>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TemporaryConfig {
    name: String,
    table_name: String,
    key: Expr<String>,
}

/// In-memory table whose contents are swapped before every batch execution.
///
/// The SQL statement is fixed per processor, so its analyzed and optimized
/// logical plan only depends on the batch schema. Caching that plan removes
/// per-batch re-analysis and re-optimization; the cached plan's table scans
/// resolve back to this provider, so execution always reads the freshly
/// swapped batch.
#[derive(Debug)]
struct SwapBatchTable {
    current: RwLock<RecordBatch>,
}

impl SwapBatchTable {
    fn new(batch: RecordBatch) -> Self {
        Self {
            current: RwLock::new(batch),
        }
    }

    fn swap(&self, batch: RecordBatch) {
        *self.current.write().unwrap() = batch;
    }
}

#[async_trait]
impl TableProvider for SwapBatchTable {
    fn schema(&self) -> SchemaRef {
        self.current.read().unwrap().schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[LogicalExpr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batch = self.current.read().unwrap().clone();
        let exec: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![batch]], self.schema(), projection.cloned())?;
        Ok(exec)
    }
}

/// Per pooled-context fast-path state: the swap table registered under the
/// configured table name plus the optimized plan cached for its schema.
struct ContextPlanCache {
    table: Arc<SwapBatchTable>,
    cached: Option<(SchemaRef, Arc<LogicalPlan>)>,
}

/// SQL processor component
struct SqlProcessor {
    config: SqlProcessorConfig,
    statement: Statement,
    #[allow(clippy::type_complexity)]
    temporary: Option<HashMap<String, (Arc<dyn Temporary>, TemporaryConfig)>>,
    context_pool: Arc<SessionContextPool>,
    /// Keyed by pooled context address; a context is used by one caller at a
    /// time, so entries are never accessed concurrently for the same key.
    context_caches: std::sync::Mutex<HashMap<usize, ContextPlanCache>>,
}

impl SqlProcessor {
    /// Create a new SQL processor component.
    pub fn new(config: SqlProcessorConfig, resource: &Resource) -> Result<Self, Error> {
        let temporary = {
            if let Some(temporary_list) = config.temporary_list.as_ref() {
                let mut temporary_map = HashMap::with_capacity(temporary_list.len());
                for temporary in temporary_list {
                    let Some(t) = resource.temporary.get(&temporary.name) else {
                        return Err(Error::Process(format!(
                            "Temporary {} not found",
                            temporary.name
                        )));
                    };
                    temporary_map.insert(temporary.name.clone(), (t.clone(), temporary.clone()));
                }

                Some(temporary_map)
            } else {
                None
            }
        };

        // Create SessionContext pool with 4 contexts
        let context_pool = Arc::new(SessionContextPool::new(4)?);

        let ctx = SessionContext::new();
        let statement = ctx
            .state()
            .sql_to_statement(&config.query, &ctx.state().options().sql_parser.dialect)
            .map_err(|e| Error::Process(format!("SQL query error: {}", e)))?;
        Ok(Self {
            config,
            statement,
            temporary,
            context_pool,
            context_caches: std::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Execute SQL query
    async fn execute_query(&self, batch: MessageBatch) -> Result<RecordBatch, Error> {
        // Acquire a session context from the pool
        let ctx_arc = self.context_pool.acquire().await?;

        let table_name = self
            .config
            .table_name
            .as_deref()
            .unwrap_or(DEFAULT_TABLE_NAME);
        let record: RecordBatch = batch.into();

        let result_batches = if self.temporary.is_some() {
            // Temporary tables vary per batch, so every batch re-plans.
            self.get_temporary_message_batch(&ctx_arc, &record).await?;
            ctx_arc
                .register_batch(table_name, record)
                .map_err(|e| Error::Process(format!("Registration failed: {}", e)))?;
            let df = self
                .execute_query_with_statement(&ctx_arc)
                .await
                .map_err(|e| Error::Process(format!("Execution query error: {}", e)))?;
            let batches = df
                .collect()
                .await
                .map_err(|e| Error::Process(format!("Collection query results error: {}", e)))?;
            let _ = ctx_arc.deregister_table(table_name);
            // Temporary tables are re-registered on every batch; leaving one
            // behind would fail the next registration on this pooled context.
            if let Some(temporary) = self.temporary.as_ref() {
                for (_, (_, config)) in temporary.iter() {
                    let _ = ctx_arc.deregister_table(&config.table_name);
                }
            }
            batches
        } else {
            self.execute_with_cached_plan(&ctx_arc, table_name, record)
                .await?
        };

        // Release the context back to the pool
        self.context_pool.release_context(ctx_arc).await;

        if result_batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        if result_batches.len() == 1 {
            return Ok(result_batches[0].clone());
        }

        arrow::compute::concat_batches(&result_batches[0].schema(), &result_batches)
            .map_err(|e| Error::Process(format!("Batch merge failed: {}", e)))
    }

    fn sql_options() -> SQLOptions {
        SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false)
    }

    /// Fast path without temporary tables: swap the batch into a provider
    /// registered on the context and reuse the cached optimized plan whenever
    /// the batch schema is unchanged; only the physical stage re-plans.
    async fn execute_with_cached_plan(
        &self,
        ctx: &Arc<SessionContext>,
        table_name: &str,
        record: RecordBatch,
    ) -> Result<Vec<RecordBatch>, Error> {
        let key = Arc::as_ptr(ctx) as usize;
        let table = {
            let mut caches = self.context_caches.lock().unwrap();
            match caches.get_mut(&key) {
                Some(cache) => {
                    cache.table.swap(record);
                    cache.table.clone()
                }
                None => {
                    let table = Arc::new(SwapBatchTable::new(record));
                    ctx.register_table(table_name, table.clone())
                        .map_err(|e| Error::Process(format!("Registration failed: {}", e)))?;
                    caches.insert(
                        key,
                        ContextPlanCache {
                            table: table.clone(),
                            cached: None,
                        },
                    );
                    table
                }
            }
        };

        let schema = table.schema();
        let cached_plan = {
            let caches = self.context_caches.lock().unwrap();
            caches
                .get(&key)
                .and_then(|cache| cache.cached.as_ref())
                .filter(|(cached_schema, _)| *cached_schema == schema)
                .map(|(_, plan)| plan.clone())
        };

        let plan = match cached_plan {
            Some(plan) => plan,
            None => {
                let state = ctx.state();
                let plan = state
                    .statement_to_plan(self.statement.clone())
                    .await
                    .map_err(|e| Error::Process(format!("SQL planning error: {}", e)))?;
                Self::sql_options()
                    .verify_plan(&plan)
                    .map_err(|e| Error::Process(format!("SQL verification error: {}", e)))?;
                let optimized = Arc::new(
                    state
                        .optimize(&plan)
                        .map_err(|e| Error::Process(format!("SQL optimize error: {}", e)))?,
                );
                let mut caches = self.context_caches.lock().unwrap();
                if let Some(cache) = caches.get_mut(&key) {
                    cache.cached = Some((schema, optimized.clone()));
                }
                optimized
            }
        };

        let state = ctx.state();
        let physical = state
            .query_planner()
            .create_physical_plan(&plan, &state)
            .await
            .map_err(|e| Error::Process(format!("Physical planning error: {}", e)))?;
        datafusion::physical_plan::collect(physical, ctx.task_ctx())
            .await
            .map_err(|e| Error::Process(format!("Collection query results error: {}", e)))
    }

    async fn get_temporary_message_batch(
        &self,
        ctx: &Arc<SessionContext>,
        batch: &RecordBatch,
    ) -> Result<(), Error> {
        let Some(temporary_map) = &self.temporary else {
            return Ok(());
        };

        use futures::future::join_all;

        let futures = temporary_map.iter().map(|(_, (temporary, config))| async {
            let columnar_value = match &config.key {
                Expr::Expr { expr: expr_str } => expr::evaluate_expr(expr_str, batch)
                    .await
                    .map_err(|e| Error::Process(format!("Evaluate expression failed: {}", e)))?,
                Expr::Value { value } => {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.clone())))
                }
            };

            if let Some(data) = temporary.get(&[columnar_value]).await? {
                ctx.register_batch(&config.table_name, data.into())
                    .map_err(|e| {
                        Error::Process(format!("Register temporary message batch failed: {}", e))
                    })?;
            }
            Ok::<_, Error>(())
        });

        let results = join_all(futures).await;
        for result in results {
            result?;
        }
        Ok(())
    }

    async fn execute_query_with_statement(
        &self,
        ctx: &Arc<SessionContext>,
    ) -> Result<DataFrame, DataFusionError> {
        let sql_options = Self::sql_options();

        let plan = ctx
            .state()
            .statement_to_plan(self.statement.clone())
            .await?;
        sql_options.verify_plan(&plan)?;

        ctx.execute_logical_plan(plan).await
    }
}

#[async_trait]
impl Processor for SqlProcessor {
    async fn process(&self, msg_batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        // If the batch is empty, return empty result
        if msg_batch.is_empty() {
            return Ok(ProcessResult::None);
        }

        // Execute SQL query
        let result_batch = self.execute_query((*msg_batch).clone()).await?;
        Ok(ProcessResult::Single(Arc::new(MessageBatch::new_arrow(
            result_batch,
        ))))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

struct SqlProcessorBuilder;
impl ProcessorBuilder for SqlProcessorBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Batch processor configuration is missing".to_string(),
            ));
        }
        let config: SqlProcessorConfig = serde_json::from_value(config.clone().unwrap())?;

        Ok(Arc::new(SqlProcessor::new(config, resource)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_processor_builder("sql", Arc::new(SqlProcessorBuilder))?;
    register_processor_metadata(ComponentMetadata::with_schema(
        "sql",
        "Runs a DataFusion SQL query against each batch. Supports window functions and joins against temporary tables.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "query": {"type": "string", "description": "SQL query to run on every batch."},
                "table_name": {"type": "string", "description": "Name used for the batch table in the query (default 'flow')."},
                "temporary_list": {
                    "type": "array",
                    "description": "Temporary tables to register before running the query.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "table_name": {"type": "string"},
                            "key": {"type": "object", "properties": {"value": {"type": "string"}}, "required": ["value"]}
                        },
                        "required": ["name", "table_name", "key"]
                    }
                }
            },
            "required": ["query"]
        }),
    ).with_example(serde_json::json!({
        "query": "SELECT *, __meta_source AS source FROM flow"
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field};
    use std::cell::RefCell;

    #[tokio::test]
    async fn test_sql_processor_basic_query() {
        let processor = SqlProcessor::new(
            SqlProcessorConfig {
                query: "SELECT * FROM flow".to_string(),
                table_name: None,
                temporary_list: None,
            },
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(batch)))
            .await
            .unwrap();

        match result {
            ProcessResult::Single(batch) => {
                assert_eq!(batch.len(), 3);
            }
            _ => panic!("Expected single result"),
        }
    }

    #[tokio::test]
    async fn test_sql_processor_empty_batch() {
        let processor = SqlProcessor::new(
            SqlProcessorConfig {
                query: "SELECT * FROM flow".to_string(),
                table_name: None,
                temporary_list: None,
            },
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        )
        .unwrap();

        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(RecordBatch::new_empty(
                Arc::new(Schema::empty()),
            ))))
            .await
            .unwrap();

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_sql_processor_invalid_query() {
        let processor = SqlProcessor::new(
            SqlProcessorConfig {
                query: "INVALID SQL QUERY".to_string(),
                table_name: None,
                temporary_list: None,
            },
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        );

        assert!(processor.is_err());
    }

    #[tokio::test]
    async fn test_sql_processor_custom_table_name() {
        let processor = SqlProcessor::new(
            SqlProcessorConfig {
                query: "SELECT * FROM custom_table".to_string(),
                table_name: Some("custom_table".to_string()),
                temporary_list: None,
            },
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42]))]).unwrap();

        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(batch)))
            .await
            .unwrap();

        match result {
            ProcessResult::Single(batch) => {
                assert_eq!(batch.len(), 1);
            }
            _ => panic!("Expected single result"),
        }
    }

    #[tokio::test]
    async fn test_sql_processor_plan_cache_reflects_each_batch() {
        let processor = SqlProcessor::new(
            SqlProcessorConfig {
                query: "SELECT sum(id) as total FROM flow".to_string(),
                table_name: None,
                temporary_list: None,
            },
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let sum_of = |values: Vec<i64>| {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(values))],
            )
            .unwrap();
            async {
                match processor
                    .process(Arc::new(MessageBatch::new_arrow(batch)))
                    .await
                    .unwrap()
                {
                    ProcessResult::Single(batch) => batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    _ => panic!("Expected single result"),
                }
            }
        };

        assert_eq!(sum_of(vec![1, 2, 3, 4, 5]).await, 15);
        // A cached plan must not serve the previous batch's data.
        assert_eq!(sum_of(vec![10, 11]).await, 21);
        assert_eq!(sum_of(vec![100]).await, 100);
    }

    #[tokio::test]
    async fn test_sql_processor_plan_cache_schema_drift_replans() {
        let processor = SqlProcessor::new(
            SqlProcessorConfig {
                query: "SELECT sum(value) as total FROM flow".to_string(),
                table_name: None,
                temporary_list: None,
            },
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        )
        .unwrap();

        let int_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let int_batch = RecordBatch::try_new(
            int_schema,
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(int_batch)))
            .await
            .unwrap();
        match result {
            ProcessResult::Single(batch) => assert_eq!(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                3
            ),
            _ => panic!("Expected single result"),
        }

        // A schema change must invalidate the cached plan, not fail on it.
        let float_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));
        let float_batch = RecordBatch::try_new(
            float_schema,
            vec![Arc::new(datafusion::arrow::array::Float64Array::from(vec![
                1.5, 2.5,
            ]))],
        )
        .unwrap();
        let result = processor
            .process(Arc::new(MessageBatch::new_arrow(float_batch)))
            .await
            .unwrap();
        match result {
            ProcessResult::Single(batch) => assert_eq!(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Float64Array>()
                    .unwrap()
                    .value(0),
                4.0
            ),
            _ => panic!("Expected single result"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_sql_processor_plan_cache_concurrent_batches() {
        let processor = Arc::new(
            SqlProcessor::new(
                SqlProcessorConfig {
                    query: "SELECT min(id) as lo, max(id) as hi FROM flow".to_string(),
                    table_name: None,
                    temporary_list: None,
                },
                &Resource {
                    temporary: Default::default(),
                    input_names: RefCell::new(Default::default()),
                },
            )
            .unwrap(),
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let mut handles = Vec::new();
        for task in 0..4i64 {
            let processor = processor.clone();
            let schema = schema.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..20i64 {
                    let value = task * 1000 + i;
                    let batch = RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(Int64Array::from(vec![value]))],
                    )
                    .unwrap();
                    match processor
                        .process(Arc::new(MessageBatch::new_arrow(batch)))
                        .await
                        .unwrap()
                    {
                        ProcessResult::Single(batch) => {
                            let lo = batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .value(0);
                            let hi = batch
                                .column(1)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .value(0);
                            // Every context must see the batch it was given.
                            assert_eq!(lo, value);
                            assert_eq!(hi, value);
                        }
                        _ => panic!("Expected single result"),
                    }
                }
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
    }

    struct StaticTemporary(RecordBatch);

    #[async_trait]
    impl Temporary for StaticTemporary {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn get(&self, _keys: &[ColumnarValue]) -> Result<Option<MessageBatch>, Error> {
            Ok(Some(MessageBatch::new_arrow(self.0.clone())))
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_sql_processor_temporary_tables_join() {
        let reference = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("extra", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["enriched"]))],
        )
        .unwrap();

        let mut temporary: HashMap<String, Arc<dyn Temporary>> = HashMap::new();
        temporary.insert("ref".to_string(), Arc::new(StaticTemporary(reference)));

        let processor = SqlProcessor::new(
            SqlProcessorConfig {
                query: "SELECT f.id, t.extra FROM flow f JOIN temp_ref t ON 1=1".to_string(),
                table_name: None,
                temporary_list: Some(vec![TemporaryConfig {
                    name: "ref".to_string(),
                    table_name: "temp_ref".to_string(),
                    key: Expr::Value {
                        value: "ignored".to_string(),
                    },
                }]),
            },
            &Resource {
                temporary,
                input_names: RefCell::new(Default::default()),
            },
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));

        // The slow path re-registers per batch; run it twice.
        for expected in [7i64, 9i64] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![expected]))],
            )
            .unwrap();
            let result = processor
                .process(Arc::new(MessageBatch::new_arrow(batch)))
                .await
                .unwrap();
            match result {
                ProcessResult::Single(batch) => {
                    assert_eq!(
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(0),
                        expected
                    );
                    assert_eq!(
                        batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(0),
                        "enriched"
                    );
                }
                _ => panic!("Expected single result"),
            }
        }
    }

    #[tokio::test]
    async fn test_sql_processor_explain_query_on_cache_path() {
        let processor = SqlProcessor::new(
            SqlProcessorConfig {
                query: "EXPLAIN SELECT sum(id) as total FROM flow".to_string(),
                table_name: None,
                temporary_list: None,
            },
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        // Twice: the second run goes through the cached optimized plan.
        for _ in 0..2 {
            let result = processor
                .process(Arc::new(MessageBatch::new_arrow(batch.clone())))
                .await
                .unwrap();
            match result {
                ProcessResult::Single(batch) => {
                    assert!(batch.num_rows() > 0);
                }
                _ => panic!("Expected single result"),
            }
        }
    }

    #[tokio::test]
    async fn test_sql_processor_context_pool_performance() {
        let processor = SqlProcessor::new(
            SqlProcessorConfig {
                query: "SELECT * FROM flow WHERE id > 0".to_string(),
                table_name: None,
                temporary_list: None,
            },
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();

        // Run multiple queries to test pool effectiveness
        let start = std::time::Instant::now();
        for _ in 0..10 {
            processor
                .process(Arc::new(MessageBatch::new_arrow(batch.clone())))
                .await
                .unwrap();
        }
        let duration = start.elapsed();

        // With context pool, 10 queries should complete in < 100ms
        // Without pool, this would typically take > 500ms
        assert!(
            duration.as_millis() < 500,
            "Context pool performance test failed: {}ms",
            duration.as_millis()
        );

        println!("10 queries completed in {:?}", duration);
    }
}
