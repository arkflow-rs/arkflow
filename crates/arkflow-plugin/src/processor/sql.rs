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

/// Built-in functions the logical optimizer folds to a literal using the
/// session's query execution start time. A cached optimized plan would freeze
/// them at cache time, so queries using them re-optimize every batch.
const TIME_FOLDING_FUNCTIONS: [&str; 4] =
    ["now", "current_date", "current_time", "current_timestamp"];

fn expr_folds_time(expr: &LogicalExpr) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut found = false;
    let _ = expr.apply(&mut |e: &LogicalExpr| {
        if let LogicalExpr::ScalarFunction(func) = e {
            if TIME_FOLDING_FUNCTIONS.contains(&func.name()) {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

fn plan_folds_time(plan: &LogicalPlan) -> bool {
    plan.expressions().iter().any(expr_folds_time)
        || plan.inputs().iter().any(|p| plan_folds_time(p))
}

/// Per pooled-context fast-path state: the swap table registered under the
/// configured table name plus the plans cached for its schema.
struct ContextPlanCache {
    table: Arc<SwapBatchTable>,
    /// Analyzed plan; analysis never folds time expressions, so it is always
    /// safe to reuse while the schema is unchanged.
    analyzed: Option<(SchemaRef, Arc<LogicalPlan>)>,
    /// Optimized plan; reused only when the query has no time-folding
    /// functions, which the optimizer would otherwise freeze at cache time.
    optimized: Option<(SchemaRef, Arc<LogicalPlan>)>,
}

/// SQL processor component
/// RAII deregistration for the per-batch tables of the temporary path:
/// the main batch table plus every temporary table. Dropping on any path
/// (success, `?` error, future cancellation) keeps pooled contexts clean.
struct TempTablesGuard {
    ctx: Arc<SessionContext>,
    main_table: String,
    temporary_tables: Vec<String>,
}

impl TempTablesGuard {
    fn new(ctx: Arc<SessionContext>, main_table: String, temporary_tables: Vec<String>) -> Self {
        Self {
            ctx,
            main_table,
            temporary_tables,
        }
    }
}

impl Drop for TempTablesGuard {
    fn drop(&mut self) {
        let _ = self.ctx.deregister_table(&self.main_table);
        for name in &self.temporary_tables {
            let _ = self.ctx.deregister_table(name);
        }
    }
}

struct SqlProcessor {
    config: SqlProcessorConfig,
    statement: Statement,
    #[allow(clippy::type_complexity)]
    temporary: Option<HashMap<String, (Arc<dyn Temporary>, TemporaryConfig)>>,
    context_pool: Arc<SessionContextPool>,
    /// Keyed by pooled context address; a context is used by one caller at a
    /// time, so entries are never accessed concurrently for the same key.
    context_caches: std::sync::Mutex<HashMap<usize, ContextPlanCache>>,
    /// Set once from the first analyzed plan; queries that fold time
    /// expressions must re-optimize every batch to keep now() fresh.
    time_dependent: std::sync::OnceLock<bool>,
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
            time_dependent: std::sync::OnceLock::new(),
        })
    }

    /// Execute SQL query
    async fn execute_query(&self, batch: MessageBatch) -> Result<RecordBatch, Error> {
        // Acquire a session context from the pool. The guard releases the
        // slot on every path — error returns below and future cancellation
        // included — so repeated failures cannot exhaust the pool and park
        // the chain silently.
        let ctx_guard = self.context_pool.acquire_guarded().await?;
        let ctx_arc = ctx_guard.context().clone();

        let table_name = self
            .config
            .table_name
            .as_deref()
            .unwrap_or(DEFAULT_TABLE_NAME);
        let record: RecordBatch = batch.into();

        let result_batches = if self.temporary.is_some() {
            // Temporary tables vary per batch, so every batch re-plans. The
            // guard deregisters the main table and every temporary table on
            // ALL paths — `?` early exits included — so a failed batch can
            // never leave stale registrations that break the next batch's
            // registration on this pooled context.
            let mut temp_table_names = Vec::new();
            if let Some(temporary) = self.temporary.as_ref() {
                for (_, config) in temporary.values() {
                    temp_table_names.push(config.table_name.clone());
                }
            }
            let _tables_guard =
                TempTablesGuard::new(ctx_arc.clone(), table_name.to_string(), temp_table_names);
            self.get_temporary_message_batch(&ctx_arc, &record).await?;
            ctx_arc
                .register_batch(table_name, record)
                .map_err(|e| Error::Process(format!("Registration failed: {}", e)))?;
            let df = self
                .execute_query_with_statement(&ctx_arc)
                .await
                .map_err(|e| Error::Process(format!("Execution query error: {}", e)))?;
            df.collect()
                .await
                .map_err(|e| Error::Process(format!("Collection query results error: {}", e)))?
        } else {
            self.execute_with_cached_plan(&ctx_arc, table_name, record)
                .await?
        };

        // The context slot is released when ctx_guard drops at the end of
        // this function, on the success and every error path alike.

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
    /// registered on the context and reuse cached plans whenever the batch
    /// schema is unchanged. The analyzed plan is always cached; the optimized
    /// plan is cached only when the query cannot fold time expressions into
    /// the plan (now/current_date/current_time), which would otherwise freeze
    /// the first batch's timestamp.
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
                            analyzed: None,
                            optimized: None,
                        },
                    );
                    table
                }
            }
        };

        let schema = table.schema();
        let cached_analyzed = {
            let caches = self.context_caches.lock().unwrap();
            caches
                .get(&key)
                .and_then(|cache| cache.analyzed.as_ref())
                .filter(|(cached_schema, _)| *cached_schema == schema)
                .map(|(_, plan)| plan.clone())
        };

        // `ctx.state()` refreshes the query execution start time per call, so
        // optimization below sees the current batch's time, not a stale one.
        let state = ctx.state();

        let analyzed = match cached_analyzed {
            Some(plan) => plan,
            None => {
                let plan = state
                    .statement_to_plan(self.statement.clone())
                    .await
                    .map_err(|e| Error::Process(format!("SQL planning error: {}", e)))?;
                Self::sql_options()
                    .verify_plan(&plan)
                    .map_err(|e| Error::Process(format!("SQL verification error: {}", e)))?;
                let _ = self.time_dependent.set(plan_folds_time(&plan));
                let plan = Arc::new(plan);
                let mut caches = self.context_caches.lock().unwrap();
                if let Some(cache) = caches.get_mut(&key) {
                    cache.analyzed = Some((schema.clone(), plan.clone()));
                }
                plan
            }
        };

        let time_dependent = self.time_dependent.get().copied().unwrap_or(false);
        let cached_optimized = if time_dependent {
            None
        } else {
            let caches = self.context_caches.lock().unwrap();
            caches
                .get(&key)
                .and_then(|cache| cache.optimized.as_ref())
                .filter(|(cached_schema, _)| *cached_schema == schema)
                .map(|(_, plan)| plan.clone())
        };

        let plan = match cached_optimized {
            Some(plan) => plan,
            None => {
                let optimized = Arc::new(
                    state
                        .optimize(&analyzed)
                        .map_err(|e| Error::Process(format!("SQL optimize error: {}", e)))?,
                );
                if !time_dependent {
                    let mut caches = self.context_caches.lock().unwrap();
                    if let Some(cache) = caches.get_mut(&key) {
                        cache.optimized = Some((schema, optimized.clone()));
                    }
                }
                optimized
            }
        };

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

    /// Spec "Processor error paths release pooled resources": repeated
    /// schema-drift planning errors must not leak pool slots. The pool has
    /// four contexts; six failing batches followed by a valid batch must
    /// surface six errors and then succeed — pre-fix, the fifth acquire
    /// busy-waited forever with no error.
    #[tokio::test]
    async fn repeated_processing_errors_do_not_exhaust_the_context_pool() {
        let processor = SqlProcessor::new(
            SqlProcessorConfig {
                query: "SELECT v, v * 2 AS doubled FROM flow".to_string(),
                table_name: None,
                temporary_list: None,
            },
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        )
        .unwrap();

        fn matching_batch() -> MessageBatchRef {
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
            Arc::new(MessageBatch::new_arrow(batch))
        }
        fn drifting_batch() -> MessageBatchRef {
            let schema = Arc::new(Schema::new(vec![Field::new("w", DataType::Int64, false)]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
            Arc::new(MessageBatch::new_arrow(batch))
        }

        // Baseline: the schema-matching batch processes.
        processor
            .process(matching_batch())
            .await
            .expect("baseline batch processes");

        // Six failing batches (pool size is 4): each surfaces its planning
        // error promptly instead of parking on a leaked pool.
        for i in 0..6 {
            let outcome = tokio::time::timeout(
                std::time::Duration::from_secs(3),
                processor.process(drifting_batch()),
            )
            .await
            .unwrap_or_else(|_| panic!("iteration {i}: acquire must fail bounded, not park"));
            assert!(
                outcome.is_err(),
                "iteration {i} must surface the planning error"
            );
        }

        // The pool survived the failures: a valid batch processes again.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            processor.process(matching_batch()),
        )
        .await
        .expect("valid batch must not park after repeated errors")
        .expect("valid batch processes");
        assert!(matches!(result, ProcessResult::Single(_)));
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

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let sum_of = |values: Vec<i64>| {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])
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
        let int_batch =
            RecordBatch::try_new(int_schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
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
            vec![Arc::new(datafusion::arrow::array::Float64Array::from(
                vec![1.5, 2.5],
            ))],
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

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
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
            Arc::new(Schema::new(vec![Field::new(
                "extra",
                DataType::Utf8,
                false,
            )])),
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

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

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

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
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
    async fn test_sql_processor_now_advances_across_batches() {
        let processor = SqlProcessor::new(
            SqlProcessorConfig {
                query: "SELECT now() as ts FROM flow".to_string(),
                table_name: None,
                temporary_list: None,
            },
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ts_of = || async {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
                    .unwrap();
            match processor
                .process(Arc::new(MessageBatch::new_arrow(batch)))
                .await
                .unwrap()
            {
                ProcessResult::Single(batch) => batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::TimestampNanosecondArray>()
                    .unwrap()
                    .value(0),
                _ => panic!("Expected single result"),
            }
        };

        let first = ts_of().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let second = ts_of().await;
        // A cached optimized plan must not freeze now() at cache time.
        assert!(
            second - first >= 40_000_000,
            "now() did not advance across batches: {first} -> {second}"
        );
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

    #[tokio::test]
    async fn temporary_tables_error_path_keeps_context_reusable() {
        // A failing statement on the temporary path used to skip
        // deregistration via `?`, leaving stale tables on the pooled
        // context; the next batch then failed registration with a
        // different ("already exists") error. Both failures must now be
        // the original query error.
        let reference = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "extra",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec!["enriched"]))],
        )
        .unwrap();

        let mut temporary: HashMap<String, Arc<dyn Temporary>> = HashMap::new();
        temporary.insert("ref".to_string(), Arc::new(StaticTemporary(reference)));

        let processor = SqlProcessor::new(
            SqlProcessorConfig {
                query: "SELECT f.nope FROM flow f JOIN temp_ref t ON 1=1".to_string(),
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

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();

        let first = processor
            .process(Arc::new(MessageBatch::new_arrow(batch.clone())))
            .await;
        let first_err = format!("{}", first.unwrap_err());
        assert!(
            !first_err.contains("already exists"),
            "first failure must be the query error, got: {first_err}"
        );

        let second = processor
            .process(Arc::new(MessageBatch::new_arrow(batch)))
            .await;
        let second_err = format!("{}", second.unwrap_err());
        assert!(
            !second_err.contains("already exists"),
            "stale tables must be deregistered on the error path, got: {second_err}"
        );
    }
}
