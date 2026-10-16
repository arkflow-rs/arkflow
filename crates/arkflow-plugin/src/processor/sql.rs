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
use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::{DataFusionError, Statistics};
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::datasource::TableType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::{Expr as LogicalExpr, LogicalPlan};
use datafusion::optimizer::OptimizerConfig;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{DisplayFormatType, ExecutionPlan};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use datafusion::sql::parser::Statement;
use expr::Expr;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

const DEFAULT_TABLE_NAME: &str = "flow";

/// Convert a key expression's evaluation result into the string lookup keys
/// consumed by `Temporary::get`. A null or non-UTF-8 key is a configuration
/// error and fails closed instead of silently dropping or panicking.
fn utf8_keys(value: ColumnarValue) -> Result<Vec<String>, Error> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(vec![s]),
        ColumnarValue::Array(array) => {
            let strings = array
                .as_string_opt::<i32>()
                .ok_or_else(|| {
                    Error::Process(
                        "temporary key expression must evaluate to UTF-8 strings".to_string(),
                    )
                })?
                .iter()
                .map(|v| {
                    v.ok_or_else(|| {
                        Error::Process("temporary key expression evaluated a null key".to_string())
                    })
                    .map(|v| v.to_string())
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(strings)
        }
        _ => Err(Error::Process(
            "temporary key expression must evaluate to UTF-8 strings".to_string(),
        )),
    }
}

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
/// swapped batch. The batch lives in a shared inner slot so the lazy
/// [`SwapBatchSource`] handed to the physical planner reads the same storage
/// the processor swaps: the resulting physical plan captures no data at
/// planning time and is safe to cache and re-execute across batch swaps.
#[derive(Debug)]
struct SwapBatchTable {
    slot: Arc<RwLock<RecordBatch>>,
}

impl SwapBatchTable {
    fn new(batch: RecordBatch) -> Self {
        Self {
            slot: Arc::new(RwLock::new(batch)),
        }
    }

    fn swap(&self, batch: RecordBatch) {
        *self.slot.write().unwrap() = batch;
    }

    fn slot(&self) -> Arc<RwLock<RecordBatch>> {
        Arc::clone(&self.slot)
    }
}

#[async_trait]
impl TableProvider for SwapBatchTable {
    fn schema(&self) -> SchemaRef {
        self.slot.read().unwrap().schema()
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
        let schema = self.slot.read().unwrap().schema();
        let source = SwapBatchSource::new(self.slot(), schema, projection.cloned())?;
        Ok(DataSourceExec::from_data_source(source))
    }
}

/// Lazy scan source over [`SwapBatchTable`]'s shared batch slot.
///
/// Planning captures only the slot handle, the planning-time schema, and the
/// projection — never batch data. The current batch is read when the stream
/// is produced (`open`), so a physical plan containing this node stays valid
/// for every later batch swapped in under the same schema; that is what
/// makes the physical plan cacheable.
#[derive(Debug)]
struct SwapBatchSource {
    slot: Arc<RwLock<RecordBatch>>,
    /// Schema captured at planning time. A batch swapped in under a
    /// different schema fails closed at `open` instead of feeding misaligned
    /// columns to a plan built for this schema (defensive: the cache is
    /// schema-keyed, so a mismatch indicates a broken invariant).
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
}

impl SwapBatchSource {
    fn new(
        slot: Arc<RwLock<RecordBatch>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self, DataFusionError> {
        let projected_schema = match &projection {
            Some(indices) => Arc::new(schema.project(indices)?),
            None => schema.clone(),
        };
        Ok(Self {
            slot,
            schema,
            projection,
            projected_schema,
        })
    }
}

impl DataSource for SwapBatchSource {
    fn open(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "SwapBatchSource has a single partition, requested partition {partition}"
            )));
        }
        let batch = self.slot.read().unwrap().clone();
        if batch.schema() != self.schema {
            return Err(DataFusionError::Internal(
                "SwapBatchSource executed with a batch whose schema differs from the schema its plan was built for".to_string(),
            ));
        }
        let projected = match &self.projection {
            Some(indices) => batch.project(indices)?,
            None => batch,
        };
        let stream = MemoryStream::try_new(vec![projected], self.projected_schema.clone(), None)?;
        Ok(Box::pin(stream))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "SwapBatchSource: projection={:?}",
            self.projection
                .as_ref()
                .map(|p| format!(
                    "[{}]",
                    p.iter()
                        .map(|i| i.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
                .unwrap_or_else(|| "none".to_string())
        )
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new(self.projected_schema.clone())
    }

    fn partition_statistics(
        &self,
        _partition: Option<usize>,
    ) -> Result<Arc<Statistics>, DataFusionError> {
        // Exact row count from the planning-time batch: the physical planner
        // consumes statistics when the cached plan is first built, and this
        // keeps that plan-shape input identical to the eager MemorySource it
        // replaces. Only called at planning time, so no per-batch cost.
        let num_rows = self.slot.read().unwrap().num_rows();
        Ok(Arc::new(Statistics {
            num_rows: Precision::Exact(num_rows),
            ..Statistics::new_unknown(&self.projected_schema)
        }))
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        // Limits stay in the plan above (same as the eager source before).
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>, DataFusionError> {
        // The planning-time column projection is applied in `open`; no
        // further projection pushdown into the source.
        Ok(None)
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

/// Recursive queries compile to a `RecursiveQuery` plan whose work-table
/// scan holds one-shot execution state in the physical plan; per
/// `reset_plan_states`' documented limitations such plans are not
/// re-executable, so they must not enter the physical-plan cache.
fn plan_is_recursive(plan: &LogicalPlan) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut found = false;
    let _ = plan.apply(&mut |p: &LogicalPlan| {
        if matches!(p, LogicalPlan::RecursiveQuery(_)) {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// Exec nodes that hold one-shot per-execution state inside the node
/// itself: repartition distribution channels, join build futures and
/// materialized build sides, TopK dynamic filters. Every other node
/// streams its state per `execute` call and is safe to share across
/// executions. The set mirrors the DataFusion nodes that override
/// `reset_state` (plus `RepartitionExec`, whose state resets through
/// `with_new_children`). The general `reset_plan_states` helper rebuilds
/// EVERY node, which measurably costs more than the physical planning
/// this cache exists to avoid.
fn carries_execution_state(plan: &Arc<dyn ExecutionPlan>) -> bool {
    use datafusion::physical_plan::joins::{
        CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PiecewiseMergeJoinExec, SortMergeJoinExec,
        SymmetricHashJoinExec,
    };
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    let any: &dyn std::any::Any = plan.as_ref();
    any.downcast_ref::<RepartitionExec>().is_some()
        || any.downcast_ref::<SortExec>().is_some()
        || any.downcast_ref::<CrossJoinExec>().is_some()
        || any.downcast_ref::<HashJoinExec>().is_some()
        || any.downcast_ref::<NestedLoopJoinExec>().is_some()
        || any.downcast_ref::<PiecewiseMergeJoinExec>().is_some()
        || any.downcast_ref::<SortMergeJoinExec>().is_some()
        || any.downcast_ref::<SymmetricHashJoinExec>().is_some()
}

/// Prepare a cached physical plan for re-execution: bottom-up, rebuild the
/// state-carrying nodes with fresh state, relink their ancestors, and
/// share every other node unchanged — the targeted equivalent of
/// `reset_plan_states` for plan trees over the lazy swap source.
fn refresh_for_reexecution(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let old_children = plan.children();
    let mut changed = false;
    let mut children = Vec::with_capacity(old_children.len());
    for child in old_children {
        let refreshed = refresh_for_reexecution(child)?;
        changed |= !Arc::ptr_eq(&refreshed, child);
        children.push(refreshed);
    }
    if carries_execution_state(plan) {
        return Arc::clone(plan).with_new_children(children)?.reset_state();
    }
    if !changed {
        return Ok(Arc::clone(plan));
    }
    Arc::clone(plan).with_new_children(children)
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
    /// Physical plan; the scan reads the shared batch slot lazily, so the
    /// plan captures no data and is reusable under the same schema. Time-
    /// folding queries are excluded for the same reason as `optimized`.
    physical: Option<(SchemaRef, Arc<dyn ExecutionPlan>)>,
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
    /// Set once from the first analyzed plan; recursive queries' physical
    /// plans hold one-shot work-table state and are never cached.
    recursive_query: std::sync::OnceLock<bool>,
    /// Fast-path physical plan builds; stops growing once every pooled
    /// context holds a plan for the current schema (the cache invariant
    /// tests assert this).
    physical_plan_builds: std::sync::atomic::AtomicUsize,
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
            recursive_query: std::sync::OnceLock::new(),
            physical_plan_builds: std::sync::atomic::AtomicUsize::new(0),
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
    /// and physical plans are cached only when the query cannot fold time
    /// expressions into the plan (now/current_date/current_time), which would
    /// otherwise freeze the first batch's timestamp. The physical plan's scan
    /// reads the shared batch slot lazily, so a cached plan serves every
    /// later batch swapped in under the same schema.
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
                            physical: None,
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
                let _ = self.recursive_query.set(plan_is_recursive(&plan));
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
                        cache.optimized = Some((schema.clone(), optimized.clone()));
                    }
                }
                optimized
            }
        };

        // The cached physical plan's scan reads the shared batch slot at
        // execution time, so reuse needs the optimized plan's guards (schema
        // match, no time-folding functions) plus the recursive-query guard:
        // a reused plan is refreshed with `refresh_for_reexecution` (targeted
        // rebuild of the nodes in `carries_execution_state`) because some
        // exec nodes (repartition channels, join build futures, TopK filters)
        // hold one-shot per-execution state inside the node.
        let recursive = self.recursive_query.get().copied().unwrap_or(false);
        let cacheable = !time_dependent && !recursive;
        let cached_physical = if cacheable {
            let caches = self.context_caches.lock().unwrap();
            caches
                .get(&key)
                .and_then(|cache| cache.physical.as_ref())
                .filter(|(cached_schema, _)| *cached_schema == schema)
                .map(|(_, plan)| plan.clone())
        } else {
            None
        };

        let physical = match cached_physical {
            Some(plan) => refresh_for_reexecution(&plan)
                .map_err(|e| Error::Process(format!("Physical plan reset error: {}", e)))?,
            None => {
                self.physical_plan_builds
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let physical = state
                    .query_planner()
                    .create_physical_plan(&plan, &state)
                    .await
                    .map_err(|e| Error::Process(format!("Physical planning error: {}", e)))?;
                if cacheable {
                    let mut caches = self.context_caches.lock().unwrap();
                    if let Some(cache) = caches.get_mut(&key) {
                        cache.physical = Some((schema, physical.clone()));
                    }
                }
                physical
            }
        };
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
            let keys: Vec<String> = match &config.key {
                Expr::Expr { expr: expr_str } => {
                    let evaluated = expr::evaluate_expr(expr_str, batch).await.map_err(|e| {
                        Error::Process(format!("Evaluate expression failed: {}", e))
                    })?;
                    utf8_keys(evaluated)?
                }
                Expr::Value { value } => vec![value.clone()],
            };

            if let Some(data) = temporary.get(&keys).await? {
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
        _name: Option<&str>,
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

    #[test]
    fn utf8_keys_fails_closed_instead_of_panicking() {
        let ok = utf8_keys(ColumnarValue::Array(std::sync::Arc::new(
            StringArray::from(vec!["a", "b"]),
        )))
        .unwrap();
        assert_eq!(ok, vec!["a".to_string(), "b".to_string()]);

        let nulls = StringArray::from(vec![Some("a"), None]);
        let err = utf8_keys(ColumnarValue::Array(std::sync::Arc::new(nulls))).unwrap_err();
        assert!(err.to_string().contains("null key"), "{err}");

        // A non-UTF-8 key expression must return an error, never panic on
        // the string downcast.
        let ints = Int64Array::from(vec![1, 2]);
        let err = utf8_keys(ColumnarValue::Array(std::sync::Arc::new(ints))).unwrap_err();
        assert!(err.to_string().contains("UTF-8"), "{err}");
    }

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

        async fn get(&self, _keys: &[String]) -> Result<Option<MessageBatch>, Error> {
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

    fn empty_resource() -> Resource {
        Resource {
            temporary: Default::default(),
            input_names: RefCell::new(Default::default()),
        }
    }

    fn int_string_batch(ids: Vec<i64>, names: Vec<&str>) -> MessageBatchRef {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();
        Arc::new(MessageBatch::new_arrow(batch))
    }

    /// Spec "Physical planning happens once per schema": the build counter
    /// rises at most once per pooled context for a fixed schema and then
    /// stops, and a schema drift re-triggers exactly one rebuild per used
    /// context.
    #[tokio::test]
    async fn physical_plan_built_at_most_once_per_schema() {
        let processor = SqlProcessor::new(
            SqlProcessorConfig {
                query: "SELECT id FROM flow WHERE id > 0".to_string(),
                table_name: None,
                temporary_list: None,
            },
            &empty_resource(),
        )
        .unwrap();

        let builds = || {
            processor
                .physical_plan_builds
                .load(std::sync::atomic::Ordering::Relaxed)
        };
        let ids_batch = |n: i64| int_string_batch(vec![n], vec!["a"]);

        for i in 0..12 {
            processor.process(ids_batch(i)).await.unwrap();
        }
        let after_first_round = builds();
        assert!(
            (1..=4).contains(&after_first_round),
            "expected 1..=4 builds (pool size 4), got {after_first_round}"
        );

        // More same-schema batches must not build any further plan.
        for i in 0..12 {
            processor.process(ids_batch(i)).await.unwrap();
        }
        assert_eq!(builds(), after_first_round, "cache must stop rebuilding");

        // A schema drift invalidates the cache: at least one new build, then
        // stable again under the new schema.
        let drifted = {
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7i64]))]).unwrap();
            Arc::new(MessageBatch::new_arrow(batch))
        };
        let drift_processor = SqlProcessor::new(
            SqlProcessorConfig {
                query: "SELECT sum(v) AS total FROM flow".to_string(),
                table_name: None,
                temporary_list: None,
            },
            &empty_resource(),
        )
        .unwrap();
        let drift_builds = || {
            drift_processor
                .physical_plan_builds
                .load(std::sync::atomic::Ordering::Relaxed)
        };
        for _ in 0..3 {
            drift_processor.process(drifted.clone()).await.unwrap();
        }
        let drifted_builds = drift_builds();
        assert_eq!(drifted_builds, 1, "one build for one schema");
        for _ in 0..3 {
            drift_processor.process(drifted.clone()).await.unwrap();
        }
        assert_eq!(drift_builds(), drifted_builds);
    }

    /// Spec "Mismatched execution batch errors instead of misreading
    /// columns": the lazy source fails closed on an execution-time schema
    /// mismatch instead of feeding misaligned columns to the plan.
    #[tokio::test]
    async fn swap_source_fails_closed_on_schema_mismatch() {
        let slot = Arc::new(RwLock::new(
            int_string_batch(vec![1], vec!["a"]).record_batch().clone(),
        ));
        let plan_schema = slot.read().unwrap().schema();

        // Happy path: matching schema streams the projected batch.
        let source = SwapBatchSource::new(
            slot.clone(),
            plan_schema.clone(),
            Some(vec![1, 0]), // reversed projection exercises column mapping
        )
        .unwrap();
        let ctx = SessionContext::new();
        let stream = source.open(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = futures::TryStreamExt::try_collect(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        let out_schema = batches[0].schema();
        assert_eq!(
            out_schema.field(0),
            plan_schema.field(1),
            "projection must reorder columns"
        );
        assert_eq!(
            out_schema.field(1),
            plan_schema.field(0),
            "projection must reorder columns"
        );
        assert_eq!(batches[0].num_rows(), 1);

        // Mismatch: a batch of a different schema swapped into the slot
        // errors at open, naming the schema mismatch.
        *slot.write().unwrap() = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)])),
            vec![Arc::new(datafusion::arrow::array::Float64Array::from(
                vec![1.5],
            ))],
        )
        .unwrap();
        let err = match source.open(0, ctx.task_ctx()) {
            Ok(_) => panic!("a swapped-in schema mismatch must fail closed"),
            Err(e) => e,
        };
        assert!(
            err.to_string().to_lowercase().contains("schema"),
            "error must name the schema mismatch, got: {err}"
        );
    }

    /// Spec "Cached plan serves each batch's own data" (equivalence leg):
    /// for every batch, the cached-physical-plan path must produce exactly
    /// what a fully independent re-plan (fresh processor, no caches at all)
    /// produces — schema, column order, and values.
    #[tokio::test]
    async fn cached_physical_plan_matches_full_replan() {
        let queries: Vec<(&str, bool)> = vec![
            ("SELECT name, id FROM flow WHERE id % 2 = 0", false),
            (
                "SELECT name, count(*) AS n, sum(id) AS total FROM flow GROUP BY name",
                false,
            ),
            (
                "SELECT name, sum(id) AS t FROM flow GROUP BY name HAVING sum(id) > 1",
                false,
            ),
            (
                "SELECT a.id, b.name FROM flow a JOIN flow b ON a.id = b.id",
                false,
            ),
            (
                "SELECT b.name FROM flow a JOIN flow b ON a.id = b.id WHERE a.id > 0",
                false,
            ),
            // TopK carries a dynamic filter inside SortExec; alternating
            // value ranges make a stale threshold from the previous batch
            // visibly wrong instead of merely slower.
            ("SELECT id FROM flow ORDER BY id DESC LIMIT 2", true),
            ("SELECT DISTINCT name FROM flow", false),
            (
                "SELECT id, row_number() OVER (PARTITION BY name ORDER BY id) AS rn FROM flow",
                false,
            ),
        ];
        for (query, alternating) in queries {
            let make = || {
                SqlProcessor::new(
                    SqlProcessorConfig {
                        query: query.to_string(),
                        table_name: None,
                        temporary_list: None,
                    },
                    &empty_resource(),
                )
                .unwrap()
            };
            let cached = make();
            for round in 0..4i64 {
                let ids = if alternating {
                    match round % 2 {
                        0 => vec![10 * (round + 1), 10 * (round + 1) + 1, 10 * (round + 1) + 2],
                        _ => vec![round, round + 1],
                    }
                } else {
                    vec![round, round + 1]
                };
                let names: Vec<&str> = vec!["x"; ids.len()];
                let batch = int_string_batch(ids.clone(), names);
                let cached_out = cached.process(batch.clone()).await.unwrap();
                let fresh_out = make().process(batch).await.unwrap();
                let (ProcessResult::Single(cached_batch), ProcessResult::Single(fresh_batch)) =
                    (cached_out, fresh_out)
                else {
                    panic!("expected single results for {query}");
                };
                assert_eq!(
                    cached_batch.record_batch().schema(),
                    fresh_batch.record_batch().schema(),
                    "query {query} round {round} (ids {ids:?}): schema differs"
                );
                assert_eq!(
                    cached_batch.record_batch(),
                    fresh_batch.record_batch(),
                    "query {query} round {round} (ids {ids:?}): values differ"
                );
            }
        }
    }

    /// Recursive queries must bypass the physical-plan cache: their
    /// work-table state is one-shot per execution. A leaked cache would
    /// return drained (empty) recursion results from the second batch on;
    /// equivalence with a fresh processor per batch pins the guard.
    #[tokio::test]
    async fn recursive_query_bypasses_physical_cache() {
        let query = "WITH RECURSIVE seq AS (\
                     SELECT 1 AS n UNION ALL SELECT n + 1 FROM seq WHERE n < 4\
                     ) SELECT n FROM seq ORDER BY n";
        let make = || {
            SqlProcessor::new(
                SqlProcessorConfig {
                    query: query.to_string(),
                    table_name: None,
                    temporary_list: None,
                },
                &empty_resource(),
            )
            .unwrap()
        };
        let cached = make();
        for round in 0..3 {
            let batch = int_string_batch(vec![round], vec!["x"]);
            let cached_out = cached.process(batch.clone()).await.unwrap();
            let fresh_out = make().process(batch).await.unwrap();
            let (ProcessResult::Single(cached_batch), ProcessResult::Single(fresh_batch)) =
                (cached_out, fresh_out)
            else {
                panic!("expected single results");
            };
            assert_eq!(
                cached_batch.record_batch(),
                fresh_batch.record_batch(),
                "recursive query round {round} differs from full replan"
            );
            assert_eq!(cached_batch.len(), 4, "round {round} must keep all rows");
        }
        // The detection ran on the first analyzed plan: this query must be
        // classified recursive (asserted AFTER the batches above, where the
        // OnceLock is guaranteed set — a fresh processor's would still be
        // unset and the check vacuous).
        assert_eq!(
            cached.recursive_query.get(),
            Some(&true),
            "detection must classify the WITH RECURSIVE plan"
        );
        // Sequential processing reuses one pooled context; every batch must
        // have re-planned (the cache never engages for recursive queries).
        assert_eq!(
            cached
                .physical_plan_builds
                .load(std::sync::atomic::Ordering::Relaxed),
            3
        );
    }

    /// Ad-hoc release timing (NOT run by CI): cached physical plan vs a
    /// per-batch physical re-plan on the same pooled context, analyzed and
    /// optimized plans cached in both. Run with:
    /// `cargo test --release -p arkflow-plugin --lib -- --ignored sql_physical_plan_timing --nocapture`
    #[tokio::test]
    #[ignore]
    async fn sql_physical_plan_timing() {
        fn drop_physical_cache(processor: &SqlProcessor) {
            let mut caches = processor.context_caches.lock().unwrap();
            for cache in caches.values_mut() {
                cache.physical = None;
            }
        }

        let batch_rows = 1_000usize;
        let batches = 200usize;
        let rounds = 5usize;

        let shapes = [
            ("aggregate", "SELECT sum(value) AS total FROM flow"),
            (
                "filter-project",
                "SELECT value * 2 AS doubled FROM flow WHERE value >= 0",
            ),
        ];
        for (label, query) in shapes {
            let make = || {
                SqlProcessor::new(
                    SqlProcessorConfig {
                        query: query.to_string(),
                        table_name: None,
                        temporary_list: None,
                    },
                    &empty_resource(),
                )
                .unwrap()
            };
            let schema = Arc::new(Schema::new(vec![
                Field::new("value", DataType::Int64, false),
                Field::new("tag", DataType::Utf8, false),
            ]));
            let make_batch = |round: usize| {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(
                            (0..batch_rows as i64)
                                .map(|i| i + round as i64)
                                .collect::<Vec<_>>(),
                        )),
                        Arc::new(StringArray::from(
                            (0..batch_rows)
                                .map(|i| format!("tag-{i}"))
                                .collect::<Vec<_>>(),
                        )),
                    ],
                )
                .unwrap();
                Arc::new(MessageBatch::new_arrow(batch))
            };

            async fn time_run(
                processor: &SqlProcessor,
                make_batch: impl Fn(usize) -> MessageBatchRef,
                drop_cache: bool,
                rounds: usize,
                batches: usize,
            ) -> std::time::Duration {
                let mut best = std::time::Duration::MAX;
                for _ in 0..rounds {
                    let started = std::time::Instant::now();
                    for i in 0..batches {
                        if drop_cache {
                            drop_physical_cache(processor);
                        }
                        processor.process(make_batch(i)).await.unwrap();
                    }
                    best = best.min(started.elapsed());
                }
                best
            }

            // Warm up code paths before measuring.
            let warm = make();
            for i in 0..4 {
                warm.process(make_batch(i)).await.unwrap();
            }

            let rows = batches * batch_rows;
            let cached_processor = make();
            let cached_time = time_run(&cached_processor, make_batch, false, rounds, batches).await;
            println!(
                "{label} cached-physical-plan: {rows} rows in {:?} ({:.0} rows/s)",
                cached_time,
                rows as f64 / cached_time.as_secs_f64()
            );
            let replan_processor = make();
            let replan_time = time_run(&replan_processor, make_batch, true, rounds, batches).await;
            println!(
                "{label} per-batch-replan:     {rows} rows in {:?} ({:.0} rows/s)",
                replan_time,
                rows as f64 / replan_time.as_secs_f64()
            );
            println!(
                "{label} planning overhead removed: {:.2}x",
                replan_time.as_secs_f64() / cached_time.as_secs_f64()
            );
        }
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
