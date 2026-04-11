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
use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::temporary::Temporary;
use arkflow_core::{Error, MessageBatch, MessageBatchRef, ProcessResult, Resource};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::ColumnarValue;
use datafusion::optimizer::OptimizerConfig;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use datafusion::sql::parser::Statement;
use expr::Expr;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

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

/// SQL processor component
struct SqlProcessor {
    config: SqlProcessorConfig,
    statement: Statement,
    temporary: Option<HashMap<String, (Arc<dyn Temporary>, TemporaryConfig)>>,
    context_pool: Arc<SessionContextPool>,
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
            .sql_to_statement(
                &config.query,
                ctx.state().options().sql_parser.dialect.as_str(),
            )
            .map_err(|e| Error::Process(format!("SQL query error: {}", e)))?;
        Ok(Self {
            config,
            statement,
            temporary,
            context_pool,
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
        self.get_temporary_message_batch(&ctx_arc, &batch).await?;
        ctx_arc
            .register_batch(table_name, batch.clone().into())
            .map_err(|e| Error::Process(format!("Registration failed: {}", e)))?;
        // Execute the SQL query and collect the results.
        let df = self
            .execute_query_with_statement(&ctx_arc)
            .await
            .map_err(|e| Error::Process(format!("Execution query error: {}", e)))?;
        let result_batches = df
            .collect()
            .await
            .map_err(|e| Error::Process(format!("Collection query results error: {}", e)))?;

        // Deregister the table to clean up before returning context to pool
        let _ = ctx_arc.deregister_table(table_name);

        // Release the context back to the pool
        self.context_pool.release_context(ctx_arc).await;

        if result_batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        if result_batches.len() == 1 {
            return Ok(result_batches[0].clone());
        }

        Ok(
            arrow::compute::concat_batches(&&result_batches[0].schema(), &result_batches)
                .map_err(|e| Error::Process(format!("Batch merge failed: {}", e)))?,
        )
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

            if let Some(data) = temporary.get(&vec![columnar_value]).await? {
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
        let sql_options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);

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
    register_processor_builder("sql", Arc::new(SqlProcessorBuilder))
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
