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
use arkflow_core::{Error, MessageBatch};
use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JoinConfig {
    pub(crate) query: String,
}

impl JoinConfig {
    pub async fn join_operation(
        ctx: &SessionContext,
        query: &str,
        table_sources: Vec<MessageBatch>,
    ) -> Result<RecordBatch, Error> {
        for x in table_sources {
            let input_name_opt = x.get_input_name();
            let Some(input_name) = input_name_opt else {
                continue;
            };
            let x1 = x.into();
            ctx.register_batch(&input_name, x1)
                .map_err(|e| Error::Process(format!("Failed to register table source: {}", e)))?;
        }

        let df = ctx
            .sql(query)
            .await
            .map_err(|e| Error::Process(format!("Failed to execute SQL query: {}", e)))?;
        let result_batches = df
            .collect()
            .await
            .map_err(|e| Error::Process(format!("Failed to collect query result: {}", e)))?;
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
}
