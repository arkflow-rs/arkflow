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
use arkflow_core::codec::{CodecConfig, Decoder};
use arkflow_core::{Error, MessageBatch};
use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::prelude::SessionContext;
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::trace;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JoinConfig {
    pub(crate) query: String,
    pub(crate) codec: CodecConfig,
}

pub struct JoinOperation {
    query: String,
    codec: Arc<dyn Decoder>,
    input_names: HashSet<String>,
}
impl JoinOperation {
    pub fn new(
        query: String,
        codec: Arc<dyn Decoder>,
        input_names: HashSet<String>,
    ) -> Result<Self, Error> {
        Ok(Self {
            query,
            codec,
            input_names,
        })
    }

    pub async fn join_operation(
        &self,
        ctx: &SessionContext,
        table_sources: Vec<MessageBatch>,
    ) -> Result<RecordBatch, Error> {
        let table_sources = stream::iter(table_sources)
            .map(|x| self.decode_batch(x))
            .buffer_unordered(num_cpus::get())
            .collect::<Vec<Result<MessageBatch, Error>>>()
            .await;

        let mut current_input_names = Vec::with_capacity(table_sources.len());
        for x in table_sources {
            let x = x?;
            let input_name_opt = x.get_input_name();
            let Some(input_name) = input_name_opt else {
                continue;
            };
            ctx.register_batch(&input_name, x.into())
                .map_err(|e| Error::Process(format!("Failed to register table source: {}", e)))?;
            current_input_names.push(input_name);
        }

        if !self
            .input_names
            .iter()
            .all(|x| current_input_names.contains(x))
        {
            trace!("Data ignored, data table missing, SQL unable to execute",);
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        };

        let df = ctx
            .sql(&self.query)
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
            arrow::compute::concat_batches(&result_batches[0].schema(), &result_batches)
                .map_err(|e| Error::Process(format!("Batch merge failed: {}", e)))?,
        )
    }

    async fn decode_batch(&self, batch: MessageBatch) -> Result<MessageBatch, Error> {
        let codec = Arc::clone(&self.codec);
        let option = batch.get_input_name();

        let mut result = codec.decode(batch)?;
        result.set_input_name(option);
        Ok(result)
    }
}
