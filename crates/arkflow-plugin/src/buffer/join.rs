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
use arkflow_core::{split_batch, Error, MessageBatch, DEFAULT_BINARY_VALUE_FIELD};
use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::trace;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JoinConfig {
    pub(crate) query: String,
    pub(crate) value_field: Option<String>,
    pub(crate) codec: CodecConfig,
    #[serde(default = "default_thread_num")]
    pub(crate) thread_num: usize,
}

pub(crate) struct JoinOperation {
    query: String,
    value_field: Option<String>,
    codec: Arc<dyn Decoder>,
    input_names: HashSet<String>,
    thread_num: usize,
}

impl JoinOperation {
    pub(crate) fn new(
        query: String,
        value_field: Option<String>,
        thread_num: usize,
        codec: Arc<dyn Decoder>,
        input_names: HashSet<String>,
    ) -> Result<Self, Error> {
        Ok(Self {
            query,
            value_field,
            thread_num,
            codec,
            input_names,
        })
    }

    pub(crate) async fn join_operation(
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
            let msg_batch = x?;
            let input_name_opt = msg_batch.get_input_name();
            let Some(input_name) = input_name_opt else {
                continue;
            };

            let vec_rb = split_batch(msg_batch.into(), self.thread_num);
            let mut batches = vec_rb.into_iter().peekable();
            let schema = if let Some(batch) = batches.peek() {
                batch.schema()
            } else {
                Arc::new(Schema::empty())
            };
            let batches = batches.map(|b| vec![b]).collect::<Vec<_>>();
            let provider = MemTable::try_new(schema, batches)
                .map_err(|e| Error::Process(format!("Failed to create MemTable: {}", e)))?;

            ctx.register_table(
                TableReference::Bare {
                    table: input_name.clone().into(),
                },
                Arc::new(provider),
            )
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
        let result = batch.to_binary(
            self.value_field
                .as_deref()
                .unwrap_or(DEFAULT_BINARY_VALUE_FIELD),
        )?;
        let mut result =
            codec.decode(result.into_iter().map(|x| x.to_vec()).collect::<Vec<_>>())?;
        result.set_input_name(option);
        Ok(result)
    }
}

fn default_thread_num() -> usize {
    num_cpus::get()
}
