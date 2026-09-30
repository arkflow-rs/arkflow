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
    /// Batches discarded because they lacked an input_name (contract
    /// break). Exposed for tests; production code reads it via logs.
    pub(crate) discarded_unnamed_batches: std::sync::atomic::AtomicUsize,
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
            discarded_unnamed_batches: std::sync::atomic::AtomicUsize::new(0),
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
                // Contract break: a mid-stream construction path dropped
                // the origin name. Warn (not trace) — the downstream join
                // data will be silently incomplete without this signal.
                tracing::warn!(
                    "join buffer discarded a batch without an input_name; \
                     downstream join data will be incomplete"
                );
                self.discarded_unnamed_batches
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

        arrow::compute::concat_batches(&result_batches[0].schema(), &result_batches)
            .map_err(|e| Error::Process(format!("Batch merge failed: {}", e)))
    }

    async fn decode_batch(&self, batch: MessageBatch) -> Result<MessageBatch, Error> {
        let codec = Arc::clone(&self.codec);
        let option = batch.get_input_name();
        let result = batch.to_binary(
            self.value_field
                .as_deref()
                .unwrap_or(DEFAULT_BINARY_VALUE_FIELD),
        )?;
        let mut result = codec
            .decode(result.into_iter().map(|x| x.to_vec()).collect::<Vec<_>>())
            .await?;
        result.set_input_name(option);
        Ok(result)
    }
}

fn default_thread_num() -> usize {
    num_cpus::get()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopCodec;
    #[async_trait::async_trait]
    impl arkflow_core::codec::Decoder for NoopCodec {
        async fn decode(&self, b: Vec<arkflow_core::Bytes>) -> Result<MessageBatch, Error> {
            MessageBatch::new_binary(b)
        }
    }
    #[async_trait::async_trait]
    impl arkflow_core::codec::Encoder for NoopCodec {
        async fn encode(&self, _m: MessageBatch) -> Result<Vec<arkflow_core::Bytes>, Error> {
            Ok(Vec::new())
        }
    }
    use arkflow_core::MessageBatch;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    fn unnamed_batch() -> MessageBatch {
        // Binary batch (the join buffer decodes via to_binary, which
        // requires the __value__ column): no input_name — simulates a
        // construction path that dropped it.
        MessageBatch::new_binary(vec![b"payload".to_vec()]).unwrap()
    }

    /// Spec "无名批次的丢弃有 warn 与计数": a batch without input_name is
    /// skipped (not crashed) but now with a warn and a counter — the old
    /// trace-level continue was silent data loss for downstream joins.
    #[tokio::test]
    async fn unnamed_batch_discard_is_counted() {
        let op = JoinOperation::new(
            "SELECT 1".into(),
            None,
            1,
            Arc::new(NoopCodec),
            ["left".into()].into(),
        )
        .unwrap();
        let before = op
            .discarded_unnamed_batches
            .load(std::sync::atomic::Ordering::Relaxed);
        // Feed a batch with no input_name — the buffer should skip it and
        // increment the counter (previously just a trace-level continue).
        let ctx = SessionContext::new();
        let result = op.join_operation(&ctx, vec![unnamed_batch()]).await;
        let _ = result; // may succeed with an empty result — the counter is the assertion
        let after = op
            .discarded_unnamed_batches
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            after > before,
            "discarding an unnamed batch must increment the counter: before={before} after={after}"
        );
    }
}
