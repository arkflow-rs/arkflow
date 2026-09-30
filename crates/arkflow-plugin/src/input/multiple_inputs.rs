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
use arkflow_core::codec::Codec;
use arkflow_core::component::{register_input_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::{
    input::{Ack, Input, InputBuilder, InputConfig},
    Error, MessageBatchRef, Resource,
};
use async_trait::async_trait;
use flume::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MultipleInputsConfig {
    inputs: Vec<InputConfig>,
}

/// Capacity of the internal forwarding channel, matching the inter-chain
/// edge bound: a stalled consumer backpressures the child inputs instead of
/// growing memory without limit.
const CHANNEL_CAPACITY: usize = 1024;

/// One generation of child reader tasks. Reconnecting replaces the whole
/// generation: the previous readers are cancelled and awaited before the
/// next set spawns, so a child input is never read by two tasks at once.
struct ReaderGeneration {
    token: CancellationToken,
    tracker: TaskTracker,
}

struct MultipleInputs {
    #[allow(unused)]
    input_name: Option<String>,
    inputs: Vec<Arc<dyn Input>>,
    sender: Sender<Msg>,
    receiver: Receiver<Msg>,
    /// Cancelled only by `close()`. Each generation's token is a child of
    /// this one, so closing releases blocked sends and the consumer's
    /// pending read; a `connect()` after close gets a born-cancelled token.
    closed: CancellationToken,
    generation: Mutex<Option<ReaderGeneration>>,
}

enum Msg {
    Message(MessageBatchRef, Arc<dyn Ack>),
    Err(Error),
}

#[async_trait]
impl Input for MultipleInputs {
    async fn connect(&self) -> Result<(), Error> {
        // The lock is held across the whole restart so a concurrent close()
        // serializes either before it (no generation left running) or after
        // it (it cancels and joins the fresh generation).
        let mut current = self.generation.lock().await;
        if let Some(previous) = current.take() {
            previous.token.cancel();
            previous.tracker.wait().await;
            // Stale errors from the dead generation must not trigger another
            // engine reconnect. Queued data messages are real deliveries:
            // drain, drop the errors, and re-queue the data in order.
            let mut retained = Vec::new();
            while let Ok(msg) = self.receiver.try_recv() {
                if matches!(msg, Msg::Message(..)) {
                    retained.push(msg);
                }
            }
            for msg in retained {
                // The queue was just drained below its capacity, so this
                // send cannot block.
                self.sender.send(msg).map_err(|_| {
                    Error::Process("multiple-inputs channel closed during reconnect".into())
                })?;
            }
        }

        for input in &self.inputs {
            input.connect().await?;
        }

        let token = self.closed.child_token();
        let tracker = TaskTracker::new();
        for input in &self.inputs {
            let input = Arc::clone(input);
            let sender = self.sender.clone();
            let cancellation_token = token.clone();
            tracker.spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            return;
                        }
                        result = input.read() => {
                            match result {
                                Ok((batch, ack)) => {
                                    // The send itself stays cancellation-aware:
                                    // a full channel must not hold the task
                                    // past close().
                                    tokio::select! {
                                        _ = cancellation_token.cancelled() => return,
                                        sent = sender.send_async(Msg::Message(batch, ack)) => {
                                            if sent.is_err() {
                                                return;
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    // Surface the error once and exit: the
                                    // engine decides reconnect vs failure,
                                    // exactly like a single input's read().
                                    tokio::select! {
                                        _ = cancellation_token.cancelled() => return,
                                        _ = sender.send_async(Msg::Err(e)) => return,
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }

        tracker.close();
        *current = Some(ReaderGeneration { token, tracker });

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        tokio::select! {
            _ = self.closed.cancelled() => {
                Err(Error::EOF)
            }
            result = self.receiver.recv_async() => {
                match result {
                     Ok(Msg::Message(batch, ack)) => Ok((batch, ack)),
                    Ok(Msg::Err(e)) => Err(e),
                    Err(_e) => Err(Error::EOF),
                }
            }
        }
    }

    async fn close(&self) -> Result<(), Error> {
        // Release the consumer's pending read and every blocked send before
        // joining the generation, so close cannot hang on a full channel.
        self.closed.cancel();
        {
            let mut current = self.generation.lock().await;
            if let Some(generation) = current.take() {
                generation.token.cancel();
                generation.tracker.wait().await;
            }
        }
        for input in &self.inputs {
            input.close().await?;
        }

        Ok(())
    }
}

impl MultipleInputs {
    fn new(
        name: Option<&String>,
        config: MultipleInputsConfig,
        resource: &Resource,
    ) -> Result<Self, Error> {
        // Zero child inputs would deploy an input whose read hangs forever:
        // no reader tasks exist, yet the channel's sender keeps it open.
        if config.inputs.is_empty() {
            return Err(Error::Config(
                "Multiple-inputs input requires at least one child input".to_string(),
            ));
        }
        let (sender, receiver) = flume::bounded(CHANNEL_CAPACITY);
        let mut inputs = Vec::with_capacity(config.inputs.len());
        let mut input_names_mut = resource.input_names.borrow_mut();
        for x in config.inputs {
            if let Some(name) = &x.name {
                if name.is_empty() {
                    return Err(Error::Config(
                        "Multiple-inputs input configuration has empty input name".to_string(),
                    ));
                }
                input_names_mut.push(name.clone());
            }
            inputs.push(x.build(resource)?);
        }
        let input_names_hash_set = input_names_mut.iter().cloned().collect::<HashSet<String>>();
        if input_names_hash_set.len() != input_names_mut.len() {
            return Err(Error::Config(
                "Multiple-inputs input configuration has duplicate input names".to_string(),
            ));
        };

        Ok(Self {
            input_name: name.cloned(),
            inputs,
            sender,
            receiver,
            closed: CancellationToken::new(),
            generation: Mutex::new(None),
        })
    }
}

struct MultipleInputsBuilder;
impl InputBuilder for MultipleInputsBuilder {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<Value>,
        codec: Option<Arc<dyn Codec>>,
        resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        let config: MultipleInputsConfig = parse_config(config, "MultipleInputs input")?;
        // Note: codec is not used here as individual inputs have their own codecs
        let _ = codec;
        Ok(Arc::new(MultipleInputs::new(name, config, resource)?))
    }
}

pub(crate) fn init() -> Result<(), Error> {
    arkflow_core::input::register_input_builder(
        "multiple_inputs",
        Arc::new(MultipleInputsBuilder),
    )?;
    register_input_metadata(ComponentMetadata::with_schema(
        "multiple_inputs",
        "Combines multiple input sources into a single stream. Each source is tagged with __meta_source.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "inputs": {
                    "type": "array",
                    "minItems": 1,
                    "description": "List of input components to combine.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string", "description": "Input type (e.g. 'kafka', 'http')."},
                            "name": {"type": "string", "description": "Optional logical name for this source."}
                        },
                        "required": ["type"]
                    }
                }
            },
            "required": ["inputs"]
        }),
    ).with_example(serde_json::json!({
        "inputs": [
            {"type": "kafka", "name": "events", "topics": ["events"]},
            {"type": "kafka", "name": "logs", "topics": ["logs"]}
        ]
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::MessageBatch;
    use arkflow_core::input::NoopAck;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Scripted input double: every `read()` takes the next scripted result
    /// (and parks when the script is exhausted), tracking how many reads are
    /// active so tests can observe reader generations.
    struct MockInput {
        script: flume::Receiver<Msg>,
        total_reads: Arc<AtomicUsize>,
        active_reads: Arc<AtomicUsize>,
    }

    impl MockInput {
        /// Parks immediately: no scripted results, script sender returned to
        /// the test for later injection.
        fn parking() -> (flume::Sender<Msg>, Self) {
            Self::scripted(Vec::new())
        }

        fn scripted(results: Vec<Msg>) -> (flume::Sender<Msg>, Self) {
            let (tx, rx) = flume::unbounded();
            for result in results {
                let _ = tx.send(result);
            }
            (
                tx,
                Self {
                    script: rx,
                    total_reads: Arc::new(AtomicUsize::new(0)),
                    active_reads: Arc::new(AtomicUsize::new(0)),
                },
            )
        }
    }

    #[async_trait]
    impl Input for MockInput {
        async fn connect(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
            struct ActiveGuard(Arc<AtomicUsize>);
            impl Drop for ActiveGuard {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }
            let _guard = ActiveGuard(Arc::clone(&self.active_reads));
            self.active_reads.fetch_add(1, Ordering::SeqCst);
            self.total_reads.fetch_add(1, Ordering::SeqCst);
            match self.script.recv_async().await {
                Ok(Msg::Message(batch, ack)) => Ok((batch, ack)),
                Ok(Msg::Err(e)) => Err(e),
                Err(_) => Err(Error::EOF),
            }
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    fn binary_msg(body: &str) -> Msg {
        Msg::Message(
            Arc::new(MessageBatch::new_binary(vec![body.as_bytes().to_vec()]).unwrap()),
            Arc::new(NoopAck),
        )
    }

    fn multiple_inputs_with(inputs: Vec<Arc<dyn Input>>) -> Arc<MultipleInputs> {
        let (sender, receiver) = flume::bounded(CHANNEL_CAPACITY);
        Arc::new(MultipleInputs {
            input_name: None,
            inputs,
            sender,
            receiver,
            closed: CancellationToken::new(),
            generation: Mutex::new(None),
        })
    }

    /// Poll until the counter reaches the expected value (spawned reader
    /// tasks start asynchronously after connect returns).
    async fn wait_reads(counter: &AtomicUsize, expected: usize) {
        for _ in 0..200 {
            if counter.load(Ordering::SeqCst) >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            counter.load(Ordering::SeqCst),
            expected,
            "reader tasks did not reach the expected read count"
        );
    }

    #[tokio::test]
    async fn test_empty_inputs_rejected() {
        let resource = Resource {
            temporary: std::collections::HashMap::new(),
            input_names: std::cell::RefCell::new(Vec::new()),
        };
        let error = MultipleInputsBuilder
            .build(None, &Some(serde_json::json!({"inputs": []})), None, &resource)
            .err()
            .expect("an empty child list must be rejected at build time");
        assert!(error.to_string().contains("at least one"));
    }

    #[tokio::test]
    async fn test_reconnect_replaces_reader_generation() {
        let (_script, mock) = MockInput::parking();
        let total_reads = Arc::clone(&mock.total_reads);
        let input = multiple_inputs_with(vec![Arc::new(mock)]);

        input.connect().await.unwrap();
        wait_reads(&total_reads, 1).await;

        // The engine's Disconnection path reconnects the same instance:
        // the previous generation must be cancelled and awaited first.
        input.connect().await.unwrap();
        wait_reads(&total_reads, 2).await;
        assert_eq!(
            input.generation.lock().await.as_ref().map(|g| {
                g.token.is_cancelled()
            }),
            Some(false),
            "the live generation must not be cancelled"
        );
    }

    #[tokio::test]
    async fn test_child_error_surfaces_once() {
        // Enough scripted errors that an old-style retry loop would keep
        // forwarding them; the fixed reader must exit after the first.
        let errors: Vec<Msg> = (0..5)
            .map(|_| Msg::Err(Error::Process("scripted failure".into())))
            .collect();
        let (_script, mock) = MockInput::scripted(errors);
        let input = multiple_inputs_with(vec![Arc::new(mock)]);

        input.connect().await.unwrap();

        assert!(input.read().await.is_err());

        let second = tokio::time::timeout(Duration::from_millis(150), input.read()).await;
        assert!(
            second.is_err(),
            "the reader must exit after surfacing one error, not hot-loop"
        );
    }

    #[tokio::test]
    async fn test_bounded_channel_backpressures_and_close_releases() {
        let messages: Vec<Msg> = (0..CHANNEL_CAPACITY + 50)
            .map(|i| binary_msg(&format!("m{}", i)))
            .collect();
        let (_script, mock) = MockInput::scripted(messages);
        let total_reads = Arc::clone(&mock.total_reads);
        let input = multiple_inputs_with(vec![Arc::new(mock)]);

        input.connect().await.unwrap();
        // No consumer: the reader fills the bounded channel and then stalls
        // in the send instead of consuming the whole script.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let consumed = total_reads.load(Ordering::SeqCst);
        assert!(consumed > 0, "the reader should have started by now");
        assert!(
            consumed <= CHANNEL_CAPACITY + 1,
            "producer must stall at the bounded capacity (consumed {consumed})"
        );
        assert!(
            consumed < CHANNEL_CAPACITY + 50,
            "script must not be exhausted while the consumer is absent"
        );

        // close() releases the blocked send and completes promptly.
        let closed = tokio::time::timeout(Duration::from_secs(2), input.close()).await;
        assert!(closed.is_ok(), "close must not hang on a full channel");
    }

    #[tokio::test]
    async fn test_reconnect_clears_stale_errors_keeps_data() {
        let (_script, mock) = MockInput::parking();
        let input = multiple_inputs_with(vec![Arc::new(mock)]);
        input.connect().await.unwrap();

        // Leftovers a dying generation could have queued: one stale error in
        // front of real data.
        let _ = input
            .sender
            .send(Msg::Err(Error::Disconnection))
            .map_err(|_| ());
        let _ = input.sender.send(binary_msg("a")).map_err(|_| ());
        let _ = input.sender.send(binary_msg("b")).map_err(|_| ());

        input.connect().await.unwrap();

        // Data survives in order...
        assert_eq!(input.read().await.unwrap().0.len(), 1);
        assert_eq!(input.read().await.unwrap().0.len(), 1);
        // ...and the stale error no longer triggers a spurious failure.
        let third = tokio::time::timeout(Duration::from_millis(150), input.read()).await;
        assert!(
            third.is_err(),
            "stale generation errors must be cleared on reconnect"
        );
    }
}
