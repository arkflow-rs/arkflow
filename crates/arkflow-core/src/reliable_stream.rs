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

//! Reliable stream processing with acknowledgment persistence
//!
//! This module provides a stream implementation that uses reliable acknowledgment
//! processing to prevent data loss during failures.

use crate::buffer::Buffer;
use crate::idempotent_ack::{AckBuilder, AckCache, AckId};
use crate::input::Ack;
use crate::reliable_ack::ReliableAckProcessor;
use crate::{input::Input, output::Output, pipeline::Pipeline, Error, MessageBatch, Resource};
use flume::{Receiver, Sender};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

const BACKPRESSURE_THRESHOLD: u64 = 1024;

/// A reliable stream structure with persistent acknowledgments
pub struct ReliableStream {
    input: Arc<dyn Input>,
    pipeline: Arc<Pipeline>,
    output: Arc<dyn Output>,
    error_output: Option<Arc<dyn Output>>,
    thread_num: u32,
    buffer: Option<Arc<dyn Buffer>>,
    resource: Resource,
    sequence_counter: Arc<AtomicU64>,
    next_seq: Arc<AtomicU64>,
    ack_processor: Option<Arc<ReliableAckProcessor>>,
    ack_cache: Arc<AckCache>,
}

enum ProcessorData {
    Err(MessageBatch, Error),
    Ok(Vec<MessageBatch>),
}

impl ReliableStream {
    /// Create a new reliable stream.
    pub fn new(
        input: Arc<dyn Input>,
        pipeline: Pipeline,
        output: Arc<dyn Output>,
        error_output: Option<Arc<dyn Output>>,
        buffer: Option<Arc<dyn Buffer>>,
        resource: Resource,
        thread_num: u32,
    ) -> Self {
        Self {
            input,
            pipeline: Arc::new(pipeline),
            output,
            error_output,
            buffer,
            resource,
            thread_num,
            sequence_counter: Arc::new(AtomicU64::new(0)),
            next_seq: Arc::new(AtomicU64::new(0)),
            ack_processor: None,
            ack_cache: Arc::new(AckCache::new()),
        }
    }

    /// Initialize reliable ack processor
    pub fn with_ack_processor(mut self, ack_processor: ReliableAckProcessor) -> Self {
        self.ack_processor = Some(Arc::new(ack_processor));
        self
    }

    /// Running reliable stream processing
    pub async fn run(&mut self, cancellation_token: CancellationToken) -> Result<(), Error> {
        // Connect input and output
        self.input.connect().await?;
        self.output.connect().await?;
        if let Some(ref error_output) = self.error_output {
            error_output.connect().await?;
        }
        for (_, temporary) in &self.resource.temporary {
            temporary.connect().await?
        }

        let (input_sender, input_receiver) =
            flume::bounded::<(MessageBatch, Arc<dyn Ack>)>(self.thread_num as usize * 4);
        let (output_sender, output_receiver) =
            flume::bounded::<(ProcessorData, Arc<dyn Ack>, u64)>(self.thread_num as usize * 4);

        let tracker = TaskTracker::new();

        // Initialize reliable ack processor if not already set
        if self.ack_processor.is_none() {
            let temp_dir = std::env::temp_dir();
            let wal_path = temp_dir.join(format!("ack_wal_{}", std::process::id()));
            let ack_processor =
                ReliableAckProcessor::new(&tracker, cancellation_token.clone(), &wal_path)?;
            self.ack_processor = Some(Arc::new(ack_processor));
        }

        let ack_processor = self.ack_processor.clone();
        let ack_cache = self.ack_cache.clone();

        // Input
        tracker.spawn(Self::do_input(
            cancellation_token.clone(),
            self.input.clone(),
            input_sender.clone(),
            self.buffer.clone(),
            ack_cache.clone(),
        ));

        // Buffer
        if let Some(buffer) = self.buffer.clone() {
            tracker.spawn(Self::do_buffer(
                cancellation_token.clone(),
                buffer,
                input_sender,
            ));
        } else {
            drop(input_sender)
        }

        // Processor
        for i in 0..self.thread_num {
            tracker.spawn(Self::do_processor(
                i,
                self.pipeline.clone(),
                input_receiver.clone(),
                output_sender.clone(),
                self.sequence_counter.clone(),
                self.next_seq.clone(),
            ));
        }

        // Close the output sender to notify all workers
        drop(output_sender);

        // Output
        tracker.spawn(Self::do_output(
            self.next_seq.clone(),
            output_receiver,
            self.output.clone(),
            self.error_output.clone(),
            ack_processor,
        ));

        tracker.close();
        tracker.wait().await;

        info!("Closing....");
        self.close().await?;
        info!("Closed.");
        info!("Exited.");

        Ok(())
    }

    async fn do_input(
        cancellation_token: CancellationToken,
        input: Arc<dyn Input>,
        input_sender: Sender<(MessageBatch, Arc<dyn Ack>)>,
        buffer_option: Option<Arc<dyn Buffer>>,
        ack_cache: Arc<AckCache>,
    ) {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                },
                result = input.read() =>{
                    match result {
                    Ok(msg) => {
                            // Create reliable ack wrapper
                            let ack_id = AckId::new(
                                "stream_input".to_string(),
                                format!("msg_{}", std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis())
                            );

                            let reliable_ack = AckBuilder::new(msg.1)
                                .with_ack_id(ack_id.clone())
                                .with_cache(ack_cache.clone())
                                .with_tracing()
                                .build();

                            if let Some(buffer) = &buffer_option {
                                if let Err(e) = buffer.write(msg.0, reliable_ack).await {
                                    error!("Failed to send input message: {}", e);
                                    break;
                                }
                            } else {
                                if let Err(e) = input_sender.send_async((msg.0, reliable_ack)).await {
                                    error!("Failed to send input message: {}", e);
                                    break;
                                }
                            }

                    }
                    Err(e) => {
                        match e {
                            Error::EOF => {
                                // When input is complete, close the sender to notify all workers
                                cancellation_token.cancel();
                                break;
                            }
                            Error::Disconnection => loop {
                                match input.connect().await {
                                    Ok(_) => {
                                        info!("input reconnected");
                                        break;
                                    }
                                    Err(e) => {
                                        error!("{}", e);
                                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                    }
                                };
                            },
                            Error::Config(e) => {
                                error!("{}", e);
                                break;
                            }
                            _ => {
                                error!("{}", e);
                            }
                        };
                    }
                    };
                }
            }
        }
        info!("Input stopped");
    }

    async fn do_buffer(
        cancellation_token: CancellationToken,
        buffer: Arc<dyn Buffer>,
        input_sender: Sender<(MessageBatch, Arc<dyn Ack>)>,
    ) {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                },
                result = buffer.read() =>{
                    match result {
                        Ok(Some(v)) => {
                             if let Err(e) = input_sender.send_async(v).await {
                                    error!("Failed to send input message: {}", e);
                                    break;
                                }
                        }
                        Err(e) => {
                            error!("Failed to read buffer:{}", e);
                        }
                        _=>{}
                    }
                }
            }
        }

        if let Err(e) = buffer.flush().await {
            error!("Failed to flush buffer: {}", e);
        }

        info!("Buffer flushed");

        match buffer.read().await {
            Ok(Some(v)) => {
                if let Err(e) = input_sender.send_async(v).await {
                    error!("Failed to send input message: {}", e);
                }
            }
            _ => {}
        }
        info!("Buffer stopped");
    }

    async fn do_processor(
        i: u32,
        pipeline: Arc<Pipeline>,
        input_receiver: Receiver<(MessageBatch, Arc<dyn Ack>)>,
        output_sender: Sender<(ProcessorData, Arc<dyn Ack>, u64)>,
        sequence_counter: Arc<AtomicU64>,
        next_seq: Arc<AtomicU64>,
    ) {
        let i = i + 1;
        info!("Reliable processor worker {} started", i);
        loop {
            let pending_messages =
                sequence_counter.load(Ordering::Acquire) - next_seq.load(Ordering::Acquire);
            if pending_messages > BACKPRESSURE_THRESHOLD {
                let wait_time = std::cmp::min(
                    500,
                    100 + (pending_messages as u64 - BACKPRESSURE_THRESHOLD) / 100 * 10,
                );
                tokio::time::sleep(std::time::Duration::from_millis(wait_time)).await;
                continue;
            }

            let Ok((msg, ack)) = input_receiver.recv_async().await else {
                break;
            };

            // Process messages through pipeline
            let processed = pipeline.process(msg.clone()).await;
            let seq = sequence_counter.fetch_add(1, Ordering::AcqRel);

            // Process result messages
            match processed {
                Ok(msgs) => {
                    if let Err(e) = output_sender
                        .send_async((ProcessorData::Ok(msgs), ack, seq))
                        .await
                    {
                        error!("Failed to send processed message: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    if let Err(e) = output_sender
                        .send_async((ProcessorData::Err(msg, e), ack, seq))
                        .await
                    {
                        error!("Failed to send processed message: {}", e);
                        break;
                    }
                }
            }
        }
        info!("Reliable processor worker {} stopped", i);
    }

    async fn do_output(
        next_seq: Arc<AtomicU64>,
        output_receiver: Receiver<(ProcessorData, Arc<dyn Ack>, u64)>,
        output: Arc<dyn Output>,
        err_output: Option<Arc<dyn Output>>,
        ack_processor: Option<Arc<ReliableAckProcessor>>,
    ) {
        let mut tree_map: BTreeMap<u64, (ProcessorData, Arc<dyn Ack>)> = BTreeMap::new();

        loop {
            let Ok((data, new_ack, new_seq)) = output_receiver.recv_async().await else {
                for (_, (data, x)) in tree_map {
                    Self::output(data, &x, &output, err_output.as_ref(), &ack_processor).await;
                }
                break;
            };

            tree_map.insert(new_seq, (data, new_ack));

            loop {
                let Some((current_seq, _)) = tree_map.first_key_value() else {
                    break;
                };
                let next_seq_val = next_seq.load(Ordering::Acquire);
                if next_seq_val != *current_seq {
                    break;
                }

                let Some((data, ack)) = tree_map.remove(&next_seq_val) else {
                    break;
                };

                Self::output(data, &ack, &output, err_output.as_ref(), &ack_processor).await;
                next_seq.fetch_add(1, Ordering::Release);
            }
        }

        info!("Reliable output stopped")
    }

    async fn output(
        data: ProcessorData,
        ack: &Arc<dyn Ack>,
        output: &Arc<dyn Output>,
        err_output: Option<&Arc<dyn Output>>,
        ack_processor: &Option<Arc<ReliableAckProcessor>>,
    ) {
        match data {
            ProcessorData::Err(msg, e) => match err_output {
                None => {
                    if let Some(processor) = ack_processor {
                        if let Err(e) = processor
                            .ack(
                                ack.clone(),
                                "error".to_string(),
                                msg.get_input_name()
                                    .unwrap_or_else(|| "unknown".to_string())
                                    .into_bytes(),
                            )
                            .await
                        {
                            error!("Failed to send error ack to reliable processor: {}", e);
                        }
                    } else {
                        ack.ack().await;
                    }
                    error!("{e}");
                }
                Some(err_output) => match err_output.write(msg).await {
                    Ok(_) => {
                        if let Some(processor) = ack_processor {
                            if let Err(e) = processor
                                .ack(
                                    ack.clone(),
                                    "error_output".to_string(),
                                    b"error_output_success".to_vec(),
                                )
                                .await
                            {
                                error!(
                                    "Failed to send error_output ack to reliable processor: {}",
                                    e
                                );
                            }
                        } else {
                            ack.ack().await;
                        }
                    }
                    Err(e) => {
                        error!("{}", e);
                    }
                },
            },
            ProcessorData::Ok(msgs) => {
                let size = msgs.len();
                let mut success_cnt = 0;
                for x in msgs {
                    match output.write(x).await {
                        Ok(_) => {
                            success_cnt = success_cnt + 1;
                        }
                        Err(e) => {
                            error!("{}", e);
                        }
                    }
                }

                if success_cnt >= size {
                    if let Some(processor) = ack_processor {
                        if let Err(e) = processor
                            .ack(
                                ack.clone(),
                                "success".to_string(),
                                b"output_success".to_vec(),
                            )
                            .await
                        {
                            error!("Failed to send success ack to reliable processor: {}", e);
                        }
                    } else {
                        ack.ack().await;
                    }
                }
            }
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        // Closing order: input -> pipeline -> buffer -> output -> error output
        info!("input close...");
        if let Err(e) = self.input.close().await {
            error!("Failed to close input: {}", e);
        }
        info!("input closed");

        info!("buffer close...");
        if let Some(buffer) = &self.buffer {
            if let Err(e) = buffer.close().await {
                error!("Failed to close buffer: {}", e);
            }
        }
        info!("buffer closed");

        info!("pipeline close...");
        if let Err(e) = self.pipeline.close().await {
            error!("Failed to close pipeline: {}", e);
        }
        info!("pipeline closed");

        info!("output close...");
        if let Err(e) = self.output.close().await {
            error!("Failed to close output: {}", e);
        }
        info!("output closed");

        info!("error output close...");
        if let Some(error_output) = &self.error_output {
            if let Err(e) = error_output.close().await {
                error!("Failed to close error output: {}", e);
            }
        }
        info!("error output closed");

        // Clear ack cache
        self.ack_cache.clear().await;

        Ok(())
    }
}

/// Reliable stream configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReliableStreamConfig {
    pub input: crate::input::InputConfig,
    pub pipeline: crate::pipeline::PipelineConfig,
    pub output: crate::output::OutputConfig,
    pub error_output: Option<crate::output::OutputConfig>,
    pub buffer: Option<crate::buffer::BufferConfig>,
    pub temporary: Option<Vec<crate::temporary::TemporaryConfig>>,
    pub enable_reliable_ack: bool,
    pub wal_path: Option<String>,
}

impl ReliableStreamConfig {
    /// Build reliable stream based on configuration
    pub fn build(&self) -> Result<ReliableStream, Error> {
        let mut resource = Resource {
            temporary: HashMap::new(),
            input_names: RefCell::default(),
        };

        if let Some(temporary_configs) = &self.temporary {
            resource.temporary = HashMap::with_capacity(temporary_configs.len());
            for temporary_config in temporary_configs {
                resource.temporary.insert(
                    temporary_config.name.clone(),
                    temporary_config.build(&resource)?,
                );
            }
        };

        let input = self.input.build(&resource)?;
        let (pipeline, thread_num) = self.pipeline.build(&resource)?;
        let output = self.output.build(&resource)?;
        let error_output = if let Some(error_output_config) = &self.error_output {
            Some(error_output_config.build(&resource)?)
        } else {
            None
        };
        let buffer = if let Some(buffer_config) = &self.buffer {
            Some(buffer_config.build(&resource)?)
        } else {
            None
        };

        let mut stream = ReliableStream::new(
            input,
            pipeline,
            output,
            error_output,
            buffer,
            resource,
            thread_num,
        );

        // Initialize ack processor if enabled
        if self.enable_reliable_ack {
            let temp_dir = std::env::temp_dir();
            let wal_path_str = self
                .wal_path
                .as_ref()
                .cloned()
                .unwrap_or_else(|| temp_dir.join("ack_wal").to_string_lossy().into_owned());
            let wal_path = std::path::Path::new(&wal_path_str);

            let tracker = TaskTracker::new();
            let cancellation_token = CancellationToken::new();
            let ack_processor = ReliableAckProcessor::new(&tracker, cancellation_token, wal_path)?;

            stream = stream.with_ack_processor(ack_processor);
        }

        Ok(stream)
    }
}
