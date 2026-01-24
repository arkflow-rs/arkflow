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

//! Stream component module
//!
//! A stream is a complete data processing unit, containing input, pipeline, and output.

use crate::buffer::Buffer;
use crate::checkpoint::{Barrier, BarrierManager};
use crate::input::Ack;
use crate::metrics;
use crate::{
    input::Input, output::Output, pipeline::Pipeline, Error, MessageBatchRef, ProcessResult,
    Resource,
};
use flume::{Receiver, Sender};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info};

const BACKPRESSURE_THRESHOLD: u64 = 1024;

/// A stream structure, containing input, pipe, output, and an optional buffer.
pub struct Stream {
    input: Arc<dyn Input>,
    pipeline: Arc<Pipeline>,
    output: Arc<dyn Output>,
    error_output: Option<Arc<dyn Output>>,
    thread_num: u32,
    buffer: Option<Arc<dyn Buffer>>,
    resource: Resource,
    sequence_counter: Arc<AtomicU64>,
    next_seq: Arc<AtomicU64>,
    /// Optional barrier manager for checkpoint alignment
    barrier_manager: Option<Arc<BarrierManager>>,
    /// Barrier sender for injecting barriers into processor workers
    barrier_sender: Option<Sender<Barrier>>,
}

enum ProcessorData {
    Err(MessageBatchRef, Error),
    Ok(Vec<MessageBatchRef>),
}

impl Stream {
    /// Create a new stream.
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
            barrier_manager: None,
            barrier_sender: None,
        }
    }

    /// Set the barrier manager for checkpoint alignment
    pub fn with_barrier_manager(mut self, barrier_manager: Arc<BarrierManager>) -> Self {
        self.barrier_manager = Some(barrier_manager);
        self
    }

    /// Running stream processing
    pub async fn run(&mut self, cancellation_token: CancellationToken) -> Result<(), Error> {
        // Connect input and output
        self.input.connect().await?;
        self.output.connect().await?;
        if let Some(ref error_output) = self.error_output {
            error_output.connect().await?;
        }
        for temporary in self.resource.temporary.values() {
            temporary.connect().await?
        }

        let (input_sender, input_receiver) =
            flume::bounded::<(MessageBatchRef, Arc<dyn Ack>)>(self.thread_num as usize * 4);
        let (output_sender, output_receiver) =
            flume::bounded::<(ProcessorData, Arc<dyn Ack>, u64)>(self.thread_num as usize * 4);

        // Create barrier channel if checkpointing is enabled
        let barrier_channel = if self.barrier_manager.is_some() {
            let (tx, rx) = flume::bounded::<Barrier>(1);
            self.barrier_sender = Some(tx.clone());
            Some((tx, rx))
        } else {
            None
        };

        let barrier_sender = barrier_channel.as_ref().map(|(tx, _)| tx.clone());
        let barrier_receiver = barrier_channel.map(|(_, rx)| rx);

        let tracker = TaskTracker::new();

        // Input
        tracker.spawn(Self::do_input(
            cancellation_token.clone(),
            self.input.clone(),
            input_sender.clone(),
            self.buffer.clone(),
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
                self.barrier_manager.clone(),
                barrier_receiver.clone(),
            ));
        }

        // Close the output sender to notify all workers
        drop(output_sender);
        // drop(error_output_sender);

        // Output
        tracker.spawn(Self::do_output(
            self.next_seq.clone(),
            output_receiver,
            self.output.clone(),
            self.error_output.clone(),
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
        input_sender: Sender<(MessageBatchRef, Arc<dyn Ack>)>,
        buffer_option: Option<Arc<dyn Buffer>>,
    ) {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                },
                result = input.read() =>{
                    match result {
                    Ok((msg, ack)) => {
                            // Record metrics if enabled
                            if metrics::is_metrics_enabled() {
                                let row_count = msg.record_batch.num_rows();
                                metrics::MESSAGES_PROCESSED.inc_by(row_count as f64);
                                metrics::INPUT_QUEUE_DEPTH.set(input_sender.len() as f64);
                            }

                            if let Some(buffer) = &buffer_option {
                                if let Err(e) = buffer.write(msg, ack).await {
                                    if metrics::is_metrics_enabled() {
                                        metrics::ERRORS_TOTAL.inc();
                                    }
                                    error!("Failed to send input message: {}", e);
                                    break;
                                }
                            } else if let Err(e) = input_sender.send_async((msg, ack)).await {
                                if metrics::is_metrics_enabled() {
                                    metrics::ERRORS_TOTAL.inc();
                                }
                                error!("Failed to send input message: {}", e);
                                break;
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
        input_sender: Sender<(MessageBatchRef, Arc<dyn Ack>)>,
    ) {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                },
                result = buffer.read() =>{
                    match result {
                        Ok(Some((v, ack))) => {
                             if let Err(e) = input_sender.send_async((v, ack)).await {
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

        if let Ok(Some((v, ack))) = buffer.read().await {
            if let Err(e) = input_sender.send_async((v, ack)).await {
                error!("Failed to send input message: {}", e);
            }
        }
        info!("Buffer stopped");
    }

    async fn do_processor(
        i: u32,
        pipeline: Arc<Pipeline>,
        input_receiver: Receiver<(MessageBatchRef, Arc<dyn Ack>)>,
        output_sender: Sender<(ProcessorData, Arc<dyn Ack>, u64)>,
        sequence_counter: Arc<AtomicU64>,
        next_seq: Arc<AtomicU64>,
        barrier_manager: Option<Arc<BarrierManager>>,
        barrier_receiver: Option<Receiver<Barrier>>,
    ) {
        let i = i + 1;
        info!("Processor worker {} started", i);

        loop {
            // Backpressure control
            let pending_messages =
                sequence_counter.load(Ordering::Acquire) - next_seq.load(Ordering::Acquire);

            // Record backpressure status
            if metrics::is_metrics_enabled() {
                if pending_messages > BACKPRESSURE_THRESHOLD {
                    metrics::BACKPRESSURE_ACTIVE.set(1.0);
                } else {
                    metrics::BACKPRESSURE_ACTIVE.set(0.0);
                }
                metrics::OUTPUT_QUEUE_DEPTH.set(output_sender.len() as f64);
            }

            if pending_messages > BACKPRESSURE_THRESHOLD {
                let wait_time = std::cmp::min(
                    500,
                    100 + (pending_messages - BACKPRESSURE_THRESHOLD) / 100 * 10,
                );
                tokio::time::sleep(std::time::Duration::from_millis(wait_time)).await;
                continue;
            }

            // Check for barrier if checkpointing is enabled
            if let Some(ref receiver) = barrier_receiver {
                if let Some(ref manager) = barrier_manager {
                    // Try to receive barrier without blocking
                    if let Ok(barrier) = receiver.try_recv() {
                        debug!("Processor {} received barrier {}", i, barrier.id);

                        // Acknowledge barrier
                        if let Err(e) = manager.acknowledge_barrier(barrier.id).await {
                            error!("Failed to acknowledge barrier {}: {}", barrier.id, e);
                        }

                        // Wait for barrier alignment (all processors to acknowledge)
                        match manager.wait_for_barrier(barrier.id).await {
                            Ok(_) => {
                                debug!("Processor {} aligned on barrier {}", i, barrier.id);
                                // Continue processing after checkpoint alignment
                            }
                            Err(e) => {
                                error!("Barrier alignment failed for {}: {}", barrier.id, e);
                            }
                        }
                    }
                }
            }

            // Receive and process data
            let Ok((msg, ack)) = input_receiver.recv_async().await else {
                break;
            };

            let start_time = std::time::Instant::now();
            let processed = pipeline.process(msg.clone()).await;
            let seq = sequence_counter.fetch_add(1, Ordering::AcqRel);

            // Record processing latency if metrics enabled
            if metrics::is_metrics_enabled() {
                let latency_ms = start_time.elapsed().as_millis() as f64;
                metrics::PROCESSING_LATENCY_MS.observe(latency_ms);
            }

            match processed {
                Ok(ProcessResult::Single(result_msg)) => {
                    if let Err(e) = output_sender
                        .send_async((ProcessorData::Ok(vec![result_msg]), ack, seq))
                        .await
                    {
                        if metrics::is_metrics_enabled() {
                            metrics::ERRORS_TOTAL.inc();
                        }
                        error!("Failed to send processed message: {}", e);
                        break;
                    }
                }
                Ok(ProcessResult::Multiple(result_msgs)) => {
                    if let Err(e) = output_sender
                        .send_async((ProcessorData::Ok(result_msgs), ack, seq))
                        .await
                    {
                        if metrics::is_metrics_enabled() {
                            metrics::ERRORS_TOTAL.inc();
                        }
                        error!("Failed to send processed message: {}", e);
                        break;
                    }
                }
                Ok(ProcessResult::None) => {
                    // Message filtered out, just ACK
                    ack.ack().await;
                }
                Err(e) => {
                    if metrics::is_metrics_enabled() {
                        metrics::ERRORS_TOTAL.inc();
                    }
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
        info!("Processor worker {} stopped", i);
    }

    async fn do_output(
        next_seq: Arc<AtomicU64>,
        output_receiver: Receiver<(ProcessorData, Arc<dyn Ack>, u64)>,
        output: Arc<dyn Output>,
        err_output: Option<Arc<dyn Output>>,
    ) {
        let mut tree_map: BTreeMap<u64, (ProcessorData, Arc<dyn Ack>)> = BTreeMap::new();

        loop {
            let Ok((data, new_ack, new_seq)) = output_receiver.recv_async().await else {
                for (_, (data, x)) in tree_map {
                    Self::output(data, &x, &output, err_output.as_ref()).await;
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

                Self::output(data, &ack, &output, err_output.as_ref()).await;
                next_seq.fetch_add(1, Ordering::Release);
            }
        }

        info!("Output stopped")
    }

    async fn output(
        data: ProcessorData,
        ack: &Arc<dyn Ack>,
        output: &Arc<dyn Output>,
        err_output: Option<&Arc<dyn Output>>,
    ) {
        match data {
            ProcessorData::Err(msg, e) => {
                if metrics::is_metrics_enabled() {
                    metrics::ERRORS_TOTAL.inc();
                }
                match err_output {
                    None => {
                        ack.ack().await;
                        error!("{e}");
                    }
                    Some(err_output) => match err_output.write(msg).await {
                        Ok(_) => {
                            ack.ack().await;
                        }
                        Err(e) => {
                            if metrics::is_metrics_enabled() {
                                metrics::ERRORS_TOTAL.inc();
                            }
                            error!("{}", e);
                        }
                    },
                }
            }
            ProcessorData::Ok(msgs) => {
                let size = msgs.len();
                let mut success_cnt = 0;
                for msg in msgs {
                    match output.write(msg).await {
                        Ok(_) => {
                            success_cnt += 1;
                        }
                        Err(e) => {
                            if metrics::is_metrics_enabled() {
                                metrics::ERRORS_TOTAL.inc();
                            }
                            error!("{}", e);
                        }
                    }
                }

                if success_cnt >= size {
                    ack.ack().await;
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

        Ok(())
    }
}

/// Stream configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StreamConfig {
    pub input: crate::input::InputConfig,
    pub pipeline: crate::pipeline::PipelineConfig,
    pub output: crate::output::OutputConfig,
    pub error_output: Option<crate::output::OutputConfig>,
    pub buffer: Option<crate::buffer::BufferConfig>,
    pub temporary: Option<Vec<crate::temporary::TemporaryConfig>>,
}

impl StreamConfig {
    /// Build stream based on configuration
    pub fn build(&self) -> Result<Stream, Error> {
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

        Ok(Stream::new(
            input,
            pipeline,
            output,
            error_output,
            buffer,
            resource,
            thread_num,
        ))
    }
}
