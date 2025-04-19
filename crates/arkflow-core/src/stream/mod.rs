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
use crate::input::Ack;
use crate::{input::Input, output::Output, pipeline::Pipeline, Error, MessageBatch};
use flume::{Receiver, Sender};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};
/// A stream structure, containing input, pipe, output, and an optional buffer.
pub struct Stream {
    input: Arc<dyn Input>,
    pipeline: Arc<Pipeline>,
    output: Arc<dyn Output>,
    error_output: Option<Arc<dyn Output>>,
    thread_num: u32,
    buffer: Option<Arc<dyn Buffer>>,
}

impl Stream {
    /// Create a new stream.
    pub fn new(
        input: Arc<dyn Input>,
        pipeline: Pipeline,
        output: Arc<dyn Output>,
        error_output: Option<Arc<dyn Output>>,
        buffer: Option<Arc<dyn Buffer>>,
        thread_num: u32,
    ) -> Self {
        Self {
            input,
            pipeline: Arc::new(pipeline),
            output,
            error_output,
            buffer,
            thread_num,
        }
    }

    /// Running stream processing
    pub async fn run(&mut self, cancellation_token: CancellationToken) -> Result<(), Error> {
        // Connect input and output
        self.input.connect().await?;
        self.output.connect().await?;
        if let Some(ref error_output) = self.error_output {
            error_output.connect().await?;
        }

        let (input_sender, input_receiver) =
            flume::bounded::<(MessageBatch, Arc<dyn Ack>)>(self.thread_num as usize * 4);
        let (output_sender, output_receiver) =
            flume::bounded::<(Vec<MessageBatch>, Arc<dyn Ack>)>(self.thread_num as usize * 4);
        let (error_output_sender, error_output_receiver) = if self.error_output.is_none() {
            (None, None)
        } else {
            let (error_output_sender, error_output_receiver) =
                flume::bounded::<(MessageBatch, Arc<dyn Ack>)>(self.thread_num as usize * 4);
            (Some(error_output_sender), Some(error_output_receiver))
        };

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
                error_output_sender.clone(),
            ));
        }

        // Close the output sender to notify all workers
        drop(output_sender);
        drop(error_output_sender);

        // Output
        tracker.spawn(Self::do_output(output_receiver, self.output.clone()));
        tracker.spawn(Self::do_error_output(
            error_output_receiver,
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
        input_sender: Sender<(MessageBatch, Arc<dyn Ack>)>,
        buffer_option: Option<Arc<dyn Buffer>>,
    ) {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                },
                result = input.read() =>{
                    match result {
                    Ok(msg) => {
                            if let Some(buffer) = &buffer_option {
                                if let Err(e) =buffer.write(msg.0,msg.1).await{
                                    error!("Failed to send input message: {}", e);
                                    break;
                                }
                            }else{
                                if let Err(e) = input_sender.send_async(msg).await {
                                    error!("Failed to send input message: {}", e);
                                    break;
                                }
                            }


                    }
                    Err(e) => {
                        match e {
                            Error::EOF => {
                                // When input is complete, close the sender to notify all workers
                                return;
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
                    _ => {}
                    }
                }
            }
        }
        if let Err(e) = buffer.flush().await {
            error!("Failed to flush buffer: {}", e);
        }

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
        output_sender: Sender<(Vec<MessageBatch>, Arc<dyn Ack>)>,
        error_output_sender: Option<Sender<(MessageBatch, Arc<dyn Ack>)>>,
    ) {
        let i = i + 1;
        info!("Processor worker {} started", i);
        loop {
            let Ok((msg, ack)) = input_receiver.recv_async().await else {
                break;
            };
            // Process messages through pipeline
            let processed = pipeline.process(msg.clone()).await;

            // Process result messages
            match processed {
                Ok(msgs) => {
                    if let Err(e) = output_sender.send_async((msgs, ack)).await {
                        error!("Failed to send processed message: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    if let Some(ref error_output_sender) = error_output_sender {
                        if let Err(e) = error_output_sender.send_async((msg, ack)).await {
                            error!("Failed to send error message: {}", e);
                            break;
                        }
                    } else {
                        ack.ack().await;
                    }
                    error!("{}", e)
                }
            }
        }
        info!("Processor worker {} stopped", i);
    }

    async fn do_output(
        output_receiver: Receiver<(Vec<MessageBatch>, Arc<dyn Ack>)>,
        output: Arc<dyn Output>,
    ) {
        loop {
            let Ok(msg) = output_receiver.recv_async().await else {
                break;
            };

            let size = msg.0.len();
            let mut success_cnt = 0;
            for x in msg.0 {
                match output.write(x).await {
                    Ok(_) => {
                        success_cnt = success_cnt + 1;
                    }
                    Err(e) => {
                        error!("{}", e);
                    }
                }
            }

            // Confirm that the message has been successfully processed
            if size == success_cnt {
                msg.1.ack().await;
            }
        }
        info!("Output stopped")
    }

    async fn do_error_output(
        error_output_receiver: Option<Receiver<(MessageBatch, Arc<dyn Ack>)>>,
        error_output: Option<Arc<dyn Output>>,
    ) {
        let Some(error_output_receiver) = error_output_receiver else {
            return;
        };
        let Some(error_output) = error_output else {
            return;
        };

        loop {
            let Ok(msg) = error_output_receiver.recv_async().await else {
                break;
            };

            match error_output.write(msg.0).await {
                Ok(_) => {
                    msg.1.ack().await;
                }
                Err(e) => {
                    error!("{}", e);
                }
            }
        }
        info!("Error output stopped")
    }

    async fn close(&mut self) -> Result<(), Error> {
        // Closing order: input -> pipeline -> buffer -> output -> error output
        if let Err(e) = self.input.close().await {
            error!("Failed to close input: {}", e);
        }

        if let Err(e) = self.pipeline.close().await {
            error!("Failed to close pipeline: {}", e);
        }

        if let Some(buffer) = &self.buffer {
            if let Err(e) = buffer.close().await {
                error!("Failed to close buffer: {}", e);
            }
        }

        if let Err(e) = self.output.close().await {
            error!("Failed to close output: {}", e);
        }

        if let Some(error_output) = &self.error_output {
            if let Err(e) = error_output.close().await {
                error!("Failed to close error output: {}", e);
            }
        }
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
}

impl StreamConfig {
    /// Build stream based on configuration
    pub fn build(&self) -> Result<Stream, Error> {
        let input = self.input.build()?;
        let (pipeline, thread_num) = self.pipeline.build()?;
        let output = self.output.build()?;
        let error_output = if let Some(error_output_config) = &self.error_output {
            Some(error_output_config.build()?)
        } else {
            None
        };
        let buffer = if let Some(buffer_config) = &self.buffer {
            Some(buffer_config.build()?)
        } else {
            None
        };

        Ok(Stream::new(
            input,
            pipeline,
            output,
            error_output,
            buffer,
            thread_num,
        ))
    }
}
