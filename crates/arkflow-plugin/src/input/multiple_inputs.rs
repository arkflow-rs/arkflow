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
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MultipleInputsConfig {
    inputs: Vec<InputConfig>,
}

struct MultipleInputs {
    #[allow(unused)]
    input_name: Option<String>,
    inputs: Vec<Arc<dyn Input>>,
    sender: Sender<Msg>,
    receiver: Receiver<Msg>,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
}

enum Msg {
    Message(MessageBatchRef, Arc<dyn Ack>),
    Err(Error),
}

#[async_trait]
impl Input for MultipleInputs {
    async fn connect(&self) -> Result<(), Error> {
        for input in &self.inputs {
            input.connect().await?;
        }

        for input in &self.inputs {
            let input = Arc::clone(input);
            let sender = self.sender.clone();
            let cancellation_token = self.cancellation_token.clone();
            self.task_tracker.spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            return;
                        }
                        result = input.read() => {
                            match result {
                                Ok((batch, ack)) => {
                                    if let Err(_) = sender.send_async(Msg::Message(batch, ack)).await {
                                        return;
                                    }
                                }
                                Err(e) => {
                                    match e {
                                        Error::Disconnection|  Error::EOF => {
                                            let _ = sender.send_async(Msg::Err(e)).await;
                                            return;
                                        }
                                        _ => {
                                            if let Err(_) = sender.send_async(Msg::Err(e)).await {
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                        }

                    }
                }
            });
        }

        self.task_tracker.close();

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        let cancellation_token = self.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
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
        self.cancellation_token.cancel();
        self.task_tracker.wait().await;
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
        let (sender, receiver) = flume::unbounded();
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
            cancellation_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        })
    }
}

struct MultipleInputsBuilder;
impl InputBuilder for MultipleInputsBuilder {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<Value>,
        resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Multiple-inputs input configuration is missing".to_string(),
            ));
        }

        let config: MultipleInputsConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(MultipleInputs::new(name, config, resource)?))
    }
}

pub(crate) fn init() -> Result<(), Error> {
    arkflow_core::input::register_input_builder(
        "multiple_inputs",
        Arc::new(MultipleInputsBuilder),
    )?;
    Ok(())
}
