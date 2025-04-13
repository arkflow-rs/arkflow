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

//! Memory input component
//!
//! Read data from an in-memory message queue

use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{Error, MessageBatch};

/// Memory input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryInputConfig {
    /// Initial message for the memory queue
    pub messages: Option<Vec<String>>,
}

/// Memory input component
pub struct MemoryInput {
    queue: Arc<Mutex<VecDeque<MessageBatch>>>,
    connected: AtomicBool,
}

impl MemoryInput {
    /// Create a new memory input component
    pub fn new(config: MemoryInputConfig) -> Result<Self, Error> {
        let mut queue = VecDeque::new();

        // If there is an initial message in the configuration, it is added to the queue
        if let Some(messages) = &config.messages {
            for msg_str in messages {
                queue.push_back(MessageBatch::from_string(msg_str)?);
            }
        }

        Ok(Self {
            queue: Arc::new(Mutex::new(queue)),
            connected: AtomicBool::new(false),
        })
    }

    /// Add a message to the memory input
    pub async fn push(&self, msg: MessageBatch) -> Result<(), Error> {
        let mut queue = self.queue.lock().await;
        queue.push_back(msg);
        Ok(())
    }
}

#[async_trait]
impl Input for MemoryInput {
    async fn connect(&self) -> Result<(), Error> {
        self.connected
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        if !self.connected.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::Connection("The input is not connected".to_string()));
        }

        // Try to get a message from the queue
        let msg_option;
        {
            let mut queue = self.queue.lock().await;
            msg_option = queue.pop_front();
        }

        if let Some(msg) = msg_option {
            Ok((msg, Arc::new(NoopAck)))
        } else {
            Err(Error::EOF)
        }
    }

    async fn close(&self) -> Result<(), Error> {
        self.connected
            .store(false, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

pub(crate) struct MemoryInputBuilder;
impl InputBuilder for MemoryInputBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Input>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Memory input configuration is missing".to_string(),
            ));
        }
        let config: MemoryInputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(MemoryInput::new(config)?))
    }
}

pub fn init() {
    register_input_builder("memory", Arc::new(MemoryInputBuilder));
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::DEFAULT_BINARY_VALUE_FIELD;

    #[tokio::test]
    async fn test_memory_input_connect() {
        let config = MemoryInputConfig { messages: None };
        let input = MemoryInput::new(config).unwrap();
        assert!(input.connect().await.is_ok());
        assert!(input.connected.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_memory_input_read() {
        let config = MemoryInputConfig {
            messages: Some(vec!["test message".to_string()]),
        };
        let input = MemoryInput::new(config).unwrap();
        input.connect().await.unwrap();

        let (msg, ack) = input.read().await.unwrap();
        let result = msg.to_binary(DEFAULT_BINARY_VALUE_FIELD).unwrap();
        assert_eq!(
            String::from_utf8_lossy(result.get(0).unwrap()),
            "test message"
        );
        ack.ack().await;
    }

    #[tokio::test]
    async fn test_memory_input_read_not_connected() {
        let config = MemoryInputConfig { messages: None };
        let input = MemoryInput::new(config).unwrap();

        let result = input.read().await;
        assert!(result.is_err());
        if let Err(Error::Connection(err)) = result {
            assert_eq!(err, "The input is not connected");
        } else {
            panic!("Expected connection error");
        }
    }

    #[tokio::test]
    async fn test_memory_input_close() {
        let config = MemoryInputConfig { messages: None };
        let input = MemoryInput::new(config).unwrap();
        input.connect().await.unwrap();
        assert!(input.close().await.is_ok());
        assert!(!input.connected.load(std::sync::atomic::Ordering::SeqCst));
    }
}
