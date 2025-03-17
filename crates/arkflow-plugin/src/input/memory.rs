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
                queue.push_back(MessageBatch::from_string(msg_str));
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
            Err(Error::Done)
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

    #[tokio::test]
    async fn test_memory_input_new() {
        // Test creating MemoryInput instance without initial messages
        let config = MemoryInputConfig { messages: None };
        let input = MemoryInput::new(config);
        assert!(input.is_ok());

        // Test creating MemoryInput instance with initial messages
        let messages = vec!["message1".to_string(), "message2".to_string()];
        let config = MemoryInputConfig {
            messages: Some(messages),
        };
        let input = MemoryInput::new(config);
        assert!(input.is_ok());
    }

    #[tokio::test]
    async fn test_memory_input_connect() {
        let config = MemoryInputConfig { messages: None };
        let input = MemoryInput::new(config).unwrap();

        // Test connection
        let result = input.connect().await;
        assert!(result.is_ok());

        // Verify connection status
        assert!(input.connected.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_memory_input_read_without_connect() {
        let config = MemoryInputConfig { messages: None };
        let input = MemoryInput::new(config).unwrap();

        // Reading without connection should return an error
        let result = input.read().await;
        assert!(result.is_err());
        match result {
            Err(Error::Connection(_)) => {} // Expected error type
            _ => panic!("Expected Connection error"),
        }
    }

    #[tokio::test]
    async fn test_memory_input_read_empty_queue() {
        let config = MemoryInputConfig { messages: None };
        let input = MemoryInput::new(config).unwrap();

        // Connect
        assert!(input.connect().await.is_ok());

        // Queue is empty, should return Done error
        let result = input.read().await;
        assert!(result.is_err());
        match result {
            Err(Error::Done) => {} // Expected error type
            _ => panic!("Expected Done error"),
        }
    }

    #[tokio::test]
    async fn test_memory_input_read_with_initial_messages() {
        let messages = vec!["message1".to_string(), "message2".to_string()];
        let config = MemoryInputConfig {
            messages: Some(messages),
        };
        let input = MemoryInput::new(config).unwrap();

        // Connect
        assert!(input.connect().await.is_ok());

        // Read the first message
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["message1"]);
        ack.ack().await;

        // Read the second message
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["message2"]);
        ack.ack().await;

        // Queue is empty, should return Done error
        let result = input.read().await;
        assert!(result.is_err());
        match result {
            Err(Error::Done) => {} // Expected error type
            _ => panic!("Expected Done error"),
        }
    }

    #[tokio::test]
    async fn test_memory_input_push() {
        let config = MemoryInputConfig { messages: None };
        let input = MemoryInput::new(config).unwrap();

        // Connect
        assert!(input.connect().await.is_ok());

        // Push message
        let msg = MessageBatch::from_string("pushed message");
        assert!(input.push(msg).await.is_ok());

        // Read the pushed message
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["pushed message"]);
        ack.ack().await;

        // Queue is empty, should return Done error
        let result = input.read().await;
        assert!(result.is_err());
        match result {
            Err(Error::Done) => {} // Expected error type
            _ => panic!("Expected Done error"),
        }
    }

    #[tokio::test]
    async fn test_memory_input_close() {
        let config = MemoryInputConfig { messages: None };
        let input = MemoryInput::new(config).unwrap();

        // Connect
        assert!(input.connect().await.is_ok());
        assert!(input.connected.load(std::sync::atomic::Ordering::SeqCst));

        // Close
        assert!(input.close().await.is_ok());
        assert!(!input.connected.load(std::sync::atomic::Ordering::SeqCst));

        // Reading after close should return error
        let result = input.read().await;
        assert!(result.is_err());
        match result {
            Err(Error::Connection(_)) => {} // Expected error type
            _ => panic!("Expected Connection error"),
        }
    }

    #[tokio::test]
    async fn test_memory_input_multiple_push_read() {
        let config = MemoryInputConfig { messages: None };
        let input = MemoryInput::new(config).unwrap();

        // Connect
        assert!(input.connect().await.is_ok());

        // Push multiple messages
        let msg1 = MessageBatch::from_string("message1");
        let msg2 = MessageBatch::from_string("message2");
        let msg3 = MessageBatch::from_string("message3");

        assert!(input.push(msg1).await.is_ok());
        assert!(input.push(msg2).await.is_ok());
        assert!(input.push(msg3).await.is_ok());

        // Read messages in order
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["message1"]);
        ack.ack().await;

        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["message2"]);
        ack.ack().await;

        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["message3"]);
        ack.ack().await;

        // Queue is empty, should return Done error
        let result = input.read().await;
        assert!(result.is_err());
        match result {
            Err(Error::Done) => {} // Expected error type
            _ => panic!("Expected Done error"),
        }
    }
}
