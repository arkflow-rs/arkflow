use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{Error, MessageBatch};
use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateInputConfig {
    context: String,
    #[serde(deserialize_with = "deserialize_duration")]
    interval: Duration,
    count: Option<usize>,
    batch_size: Option<usize>,
}

pub struct GenerateInput {
    config: GenerateInputConfig,
    count: AtomicI64,
    batch_size: usize,
}
impl GenerateInput {
    pub fn new(config: GenerateInputConfig) -> Result<Self, Error> {
        let batch_size = config.batch_size.unwrap_or(1);

        Ok(Self {
            config,
            count: AtomicI64::new(0),
            batch_size,
        })
    }
}

#[async_trait]
impl Input for GenerateInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        tokio::time::sleep(self.config.interval).await;

        if let Some(count) = self.config.count {
            let current_count = self.count.load(Ordering::SeqCst);
            if current_count >= count as i64 {
                return Err(Error::EOF);
            }
            // Check if adding the current batch would exceed the total count limit
            if current_count + self.batch_size as i64 > count as i64 {
                return Err(Error::EOF);
            }
        }
        let mut msgs = Vec::with_capacity(self.batch_size);
        for _ in 0..self.batch_size {
            let s = self.config.context.clone();
            msgs.push(s.into_bytes())
        }

        self.count
            .fetch_add(self.batch_size as i64, Ordering::SeqCst);

        Ok((MessageBatch::new_binary(msgs), Arc::new(NoopAck)))
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    humantime::parse_duration(&s).map_err(serde::de::Error::custom)
}

pub(crate) struct GenerateInputBuilder;
impl InputBuilder for GenerateInputBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Input>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Generate input configuration is missing".to_string(),
            ));
        }
        let config: GenerateInputConfig =
            serde_json::from_value::<GenerateInputConfig>(config.clone().unwrap())?;
        Ok(Arc::new(GenerateInput::new(config)?))
    }
}

pub fn init() {
    register_input_builder("generate", Arc::new(GenerateInputBuilder));
}

#[cfg(test)]
mod tests {
    use crate::input::{generate::GenerateInput, generate::GenerateInputConfig};
    use arkflow_core::input::Input;
    use arkflow_core::Error;
    use std::time::Duration;

    #[tokio::test]
    async fn test_generate_input_new() {
        // Test creating GenerateInput instance
        let config = GenerateInputConfig {
            context: "test message".to_string(),
            interval: Duration::from_millis(10),
            count: Some(5),
            batch_size: Some(2),
        };
        let input = GenerateInput::new(config).unwrap();
        assert_eq!(input.batch_size, 2);
    }

    #[tokio::test]
    async fn test_generate_input_default_batch_size() {
        // Test default batch size
        let config = GenerateInputConfig {
            context: "test message".to_string(),
            interval: Duration::from_millis(10),
            count: Some(5),
            batch_size: None,
        };
        let input = GenerateInput::new(config).unwrap();
        assert_eq!(input.batch_size, 1); // Default batch size should be 1
    }

    #[tokio::test]
    async fn test_generate_input_connect() {
        // Test connection method
        let config = GenerateInputConfig {
            context: "test message".to_string(),
            interval: Duration::from_millis(10),
            count: Some(5),
            batch_size: Some(2),
        };
        let input = GenerateInput::new(config).unwrap();
        assert!(input.connect().await.is_ok()); // Connection should succeed
    }

    #[tokio::test]
    async fn test_generate_input_read() {
        // Test reading messages
        let config = GenerateInputConfig {
            context: "test message".to_string(),
            interval: Duration::from_millis(10),
            count: Some(5),
            batch_size: Some(2),
        };
        let input = GenerateInput::new(config).unwrap();

        // Read the first batch of messages
        let (batch, ack) = input.read().await.unwrap();
        let messages = batch.as_binary();
        assert_eq!(messages.len(), 2); // Batch size is 2
        for msg in messages {
            assert_eq!(String::from_utf8(msg.to_vec()).unwrap(), "test message");
        }
        ack.ack().await;

        // Read the second batch of messages
        let (batch, ack) = input.read().await.unwrap();
        let messages = batch.as_binary();
        assert_eq!(messages.len(), 2);
        ack.ack().await;

        // Read the third batch of messages (reached the limit of count=5, because 2+2+2>5)
        let result = input.read().await;
        assert!(matches!(result, Err(Error::EOF)));
    }

    #[tokio::test]
    async fn test_generate_input_without_count_limit() {
        // Test the case without message count limit
        let config = GenerateInputConfig {
            context: "test message".to_string(),
            interval: Duration::from_millis(10),
            count: None, // No limit
            batch_size: Some(1),
        };
        let input = GenerateInput::new(config).unwrap();

        // Can read multiple times consecutively
        for _ in 0..10 {
            let result = input.read().await;
            assert!(result.is_ok());
            let (batch, ack) = result.unwrap();
            let messages = batch.as_binary();
            assert_eq!(messages.len(), 1);
            ack.ack().await;
        }
    }

    #[tokio::test]
    async fn test_generate_input_close() {
        // Test closing connection
        let config = GenerateInputConfig {
            context: "test message".to_string(),
            interval: Duration::from_millis(10),
            count: Some(5),
            batch_size: Some(2),
        };
        let input = GenerateInput::new(config).unwrap();
        assert!(input.close().await.is_ok()); // Closing should succeed
    }

    #[tokio::test]
    async fn test_generate_input_exact_count() {
        // Test exact count limit
        let config = GenerateInputConfig {
            context: "test message".to_string(),
            interval: Duration::from_millis(10),
            count: Some(4),
            batch_size: Some(2),
        };
        let input = GenerateInput::new(config).unwrap();

        // Read the first batch of messages (2 messages)
        let result = input.read().await;
        assert!(result.is_ok());

        // Read the second batch of messages (2 messages, reaching the limit)
        let result = input.read().await;
        assert!(result.is_ok());

        // Try to read the third batch of messages (should return Done error)
        let result = input.read().await;
        assert!(matches!(result, Err(Error::EOF)));
    }

    #[tokio::test]
    async fn test_deserialize_duration() {
        // Test deserialization from JSON
        let json = r#"{
            "context": "test message",
            "interval": "10ms",
            "count": 5,
            "batch_size": 2
        }"#;

        let config: GenerateInputConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.context, "test message");
        assert_eq!(config.interval, Duration::from_millis(10));
        assert_eq!(config.count, Some(5));
        assert_eq!(config.batch_size, Some(2));
    }
}
