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

        Ok((MessageBatch::new_binary(msgs)?, Arc::new(NoopAck)))
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
