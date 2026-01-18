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

use crate::time::deserialize_duration;
use arkflow_core::codec::Codec;
use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{Error, MessageBatchRef, Resource};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GenerateInputConfig {
    context: String,
    #[serde(deserialize_with = "deserialize_duration")]
    interval: Duration,
    count: Option<usize>,
    batch_size: Option<usize>,
}

struct GenerateInput {
    input_name: Option<String>,
    config: GenerateInputConfig,
    count: AtomicI64,
    batch_size: usize,
    first: AtomicBool,
    codec: Option<Arc<dyn Codec>>,
}
impl GenerateInput {
    fn new(
        name: Option<&String>,
        config: GenerateInputConfig,
        codec: Option<Arc<dyn Codec>>,
    ) -> Result<Self, Error> {
        let batch_size = config.batch_size.unwrap_or(1);
        Ok(Self {
            input_name: name.cloned(),
            config,
            count: AtomicI64::new(0),
            batch_size,
            first: AtomicBool::new(true),
            codec,
        })
    }
}

#[async_trait]
impl Input for GenerateInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatchRef, Arc<dyn Ack>), Error> {
        if !self.first.swap(false, Ordering::SeqCst) {
            tokio::time::sleep(self.config.interval).await;
        }

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

        // Apply codec if configured
        let mut message_batch =
            crate::input::codec_helper::apply_codec_to_payloads(msgs, &self.codec)?;
        message_batch.set_input_name(self.input_name.clone());
        Ok((Arc::new(message_batch), Arc::new(NoopAck)))
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

pub(crate) struct GenerateInputBuilder;
impl InputBuilder for GenerateInputBuilder {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Generate input configuration is missing".to_string(),
            ));
        }
        let config: GenerateInputConfig =
            serde_json::from_value::<GenerateInputConfig>(config.clone().unwrap())?;
        Ok(Arc::new(GenerateInput::new(name, config, codec)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_input_builder("generate", Arc::new(GenerateInputBuilder))
}

#[cfg(test)]
mod tests {
    use arkflow_core::DEFAULT_BINARY_VALUE_FIELD;
    use std::cell::RefCell;

    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_basic_functionality() {
        let config = GenerateInputConfig {
            context: String::from("test message"),
            interval: Duration::from_millis(100),
            count: None,
            batch_size: None,
        };
        let input = GenerateInput::new(None, config, None).unwrap();

        // Test connect
        assert!(input.connect().await.is_ok());

        // Test read
        let (msg_batch, _) = input.read().await.unwrap();
        assert_eq!(msg_batch.len(), 1);
        assert_eq!(
            String::from_utf8(msg_batch.to_binary(DEFAULT_BINARY_VALUE_FIELD).unwrap()[0].to_vec())
                .unwrap(),
            "test message"
        );

        // Test close
        assert!(input.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_batch_size() {
        let config = GenerateInputConfig {
            context: String::from("test"),
            interval: Duration::from_millis(100),
            count: None,
            batch_size: Some(3),
        };
        let input = GenerateInput::new(None, config, None).unwrap();

        let (msg_batch, _) = input.read().await.unwrap();
        assert_eq!(msg_batch.len(), 3);
        for i in 0..3 {
            assert_eq!(
                String::from_utf8(
                    msg_batch.to_binary(DEFAULT_BINARY_VALUE_FIELD).unwrap()[i].to_vec()
                )
                .unwrap(),
                "test"
            );
        }
    }

    #[tokio::test]
    async fn test_count_limit() {
        let config = GenerateInputConfig {
            context: String::from("test"),
            interval: Duration::from_millis(100),
            count: Some(2),
            batch_size: Some(1),
        };
        let input = GenerateInput::new(None, config, None).unwrap();

        // First read should succeed
        assert!(input.read().await.is_ok());
        // Second read should succeed
        assert!(input.read().await.is_ok());
        // Third read should return EOF
        assert!(matches!(input.read().await, Err(Error::EOF)));
    }

    #[tokio::test]
    async fn test_count_with_batch_size() {
        let config = GenerateInputConfig {
            context: String::from("test"),
            interval: Duration::from_millis(100),
            count: Some(3),
            batch_size: Some(2),
        };
        let input = GenerateInput::new(None, config, None).unwrap();

        // First read should succeed (2 messages)
        let (msg_batch, _) = input.read().await.unwrap();
        assert_eq!(msg_batch.len(), 2);

        // Second read should return EOF because next batch would exceed count
        assert!(matches!(input.read().await, Err(Error::EOF)));
    }

    #[tokio::test]
    async fn test_interval_delay() {
        let config = GenerateInputConfig {
            context: String::from("test"),
            interval: Duration::from_millis(100),
            count: None,
            batch_size: None,
        };
        let input = GenerateInput::new(None, config, None).unwrap();

        // First read should be immediate
        let start = std::time::Instant::now();
        assert!(input.read().await.is_ok());
        assert!(start.elapsed() < Duration::from_millis(50));

        // Second read should wait for interval
        let start = std::time::Instant::now();
        assert!(input.read().await.is_ok());
        assert!(start.elapsed() >= Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_builder() {
        let config_json = serde_json::json!({
            "context": "test",
            "interval": "100ms",
            "count": 1,
            "batch_size": 1
        });

        let builder = GenerateInputBuilder;
        let input = builder
            .build(
                None,
                &Some(config_json),
                None,
                &Resource {
                    temporary: Default::default(),
                    input_names: RefCell::new(Default::default()),
                },
            )
            .unwrap();

        assert!(input.connect().await.is_ok());
        assert!(input.read().await.is_ok());
        assert!(matches!(input.read().await, Err(Error::EOF)));
    }

    #[tokio::test]
    async fn test_builder_missing_config() {
        let builder = GenerateInputBuilder;
        assert!(matches!(
            builder.build(
                None,
                &None,
                None,
                &Resource {
                    temporary: Default::default(),
                    input_names: RefCell::new(Default::default()),
                },
            ),
            Err(Error::Config(_))
        ));
    }
}
