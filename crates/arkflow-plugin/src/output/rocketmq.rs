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

//! RocketMQ output component
//!
//! Send the processed data to a RocketMQ topic using HTTP API

use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatch, Resource, DEFAULT_BINARY_VALUE_FIELD};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// RocketMQ output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocketMQOutputConfig {
    /// RocketMQ nameserver addresses (comma separated)
    pub nameserver_addr: String,
    /// Topic to send messages to
    pub topic: String,
    /// Tag for messages (optional)
    pub tag: Option<String>,
    /// Message group for FIFO messages (optional)
    pub message_group: Option<String>,
    /// RocketMQ HTTP API port (default: 8080)
    pub http_port: Option<u16>,
    /// Enable TLS encryption
    pub enable_tls: Option<bool>,
    /// Access key for authentication
    pub access_key: Option<String>,
    /// Secret key for authentication
    pub secret_key: Option<String>,
    /// Send timeout in milliseconds
    pub send_timeout_ms: Option<u64>,
    /// Number of retry attempts for send failures
    pub retry_times: Option<u32>,
    /// Maximum message size in bytes
    pub max_message_size: Option<u32>,
    /// Enable batch sending
    pub enable_batch: Option<bool>,
    /// Batch size for batch sending
    pub batch_size: Option<usize>,
    /// Batch timeout in milliseconds
    pub batch_timeout_ms: Option<u64>,
}

/// RocketMQ HTTP API response structures
#[derive(Debug, Deserialize)]
pub struct RocketMQSendResponse {
    pub message_id: String,
    pub status: String,
    pub code: i32,
    pub message: String,
}

/// RocketMQ output component
pub struct RocketMQOutput {
    output_name: Option<String>,
    config: RocketMQOutputConfig,
    http_client: Arc<Client>,
    base_url: String,
}

impl RocketMQOutput {
    pub fn new(output_name: Option<String>, config: RocketMQOutputConfig) -> Result<Self, Error> {
        let http_port = config.http_port.unwrap_or(8080);
        let base_url = format!("{}:{}", config.nameserver_addr, http_port);
        
        let mut client_builder = Client::builder()
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(60));
        
        if let Some(access_key) = &config.access_key {
            let auth_header = format!("Basic {}:{}", access_key, config.secret_key.as_deref().unwrap_or(""));
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert("Authorization", auth_header.parse().unwrap());
            client_builder = client_builder.default_headers(headers);
        }
        
        let http_client = client_builder.build()
            .map_err(|e| Error::Connection(format!("Failed to create HTTP client: {}", e)))?;
        
        Ok(Self {
            output_name,
            config,
            http_client: Arc::new(http_client),
            base_url,
        })
    }

    async fn connect_with_retry(&self) -> Result<(), Error> {
        let retry_count = self.config.retry_times.unwrap_or(3);
        let retry_delay = Duration::from_millis(self.config.send_timeout_ms.unwrap_or(5000));
        
        for attempt in 0..retry_count {
            let url = format!("http://{}/api/v1/topics/{}", self.base_url, self.config.topic);
            
            match self.http_client.get(&url).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        return Ok(());
                    } else {
                        let error_msg = format!("HTTP error: {}", response.status());
                        if attempt == retry_count - 1 {
                            return Err(Error::Connection(error_msg));
                        }
                    }
                }
                Err(e) => {
                    if attempt == retry_count - 1 {
                        return Err(Error::Connection(format!("Failed to connect to RocketMQ: {}", e)));
                    }
                }
            }
            
            tracing::warn!(
                "RocketMQ connection attempt {} failed, retrying in {:?}",
                attempt + 1,
                retry_delay
            );
            sleep(retry_delay).await;
        }
        
        Err(Error::Connection("Failed to connect to RocketMQ after retries".to_string()))
    }

    async fn send_single_message(&self, msg_batch: &MessageBatch) -> Result<(), Error> {
        let retry_count = self.config.retry_times.unwrap_or(3);
        let retry_delay = Duration::from_millis(self.config.send_timeout_ms.unwrap_or(1000));
        
        for attempt in 0..retry_count {
            match self.send_message_internal(msg_batch).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if attempt == retry_count - 1 {
                        return Err(Error::Process(format!("Failed to send message after {} attempts: {}", retry_count, e)));
                    }
                    tracing::warn!(
                        "RocketMQ send attempt {} failed: {}, retrying in {:?}",
                        attempt + 1,
                        e,
                        retry_delay
                    );
                    sleep(retry_delay).await;
                }
            }
        }
        
        Ok(())
    }

    async fn send_message_internal(&self, msg_batch: &MessageBatch) -> Result<(), Error> {
        let url = format!("http://{}/api/v1/messages/send", self.base_url);
        
        // Extract binary data from the message batch
        let binary_data = msg_batch.to_binary(DEFAULT_BINARY_VALUE_FIELD)?;
        
        for data in binary_data {
            let mut message_data = HashMap::new();
            message_data.insert("topic", self.config.topic.clone());
            message_data.insert("body", String::from_utf8_lossy(data).to_string());
            
            if let Some(tag) = &self.config.tag {
                message_data.insert("tag", tag.clone());
            }
            
            if let Some(message_group) = &self.config.message_group {
                message_data.insert("message_group", message_group.clone());
            }
            
            if let Some(max_message_size) = self.config.max_message_size {
                if data.len() as u32 > max_message_size {
                    return Err(Error::Process(format!("Message size {} exceeds maximum size {}", data.len(), max_message_size)));
                }
            }
            
            // Send the message
            let response = self.http_client.post(&url).json(&message_data).send()
                .await
                .map_err(|e| Error::Process(format!("Failed to send message to RocketMQ: {}", e)))?;
            
            if !response.status().is_success() {
                return Err(Error::Process(format!("HTTP error: {}", response.status())));
            }
            
            let send_response: RocketMQSendResponse = response.json()
                .await
                .map_err(|e| Error::Process(format!("Failed to parse RocketMQ response: {}", e)))?;
            
            if send_response.code != 0 {
                return Err(Error::Process(format!("RocketMQ send failed: {}", send_response.message)));
            }
        }
        
        Ok(())
    }

    async fn send_batch_messages(&self, batch: &MessageBatch) -> Result<(), Error> {
        let binary_data = batch.to_binary(DEFAULT_BINARY_VALUE_FIELD)?;
        let batch_size = self.config.batch_size.unwrap_or(32);
        
        let mut messages = Vec::new();
        
        for data in binary_data {
            messages.push(data.to_vec());
            
            if messages.len() >= batch_size {
                self.send_batch_to_rocketmq(&messages).await?;
                messages.clear();
            }
        }
        
        if !messages.is_empty() {
            self.send_batch_to_rocketmq(&messages).await?;
        }
        
        Ok(())
    }

    async fn send_batch_to_rocketmq(&self, messages: &[Vec<u8>]) -> Result<(), Error> {
        let url = format!("http://{}/api/v1/messages/send_batch", self.base_url);
        
        let mut batch_data = HashMap::new();
        batch_data.insert("topic", self.config.topic.clone());
        
        let message_list: Vec<HashMap<String, String>> = messages.iter().map(|msg| {
            let mut message_data = HashMap::new();
            message_data.insert("body".to_string(), String::from_utf8_lossy(msg).to_string());
            
            if let Some(tag) = &self.config.tag {
                message_data.insert("tag".to_string(), tag.clone());
            }
            
            if let Some(message_group) = &self.config.message_group {
                message_data.insert("message_group".to_string(), message_group.clone());
            }
            
            message_data
        }).collect();
        
        batch_data.insert("messages", serde_json::to_string(&message_list).unwrap_or_default());
        
        let response = self.http_client.post(&url).json(&batch_data).send()
            .await
            .map_err(|e| Error::Process(format!("Failed to send batch messages to RocketMQ: {}", e)))?;
        
        if !response.status().is_success() {
            return Err(Error::Process(format!("HTTP error: {}", response.status())));
        }
        
        Ok(())
    }
}

#[async_trait]
impl Output for RocketMQOutput {
    async fn connect(&self) -> Result<(), Error> {
        self.connect_with_retry().await
    }

    async fn write(&self, msg: MessageBatch) -> Result<(), Error> {
        let enable_batch = self.config.enable_batch.unwrap_or(false);
        
        if enable_batch {
            self.send_batch_messages(&msg).await
        } else {
            self.send_single_message(&msg).await
        }
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// RocketMQ output builder
pub struct RocketMQOutputBuilder;

#[async_trait]
impl OutputBuilder for RocketMQOutputBuilder {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        let config_value = config
            .as_ref()
            .ok_or_else(|| Error::Config("RocketMQ output config is required".to_string()))?;
        
        let config: RocketMQOutputConfig = serde_json::from_value(config_value.clone())
            .map_err(|e| Error::Config(format!("Invalid RocketMQ output config: {}", e)))?;
        
        let output = RocketMQOutput::new(name.cloned(), config)?;
        Ok(Arc::new(output))
    }
}

/// Initialize RocketMQ output plugin
pub fn init() -> Result<(), Error> {
    register_output_builder("rocketmq", Arc::new(RocketMQOutputBuilder))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rocketmq_output_config_deserialization() {
        let config_json = r#"
        {
            "nameserver_addr": "localhost:9876",
            "topic": "test_topic",
            "tag": "test_tag",
            "message_group": "test_group",
            "http_port": 8080,
            "enable_tls": true,
            "access_key": "test_key",
            "secret_key": "test_secret",
            "send_timeout_ms": 5000,
            "retry_times": 3,
            "max_message_size": 1048576,
            "enable_batch": true,
            "batch_size": 32,
            "batch_timeout_ms": 1000
        }
        "#;
        
        let config: RocketMQOutputConfig = serde_json::from_str(config_json).unwrap();
        assert_eq!(config.nameserver_addr, "localhost:9876");
        assert_eq!(config.topic, "test_topic");
        assert_eq!(config.tag, Some("test_tag".to_string()));
        assert_eq!(config.message_group, Some("test_group".to_string()));
        assert_eq!(config.http_port, Some(8080));
        assert_eq!(config.enable_tls, Some(true));
        assert_eq!(config.access_key, Some("test_key".to_string()));
        assert_eq!(config.secret_key, Some("test_secret".to_string()));
        assert_eq!(config.send_timeout_ms, Some(5000));
        assert_eq!(config.retry_times, Some(3));
        assert_eq!(config.max_message_size, Some(1048576));
        assert_eq!(config.enable_batch, Some(true));
        assert_eq!(config.batch_size, Some(32));
        assert_eq!(config.batch_timeout_ms, Some(1000));
    }
}