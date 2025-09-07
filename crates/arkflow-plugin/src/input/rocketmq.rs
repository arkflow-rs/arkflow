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

//! RocketMQ input component
//!
//! Receive data from a RocketMQ topic using HTTP API

use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder};
use arkflow_core::{Error, MessageBatch, Resource};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// RocketMQ input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocketMQInputConfig {
    /// RocketMQ nameserver addresses (comma separated)
    pub nameserver_addr: String,
    /// Consumer group name
    pub consumer_group: String,
    /// Topic to consume from
    pub topic: String,
    /// Tag filter for messages (optional)
    pub tag_filter: Option<String>,
    /// Maximum batch size for message retrieval
    pub max_batch_size: Option<usize>,
    /// Wait timeout for message retrieval in milliseconds
    pub wait_timeout_ms: Option<u64>,
    /// RocketMQ HTTP API port (default: 8080)
    pub http_port: Option<u16>,
    /// Enable TLS encryption
    pub enable_tls: Option<bool>,
    /// Access key for authentication
    pub access_key: Option<String>,
    /// Secret key for authentication
    pub secret_key: Option<String>,
    /// Number of retry attempts for connection failures
    pub retry_count: Option<u32>,
    /// Delay between retry attempts in milliseconds
    pub retry_delay_ms: Option<u64>,
}

/// RocketMQ HTTP API response structures
#[derive(Debug, Deserialize)]
pub struct RocketMQMessage {
    pub message_id: String,
    pub topic: String,
    pub born_host: String,
    pub born_timestamp: i64,
    pub store_host: String,
    pub store_timestamp: i64,
    pub body: String,
    pub tags: Option<String>,
    pub keys: Option<Vec<String>>,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub reconsume_times: i32,
}

#[derive(Debug, Deserialize)]
pub struct RocketMQPullResponse {
    pub messages: Vec<RocketMQMessage>,
    pub code: i32,
    pub message: String,
}

/// RocketMQ input component
pub struct RocketMQInput {
    input_name: Option<String>,
    config: RocketMQInputConfig,
    http_client: Arc<Client>,
    base_url: String,
}

/// RocketMQ message acknowledgment
pub struct RocketMQAck {
    message_id: String,
    http_client: Arc<Client>,
    base_url: String,
    consumer_group: String,
    input_name: Option<String>,
}

impl RocketMQInput {
    pub fn new(input_name: Option<String>, config: RocketMQInputConfig) -> Result<Self, Error> {
        let http_port = config.http_port.unwrap_or(8080);
        let base_url = format!("{}:{}", config.nameserver_addr, http_port);
        
        let mut client_builder = Client::builder()
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(60));
        
        if let Some(access_key) = &config.access_key {
            let secret_key = config.secret_key.as_deref().unwrap_or("");
            let auth_header = format!("Basic {}:{}", access_key, secret_key);
            let mut headers = reqwest::header::HeaderMap::new();
            let header_value = auth_header.parse()
                .map_err(|e| Error::Connection(format!("Failed to parse auth header: {}", e)))?;
            headers.insert("Authorization", header_value);
            client_builder = client_builder.default_headers(headers);
        }
        
        let http_client = client_builder.build()
            .map_err(|e| Error::Connection(format!("Failed to create HTTP client: {}", e)))?;
        
        Ok(Self {
            input_name,
            config,
            http_client: Arc::new(http_client),
            base_url,
        })
    }

    async fn connect_with_retry(&self) -> Result<(), Error> {
        let retry_count = self.config.retry_count.unwrap_or(3);
        let retry_delay = Duration::from_millis(self.config.retry_delay_ms.unwrap_or(5000));
        
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
                "RocketMQ input '{}' connection attempt {} failed, retrying in {:?}",
                self.input_name.as_deref().unwrap_or("unnamed"),
                attempt + 1,
                retry_delay
            );
            sleep(retry_delay).await;
        }
        
        Err(Error::Connection("Failed to connect to RocketMQ after retries".to_string()))
    }
}

#[async_trait]
impl Input for RocketMQInput {
    async fn connect(&self) -> Result<(), Error> {
        self.connect_with_retry().await
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        let max_batch_size = self.config.max_batch_size.unwrap_or(32);
        let wait_timeout = Duration::from_millis(self.config.wait_timeout_ms.unwrap_or(5000));
        
        let mut query_params = HashMap::new();
        query_params.insert("topic", self.config.topic.clone());
        query_params.insert("consumer_group", self.config.consumer_group.clone());
        query_params.insert("max_num_messages", max_batch_size.to_string());
        query_params.insert("wait_timeout_ms", wait_timeout.as_millis().to_string());
        
        if let Some(tag_filter) = &self.config.tag_filter {
            query_params.insert("tag", tag_filter.clone());
        }
        
        let url = format!("http://{}/api/v1/messages/pull", self.base_url);
        
        let response = self.http_client.get(&url).query(&query_params).send()
            .await
            .map_err(|e| Error::Read(format!("Failed to pull messages from RocketMQ: {}", e)))?;
        
        if !response.status().is_success() {
            return Err(Error::Read(format!("HTTP error: {}", response.status())));
        }
        
        let pull_response: RocketMQPullResponse = response.json()
            .await
            .map_err(|e| Error::Read(format!("Failed to parse RocketMQ response: {}", e)))?;
        
        if pull_response.messages.is_empty() {
            return Err(Error::EOF);
        }
        
        let mut messages = Vec::new();
        let mut acks = Vec::new();
        
        for message in pull_response.messages {
            let mut msg_data = HashMap::new();
            msg_data.insert("body".to_string(), message.body);
            msg_data.insert("message_id".to_string(), message.message_id.clone());
            msg_data.insert("topic".to_string(), message.topic.clone());
            msg_data.insert("born_timestamp".to_string(), message.born_timestamp.to_string());
            msg_data.insert("store_timestamp".to_string(), message.store_timestamp.to_string());
            msg_data.insert("queue_id".to_string(), message.queue_id.to_string());
            msg_data.insert("queue_offset".to_string(), message.queue_offset.to_string());
            msg_data.insert("reconsume_times".to_string(), message.reconsume_times.to_string());
            
            if let Some(tags) = &message.tags {
                msg_data.insert("tag".to_string(), tags.clone());
            }
            
            if let Some(keys) = &message.keys {
                let keys_json = serde_json::to_string(keys)
                    .map_err(|e| Error::Read(format!("Failed to serialize keys: {}", e)))?;
                msg_data.insert("keys".to_string(), keys_json);
            }
            
            let msg_json = serde_json::to_string(&msg_data)
                .map_err(|e| Error::Read(format!("Failed to serialize message data: {}", e)))?;
            messages.push(msg_json.into_bytes());
            
            let ack = RocketMQAck {
                message_id: message.message_id.clone(),
                http_client: self.http_client.clone(),
                base_url: self.base_url.clone(),
                consumer_group: self.config.consumer_group.clone(),
                input_name: self.input_name.clone(),
            };
            acks.push(Arc::new(ack) as Arc<dyn Ack>);
        }
        
        let mut batch = MessageBatch::new_binary(messages)?;
        batch.set_input_name(self.input_name.clone());
        
        let ack = if acks.len() == 1 {
            acks.into_iter().next().expect("Expected exactly one ack")
        } else {
            Arc::new(arkflow_core::input::VecAck(acks))
        };
        
        Ok((batch, ack))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

#[async_trait]
impl Ack for RocketMQAck {
    async fn ack(&self) {
        let url = format!("http://{}/api/v1/messages/ack", self.base_url);
        
        let mut params = HashMap::new();
        params.insert("consumer_group", self.consumer_group.clone());
        params.insert("message_id", self.message_id.clone());
        
        if let Err(e) = self.http_client.post(&url).form(&params).send().await {
            tracing::error!("Failed to acknowledge RocketMQ message {} from input '{}': {}", self.message_id, self.input_name.as_deref().unwrap_or("unnamed"), e);
        }
    }
}

/// RocketMQ input builder
pub struct RocketMQInputBuilder;

#[async_trait]
impl InputBuilder for RocketMQInputBuilder {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        let config_value = config
            .as_ref()
            .ok_or_else(|| Error::Config("RocketMQ input config is required".to_string()))?;
        
        let config: RocketMQInputConfig = serde_json::from_value(config_value.clone())
            .map_err(|e| Error::Config(format!("Invalid RocketMQ input config: {}", e)))?;
        
        let input = RocketMQInput::new(name.cloned(), config)?;
        Ok(Arc::new(input))
    }
}

/// Initialize RocketMQ input plugin
pub fn init() -> Result<(), Error> {
    register_input_builder("rocketmq", Arc::new(RocketMQInputBuilder))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rocketmq_input_config_deserialization() {
        let config_json = r#"
        {
            "nameserver_addr": "localhost:9876",
            "consumer_group": "test_group",
            "topic": "test_topic",
            "tag_filter": "test_tag",
            "max_batch_size": 10,
            "wait_timeout_ms": 5000,
            "http_port": 8080,
            "enable_tls": true,
            "access_key": "test_key",
            "secret_key": "test_secret",
            "retry_count": 3,
            "retry_delay_ms": 1000
        }
        "#;
        
        let config: RocketMQInputConfig = serde_json::from_str(config_json)
            .expect("Failed to deserialize test config");
        assert_eq!(config.nameserver_addr, "localhost:9876");
        assert_eq!(config.consumer_group, "test_group");
        assert_eq!(config.topic, "test_topic");
        assert_eq!(config.tag_filter, Some("test_tag".to_string()));
        assert_eq!(config.max_batch_size, Some(10));
        assert_eq!(config.wait_timeout_ms, Some(5000));
        assert_eq!(config.http_port, Some(8080));
        assert_eq!(config.enable_tls, Some(true));
        assert_eq!(config.access_key, Some("test_key".to_string()));
        assert_eq!(config.secret_key, Some("test_secret".to_string()));
        assert_eq!(config.retry_count, Some(3));
        assert_eq!(config.retry_delay_ms, Some(1000));
    }
}