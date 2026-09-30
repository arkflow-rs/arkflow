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

//! Pulsar output component
//!
//! Send data to a Pulsar topic

use crate::expr::Expr;
use crate::pulsar::{
    PulsarAuth, PulsarClient, PulsarClientUtils, PulsarConfigValidator, PulsarProducer,
};
use arkflow_core::component::{register_output_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::{
    codec::Codec,
    output::{register_output_builder, Output, OutputBuilder},
    Error, MessageBatchRef, Resource,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// How long a single broker operation may take in `write` — both the
/// per-topic producer build and the receipt wait. The pulsar client
/// reconnects internally and retries operations without limit, so an
/// unbounded wait would hang `write` forever on broker loss instead of
/// surfacing an error.
const WRITE_RECEIPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Pulsar output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PulsarOutputConfig {
    /// Pulsar service URL
    pub service_url: String,
    /// Topic to publish to
    pub topic: Expr<String>,
    /// Authentication (optional)
    pub auth: Option<PulsarAuth>,
    /// TLS configuration (optional)
    #[serde(default)]
    pub tls: Option<crate::pulsar::common::PulsarTlsConfig>,
    /// Value field to use for message payload
    pub value_field: Option<String>,
}

/// Pulsar output component
pub struct PulsarOutput {
    config: PulsarOutputConfig,
    client: Arc<RwLock<Option<PulsarClient>>>,
    /// Producers are topic-bound at build time in pulsar 6.x (a build
    /// without a topic fails with "topic not set"), and the topic is an
    /// expression that can change per batch — so build one producer per
    /// topic on first use and reuse it. Each producer gets its own lock:
    /// the map lock only guards get-or-build, so one stalled topic's
    /// bounded receipt wait cannot block sends to healthy topics.
    producers: Arc<Mutex<HashMap<String, Arc<Mutex<PulsarProducer>>>>>,
    codec: Option<Arc<dyn Codec>>,
}

impl PulsarOutput {
    /// Create a new Pulsar output component
    fn new(config: PulsarOutputConfig, codec: Option<Arc<dyn Codec>>) -> Result<Self, Error> {
        Ok(Self {
            config,
            client: Arc::new(RwLock::new(None)),
            producers: Arc::new(Mutex::new(HashMap::new())),
            codec,
        })
    }

    /// Fetch (or build) the producer for a topic and send one payload,
    /// awaiting the broker receipt before returning.
    async fn send_to_topic(
        &self,
        client: &PulsarClient,
        topic: &str,
        payload: Vec<u8>,
    ) -> Result<(), Error> {
        let producer = {
            let mut producers = self.producers.lock().await;
            match producers.get(topic) {
                Some(producer) => Arc::clone(producer),
                None => {
                    // The build (topic lookup + handshake) is bounded too:
                    // pulsar retries operations without limit, so without
                    // this timeout a first write to a lost broker would
                    // hang before the receipt wait even starts.
                    let producer = tokio::time::timeout(
                        WRITE_RECEIPT_TIMEOUT,
                        client.producer().with_topic(topic).build(),
                    )
                    .await
                    .map_err(|_| {
                        Error::Connection(format!(
                            "Timed out creating Pulsar producer for topic {topic}"
                        ))
                    })?
                    .map_err(|e| {
                        Error::Connection(format!(
                            "Failed to create Pulsar producer for topic {topic}: {e}"
                        ))
                    })?;
                    let producer = Arc::new(Mutex::new(producer));
                    producers.insert(topic.to_string(), Arc::clone(&producer));
                    producer
                }
            }
        };

        let mut producer = producer.lock().await;

        // `send_non_blocking` only enqueues; the returned future resolves
        // with the broker receipt. Awaiting it is what makes write reliable
        // — fire-and-forget would report success for messages the broker
        // never accepted. The wait is bounded so a lost broker surfaces as
        // an error instead of an indefinite hang (the client itself keeps
        // reconnecting and never fails the pending receipt).
        let receipt = producer
            .send_non_blocking(payload)
            .await
            .map_err(|e| Error::Process(format!("Failed to send to Pulsar topic {topic}: {e}")))?;
        tokio::time::timeout(WRITE_RECEIPT_TIMEOUT, receipt)
            .await
            .map_err(|_| {
                Error::Connection(format!(
                    "Timed out waiting for Pulsar receipt on topic {topic}"
                ))
            })?
            .map_err(|e| {
                Error::Process(format!("Pulsar broker rejected message on topic {topic}: {e}"))
            })?;
        Ok(())
    }
}

#[async_trait]
impl Output for PulsarOutput {
    async fn connect(&self) -> Result<(), Error> {
        // Validate configuration before connecting
        PulsarConfigValidator::validate_service_url(&self.config.service_url)?;

        if let Some(ref auth) = self.config.auth {
            PulsarConfigValidator::validate_auth_config(auth)?;
        }

        // Use shared client builder with authentication and optional TLS
        let mut builder =
            PulsarClientUtils::create_client_builder(&self.config.service_url, &self.config.auth)?;
        if let Some(tls) = &self.config.tls {
            if let Some(chain_file) = &tls.certificate_chain_file {
                builder = builder.with_certificate_chain_file(chain_file).map_err(|e| {
                    Error::Config(format!("pulsar: failed to load certificate chain: {e}"))
                })?;
            }
            if let Some(enabled) = tls.hostname_verification {
                builder = builder.with_tls_hostname_verification_enabled(enabled);
            }
        }

        // Connect to Pulsar
        let client = builder
            .build()
            .await
            .map_err(|e| Error::Connection(format!("Failed to connect to Pulsar: {}", e)))?;

        // Store client
        let mut client_guard = self.client.write().await;
        *client_guard = Some(client.clone());

        // Producers are built lazily per topic on first write; drop any
        // producers left over from a previous connection.
        self.producers.lock().await.clear();

        Ok(())
    }

    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
        // Check client connection
        let client = {
            let client_guard = self.client.read().await;
            client_guard
                .as_ref()
                .cloned()
                .ok_or_else(|| Error::Connection("Pulsar client not connected".to_string()))?
        };

        // Payload selection: an explicit `value_field` takes the named
        // column's value per row (binary or string), otherwise the codec
        // (or the default binary-field) encoding applies.
        let owned_payloads: Vec<Vec<u8>> = if let Some(field) = &self.config.value_field {
            field_payloads(&msg, field)?
        } else {
            let payloads =
                crate::output::codec_helper::apply_codec_encode(&msg, &self.codec).await?;
            payloads.into_iter().map(|p| p.to_vec()).collect()
        };
        if owned_payloads.is_empty() {
            return Ok(());
        }

        // Resolve the topic per message, matching the kafka output's
        // expression semantics: a scalar sends every message to the same
        // topic, a vector is indexed per message.
        let topics = self.config.topic.evaluate_expr(&msg).await?;
        for (index, payload) in owned_payloads.into_iter().enumerate() {
            let topic = match &topics {
                crate::expr::EvaluateResult::Scalar(topic) => topic.clone(),
                crate::expr::EvaluateResult::Vec(topics_vec) => topics_vec
                    .get(index)
                    .ok_or_else(|| {
                        Error::Config(format!(
                            "pulsar topic expression resolved to {} topics for {} messages",
                            topics_vec.len(),
                            index + 1
                        ))
                    })?
                    .clone(),
            };
            self.send_to_topic(&client, &topic, payload).await?;
        }

        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        // Close producers if active
        self.producers.lock().await.clear();

        // Close Pulsar client
        let mut client_guard = self.client.write().await;
        if let Some(_client) = client_guard.take() {
            // Client will be dropped automatically
        }

        Ok(())
    }
}

/// One payload per row, taken from the named column. Binary columns are
/// sent verbatim, string columns as their UTF-8 bytes; nulls and any other
/// type fail closed rather than guessing a serialization (silently dropping
/// null rows would shift every later payload onto the previous row's topic
/// when topics resolve per message).
fn null_value_field(field: &str) -> Error {
    Error::Config(format!("pulsar value_field '{field}' contains a null value"))
}

fn field_payloads(msg: &MessageBatchRef, field: &str) -> Result<Vec<Vec<u8>>, Error> {
    use datafusion::arrow::array::{
        Array, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray,
    };
    use datafusion::arrow::datatypes::DataType;

    let column = msg.column_by_name(field).ok_or_else(|| {
        Error::Config(format!("pulsar value_field '{field}' not found in the batch"))
    })?;
    match column.data_type() {
        DataType::Binary => Ok(column
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("checked binary column")
            .iter()
            .map(|v| v.map(<[u8]>::to_vec).ok_or_else(|| null_value_field(field)))
            .collect::<Result<Vec<_>, _>>()?),
        DataType::LargeBinary => Ok(column
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("checked large binary column")
            .iter()
            .flatten()
            .map(<[u8]>::to_vec)
            .collect()),
        DataType::Utf8 => Ok(column
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("checked utf8 column")
            .iter()
            .map(|v| v.map(|s| s.as_bytes().to_vec()).ok_or_else(|| null_value_field(field)))
            .collect::<Result<Vec<_>, _>>()?),
        DataType::LargeUtf8 => Ok(column
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .expect("checked large utf8 column")
            .iter()
            .flatten()
            .map(|s| s.as_bytes().to_vec())
            .collect()),
        other => Err(Error::Config(format!(
            "pulsar value_field '{field}' has unsupported type {other} (use a binary or string column)"
        ))),
    }
}

pub struct PulsarOutputBuilder;

impl OutputBuilder for PulsarOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        let config: PulsarOutputConfig = parse_config(config, "PulsarOutput input")?;

        // Validate configuration during build
        PulsarConfigValidator::validate_service_url(&config.service_url)?;

        // Note: We can't fully validate the topic here since it's an expression
        // that needs to be evaluated at runtime with a MessageBatch

        if let Some(ref auth) = config.auth {
            PulsarConfigValidator::validate_auth_config(auth)?;
        }

        Ok(Arc::new(PulsarOutput::new(config, codec)?))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("pulsar", Arc::new(PulsarOutputBuilder))?;
    register_output_metadata(ComponentMetadata::with_schema(
        "pulsar",
        "Produces messages to an Apache Pulsar topic.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "service_url": {"type": "string", "description": "Pulsar service URL."},
                "topic": {"type": "string", "description": "Destination topic (supports {field} placeholders)."},
                "auth": {"type": "object", "description": "Pulsar authentication configuration."},
                "value_field": {"type": "string", "description": "Record field used as the payload."}
            },
            "required": ["service_url", "topic"]
        }),
    ).with_example(serde_json::json!({
        "service_url": "pulsar://localhost:6650",
        "topic": "persistent://public/default/events"
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::MessageBatch;
    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn utf8_batch(rows: Vec<Option<&str>>) -> MessageBatchRef {
        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(rows))]).expect("utf8 batch");
        Arc::new(MessageBatch::new_arrow(batch))
    }

    #[test]
    fn value_field_rejects_null_rows() {
        // Silently dropping nulls would shift later payloads onto the
        // previous row's topic when topics resolve per message.
        let error = field_payloads(&utf8_batch(vec![Some("a"), None]), "col")
            .err()
            .expect("null rows must be rejected");
        assert!(error.to_string().contains("null"));
    }

    #[test]
    fn value_field_rejects_unsupported_types() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        )
        .expect("int batch");
        let error = field_payloads(&Arc::new(MessageBatch::new_arrow(batch)), "n")
            .err()
            .expect("non-string/binary columns must be rejected");
        assert!(error.to_string().contains("unsupported type"));
    }
}
