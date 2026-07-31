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
//! Schema Registry codec.
//!
//! Decodes Confluent wire-format Protobuf messages by resolving the schema id
//! via a `SchemaResolver` (default: Confluent Schema Registry REST). The wire
//! format is `[0x00 magic][4-byte big-endian schema id][payload]`. Descriptors
//! are cached per id so each schema version is fetched at most once.

use crate::component::protobuf::{parse_proto_source, protobuf_to_arrow};
use async_trait::async_trait;
use arkflow_core::codec::{Codec, CodecBuilder, Decoder, Encoder};
use arkflow_core::component::{register_codec_metadata, ComponentMetadata};
use arkflow_core::{Bytes, Error, MessageBatch, Resource};
use dashmap::DashMap;
use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use prost_reflect::MessageDescriptor;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

/// Resolve a Protobuf schema string by Confluent schema id.
#[async_trait]
pub trait SchemaResolver: Send + Sync {
    async fn fetch_schema(&self, id: u32) -> Result<String, Error>;
}

/// Schema Registry codec: decodes Confluent wire-format Protobuf messages by
/// resolving the schema id via a `SchemaResolver` and caching the descriptor.
pub struct SchemaRegistryCodec {
    message_type: String,
    resolver: Arc<dyn SchemaResolver>,
    cache: DashMap<u32, MessageDescriptor>,
}

impl SchemaRegistryCodec {
    pub fn new(message_type: String, resolver: Arc<dyn SchemaResolver>) -> Self {
        Self {
            message_type,
            resolver,
            cache: DashMap::new(),
        }
    }

    async fn resolve_descriptor(&self, id: u32) -> Result<MessageDescriptor, Error> {
        if let Some(desc) = self.cache.get(&id) {
            return Ok(desc.clone());
        }
        let schema = self.resolver.fetch_schema(id).await?;
        let descriptor = parse_proto_source(&schema, &self.message_type)?;
        self.cache.insert(id, descriptor.clone());
        Ok(descriptor)
    }
}

#[async_trait]
impl Encoder for SchemaRegistryCodec {
    async fn encode(&self, batch: MessageBatch) -> Result<Vec<Bytes>, Error> {
        // Encoding is not registry-specific; emit Arrow as line-delimited JSON to
        // satisfy the `Codec` contract and stay round-trippable.
        let mut buf = Vec::new();
        let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
        writer
            .write(&batch)
            .map_err(|e| Error::Process(format!("Schema registry codec encode error: {}", e)))?;
        writer
            .finish()
            .map_err(|e| Error::Process(format!("Schema registry codec encode finish error: {}", e)))?;
        let s = String::from_utf8(buf)
            .map_err(|e| Error::Process(format!("UTF-8 conversion failed: {}", e)))?;
        Ok(s.lines().map(|l| l.as_bytes().to_vec()).collect())
    }
}

#[async_trait]
impl Decoder for SchemaRegistryCodec {
    async fn decode(&self, b: Vec<Bytes>) -> Result<MessageBatch, Error> {
        let mut batches = Vec::with_capacity(b.len());
        for msg in b {
            let (id, payload) = parse_wire_format(&msg)?;
            let descriptor = self.resolve_descriptor(id).await?;
            batches.push(protobuf_to_arrow(&descriptor, payload)?);
        }
        if batches.is_empty() {
            return Ok(MessageBatch::new_arrow(RecordBatch::new_empty(Arc::new(
                Schema::empty(),
            ))));
        }
        let schema = batches[0].schema();
        let merged = arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| Error::Process(format!("Batch merge failed: {}", e)))?;
        Ok(MessageBatch::new_arrow(merged))
    }
}

/// Parse Confluent wire format: `[0x00 magic][4-byte big-endian schema id][payload]`.
fn parse_wire_format(msg: &[u8]) -> Result<(u32, &[u8]), Error> {
    if msg.len() < 5 {
        return Err(Error::Process(
            "Message too short for Confluent wire format".to_string(),
        ));
    }
    if msg[0] != 0x00 {
        return Err(Error::Process(format!(
            "Invalid Confluent magic byte: 0x{:02X}",
            msg[0]
        )));
    }
    let id = u32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
    Ok((id, &msg[5..]))
}

// ===== RestSchemaResolver (Confluent REST, reqwest async) =====

/// Authentication for the Schema Registry.
pub enum Auth {
    Basic(String, String),
    Bearer(String),
}

/// Resolves schemas from a Confluent Schema Registry via REST.
pub struct RestSchemaResolver {
    client: reqwest::Client,
    base_url: String,
    auth: Option<Auth>,
}

impl RestSchemaResolver {
    pub fn new(base_url: String, auth: Option<Auth>) -> Result<Self, Error> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| Error::Config(format!("Failed to build HTTP client: {}", e)))?;
        Ok(Self {
            client,
            base_url,
            auth,
        })
    }
}

#[derive(Deserialize)]
struct SchemaResponse {
    schema: String,
    #[serde(rename = "schemaType", default)]
    schema_type: Option<String>,
}

#[async_trait]
impl SchemaResolver for RestSchemaResolver {
    async fn fetch_schema(&self, id: u32) -> Result<String, Error> {
        let url = format!(
            "{}/schemas/ids/{}",
            self.base_url.trim_end_matches('/'),
            id
        );
        let mut req = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json");
        if let Some(auth) = &self.auth {
            req = match auth {
                Auth::Basic(u, p) => req.basic_auth(u, Some(p)),
                Auth::Bearer(t) => req.bearer_auth(t),
            };
        }
        let resp = req
            .send()
            .await
            .map_err(|e| Error::Process(format!("Schema Registry request failed: {}", e)))?;
        if !resp.status().is_success() {
            return Err(Error::Process(format!(
                "Schema Registry returned status {}",
                resp.status()
            )));
        }
        let body: SchemaResponse = resp
            .json()
            .await
            .map_err(|e| Error::Process(format!("Schema Registry response parse failed: {}", e)))?;
        if let Some(t) = &body.schema_type {
            if t.to_uppercase() != "PROTOBUF" {
                return Err(Error::Process(format!(
                    "Unsupported schema type: {} (only PROTOBUF supported)",
                    t
                )));
            }
        }
        Ok(body.schema)
    }
}

// ===== Config + Builder + init =====

#[derive(Deserialize)]
struct SchemaRegistryCodecConfig {
    registry_url: String,
    message_type: String,
    #[serde(default)]
    auth: Option<AuthConfig>,
}

#[derive(Deserialize)]
struct AuthConfig {
    #[serde(rename = "type")]
    auth_type: String,
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
}

struct SchemaRegistryCodecBuilder;

impl CodecBuilder for SchemaRegistryCodecBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Codec>, Error> {
        let config = config
            .as_ref()
            .ok_or_else(|| Error::Config("schema_registry codec configuration is missing".to_string()))?;
        let config: SchemaRegistryCodecConfig = serde_json::from_value(config.clone())?;
        let auth = match config.auth {
            None => None,
            Some(a) => Some(match a.auth_type.as_str() {
                "basic" => Auth::Basic(a.username.unwrap_or_default(), a.password.unwrap_or_default()),
                "bearer" => Auth::Bearer(a.token.unwrap_or_default()),
                other => return Err(Error::Config(format!("Unsupported auth type: {}", other))),
            }),
        };
        let resolver = Arc::new(RestSchemaResolver::new(config.registry_url, auth)?);
        Ok(Arc::new(SchemaRegistryCodec::new(config.message_type, resolver)))
    }
}

pub(crate) fn init() -> Result<(), Error> {
    arkflow_core::codec::register_codec_builder("schema_registry", Arc::new(SchemaRegistryCodecBuilder))?;
    register_codec_metadata(
        ComponentMetadata::with_schema(
            "schema_registry",
            "Decodes Confluent wire-format Protobuf messages by resolving the schema id from a Schema Registry.",
            serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "registry_url": {"type": "string", "description": "Confluent Schema Registry base URL."},
                    "message_type": {"type": "string", "description": "Fully-qualified Protobuf message type."},
                    "auth": {"type": "object", "description": "Optional registry authentication.", "properties": {
                        "type": {"type": "string", "enum": ["basic", "bearer"]},
                        "username": {"type": "string"},
                        "password": {"type": "string"},
                        "token": {"type": "string"}
                    }}
                },
                "required": ["registry_url", "message_type"]
            }),
        ),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU32, Ordering};

    const TEST_SCHEMA: &str = "syntax = \"proto3\";\npackage test;\nmessage M { int64 id = 1; }";

    /// Payload for `M { id = 42 }`: field 1 (int64, varint), tag=0x08, value=0x2A.
    fn payload_id_42() -> Vec<u8> {
        vec![0x08, 0x2A]
    }

    fn wire(id: u32, payload: &[u8]) -> Vec<u8> {
        let mut m = vec![0x00];
        m.extend_from_slice(&id.to_be_bytes());
        m.extend_from_slice(payload);
        m
    }

    struct InMemorySchemaResolver {
        schemas: HashMap<u32, String>,
        fetch_count: AtomicU32,
    }
    impl InMemorySchemaResolver {
        fn new(map: HashMap<u32, String>) -> Self {
            Self {
                schemas: map,
                fetch_count: AtomicU32::new(0),
            }
        }
        fn fetches(&self) -> u32 {
            self.fetch_count.load(Ordering::SeqCst)
        }
    }
    #[async_trait]
    impl SchemaResolver for InMemorySchemaResolver {
        async fn fetch_schema(&self, id: u32) -> Result<String, Error> {
            self.fetch_count.fetch_add(1, Ordering::SeqCst);
            self.schemas
                .get(&id)
                .cloned()
                .ok_or_else(|| Error::Process(format!("schema id {} not in test resolver", id)))
        }
    }

    fn build_codec(resolver: Arc<dyn SchemaResolver>) -> SchemaRegistryCodec {
        SchemaRegistryCodec::new("test.M".to_string(), resolver)
    }

    #[test]
    fn test_parse_wire_format_valid() {
        let msg = wire(1, &[0x08, 0x2A]);
        let (id, payload) = parse_wire_format(&msg).unwrap();
        assert_eq!(id, 1);
        assert_eq!(payload, &[0x08, 0x2A]);
    }

    #[test]
    fn test_parse_wire_format_bad_magic() {
        let mut bad = wire(1, &[]);
        bad[0] = 0x01;
        assert!(parse_wire_format(&bad).is_err());
    }

    #[test]
    fn test_parse_wire_format_too_short() {
        assert!(parse_wire_format(&[0x00, 0x00, 0x00]).is_err());
    }

    #[tokio::test]
    async fn test_decode_single() {
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::from([(
            1u32,
            TEST_SCHEMA.to_string(),
        )])));
        let codec = build_codec(resolver.clone());
        let batch = codec.decode(vec![wire(1, &payload_id_42())]).await.unwrap();
        assert_eq!(batch.len(), 1);
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::Int64Type;
        let id_col = batch
            .record_batch()
            .column_by_name("id")
            .expect("id column");
        assert_eq!(id_col.as_primitive::<Int64Type>().value(0), 42);
        assert_eq!(resolver.fetches(), 1);
    }

    #[tokio::test]
    async fn test_cache_hits() {
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::from([(
            1u32,
            TEST_SCHEMA.to_string(),
        )])));
        let codec = build_codec(resolver.clone());
        let batch = codec
            .decode(vec![wire(1, &payload_id_42()), wire(1, &payload_id_42())])
            .await
            .unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(resolver.fetches(), 1); // only the first fetches
    }

    #[tokio::test]
    async fn test_multi_version_each_resolves() {
        // two ids, same (compatible) schema; each resolves its own descriptor.
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::from([
            (1u32, TEST_SCHEMA.to_string()),
            (2u32, TEST_SCHEMA.to_string()),
        ])));
        let codec = build_codec(resolver.clone());
        let batch = codec
            .decode(vec![wire(1, &payload_id_42()), wire(2, &payload_id_42())])
            .await
            .unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(resolver.fetches(), 2); // both ids resolved
    }

    #[tokio::test]
    async fn test_resolver_error() {
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::new())); // empty -> all error
        let codec = build_codec(resolver);
        let result = codec.decode(vec![wire(99, &payload_id_42())]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rest_resolver_basic_auth() {
        // base64("user:pass") == "dXNlcjpwYXNz"
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/1"))
            .and(header("authorization", "Basic dXNlcjpwYXNz"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"schema": "syntax = \"proto3\"; message M {}", "schemaType": "PROTOBUF"}),
            ))
            .mount(&server)
            .await;
        let resolver = RestSchemaResolver::new(
            server.uri(),
            Some(Auth::Basic("user".into(), "pass".into())),
        )
        .unwrap();
        let schema = resolver.fetch_schema(1).await.unwrap();
        assert!(schema.contains("message M"));
    }

    #[tokio::test]
    async fn test_rest_resolver_bearer_auth() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/2"))
            .and(header("authorization", "Bearer tok"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"schema": "syntax = \"proto3\"; message M {}"})),
            )
            .mount(&server)
            .await;
        let resolver =
            RestSchemaResolver::new(server.uri(), Some(Auth::Bearer("tok".into()))).unwrap();
        let schema = resolver.fetch_schema(2).await.unwrap();
        assert!(schema.contains("message M"));
    }
}
