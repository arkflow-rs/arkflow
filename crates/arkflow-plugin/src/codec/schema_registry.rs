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
//! Decodes Confluent wire-format Protobuf and Avro messages by resolving the
//! schema id via a `SchemaResolver` (default: Confluent Schema Registry REST).
//! The wire format is `[0x00 magic][4-byte big-endian schema id][payload]`.
//! Schemas are cached per id so each schema version is fetched at most once.
//! The decode side is dispatched on the registry's `schemaType` response.

use crate::codec::avro_arrow::avro_to_arrow;
use crate::component::protobuf::{parse_proto_source, protobuf_to_arrow};
use arkflow_core::codec::{Codec, CodecBuilder, Decoder, Encoder};
use arkflow_core::component::{register_codec_metadata, ComponentMetadata};
use arkflow_core::{Bytes, Error, MessageBatch, Resource};
use async_trait::async_trait;
use apache_avro::Schema as AvroSchema;
use dashmap::DashMap;
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use prost_reflect::MessageDescriptor;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

/// Characters percent-encoded when a registry subject is placed into the URL
/// path: `/` splits path segments, `?`/`#` start query/fragment, `%`
/// introduces an escape, and the remainder are controls or illegal in a path
/// per RFC 3986. Legal path characters (`:`, `@`, sub-delims) are preserved so
/// Confluent context subjects like `:ctx:subject` stay readable.
const SUBJECT_PATH_SEGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'/')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

/// A schema fetched from the registry, typed by the registry's `schemaType`.
#[derive(Debug, Clone)]
pub enum FetchedSchema {
    Protobuf(String),
    Avro(AvroSchema),
}

/// A schema parsed and cached per registry id.
#[derive(Clone)]
pub enum CachedSchema {
    Protobuf(MessageDescriptor),
    Avro(AvroSchema),
}

/// Resolve a schema by Confluent schema id.
#[async_trait]
pub trait SchemaResolver: Send + Sync {
    async fn fetch_schema(&self, id: u32) -> Result<FetchedSchema, Error>;

    /// Fetches the registered compatibility level of a subject
    /// (`GET /config/{subject}?defaultToGlobal=true` response's
    /// `compatibilityLevel`). Only needed by the compatibility gate.
    async fn fetch_subject_compatibility(&self, _subject: &str) -> Result<String, Error> {
        Err(Error::Process(
            "subject compatibility lookup is not supported by this resolver".to_string(),
        ))
    }
}

/// Minimum subject compatibility level enforced by the compatibility gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MinCompatibility {
    None,
    Backward,
    Forward,
    Full,
}

/// Registry levels ranked for gate comparison:
/// `NONE` < `BACKWARD`/`FORWARD` (incl. `*_TRANSITIVE`) < `FULL` (incl. `FULL_TRANSITIVE`).
fn compatibility_rank(level: &str) -> Option<u8> {
    match level.to_ascii_uppercase().as_str() {
        "NONE" => Some(0),
        "BACKWARD" | "BACKWARD_TRANSITIVE" | "FORWARD" | "FORWARD_TRANSITIVE" => Some(1),
        "FULL" | "FULL_TRANSITIVE" => Some(2),
        _ => None,
    }
}

fn required_rank(min: MinCompatibility) -> u8 {
    match min {
        MinCompatibility::None => 0,
        MinCompatibility::Backward | MinCompatibility::Forward => 1,
        MinCompatibility::Full => 2,
    }
}

/// Governance gate: fails the first decode when the subject's registered
/// compatibility level is below the configured minimum.
pub struct CompatibilityGate {
    subject: String,
    min: MinCompatibility,
}

impl CompatibilityGate {
    async fn check(&self, resolver: &dyn SchemaResolver) -> Result<(), String> {
        let level = resolver
            .fetch_subject_compatibility(&self.subject)
            .await
            .map_err(|e| {
                format!(
                    "compatibility gate request failed for subject '{}': {}",
                    self.subject, e
                )
            })?;
        let rank = compatibility_rank(&level).ok_or_else(|| {
            format!(
                "compatibility gate for subject '{}': registry returned unknown compatibility level '{}'",
                self.subject, level
            )
        })?;
        if rank < required_rank(self.min) {
            return Err(format!(
                "compatibility gate failed for subject '{}': level '{}' is below required '{}'",
                self.subject,
                level,
                format!("{:?}", self.min).to_lowercase()
            ));
        }
        Ok(())
    }
}

/// Schema Registry codec: decodes Confluent wire-format Protobuf and Avro
/// messages by resolving the schema id via a `SchemaResolver` and caching the
/// parsed schema per id.
pub struct SchemaRegistryCodec {
    message_type: Option<String>,
    resolver: Arc<dyn SchemaResolver>,
    cache: DashMap<u32, CachedSchema>,
    gate: Option<CompatibilityGate>,
    /// Gate verdict for the codec lifetime (failures included), so the config
    /// endpoint is hit at most once.
    gate_state: tokio::sync::OnceCell<Result<(), String>>,
}

impl SchemaRegistryCodec {
    pub fn new(
        message_type: Option<String>,
        resolver: Arc<dyn SchemaResolver>,
        gate: Option<CompatibilityGate>,
    ) -> Self {
        Self {
            message_type,
            resolver,
            cache: DashMap::new(),
            gate,
            gate_state: tokio::sync::OnceCell::new(),
        }
    }

    async fn ensure_gate(&self) -> Result<(), Error> {
        let Some(gate) = &self.gate else {
            return Ok(());
        };
        let verdict = self
            .gate_state
            .get_or_init(|| async { gate.check(self.resolver.as_ref()).await })
            .await;
        verdict.clone().map_err(Error::Process)
    }

    async fn resolve_cached(&self, id: u32) -> Result<CachedSchema, Error> {
        if let Some(cached) = self.cache.get(&id) {
            return Ok(cached.clone());
        }
        let fetched = self.resolver.fetch_schema(id).await?;
        let cached = match fetched {
            FetchedSchema::Protobuf(text) => {
                let message_type = self.message_type.as_deref().ok_or_else(|| {
                    Error::Process(format!(
                        "schema id {} resolved to a Protobuf schema but the codec configuration has no `message_type`",
                        id
                    ))
                })?;
                CachedSchema::Protobuf(parse_proto_source(&text, message_type)?)
            }
            FetchedSchema::Avro(schema) => CachedSchema::Avro(schema),
        };
        self.cache.insert(id, cached.clone());
        Ok(cached)
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
        writer.finish().map_err(|e| {
            Error::Process(format!("Schema registry codec encode finish error: {}", e))
        })?;
        let s = String::from_utf8(buf)
            .map_err(|e| Error::Process(format!("UTF-8 conversion failed: {}", e)))?;
        Ok(s.lines().map(|l| l.as_bytes().to_vec()).collect())
    }
}

#[async_trait]
impl Decoder for SchemaRegistryCodec {
    async fn decode(&self, b: Vec<Bytes>) -> Result<MessageBatch, Error> {
        self.ensure_gate().await?;
        let mut batches = Vec::with_capacity(b.len());
        for msg in b {
            let (id, payload) = parse_wire_format(&msg)?;
            let cached = self.resolve_cached(id).await?;
            let batch = match cached {
                CachedSchema::Protobuf(descriptor) => protobuf_to_arrow(&descriptor, payload)?,
                CachedSchema::Avro(schema) => avro_to_arrow(&schema, payload)?,
            };
            batches.push(batch);
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

#[derive(Deserialize)]
struct SubjectConfigResponse {
    #[serde(rename = "compatibilityLevel", default)]
    compatibility_level: Option<String>,
}

#[async_trait]
impl SchemaResolver for RestSchemaResolver {
    async fn fetch_schema(&self, id: u32) -> Result<FetchedSchema, Error> {
        let body: SchemaResponse = self.get_json(&format!("/schemas/ids/{}", id)).await?;
        let schema_type = body
            .schema_type
            .as_deref()
            .map(str::to_ascii_uppercase)
            .unwrap_or_else(|| "PROTOBUF".to_string());
        match schema_type.as_str() {
            "PROTOBUF" => Ok(FetchedSchema::Protobuf(body.schema)),
            "AVRO" => {
                let schema = AvroSchema::parse_str(&body.schema).map_err(|e| {
                    Error::Process(format!(
                        "Schema Registry returned an invalid Avro schema for id {}: {}",
                        id, e
                    ))
                })?;
                Ok(FetchedSchema::Avro(schema))
            }
            other => Err(Error::Process(format!(
                "Unsupported schema type: {} (supported: PROTOBUF, AVRO)",
                other
            ))),
        }
    }

    async fn fetch_subject_compatibility(&self, subject: &str) -> Result<String, Error> {
        let encoded = utf8_percent_encode(subject, SUBJECT_PATH_SEGMENT);
        let body: SubjectConfigResponse = self
            .get_json(&format!("/config/{encoded}?defaultToGlobal=true"))
            .await?;
        body.compatibility_level.ok_or_else(|| {
            Error::Process(format!(
                "Schema Registry config response for subject '{}' has no compatibilityLevel",
                subject
            ))
        })
    }
}

impl RestSchemaResolver {
    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        path_and_query: &str,
    ) -> Result<T, Error> {
        let url = format!("{}{}", self.base_url.trim_end_matches('/'), path_and_query);
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
        resp.json()
            .await
            .map_err(|e| Error::Process(format!("Schema Registry response parse failed: {}", e)))
    }
}

// ===== Config + Builder + init =====

#[derive(Deserialize)]
struct SchemaRegistryCodecConfig {
    registry_url: String,
    #[serde(default)]
    message_type: Option<String>,
    #[serde(default)]
    auth: Option<AuthConfig>,
    /// Registry subject for the compatibility gate.
    #[serde(default)]
    subject: Option<String>,
    /// Minimum subject compatibility level (requires `subject`).
    #[serde(default)]
    min_compatibility: Option<String>,
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
        let config = config.as_ref().ok_or_else(|| {
            Error::Config("schema_registry codec configuration is missing".to_string())
        })?;
        let config: SchemaRegistryCodecConfig = serde_json::from_value(config.clone())?;
        let auth = match config.auth {
            None => None,
            Some(a) => Some(match a.auth_type.as_str() {
                "basic" => Auth::Basic(
                    a.username.unwrap_or_default(),
                    a.password.unwrap_or_default(),
                ),
                "bearer" => Auth::Bearer(a.token.unwrap_or_default()),
                other => return Err(Error::Config(format!("Unsupported auth type: {}", other))),
            }),
        };
        let min_compatibility = match config.min_compatibility.as_deref() {
            None => None,
            Some("none") => Some(MinCompatibility::None),
            Some("backward") => Some(MinCompatibility::Backward),
            Some("forward") => Some(MinCompatibility::Forward),
            Some("full") => Some(MinCompatibility::Full),
            Some(other) => {
                return Err(Error::Config(format!(
                    "schema_registry codec: unsupported min_compatibility '{}' (supported: none, backward, forward, full)",
                    other
                )));
            }
        };
        let gate = match (&config.subject, min_compatibility) {
            (Some(subject), Some(min)) if min != MinCompatibility::None => {
                Some(CompatibilityGate {
                    subject: subject.clone(),
                    min,
                })
            }
            (None, Some(_)) => {
                return Err(Error::Config(
                    "schema_registry codec: min_compatibility requires `subject`".to_string(),
                ));
            }
            _ => None,
        };
        let resolver = Arc::new(RestSchemaResolver::new(config.registry_url, auth)?);
        Ok(Arc::new(SchemaRegistryCodec::new(
            config.message_type,
            resolver,
            gate,
        )))
    }
}

pub(crate) fn init() -> Result<(), Error> {
    arkflow_core::codec::register_codec_builder(
        "schema_registry",
        Arc::new(SchemaRegistryCodecBuilder),
    )?;
    register_codec_metadata(
        ComponentMetadata::with_schema(
            "schema_registry",
            "Decodes Confluent wire-format Protobuf and Avro messages by resolving the schema id from a Schema Registry, with an optional subject compatibility gate.",
            serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "registry_url": {"type": "string", "description": "Confluent Schema Registry base URL."},
                    "message_type": {"type": "string", "description": "Fully-qualified Protobuf message type. Required for Protobuf schemas; omit for Avro."},
                    "subject": {"type": "string", "description": "Registry subject for the compatibility gate."},
                    "min_compatibility": {"type": "string", "enum": ["none", "backward", "forward", "full"], "description": "Minimum subject compatibility level enforced on the first decoded message. Requires `subject`."},
                    "auth": {"type": "object", "description": "Optional registry authentication.", "properties": {
                        "type": {"type": "string", "enum": ["basic", "bearer"]},
                        "username": {"type": "string"},
                        "password": {"type": "string"},
                        "token": {"type": "string"}
                    }}
                },
                "required": ["registry_url"]
            }),
        ),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::Value as AvroValue;
    use apache_avro::writer::datum::GenericDatumWriter;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU32, Ordering};

    const TEST_SCHEMA: &str = "syntax = \"proto3\";\npackage test;\nmessage M { int64 id = 1; }";

    const AVRO_SCHEMA: &str = r#"{
        "type": "record", "name": "M", "fields": [
            {"name": "id", "type": "long"}
        ]
    }"#;

    /// Payload for `M { id = 42 }`: field 1 (int64, varint), tag=0x08, value=0x2A.
    fn protobuf_payload_id_42() -> Vec<u8> {
        vec![0x08, 0x2A]
    }

    fn avro_schema() -> AvroSchema {
        AvroSchema::parse_str(AVRO_SCHEMA).unwrap()
    }

    fn avro_payload_id_42() -> Vec<u8> {
        let schema = avro_schema();
        GenericDatumWriter::builder(&schema)
            .build()
            .unwrap()
            .write_value_to_vec(AvroValue::Record(vec![(
                "id".to_string(),
                AvroValue::Long(42),
            )]))
            .unwrap()
    }

    fn wire(id: u32, payload: &[u8]) -> Vec<u8> {
        let mut m = vec![0x00];
        m.extend_from_slice(&id.to_be_bytes());
        m.extend_from_slice(payload);
        m
    }

    struct InMemorySchemaResolver {
        schemas: HashMap<u32, FetchedSchema>,
        subject_levels: HashMap<String, String>,
        fetch_count: AtomicU32,
        config_count: AtomicU32,
    }
    impl InMemorySchemaResolver {
        fn new(map: HashMap<u32, FetchedSchema>) -> Self {
            Self {
                schemas: map,
                subject_levels: HashMap::new(),
                fetch_count: AtomicU32::new(0),
                config_count: AtomicU32::new(0),
            }
        }
        fn with_subject_levels(
            map: HashMap<u32, FetchedSchema>,
            levels: HashMap<String, String>,
        ) -> Self {
            Self {
                schemas: map,
                subject_levels: levels,
                fetch_count: AtomicU32::new(0),
                config_count: AtomicU32::new(0),
            }
        }
        fn fetches(&self) -> u32 {
            self.fetch_count.load(Ordering::SeqCst)
        }
        fn config_fetches(&self) -> u32 {
            self.config_count.load(Ordering::SeqCst)
        }
    }
    #[async_trait]
    impl SchemaResolver for InMemorySchemaResolver {
        async fn fetch_schema(&self, id: u32) -> Result<FetchedSchema, Error> {
            self.fetch_count.fetch_add(1, Ordering::SeqCst);
            self.schemas
                .get(&id)
                .cloned()
                .ok_or_else(|| Error::Process(format!("schema id {} not in test resolver", id)))
        }

        async fn fetch_subject_compatibility(&self, subject: &str) -> Result<String, Error> {
            self.config_count.fetch_add(1, Ordering::SeqCst);
            self.subject_levels.get(subject).cloned().ok_or_else(|| {
                Error::Process(format!("subject {} not in test resolver", subject))
            })
        }
    }

    fn build_codec(resolver: Arc<dyn SchemaResolver>) -> SchemaRegistryCodec {
        SchemaRegistryCodec::new(Some("test.M".to_string()), resolver, None)
    }

    fn avro_codec(resolver: Arc<dyn SchemaResolver>) -> SchemaRegistryCodec {
        SchemaRegistryCodec::new(None, resolver, None)
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

    #[test]
    fn test_compatibility_rank() {
        assert_eq!(compatibility_rank("NONE"), Some(0));
        assert_eq!(compatibility_rank("BACKWARD"), Some(1));
        assert_eq!(compatibility_rank("BACKWARD_TRANSITIVE"), Some(1));
        assert_eq!(compatibility_rank("FORWARD"), Some(1));
        assert_eq!(compatibility_rank("FORWARD_TRANSITIVE"), Some(1));
        assert_eq!(compatibility_rank("FULL"), Some(2));
        assert_eq!(compatibility_rank("FULL_TRANSITIVE"), Some(2));
        assert_eq!(compatibility_rank("WEIRD"), None);
    }

    #[tokio::test]
    async fn test_decode_single_protobuf() {
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::from([(
            1u32,
            FetchedSchema::Protobuf(TEST_SCHEMA.to_string()),
        )])));
        let codec = build_codec(resolver.clone());
        let batch = codec
            .decode(vec![wire(1, &protobuf_payload_id_42())])
            .await
            .unwrap();
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
    async fn test_decode_single_avro() {
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::from([(
            1u32,
            FetchedSchema::Avro(avro_schema()),
        )])));
        let codec = avro_codec(resolver.clone());
        let batch = codec
            .decode(vec![wire(1, &avro_payload_id_42())])
            .await
            .unwrap();
        assert_eq!(batch.len(), 1);
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::Int64Type;
        let id_col = batch
            .record_batch()
            .column_by_name("id")
            .expect("id column");
        assert_eq!(id_col.as_primitive::<Int64Type>().value(0), 42);
    }

    #[tokio::test]
    async fn test_avro_without_message_type_is_ok_but_protobuf_id_errors() {
        // Avro id decodes fine with no message_type configured...
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::from([
            (1u32, FetchedSchema::Avro(avro_schema())),
            (2u32, FetchedSchema::Protobuf(TEST_SCHEMA.to_string())),
        ])));
        let codec = avro_codec(resolver);
        codec
            .decode(vec![wire(1, &avro_payload_id_42())])
            .await
            .expect("avro id must decode without message_type");
        // ...while a protobuf id surfaces a clear error.
        let err = codec
            .decode(vec![wire(2, &protobuf_payload_id_42())])
            .await
            .unwrap_err();
        assert!(
            format!("{err}").contains("message_type"),
            "expected message_type in error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_cache_hits() {
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::from([(
            1u32,
            FetchedSchema::Protobuf(TEST_SCHEMA.to_string()),
        )])));
        let codec = build_codec(resolver.clone());
        let batch = codec
            .decode(vec![
                wire(1, &protobuf_payload_id_42()),
                wire(1, &protobuf_payload_id_42()),
            ])
            .await
            .unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(resolver.fetches(), 1); // only the first fetches
    }

    #[tokio::test]
    async fn test_multi_version_each_resolves() {
        // two ids, same (compatible) schema; each resolves its own descriptor.
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::from([
            (1u32, FetchedSchema::Protobuf(TEST_SCHEMA.to_string())),
            (2u32, FetchedSchema::Protobuf(TEST_SCHEMA.to_string())),
        ])));
        let codec = build_codec(resolver.clone());
        let batch = codec
            .decode(vec![
                wire(1, &protobuf_payload_id_42()),
                wire(2, &protobuf_payload_id_42()),
            ])
            .await
            .unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(resolver.fetches(), 2); // both ids resolved
    }

    #[tokio::test]
    async fn test_multi_version_avro() {
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::from([
            (1u32, FetchedSchema::Avro(avro_schema())),
            (2u32, FetchedSchema::Avro(avro_schema())),
        ])));
        let codec = avro_codec(resolver.clone());
        let batch = codec
            .decode(vec![
                wire(1, &avro_payload_id_42()),
                wire(2, &avro_payload_id_42()),
            ])
            .await
            .unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(resolver.fetches(), 2);
    }

    #[tokio::test]
    async fn test_resolver_error() {
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::new())); // empty -> all error
        let codec = build_codec(resolver);
        let result = codec
            .decode(vec![wire(99, &protobuf_payload_id_42())])
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_gate_passes_when_level_meets_minimum() {
        let resolver = Arc::new(InMemorySchemaResolver::with_subject_levels(
            HashMap::from([(1u32, FetchedSchema::Avro(avro_schema()))]),
            HashMap::from([("s".to_string(), "BACKWARD".to_string())]),
        ));
        let codec = SchemaRegistryCodec::new(
            None,
            resolver.clone(),
            Some(CompatibilityGate {
                subject: "s".to_string(),
                min: MinCompatibility::Backward,
            }),
        );
        codec
            .decode(vec![wire(1, &avro_payload_id_42())])
            .await
            .expect("gate must pass at BACKWARD >= backward");
        assert_eq!(resolver.config_fetches(), 1);
    }

    #[tokio::test]
    async fn test_gate_fails_when_level_below_minimum() {
        let resolver = Arc::new(InMemorySchemaResolver::with_subject_levels(
            HashMap::from([(1u32, FetchedSchema::Avro(avro_schema()))]),
            HashMap::from([("s".to_string(), "NONE".to_string())]),
        ));
        let codec = SchemaRegistryCodec::new(
            None,
            resolver,
            Some(CompatibilityGate {
                subject: "s".to_string(),
                min: MinCompatibility::Full,
            }),
        );
        let err = codec
            .decode(vec![wire(1, &avro_payload_id_42())])
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("'NONE'") && msg.contains("full"),
            "expected level and requirement in error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_gate_transitive_variant_passes() {
        let resolver = Arc::new(InMemorySchemaResolver::with_subject_levels(
            HashMap::from([(1u32, FetchedSchema::Avro(avro_schema()))]),
            HashMap::from([("s".to_string(), "BACKWARD_TRANSITIVE".to_string())]),
        ));
        let codec = SchemaRegistryCodec::new(
            None,
            resolver,
            Some(CompatibilityGate {
                subject: "s".to_string(),
                min: MinCompatibility::Backward,
            }),
        );
        codec
            .decode(vec![wire(1, &avro_payload_id_42())])
            .await
            .expect("BACKWARD_TRANSITIVE must satisfy backward");
    }

    #[tokio::test]
    async fn test_gate_result_is_cached() {
        let resolver = Arc::new(InMemorySchemaResolver::with_subject_levels(
            HashMap::from([(1u32, FetchedSchema::Avro(avro_schema()))]),
            HashMap::from([("s".to_string(), "NONE".to_string())]),
        ));
        let codec = SchemaRegistryCodec::new(
            None,
            resolver.clone(),
            Some(CompatibilityGate {
                subject: "s".to_string(),
                min: MinCompatibility::Backward,
            }),
        );
        // Every decode fails at the gate, but the config endpoint is hit once.
        for _ in 0..3 {
            assert!(codec
                .decode(vec![wire(1, &avro_payload_id_42())])
                .await
                .is_err());
        }
        assert_eq!(resolver.config_fetches(), 1);
    }

    #[tokio::test]
    async fn test_no_gate_no_config_requests() {
        let resolver = Arc::new(InMemorySchemaResolver::with_subject_levels(
            HashMap::from([(1u32, FetchedSchema::Avro(avro_schema()))]),
            HashMap::new(),
        ));
        let codec = avro_codec(resolver.clone());
        codec
            .decode(vec![wire(1, &avro_payload_id_42())])
            .await
            .unwrap();
        assert_eq!(resolver.config_fetches(), 0);
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
        assert!(matches!(
            schema,
            FetchedSchema::Protobuf(ref s) if s.contains("message M")
        ));
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
                ResponseTemplate::new(200).set_body_json(
                    serde_json::json!({"schema": "syntax = \"proto3\"; message M {}"}),
                ),
            )
            .mount(&server)
            .await;
        let resolver =
            RestSchemaResolver::new(server.uri(), Some(Auth::Bearer("tok".into()))).unwrap();
        let schema = resolver.fetch_schema(2).await.unwrap();
        assert!(matches!(
            schema,
            FetchedSchema::Protobuf(ref s) if s.contains("message M")
        ));
    }

    #[tokio::test]
    async fn test_rest_resolver_avro_schema_type() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/3"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"schema": AVRO_SCHEMA, "schemaType": "AVRO"}),
            ))
            .mount(&server)
            .await;
        let resolver = RestSchemaResolver::new(server.uri(), None).unwrap();
        let schema = resolver.fetch_schema(3).await.unwrap();
        assert!(matches!(schema, FetchedSchema::Avro(_)));
    }

    #[tokio::test]
    async fn test_rest_resolver_rejects_unknown_schema_type() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/4"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"schema": "{}", "schemaType": "JSON"}),
            ))
            .mount(&server)
            .await;
        let resolver = RestSchemaResolver::new(server.uri(), None).unwrap();
        let err = resolver.fetch_schema(4).await.unwrap_err();
        assert!(format!("{err}").contains("JSON"));
    }

    #[tokio::test]
    async fn test_rest_resolver_non_200_is_an_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/99"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let resolver = RestSchemaResolver::new(server.uri(), None).unwrap();
        let err = resolver.fetch_schema(99).await.unwrap_err();
        assert!(format!("{err}").contains("404"), "got: {err}");
    }

    #[tokio::test]
    async fn test_rest_resolver_config_non_200_is_an_error() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/config/orders"))
            .and(query_param("defaultToGlobal", "true"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let resolver = RestSchemaResolver::new(server.uri(), None).unwrap();
        let err = resolver
            .fetch_subject_compatibility("orders")
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("500"), "got: {err}");
    }

    #[tokio::test]
    async fn test_rest_resolver_subject_compatibility() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/config/orders"))
            .and(query_param("defaultToGlobal", "true"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"compatibilityLevel": "FULL_TRANSITIVE"})),
            )
            .mount(&server)
            .await;
        let resolver = RestSchemaResolver::new(server.uri(), None).unwrap();
        let level = resolver.fetch_subject_compatibility("orders").await.unwrap();
        assert_eq!(level, "FULL_TRANSITIVE");
    }

    #[tokio::test]
    async fn test_rest_resolver_subject_compatibility_percent_encodes_subject() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        // `orders/v2 prod%final` must arrive as one encoded path segment,
        // not as nested paths / a broken escape sequence.
        Mock::given(method("GET"))
            .and(path("/config/orders%2Fv2%20prod%25final"))
            .and(query_param("defaultToGlobal", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"compatibilityLevel": "BACKWARD"}),
            ))
            .mount(&server)
            .await;
        let resolver = RestSchemaResolver::new(server.uri(), None).unwrap();
        let level = resolver
            .fetch_subject_compatibility("orders/v2 prod%final")
            .await
            .unwrap();
        assert_eq!(level, "BACKWARD");
    }

    #[test]
    fn test_builder_min_compatibility_without_subject() {
        let config = serde_json::json!({
            "registry_url": "http://localhost:8081",
            "min_compatibility": "backward"
        });
        let err = match SchemaRegistryCodecBuilder.build(None, &Some(config), &test_resource()) {
            Ok(_) => panic!("expected build to fail when min_compatibility has no subject"),
            Err(e) => e,
        };
        assert!(format!("{err}").contains("subject"));
    }

    #[test]
    fn test_builder_rejects_unknown_min_compatibility() {
        let config = serde_json::json!({
            "registry_url": "http://localhost:8081",
            "subject": "s",
            "min_compatibility": "sideways"
        });
        assert!(SchemaRegistryCodecBuilder
            .build(None, &Some(config), &test_resource())
            .is_err());
    }

    #[test]
    fn test_builder_accepts_avro_style_config() {
        let config = serde_json::json!({
            "registry_url": "http://localhost:8081"
        });
        SchemaRegistryCodecBuilder
            .build(None, &Some(config), &test_resource())
            .expect("config without message_type must build");
    }

    fn test_resource() -> Resource {
        Resource {
            temporary: std::collections::HashMap::new(),
            input_names: std::cell::RefCell::new(vec![]),
        }
    }
}
