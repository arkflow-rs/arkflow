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

use crate::codec::avro_arrow::{avro_read_value, AvroArrowAccumulator};
use crate::component::protobuf::{parse_proto_source, ProtobufBatchConverter};
use apache_avro::Schema as AvroSchema;
use arkflow_core::codec::{Codec, CodecBuilder, Decoder, Encoder};
use arkflow_core::component::{register_codec_metadata, ComponentMetadata};
use arkflow_core::{Bytes, Error, MessageBatch, Resource};
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
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
    /// `schemaType` was omitted by the registry and content detection chose
    /// Protobuf (the text is not a JSON object). Parsing happens lazily like
    /// [`FetchedSchema::Protobuf`]; the variant only carries how the choice
    /// was made so a parse failure can name both detection attempts.
    ProtobufByDetection(String),
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
    /// Cached per schema id behind an `Arc`: the decode hot path hits the
    /// cache per message, and cloning the schema tree per hit is a measurable
    /// per-message cost (`apache_avro::Schema` has no interior sharing).
    cache: DashMap<u32, Arc<CachedSchema>>,
    gate: Option<CompatibilityGate>,
    /// Gate state: a pass verdict is cached for the codec lifetime; a failure
    /// is retried on later decodes no sooner than `gate_retry_interval` so a
    /// transient registry outage does not brick the codec.
    gate_state: tokio::sync::RwLock<GateState>,
    gate_retry_interval: std::time::Duration,
}

/// How long after a failed gate check before the config endpoint is retried.
const GATE_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

struct GateState {
    verdict: Option<Result<(), String>>,
    last_attempt: Option<std::time::Instant>,
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
            gate_state: tokio::sync::RwLock::new(GateState {
                verdict: None,
                last_attempt: None,
            }),
            gate_retry_interval: GATE_RETRY_INTERVAL,
        }
    }

    /// Test-only override of the gate retry interval.
    #[cfg(test)]
    pub(crate) fn set_gate_retry_interval(&mut self, interval: std::time::Duration) {
        self.gate_retry_interval = interval;
    }

    async fn ensure_gate(&self) -> Result<(), Error> {
        let Some(gate) = &self.gate else {
            return Ok(());
        };
        // Fast path: a pass verdict is permanent.
        {
            let state = self.gate_state.read().await;
            if let Some(Ok(())) = state.verdict {
                return Ok(());
            }
        }
        let mut state = self.gate_state.write().await;
        if let Some(Ok(())) = state.verdict {
            return Ok(());
        }
        let now = std::time::Instant::now();
        // A recent failure is returned as-is without hitting the registry
        // again; only after the retry interval may the check be re-issued.
        if let (Some(Err(err)), Some(last)) = (&state.verdict, state.last_attempt) {
            if now.duration_since(last) < self.gate_retry_interval {
                return Err(Error::Process(err.clone()));
            }
        }
        let verdict = gate.check(self.resolver.as_ref()).await;
        state.verdict = Some(verdict.clone());
        state.last_attempt = Some(now);
        verdict.map_err(Error::Process)
    }

    async fn resolve_cached(&self, id: u32) -> Result<Arc<CachedSchema>, Error> {
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
            FetchedSchema::ProtobufByDetection(text) => {
                let message_type = self.message_type.as_deref().ok_or_else(|| {
                    Error::Process(format!(
                        "schema id {} resolved to a Protobuf schema but the codec configuration has no `message_type`",
                        id
                    ))
                })?;
                CachedSchema::Protobuf(parse_proto_source(&text, message_type).map_err(|e| {
                    Error::Process(format!(
                        "schema id {}: the registry omitted `schemaType`; content detection chose Protobuf because the text is not a JSON object, but parsing it as Protobuf failed ({e}). Neither Avro nor Protobuf can read this schema",
                        id
                    ))
                })?)
            }
            FetchedSchema::Avro(schema) => CachedSchema::Avro(schema),
        };
        let cached = Arc::new(cached);
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
        // Messages accumulate columnar per schema id (groups in
        // first-appearance order, rows in message order within a group);
        // protobuf groups accumulate columnar exactly like Avro groups, so
        // relative order is stable across kinds.
        enum Group {
            Avro(Box<AvroArrowAccumulator>),
            Protobuf(ProtobufBatchConverter),
        }
        let mut groups: Vec<(u32, Group)> = Vec::new();
        for msg in b {
            let (id, payload) = parse_wire_format(&msg)?;
            let cached = self.resolve_cached(id).await?;
            match cached.as_ref() {
                CachedSchema::Protobuf(descriptor) => {
                    match groups.iter_mut().find(|(gid, _)| gid == &id) {
                        Some((_, Group::Protobuf(converter))) => converter.push(payload)?,
                        _ => {
                            // `MessageDescriptor` clones are cheap Arc bumps.
                            let mut converter = ProtobufBatchConverter::new(descriptor.clone());
                            converter.push(payload)?;
                            groups.push((id, Group::Protobuf(converter)));
                        }
                    }
                }
                CachedSchema::Avro(schema) => {
                    let value = avro_read_value(schema, payload)?;
                    match groups.iter_mut().find(|(gid, _)| gid == &id) {
                        Some((_, Group::Avro(accumulator))) => accumulator.push(&value)?,
                        _ => {
                            let mut accumulator = Box::new(AvroArrowAccumulator::new(schema)?);
                            accumulator.push(&value)?;
                            groups.push((id, Group::Avro(accumulator)));
                        }
                    }
                }
            }
        }
        let mut batches = Vec::with_capacity(groups.len());
        for (_, group) in groups {
            match group {
                Group::Avro(mut accumulator) => batches.push(accumulator.finish()?),
                Group::Protobuf(converter) => batches.push(converter.finish()?),
            }
        }
        if batches.is_empty() {
            return Ok(MessageBatch::new_arrow(RecordBatch::new_empty(Arc::new(
                Schema::empty(),
            ))));
        }
        // Groups decoded under different schema versions may legitimately
        // carry different schemas (real schema evolution); normalize to the
        // field union instead of failing the concat.
        let merged = crate::component::batch_merge::normalize_and_concat(&batches)?;
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

fn parse_avro_schema(text: &str, id: u32) -> Result<AvroSchema, Error> {
    AvroSchema::parse_str(text).map_err(|e| {
        Error::Process(format!(
            "Schema Registry returned an invalid Avro schema for id {}: {}",
            id, e
        ))
    })
}

#[async_trait]
impl SchemaResolver for RestSchemaResolver {
    async fn fetch_schema(&self, id: u32) -> Result<FetchedSchema, Error> {
        let body: SchemaResponse = self.get_json(&format!("/schemas/ids/{}", id)).await?;
        let schema_type = body.schema_type.as_deref().map(str::to_ascii_uppercase);
        match schema_type.as_deref() {
            Some("PROTOBUF") => Ok(FetchedSchema::Protobuf(body.schema)),
            Some("AVRO") => Ok(FetchedSchema::Avro(parse_avro_schema(&body.schema, id)?)),
            Some(other) => Err(Error::Process(format!(
                "Unsupported schema type: {} (supported: PROTOBUF, AVRO)",
                other
            ))),
            // Older registries omit `schemaType`: Avro was the only original
            // format, but this codec historically defaulted to Protobuf, so
            // detect by content instead of guessing. An Avro schema is always
            // a JSON object; a proto source never is.
            None => {
                let trimmed = body.schema.trim();
                if trimmed.starts_with('{') {
                    Ok(FetchedSchema::Avro(parse_avro_schema(trimmed, id)?))
                } else {
                    Ok(FetchedSchema::ProtobufByDetection(body.schema))
                }
            }
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
        _name: Option<&str>,
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

    /// Real schema evolution: v2 adds a `name` column (different schema text).
    const TEST_SCHEMA_V2: &str =
        "syntax = \"proto3\";\npackage test;\nmessage M { int64 id = 1; string name = 2; }";

    /// Same column name with a different type: normalization must fail loudly.
    const TEST_SCHEMA_CONFLICT: &str =
        "syntax = \"proto3\";\npackage test;\nmessage M { string id = 1; }";

    const AVRO_SCHEMA: &str = r#"{
        "type": "record", "name": "M", "fields": [
            {"name": "id", "type": "long"}
        ]
    }"#;

    /// Avro evolution: v2 adds a `name` field.
    const AVRO_SCHEMA_V2: &str = r#"{
        "type": "record", "name": "M", "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"}
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

    fn avro_payload_v2(id: i64, name: &str) -> Vec<u8> {
        let schema = AvroSchema::parse_str(AVRO_SCHEMA_V2).unwrap();
        GenericDatumWriter::builder(&schema)
            .build()
            .unwrap()
            .write_value_to_vec(AvroValue::Record(vec![
                ("id".to_string(), AvroValue::Long(id)),
                ("name".to_string(), AvroValue::String(name.to_string())),
            ]))
            .unwrap()
    }

    fn avro_payload_v1(id: i64) -> Vec<u8> {
        let schema = avro_schema();
        GenericDatumWriter::builder(&schema)
            .build()
            .unwrap()
            .write_value_to_vec(AvroValue::Record(vec![(
                "id".to_string(),
                AvroValue::Long(id),
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
        subject_levels: std::sync::Mutex<HashMap<String, String>>,
        fetch_count: AtomicU32,
        config_count: AtomicU32,
    }
    impl InMemorySchemaResolver {
        fn new(map: HashMap<u32, FetchedSchema>) -> Self {
            Self {
                schemas: map,
                subject_levels: std::sync::Mutex::new(HashMap::new()),
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
                subject_levels: std::sync::Mutex::new(levels),
                fetch_count: AtomicU32::new(0),
                config_count: AtomicU32::new(0),
            }
        }
        fn set_subject_level(&self, subject: &str, level: &str) {
            self.subject_levels
                .lock()
                .unwrap()
                .insert(subject.to_string(), level.to_string());
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
            self.subject_levels
                .lock()
                .unwrap()
                .get(subject)
                .cloned()
                .ok_or_else(|| Error::Process(format!("subject {} not in test resolver", subject)))
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
        // Real evolution: id 1 is v1 (id only), id 2 is v2 (adds `name`).
        // Each id resolves its own descriptor; the merged batch carries the
        // union schema with the v1 row's `name` null-filled.
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::from([
            (1u32, FetchedSchema::Protobuf(TEST_SCHEMA.to_string())),
            (2u32, FetchedSchema::Protobuf(TEST_SCHEMA_V2.to_string())),
        ])));
        let codec = build_codec(resolver.clone());
        let batch = codec
            .decode(vec![
                wire(1, &protobuf_payload_id_42()),
                wire(2, &[0x08, 0x2A, 0x12, 0x02, b'o', b'k']), // id=42, name="ok"
            ])
            .await
            .unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(resolver.fetches(), 2); // both ids resolved
        use datafusion::arrow::array::{Array, AsArray};
        use datafusion::arrow::datatypes::Int64Type;
        let rb = batch.record_batch();
        assert_eq!(rb.num_columns(), 2, "union schema: id + name");
        let id_col = rb.column_by_name("id").expect("id column");
        assert_eq!(id_col.as_primitive::<Int64Type>().value(1), 42);
        let name_col = rb.column_by_name("name").expect("name column");
        let name = name_col.as_string::<i32>();
        assert_eq!(name.value(1), "ok");
        assert!(name.is_null(0), "v1 row's `name` must be null-filled");
    }

    #[tokio::test]
    async fn test_multi_version_avro() {
        // Real Avro evolution: v2 adds a `name` field; the union batch
        // null-fills it for the v1 row.
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::from([
            (1u32, FetchedSchema::Avro(avro_schema())),
            (
                2u32,
                FetchedSchema::Avro(AvroSchema::parse_str(AVRO_SCHEMA_V2).unwrap()),
            ),
        ])));
        let codec = avro_codec(resolver.clone());
        let batch = codec
            .decode(vec![
                wire(1, &avro_payload_id_42()),
                wire(2, &avro_payload_v2(7, "seven")),
            ])
            .await
            .unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(resolver.fetches(), 2);
        use datafusion::arrow::array::{Array, AsArray};
        use datafusion::arrow::datatypes::Int64Type;
        let rb = batch.record_batch();
        assert_eq!(rb.num_columns(), 2, "union schema: id + name");
        let id_col = rb.column_by_name("id").expect("id column");
        assert_eq!(id_col.as_primitive::<Int64Type>().value(1), 7);
        let name_col = rb.column_by_name("name").expect("name column");
        let name = name_col.as_string::<i32>();
        assert_eq!(name.value(1), "seven");
        assert!(name.is_null(0), "v1 row's `name` must be null-filled");
    }

    #[tokio::test]
    async fn test_avro_single_id_batch_accumulates_columnar() {
        // Single-id batches accumulate into one multi-row batch whose schema
        // keeps the writer's nullability (id is non-nullable).
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::from([(
            1u32,
            FetchedSchema::Avro(avro_schema()),
        )])));
        let codec = avro_codec(resolver.clone());
        let batch = codec
            .decode(vec![
                wire(1, &avro_payload_v1(10)),
                wire(1, &avro_payload_v1(11)),
                wire(1, &avro_payload_v1(12)),
            ])
            .await
            .unwrap();
        assert_eq!(batch.len(), 3);
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::Int64Type;
        let rb = batch.record_batch();
        assert_eq!(rb.num_columns(), 1);
        assert!(
            !rb.schema().field(0).is_nullable(),
            "writer schema keeps `id` non-nullable"
        );
        let id_col = rb.column_by_name("id").expect("id column");
        let ids = id_col.as_primitive::<Int64Type>();
        assert_eq!((ids.value(0), ids.value(1), ids.value(2)), (10, 11, 12));
    }

    #[tokio::test]
    async fn test_avro_mixed_id_batch_groups_by_first_appearance() {
        // Interleaved ids: rows come out grouped by schema id in
        // first-appearance order (group order, message order within a
        // group); the union schema puts the first group's columns first.
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::from([
            (1u32, FetchedSchema::Avro(avro_schema())),
            (
                2u32,
                FetchedSchema::Avro(AvroSchema::parse_str(AVRO_SCHEMA_V2).unwrap()),
            ),
        ])));
        let codec = avro_codec(resolver);
        let batch = codec
            .decode(vec![
                wire(1, &avro_payload_v1(10)),
                wire(2, &avro_payload_v2(20, "b")),
                wire(1, &avro_payload_v1(11)),
                wire(2, &avro_payload_v2(21, "c")),
            ])
            .await
            .unwrap();
        assert_eq!(batch.len(), 4);
        use datafusion::arrow::array::{Array, AsArray};
        use datafusion::arrow::datatypes::Int64Type;
        let rb = batch.record_batch();
        assert_eq!(
            rb.schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            vec!["id", "name"],
            "first group's columns first"
        );
        let ids = rb.column_by_name("id").unwrap().as_primitive::<Int64Type>();
        assert_eq!(
            (ids.value(0), ids.value(1), ids.value(2), ids.value(3)),
            (10, 11, 20, 21),
            "rows grouped by schema id, message order within a group"
        );
        let names = rb.column_by_name("name").unwrap().as_string::<i32>();
        assert!(names.is_null(0) && names.is_null(1));
        assert_eq!(names.value(2), "b");
        assert_eq!(names.value(3), "c");
    }

    #[tokio::test]
    async fn test_multi_version_conflicting_column_types_error() {
        // `id` is int64 in v1 and string in v2: normalization must name the
        // column and both types instead of guessing a cast.
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::from([
            (1u32, FetchedSchema::Protobuf(TEST_SCHEMA.to_string())),
            (
                2u32,
                FetchedSchema::Protobuf(TEST_SCHEMA_CONFLICT.to_string()),
            ),
        ])));
        let codec = build_codec(resolver);
        let err = codec
            .decode(vec![
                wire(1, &protobuf_payload_id_42()),
                wire(2, &[0x0A, 0x01, b'x']), // string id = "x"
            ])
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("`id`") && msg.contains("Int64") && msg.contains("Utf8"),
            "error must name the column and both types, got: {msg}"
        );
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
        // Every rapid decode fails at the gate; the config endpoint is hit
        // once because the failures are inside the retry interval.
        for _ in 0..3 {
            assert!(codec
                .decode(vec![wire(1, &avro_payload_id_42())])
                .await
                .is_err());
        }
        assert_eq!(resolver.config_fetches(), 1);
    }

    #[tokio::test]
    async fn test_gate_failure_recovers_after_retry_interval() {
        // A transient registry failure is not cached forever: once the retry
        // interval elapses and the registry recovers, decode heals.
        let resolver = Arc::new(InMemorySchemaResolver::with_subject_levels(
            HashMap::from([(1u32, FetchedSchema::Avro(avro_schema()))]),
            HashMap::from([("s".to_string(), "NONE".to_string())]),
        ));
        let mut codec = SchemaRegistryCodec::new(
            None,
            resolver.clone(),
            Some(CompatibilityGate {
                subject: "s".to_string(),
                min: MinCompatibility::Backward,
            }),
        );
        codec.set_gate_retry_interval(std::time::Duration::from_millis(20));
        // Fails at the gate while the level is below the minimum.
        assert!(codec
            .decode(vec![wire(1, &avro_payload_id_42())])
            .await
            .is_err());
        // Registry side recovers (subject level raised).
        resolver.set_subject_level("s", "BACKWARD");
        // Inside the retry interval: last error is returned, no new request.
        assert!(codec
            .decode(vec![wire(1, &avro_payload_id_42())])
            .await
            .is_err());
        assert_eq!(resolver.config_fetches(), 1);
        // After the interval the check is re-issued and passes; the pass
        // verdict is then cached (no further config requests).
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        codec
            .decode(vec![wire(1, &avro_payload_id_42())])
            .await
            .expect("gate must recover after the registry heals");
        codec
            .decode(vec![wire(1, &avro_payload_id_42())])
            .await
            .expect("pass verdict must be cached");
        assert_eq!(resolver.config_fetches(), 2);
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
        // No `schemaType` in the response; proto source text is not a JSON
        // object, so content detection still chooses Protobuf.
        assert!(matches!(
            schema,
            FetchedSchema::ProtobufByDetection(ref s) if s.contains("message M")
        ));
    }

    #[tokio::test]
    async fn test_rest_resolver_schema_type_absent_avro_text_detected() {
        // Old registries omit `schemaType` for Avro schemas; the JSON-object
        // text must be detected as Avro instead of failing as Protobuf.
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/5"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"schema": AVRO_SCHEMA})),
            )
            .mount(&server)
            .await;
        let resolver = RestSchemaResolver::new(server.uri(), None).unwrap();
        let schema = resolver.fetch_schema(5).await.unwrap();
        assert!(matches!(schema, FetchedSchema::Avro(_)));
    }

    #[tokio::test]
    async fn test_rest_resolver_schema_type_absent_neither_parses() {
        // Text that looks like JSON but is not valid Avro: detection tried
        // Avro (only applicable choice) and must fail with a clear error.
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/6"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"schema": "{\"not\": "})),
            )
            .mount(&server)
            .await;
        let resolver = RestSchemaResolver::new(server.uri(), None).unwrap();
        let err = resolver.fetch_schema(6).await.unwrap_err();
        assert!(
            format!("{err}").contains("Avro"),
            "error should name the failed Avro attempt, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_detected_protobuf_parse_failure_lists_both_attempts() {
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::from([(
            1u32,
            FetchedSchema::ProtobufByDetection("}}not-a-proto{{".to_string()),
        )])));
        let codec = build_codec(resolver);
        let err = codec
            .decode(vec![wire(1, &protobuf_payload_id_42())])
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("neither") || (msg.contains("Avro") && msg.contains("Protobuf")),
            "error should list both detection attempts, got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_rest_resolver_avro_schema_type() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/3"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    serde_json::json!({"schema": AVRO_SCHEMA, "schemaType": "AVRO"}),
                ),
            )
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
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"schema": "{}", "schemaType": "JSON"})),
            )
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
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"compatibilityLevel": "FULL_TRANSITIVE"})),
            )
            .mount(&server)
            .await;
        let resolver = RestSchemaResolver::new(server.uri(), None).unwrap();
        let level = resolver
            .fetch_subject_compatibility("orders")
            .await
            .unwrap();
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
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"compatibilityLevel": "BACKWARD"})),
            )
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

    // ===== Protobuf 批级列式累积 =====

    fn protobuf_payload(id: i64) -> Vec<u8> {
        vec![0x08, id as u8]
    }

    #[tokio::test]
    async fn protobuf_same_id_messages_accumulate_in_order() {
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::from([(
            1u32,
            FetchedSchema::Protobuf(TEST_SCHEMA.to_string()),
        )])));
        let codec = build_codec(resolver);
        let batch = codec
            .decode(vec![
                wire(1, &protobuf_payload(11)),
                wire(1, &protobuf_payload(22)),
                wire(1, &protobuf_payload(33)),
            ])
            .await
            .unwrap();
        assert_eq!(batch.len(), 3);
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::Int64Type;
        let ids = batch
            .record_batch()
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int64Type>();
        assert_eq!(
            (0..3).map(|i| ids.value(i)).collect::<Vec<_>>(),
            vec![11, 22, 33]
        );
        // Descriptor-driven convention: every column nullable.
        assert!(batch
            .record_batch()
            .schema()
            .fields()
            .iter()
            .all(|f| f.is_nullable()));
    }

    #[tokio::test]
    async fn fetch_error_precedes_later_bad_wire_header() {
        // A missing schema id on the FIRST message must win over a corrupt
        // header on the second — the sequential order the per-message path
        // always had.
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::from([(
            1u32,
            FetchedSchema::Avro(avro_schema()),
        )])));
        let codec = avro_codec(resolver);
        let err = codec
            .decode(vec![wire(99, &[0x01]), vec![0x01, 0x00, 0x00, 0x00, 0x01]])
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("schema id 99 not in test resolver"), "{msg}");
    }

    #[tokio::test]
    async fn payload_decode_error_precedes_schema_shape_error() {
        // The schema contains an unsupported nested field, but the payload
        // itself fails to decode first — the accumulator is built only
        // after a successful read.
        let nested = AvroSchema::parse_str(
            r#"{"type": "record", "name": "M", "fields": [
                {"name": "id", "type": "long"},
                {"name": "tags", "type": {"type": "array", "items": "string"}}
            ]}"#,
        )
        .unwrap();
        let resolver = Arc::new(InMemorySchemaResolver::new(HashMap::from([(
            1u32,
            FetchedSchema::Avro(nested),
        )])));
        let codec = avro_codec(resolver);
        let err = codec
            .decode(vec![wire(1, &[0xDE, 0xAD])])
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Avro decode failed"), "{msg}");
    }
}
