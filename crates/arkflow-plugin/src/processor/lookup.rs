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

//! Dimension-table lookup processor component
//!
//! Resolves each row's lookup key against an external dimension service and
//! appends the configured result columns to the batch. Key values are
//! normalized to strings and resolved through a bounded in-process cache
//! (TTL, capacity bound, negative caching); cache misses fan out to the
//! backend with bounded, order-preserving concurrency. Backend failures fail
//! the batch by default (`on_backend_error: null` degrades them to misses
//! without caching); unresolved keys emit null lookup columns by default
//! (`on_missing: discard` drops the rows instead).

use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arkflow_core::component::{register_processor_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, ProcessResult, Resource};
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use futures_util::StreamExt;
use percent_encoding::AsciiSet;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;

pub fn init() -> Result<(), Error> {
    register_processor_builder("lookup", Arc::new(LookupProcessorBuilder))?;
    register_processor_metadata(ComponentMetadata::with_schema(
        "lookup",
        "Resolves each row's lookup key against an external dimension service and appends the configured result columns, with an in-process TTL cache, negative caching, and bounded ordered concurrency.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "key_columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 1,
                    "description": "Input column(s) forming the lookup key (Utf8/Int64/UInt64/Boolean). Multiple columns join into a composite key in configured order; a null key cell follows the missing-key semantics."
                },
                "columns": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": {
                            "name": {"type": "string", "description": "Appended output column name."},
                            "path": {"type": "string", "description": "Dot path of the value inside the backend's JSON record, e.g. 'profile.age'."},
                            "type": {"type": "string", "enum": ["utf8", "int64", "float64", "bool"], "description": "Output column type. A value that is absent or not coercible yields a null cell. Defaults to 'utf8'."}
                        },
                        "required": ["name", "path"]
                    },
                    "minItems": 1,
                    "description": "Result columns appended to the batch. Names must not collide with existing input columns."
                },
                "backend": {
                    "type": "object",
                    "description": "Dimension service backend. Currently only 'http' is available.",
                    "oneOf": [
                        {
                            "type": "object",
                            "additionalProperties": false,
                            "properties": {
                                "http": {
                                    "type": "object",
                                    "additionalProperties": false,
                                    "properties": {
                                        "url": {"type": "string", "description": "URL template containing '{{key}}', substituted per lookup key."},
                                        "method": {"type": "string", "description": "HTTP method. Defaults to 'GET'."},
                                        "headers": {"type": "object", "additionalProperties": {"type": "string"}, "description": "Extra HTTP headers."},
                                        "encode_key": {"type": "boolean", "description": "Percent-encode the substituted key (RFC 3986 unreserved set kept literal). Defaults to true."}
                                    },
                                    "required": ["url"]
                                }
                            },
                            "required": ["http"]
                        }
                    ]
                },
                "ttl_ms": {"type": "integer", "description": "Cache TTL for resolved records in milliseconds. Defaults to 60000."},
                "negative_ttl_ms": {"type": "integer", "description": "Cache TTL for misses (negative caching) in milliseconds. Defaults to ttl_ms."},
                "max_entries": {"type": "integer", "description": "Cache capacity bound; expired entries are swept first, then the least recently used are evicted. Defaults to 100000."},
                "concurrency": {"type": "integer", "description": "Maximum in-flight backend requests. Defaults to 8."},
                "timeout_ms": {"type": "integer", "description": "HTTP request timeout in milliseconds. Defaults to 30000."},
                "on_missing": {"type": "string", "enum": ["null", "discard"], "description": "Unresolved key handling: keep the row with null lookup columns (default) or drop the row."},
                "on_backend_error": {"type": "string", "enum": ["fail", "null"], "description": "Backend failure handling: fail the batch (default) or degrade the affected keys to uncached misses."}
            },
            "required": ["key_columns", "columns", "backend"]
        }),
    )
    .with_example(serde_json::json!({
        "key_columns": ["user_id"],
        "columns": [
            {"name": "user_name", "path": "profile.name", "type": "utf8"},
            {"name": "tier", "path": "profile.tier", "type": "utf8"}
        ],
        "backend": {"http": {"url": "http://dim-service/users/{{key}}"}},
        "ttl_ms": 60000
    })))?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum ColumnType {
    #[default]
    Utf8,
    Int64,
    Float64,
    Bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LookupColumnConfig {
    name: String,
    path: String,
    #[serde(rename = "type", default)]
    column_type: ColumnType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum MissingPolicy {
    #[default]
    Null,
    Discard,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum BackendErrorPolicy {
    #[default]
    Fail,
    Null,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BackendConfig {
    Http(HttpBackendConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HttpBackendConfig {
    url: String,
    #[serde(default = "default_method")]
    method: String,
    #[serde(default)]
    headers: Option<HashMap<String, String>>,
    #[serde(default = "default_true")]
    encode_key: bool,
}

fn default_method() -> String {
    "GET".to_string()
}
fn default_true() -> bool {
    true
}
fn default_ttl_ms() -> u64 {
    60_000
}
fn default_max_entries() -> usize {
    100_000
}
fn default_concurrency() -> usize {
    8
}
fn default_timeout_ms() -> u64 {
    30_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LookupConfig {
    key_columns: Vec<String>,
    columns: Vec<LookupColumnConfig>,
    backend: BackendConfig,
    #[serde(default = "default_ttl_ms")]
    ttl_ms: u64,
    #[serde(default)]
    negative_ttl_ms: Option<u64>,
    #[serde(default = "default_max_entries")]
    max_entries: usize,
    #[serde(default = "default_concurrency")]
    concurrency: usize,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default)]
    on_missing: MissingPolicy,
    #[serde(default)]
    on_backend_error: BackendErrorPolicy,
}

/// One resolved record: position-aligned cell per configured column. A
/// `None` cell is an absent path or a value not coercible to the column
/// type.
type LookupRow = Vec<Option<Cell>>;

#[derive(Debug, Clone)]
enum Cell {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

/// Bounded in-process cache with TTL and approximate LRU eviction. Hits and
/// misses (negative entries) share the bound; `get` returning `Some(None)`
/// distinguishes a live negative entry from an absent key.
struct LookupCache {
    ttl: Duration,
    negative_ttl: Duration,
    max_entries: usize,
    inner: Mutex<CacheInner>,
}

struct CacheInner {
    map: HashMap<Arc<str>, CacheEntry>,
    /// LRU access order as (key, generation). Stale pairs (re-accessed or
    /// removed keys) are skipped during eviction and compacted in bulk once
    /// they dominate the live entries — the hot-key path appends on every
    /// hit, so without compaction the deque would grow for process lifetime
    /// when the working set stays below the capacity bound.
    order: VecDeque<(Arc<str>, u64)>,
    /// Earliest-expiry index as (expires_at, key). The front is the globally
    /// earliest-expiring live record: if it is not yet expired, no entry is,
    /// so eviction can go straight to LRU without a full-map scan. Stale
    /// records (key replaced or lazily expired) are skipped during eviction
    /// and compacted like `order`.
    expiry: BinaryHeap<std::cmp::Reverse<(Instant, Arc<str>)>>,
    next_gen: u64,
}

struct CacheEntry {
    expires_at: Instant,
    value: Option<Arc<LookupRow>>,
    gen: u64,
}

impl LookupCache {
    fn new(ttl: Duration, negative_ttl: Duration, max_entries: usize) -> Self {
        Self {
            ttl,
            negative_ttl,
            max_entries,
            inner: Mutex::new(CacheInner {
                map: HashMap::new(),
                order: VecDeque::new(),
                expiry: BinaryHeap::new(),
                next_gen: 0,
            }),
        }
    }

    fn get(&self, key: &str) -> Option<Option<Arc<LookupRow>>> {
        let inner = &mut *self.inner.lock().unwrap();
        let CacheInner {
            map,
            order,
            expiry,
            next_gen,
        } = inner;
        let expired = map
            .get(key)
            .map(|entry| Instant::now() >= entry.expires_at)
            .unwrap_or(false);
        if expired {
            map.remove(key);
            return None;
        }
        // Clone the map's canonical key so the order pair does not allocate
        // a fresh Arc on every hit.
        let canonical = map.get_key_value(key).map(|(k, _)| k.clone())?;
        let (value, gen) = {
            let entry = map.get_mut(key).expect("checked present above");
            entry.gen = *next_gen;
            *next_gen += 1;
            (entry.value.clone(), entry.gen)
        };
        order.push_back((canonical, gen));
        Self::compact(map, order, expiry);
        Some(value)
    }

    fn insert(&self, key: Arc<str>, value: Option<Arc<LookupRow>>) {
        let ttl = if value.is_some() {
            self.ttl
        } else {
            self.negative_ttl
        };
        let inner = &mut *self.inner.lock().unwrap();
        let CacheInner {
            map,
            order,
            expiry,
            next_gen,
        } = inner;
        let gen = *next_gen;
        *next_gen += 1;
        order.push_back((key.clone(), gen));
        let expires_at = Instant::now() + ttl;
        expiry.push(std::cmp::Reverse((expires_at, key.clone())));
        map.insert(
            key,
            CacheEntry {
                expires_at,
                value,
                gen,
            },
        );
        while map.len() > self.max_entries {
            if !Self::evict_one(map, order, expiry) {
                break;
            }
        }
        Self::compact(map, order, expiry);
    }

    /// Removes one entry per the bound contract: expired entries first, the
    /// least recently used otherwise. Returns false when nothing is
    /// removable.
    fn evict_one(
        map: &mut HashMap<Arc<str>, CacheEntry>,
        order: &mut VecDeque<(Arc<str>, u64)>,
        expiry: &mut BinaryHeap<std::cmp::Reverse<(Instant, Arc<str>)>>,
    ) -> bool {
        // Drop stale heap records from the front until the top reflects a
        // live entry.
        loop {
            let front = expiry
                .peek()
                .map(|std::cmp::Reverse((instant, key))| (*instant, key.clone()));
            let Some((instant, key)) = front else {
                break;
            };
            if map
                .get(&key)
                .is_some_and(|entry| entry.expires_at == instant)
            {
                break;
            }
            expiry.pop();
        }
        // The heap front is the earliest-expiring live entry: if it is not
        // expired, nothing in the map is, so expired-first is satisfied by
        // going straight to LRU.
        let front = expiry
            .peek()
            .map(|std::cmp::Reverse((instant, key))| (*instant, key.clone()));
        if let Some((instant, key)) = front {
            if Instant::now() >= instant {
                expiry.pop();
                map.remove(&key);
                return true;
            }
        }
        while let Some((key, gen)) = order.pop_front() {
            if map.get(&key).is_some_and(|entry| entry.gen == gen) {
                map.remove(&key);
                return true;
            }
        }
        false
    }

    /// Drops stale index records once they dominate the live entries so
    /// both indices stay a constant factor above the map size.
    fn compact(
        map: &HashMap<Arc<str>, CacheEntry>,
        order: &mut VecDeque<(Arc<str>, u64)>,
        expiry: &mut BinaryHeap<std::cmp::Reverse<(Instant, Arc<str>)>>,
    ) {
        if order.len() > 2 * map.len() + 64 {
            order.retain(|(key, gen)| map.get(key).is_some_and(|entry| entry.gen == *gen));
        }
        if expiry.len() > 2 * map.len() + 64 {
            expiry.retain(|std::cmp::Reverse((instant, key))| {
                map.get(key)
                    .is_some_and(|entry| entry.expires_at == *instant)
            });
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.lock().unwrap().map.len()
    }

    #[cfg(test)]
    fn order_len(&self) -> usize {
        self.inner.lock().unwrap().order.len()
    }

    #[cfg(test)]
    fn expiry_len(&self) -> usize {
        self.inner.lock().unwrap().expiry.len()
    }
}

/// A dimension-table backend: resolves one key to an optional JSON record.
/// `lookup_many` fans keys out with bounded, order-preserving concurrency
/// and reports one outcome per key, position-aligned with `keys`; backends
/// with a native batch API override it.
#[async_trait]
trait LookupBackend: Send + Sync {
    async fn lookup(&self, key: &str) -> Result<Option<Value>, Error>;

    async fn lookup_many(
        &self,
        keys: &[String],
        concurrency: usize,
    ) -> Vec<Result<Option<Value>, Error>> {
        // Materialize the per-key futures first: async_trait boxes each
        // call with concrete lifetimes, which the closure above would not
        // generalize.
        let calls: Vec<_> = keys.iter().map(|key| self.lookup(key)).collect();
        futures_util::stream::iter(calls)
            .buffered(concurrency)
            .collect::<Vec<Result<Option<Value>, Error>>>()
            .await
    }
}

struct HttpBackend {
    config: HttpBackendConfig,
    client: Client,
}

/// RFC 3986 unreserved set stays literal in substituted keys; everything
/// else is percent-encoded so a key value cannot alter the URL structure.
const KEY_PERCENT_SET: &AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

#[async_trait]
impl LookupBackend for HttpBackend {
    async fn lookup(&self, key: &str) -> Result<Option<Value>, Error> {
        let substituted = if self.config.encode_key {
            percent_encoding::utf8_percent_encode(key, KEY_PERCENT_SET).to_string()
        } else {
            key.to_string()
        };
        let url = self.config.url.replace("{{key}}", &substituted);
        let mut request = match self.config.method.to_ascii_uppercase().as_str() {
            "GET" => self.client.get(&url),
            "POST" => self.client.post(&url),
            "PUT" => self.client.put(&url),
            "DELETE" => self.client.delete(&url),
            other => {
                return Err(Error::Config(format!(
                    "lookup processor: unsupported http method '{other}'"
                )))
            }
        };
        if let Some(headers) = &self.config.headers {
            for (name, value) in headers {
                request = request.header(name, value);
            }
        }
        let response = request.send().await.map_err(|e| {
            Error::Process(format!("lookup processor: request for key failed: {e}"))
        })?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| Error::Process(format!("lookup processor: response read failed: {e}")))?;
        if !status.is_success() {
            // 404 is the conventional "no record" signal for a dimension
            // lookup; it participates in negative caching. Any other
            // non-2xx is a backend error.
            if status == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }
            return Err(Error::Process(format!(
                "lookup processor: backend returned {status} for key: {}",
                truncate_body(&body)
            )));
        }
        match serde_json::from_str::<Value>(&body) {
            Ok(record @ Value::Object(_)) => Ok(Some(record)),
            Ok(Value::Null) => Ok(None),
            Ok(_) => Err(Error::Process(
                "lookup processor: backend response is not a JSON object".to_string(),
            )),
            Err(e) => Err(Error::Process(format!(
                "lookup processor: backend response parse failed: {e}"
            ))),
        }
    }
}

/// Navigates a dot-separated path inside a JSON record. Numeric segments
/// index into arrays.
fn value_at_path<'a>(record: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = record;
    for segment in path.split('.') {
        current = match current {
            Value::Object(map) => map.get(segment)?,
            Value::Array(items) => items.get(segment.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(current)
}

/// Coerces a JSON value to the configured column type; anything not
/// coercible (including a number into `utf8`) becomes a null cell.
fn coerce(value: &Value, column_type: ColumnType) -> Option<Cell> {
    match (column_type, value) {
        (ColumnType::Utf8, Value::String(s)) => Some(Cell::Str(s.clone())),
        (ColumnType::Int64, Value::Number(n)) => n.as_i64().map(Cell::Int),
        (ColumnType::Float64, Value::Number(n)) => n.as_f64().map(Cell::Float),
        (ColumnType::Bool, Value::Bool(b)) => Some(Cell::Bool(*b)),
        _ => None,
    }
}

struct LookupProcessor {
    config: LookupConfig,
    backend: Arc<dyn LookupBackend>,
    cache: Arc<LookupCache>,
}

#[async_trait]
impl Processor for LookupProcessor {
    async fn process(&self, msg_batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        let rows = msg_batch.num_rows();
        if rows == 0 {
            return Ok(ProcessResult::None);
        }

        let keys = self.extract_keys(&msg_batch)?;
        self.reject_column_collisions(&msg_batch)?;

        // Fill from cache, collect the rest in first-appearance order.
        let mut resolved: HashMap<Arc<str>, Option<Arc<LookupRow>>> = HashMap::new();
        let mut pending: Vec<Arc<str>> = Vec::new();
        let mut seen: HashSet<Arc<str>> = HashSet::new();
        for key in keys.iter().flatten() {
            let key: Arc<str> = Arc::from(key.as_str());
            if !seen.insert(key.clone()) {
                continue;
            }
            match self.cache.get(&key) {
                Some(value) => {
                    resolved.insert(key, value);
                }
                None => pending.push(key),
            }
        }

        if !pending.is_empty() {
            let keys_ref: Vec<String> = pending.iter().map(|key| key.to_string()).collect();
            let outcomes = self
                .backend
                .lookup_many(&keys_ref, self.config.concurrency)
                .await;
            for (key, outcome) in pending.into_iter().zip(outcomes) {
                match outcome {
                    Ok(Some(record)) => {
                        let row = self.map_record(&record)?;
                        self.cache.insert(key.clone(), Some(Arc::new(row.clone())));
                        resolved.insert(key, Some(Arc::new(row)));
                    }
                    Ok(None) => {
                        self.cache.insert(key.clone(), None);
                        resolved.insert(key, None);
                    }
                    Err(e) => match self.config.on_backend_error {
                        BackendErrorPolicy::Fail => return Err(e),
                        BackendErrorPolicy::Null => {
                            warn!("lookup processor: degrading key to null: {e}");
                            resolved.insert(key, None);
                        }
                    },
                }
            }
        }

        // Per-row resolution: a null key or an uncached/unresolved key is a
        // miss for that row.
        let mut row_records: Vec<Option<&Arc<LookupRow>>> = Vec::with_capacity(rows);
        for key in keys.iter() {
            match key.as_ref().and_then(|key| resolved.get(key.as_str())) {
                Some(record) => row_records.push(record.as_ref()),
                None => row_records.push(None),
            }
        }

        let keep: Vec<usize> = match self.config.on_missing {
            MissingPolicy::Null => (0..rows).collect(),
            MissingPolicy::Discard => (0..rows)
                .filter(|row| row_records[*row].is_some())
                .collect(),
        };

        let mut fields: Vec<Arc<Field>> = msg_batch.schema().fields().iter().cloned().collect();
        let mut columns: Vec<ArrayRef> =
            Vec::with_capacity(fields.len() + self.config.columns.len());
        for index in 0..msg_batch.num_columns() {
            let column = msg_batch.column(index).clone();
            if keep.len() != rows {
                let indices =
                    UInt32Array::from(keep.iter().map(|row| *row as u32).collect::<Vec<_>>());
                let taken = take(&column, &indices, None).map_err(|e| {
                    Error::Process(format!("lookup processor: row selection failed: {e}"))
                })?;
                columns.push(taken);
            } else {
                columns.push(column);
            }
        }
        for (column_index, column_config) in self.config.columns.iter().enumerate() {
            let data_type = match column_config.column_type {
                ColumnType::Utf8 => DataType::Utf8,
                ColumnType::Int64 => DataType::Int64,
                ColumnType::Float64 => DataType::Float64,
                ColumnType::Bool => DataType::Boolean,
            };
            fields.push(Arc::new(Field::new(
                column_config.name.clone(),
                data_type,
                true,
            )));
            let cells: Vec<Option<&Cell>> = keep
                .iter()
                .map(|row| {
                    row_records[*row]
                        .as_ref()
                        .and_then(|record| record.get(column_index))
                        .and_then(|cell| cell.as_ref())
                })
                .collect();
            columns.push(build_column(&cells, column_config.column_type));
        }

        let record_batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|e| Error::Process(format!("lookup processor: batch rebuild failed: {e}")))?;
        Ok(ProcessResult::Single(Arc::new(MessageBatch::new_arrow(
            record_batch,
        ))))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl LookupProcessor {
    /// Per-row normalized key; `None` for a null cell. Column types outside
    /// the supported set fail here, where the schema is first known.
    fn extract_keys(&self, batch: &MessageBatch) -> Result<Vec<Option<String>>, Error> {
        let mut per_column: Vec<Vec<Option<String>>> =
            Vec::with_capacity(self.config.key_columns.len());
        for key_column in &self.config.key_columns {
            let index = batch
                .schema()
                .fields()
                .iter()
                .position(|field| field.name() == key_column)
                .ok_or_else(|| {
                    Error::Process(format!(
                        "lookup processor: key column '{key_column}' not found"
                    ))
                })?;
            let column = batch.column(index);
            let values: Vec<Option<String>> = match column.data_type() {
                DataType::Utf8 => column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8 column")
                    .iter()
                    .map(|value| value.map(str::to_string))
                    .collect(),
                DataType::LargeUtf8 => column
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
                    .expect("large utf8 column")
                    .iter()
                    .map(|value| value.map(str::to_string))
                    .collect(),
                DataType::Int64 => column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64 column")
                    .iter()
                    .map(|value| value.map(|v| v.to_string()))
                    .collect(),
                DataType::UInt64 => column
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("uint64 column")
                    .iter()
                    .map(|value| value.map(|v| v.to_string()))
                    .collect(),
                DataType::Boolean => column
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("boolean column")
                    .iter()
                    .map(|value| value.map(|v| v.to_string()))
                    .collect(),
                other => {
                    return Err(Error::Process(format!(
                        "lookup processor: key column '{key_column}' has unsupported type {other:?}; supported: Utf8, Int64, UInt64, Boolean"
                    )))
                }
            };
            per_column.push(values);
        }

        let rows = batch.num_rows();
        let separator = '\u{1f}';
        let mut keys = Vec::with_capacity(rows);
        for row in 0..rows {
            let mut key = String::new();
            let mut is_null = false;
            for (column_index, values) in per_column.iter().enumerate() {
                if column_index > 0 {
                    key.push(separator);
                }
                match &values[row] {
                    Some(value) => key.push_str(value),
                    None => {
                        is_null = true;
                        break;
                    }
                }
            }
            keys.push(if is_null { None } else { Some(key) });
        }
        Ok(keys)
    }

    fn reject_column_collisions(&self, batch: &MessageBatch) -> Result<(), Error> {
        for column_config in &self.config.columns {
            if batch
                .schema()
                .fields()
                .iter()
                .any(|field| field.name() == &column_config.name)
            {
                return Err(Error::Process(format!(
                    "lookup processor: lookup column '{}' collides with an existing input column",
                    column_config.name
                )));
            }
        }
        Ok(())
    }

    /// Applies the configured column mapping once per record; the mapped
    /// fragment (not the raw response) is what enters the cache.
    fn map_record(&self, record: &Value) -> Result<LookupRow, Error> {
        Ok(self
            .config
            .columns
            .iter()
            .map(|column| {
                value_at_path(record, &column.path)
                    .and_then(|value| coerce(value, column.column_type))
            })
            .collect())
    }
}

fn build_column(cells: &[Option<&Cell>], column_type: ColumnType) -> ArrayRef {
    match column_type {
        ColumnType::Utf8 => Arc::new(StringArray::from(
            cells
                .iter()
                .map(|cell| match cell {
                    Some(Cell::Str(value)) => Some(value.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        ColumnType::Int64 => Arc::new(Int64Array::from(
            cells
                .iter()
                .map(|cell| match cell {
                    Some(Cell::Int(value)) => Some(*value),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        ColumnType::Float64 => Arc::new(Float64Array::from(
            cells
                .iter()
                .map(|cell| match cell {
                    Some(Cell::Float(value)) => Some(*value),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        ColumnType::Bool => Arc::new(BooleanArray::from(
            cells
                .iter()
                .map(|cell| match cell {
                    Some(Cell::Bool(value)) => Some(*value),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
    }
}

struct LookupProcessorBuilder;

impl ProcessorBuilder for LookupProcessorBuilder {
    fn build(
        &self,
        _name: Option<&str>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        let config: LookupConfig = parse_config(config, "lookup processor")?;
        if config.key_columns.is_empty() {
            return Err(Error::Config(
                "lookup processor: 'key_columns' must not be empty".to_string(),
            ));
        }
        if config.columns.is_empty() {
            return Err(Error::Config(
                "lookup processor: 'columns' must not be empty".to_string(),
            ));
        }
        for column in &config.columns {
            if column.name.trim().is_empty() || column.path.trim().is_empty() {
                return Err(Error::Config(
                    "lookup processor: each column needs non-empty 'name' and 'path'".to_string(),
                ));
            }
        }
        if config.concurrency == 0 {
            return Err(Error::Config(
                "lookup processor: 'concurrency' must be at least 1".to_string(),
            ));
        }
        if config.max_entries == 0 {
            return Err(Error::Config(
                "lookup processor: 'max_entries' must be at least 1".to_string(),
            ));
        }
        if config.timeout_ms == 0 {
            return Err(Error::Config(
                "lookup processor: 'timeout_ms' must be at least 1".to_string(),
            ));
        }
        let backend: Arc<dyn LookupBackend> = match &config.backend {
            BackendConfig::Http(http) => {
                if http.url.trim().is_empty() {
                    return Err(Error::Config(
                        "lookup processor: http backend 'url' must not be empty".to_string(),
                    ));
                }
                if !http.url.contains("{{key}}") {
                    return Err(Error::Config(
                        "lookup processor: http backend 'url' must contain the '{{key}}' placeholder"
                            .to_string(),
                    ));
                }
                let probe = http.url.replace("{{key}}", "probe");
                // Loopback endpoints (local dimension services, tests)
                // bypass a system proxy — proxying localhost is never what a
                // user means.
                let is_loopback = reqwest::Url::parse(&probe)
                    .ok()
                    .and_then(|url| url.host_str().map(|host| host.to_ascii_lowercase()))
                    .map(|host| {
                        host == "localhost"
                            || host == "127.0.0.1"
                            || host == "::1"
                            || host == "[::1]"
                    })
                    .unwrap_or(false);
                let mut builder =
                    Client::builder().timeout(Duration::from_millis(config.timeout_ms));
                if is_loopback {
                    builder = builder.no_proxy();
                }
                let client = builder
                    .build()
                    .map_err(|e| Error::Config(format!("Unable to create HTTP client: {e}")))?;
                Arc::new(HttpBackend {
                    config: http.clone(),
                    client,
                })
            }
        };
        let cache = Arc::new(LookupCache::new(
            Duration::from_millis(config.ttl_ms),
            Duration::from_millis(config.negative_ttl_ms.unwrap_or(config.ttl_ms)),
            config.max_entries,
        ));
        Ok(Arc::new(LookupProcessor {
            config,
            backend,
            cache,
        }))
    }
}

fn truncate_body(body: &str) -> &str {
    match body.char_indices().nth(512) {
        Some((index, _)) => &body[..index],
        None => body,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    pub(crate) fn test_resource() -> Resource {
        Resource {
            temporary: Default::default(),
            input_names: RefCell::new(Default::default()),
        }
    }

    /// Minimal in-process HTTP server: one request per connection, canned
    /// status/body per request, logs every request (path and head) with an
    /// in-flight watermark.
    struct MockDimService {
        addr: std::net::SocketAddr,
        requests: Arc<std::sync::Mutex<Vec<(String, String)>>>,
        max_in_flight: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl MockDimService {
        fn spawn<F>(handler: F) -> Self
        where
            F: Fn(&str) -> (u16, String) + Send + Sync + 'static,
        {
            let handler = Arc::new(handler);
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let max_in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let tracker_in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let tracker_max = max_in_flight.clone();
            let request_log = requests.clone();
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let mut stream = match stream {
                        Ok(stream) => stream,
                        Err(_) => break,
                    };
                    let handler = handler.clone();
                    let request_log = request_log.clone();
                    let tracker_in_flight = tracker_in_flight.clone();
                    let tracker_max = tracker_max.clone();
                    std::thread::spawn(move || {
                        let now_in_flight =
                            tracker_in_flight.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                        tracker_max.fetch_max(now_in_flight, std::sync::atomic::Ordering::SeqCst);

                        let mut buffer = Vec::new();
                        let mut byte = [0u8; 1];
                        loop {
                            use std::io::Read;
                            if stream.read_exact(&mut byte).is_err() {
                                break;
                            }
                            buffer.push(byte[0]);
                            if buffer.ends_with(b"\r\n\r\n") {
                                break;
                            }
                        }
                        let head = String::from_utf8_lossy(&buffer).to_string();
                        let path = head
                            .split_whitespace()
                            .nth(1)
                            .unwrap_or_default()
                            .to_string();
                        request_log
                            .lock()
                            .unwrap()
                            .push((path.clone(), head.clone()));

                        let (status, response_body) = handler(&path);
                        // Decrement before the response is written: with
                        // `Connection: close` the client opens its next
                        // request's connection as soon as it sees the bytes.
                        tracker_in_flight.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        let response = format!(
                            "HTTP/1.1 {status} MOCK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
                            response_body.len()
                        );
                        use std::io::Write;
                        let _ = stream.write_all(response.as_bytes());
                        let _ = stream.flush();
                    });
                }
            });
            Self {
                addr,
                requests,
                max_in_flight,
            }
        }

        fn requests(&self) -> Vec<String> {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .map(|(path, _)| path.clone())
                .collect()
        }

        fn request_heads(&self) -> Vec<String> {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .map(|(_, head)| head.clone())
                .collect()
        }

        fn max_in_flight(&self) -> usize {
            self.max_in_flight.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    fn record_body(name: &str, age: i64) -> String {
        serde_json::json!({"profile": {"name": name, "age": age}}).to_string()
    }

    fn build_processor(config: Value) -> Arc<dyn Processor> {
        LookupProcessorBuilder
            .build(None, &Some(config), &test_resource())
            .unwrap()
    }

    fn base_config(addr: std::net::SocketAddr, extra: Value) -> Value {
        let mut config = serde_json::json!({
            "key_columns": ["user_id"],
            "columns": [
                {"name": "user_name", "path": "profile.name", "type": "utf8"},
                {"name": "age", "path": "profile.age", "type": "int64"}
            ],
            "backend": {"http": {"url": format!("http://{addr}/users/{{{{key}}}}")}},
        });
        let obj = config.as_object_mut().unwrap();
        for (key, value) in extra.as_object().unwrap() {
            obj.insert(key.clone(), value.clone());
        }
        config
    }

    fn users_batch(values: Vec<Option<i64>>) -> MessageBatchRef {
        users_batch_with_meta(values, false)
    }

    fn users_batch_with_meta(values: Vec<Option<i64>>, with_meta: bool) -> MessageBatchRef {
        let mut fields = vec![Field::new("user_id", DataType::Int64, true)];
        let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(values))];
        if with_meta {
            fields.push(Field::new("__meta_source", DataType::Utf8, true));
            columns.push(Arc::new(StringArray::from(vec!["kafka"; columns[0].len()])));
        }
        let schema = Arc::new(Schema::new(fields));
        Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(schema, columns).unwrap(),
        ))
    }

    fn column_by_name<'a>(batch: &'a MessageBatchRef, name: &str) -> &'a ArrayRef {
        let index = batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == name)
            .unwrap();
        batch.column(index)
    }

    async fn process_single(
        processor: &Arc<dyn Processor>,
        batch: MessageBatchRef,
    ) -> MessageBatchRef {
        match processor.process(batch).await.unwrap() {
            ProcessResult::Single(output) => output,
            other => panic!("expected single result, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn enriches_rows_aligned_and_preserves_metadata() {
        let mock = MockDimService::spawn(|path| {
            let key = path.rsplit('/').next().unwrap();
            (200, record_body(&format!("name-{key}"), key.len() as i64))
        });
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let batch = users_batch_with_meta(vec![Some(1), Some(2), Some(1)], true);
        let output = process_single(&processor, batch).await;

        assert_eq!(output.num_rows(), 3);
        let meta = column_by_name(&output, "__meta_source")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..3 {
            assert_eq!(meta.value(row), "kafka", "metadata must be untouched");
        }
        let names = column_by_name(&output, "user_name")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ages = column_by_name(&output, "age")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(names.value(0), "name-1");
        assert_eq!(ages.value(0), 1);
        assert_eq!(names.value(1), "name-2");
        assert_eq!(ages.value(1), 1);
        assert_eq!(names.value(2), "name-1", "duplicate key shares the record");
    }

    #[tokio::test]
    async fn composite_key_joined_with_unit_separator() {
        let mock = MockDimService::spawn(|_path| (200, record_body("x", 1)));
        let mut config = base_config(mock.addr, serde_json::json!({}));
        config["key_columns"] = serde_json::json!(["tenant_id", "user_id"]);
        config["columns"] = serde_json::json!([{"name": "user_name", "path": "profile.name"}]);
        let processor = build_processor(config);

        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Int64, true),
            Field::new("user_id", DataType::Int64, true),
        ]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![42])),
                    Arc::new(Int64Array::from(vec![7])),
                ],
            )
            .unwrap(),
        ));
        process_single(&processor, batch).await;
        let path = &mock.requests()[0];
        // U+001F percent-encoded inside the key segment.
        assert!(path.contains("42%1F7"), "{path}");
    }

    #[tokio::test]
    async fn unsupported_key_type_errors_with_column_and_type() {
        let mock = MockDimService::spawn(|_path| (200, record_body("x", 1)));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "user_id",
            DataType::Float64,
            true,
        )]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![1.5]))]).unwrap(),
        ));
        let err = processor.process(batch).await.unwrap_err().to_string();
        assert!(err.contains("user_id"), "{err}");
        assert!(err.contains("Float64"), "{err}");
    }

    #[tokio::test]
    async fn repeated_key_within_ttl_hits_cache_once() {
        let mock = MockDimService::spawn(|path| {
            let key = path.rsplit('/').next().unwrap();
            (200, record_body(&format!("name-{key}"), 1))
        });
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        process_single(&processor, users_batch(vec![Some(5)])).await;
        process_single(&processor, users_batch(vec![Some(5), Some(5)])).await;
        assert_eq!(
            mock.requests().len(),
            1,
            "second batch must resolve from cache"
        );
    }

    #[tokio::test]
    async fn miss_is_negatively_cached_within_negative_ttl() {
        let mock = MockDimService::spawn(|_path| (404, "{}".to_string()));
        let processor = build_processor(base_config(
            mock.addr,
            serde_json::json!({"negative_ttl_ms": 60000}),
        ));
        let output = process_single(&processor, users_batch(vec![Some(404)])).await;
        let names = column_by_name(&output, "user_name")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(names.is_null(0), "missing key yields null columns");

        process_single(&processor, users_batch(vec![Some(404)])).await;
        assert_eq!(mock.requests().len(), 1, "miss must be negatively cached");
    }

    #[tokio::test]
    async fn expired_entry_re_resolves() {
        let mock = MockDimService::spawn(|_path| (200, record_body("x", 1)));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({"ttl_ms": 20})));
        process_single(&processor, users_batch(vec![Some(9)])).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        process_single(&processor, users_batch(vec![Some(9)])).await;
        assert_eq!(mock.requests().len(), 2, "expired entry must re-resolve");
    }

    #[test]
    fn cache_bounded_under_distinct_key_flood() {
        let cache = LookupCache::new(Duration::from_secs(60), Duration::from_secs(60), 10);
        for i in 0..100 {
            cache.insert(
                Arc::from(format!("key-{i}").as_str()),
                Some(Arc::new(vec![])),
            );
        }
        assert!(
            cache.len() <= 10,
            "cache exceeded capacity bound: {}",
            cache.len()
        );
    }

    #[test]
    fn hot_key_access_keeps_order_deque_bounded() {
        let cache = LookupCache::new(Duration::from_secs(60), Duration::from_secs(60), 1000);
        cache.insert(Arc::from("hot"), Some(Arc::new(vec![])));
        for _ in 0..1000 {
            assert!(cache.get("hot").is_some(), "hot key must stay cached");
        }
        // Every hit appends an order pair; compaction must keep the deque
        // within a constant factor of the live entry count.
        assert!(
            cache.order_len() <= 2 + 64,
            "order deque grew unbounded: {}",
            cache.order_len()
        );
    }

    #[test]
    fn repeated_reinsertion_keeps_expiry_index_bounded() {
        let cache = LookupCache::new(Duration::from_secs(60), Duration::from_secs(60), 1000);
        for _ in 0..500 {
            cache.insert(Arc::from("same"), Some(Arc::new(vec![])));
        }
        // Each insert pushes an expiry record; the replaced record is stale
        // and compaction must keep the heap near the live entry count.
        assert!(
            cache.expiry_len() <= 2 + 64,
            "expiry heap grew unbounded: {}",
            cache.expiry_len()
        );
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn expired_entry_evicted_before_lru_live_entry() {
        // k1 gets the short negative TTL; k2..k4 the long positive one.
        // After k1 lapses, inserting k5 over capacity must evict the
        // expired k1 — not the least-recently-used live k2.
        let cache = LookupCache::new(Duration::from_secs(60), Duration::from_millis(20), 4);
        cache.insert(Arc::from("k1"), None);
        cache.insert(Arc::from("k2"), Some(Arc::new(vec![])));
        cache.insert(Arc::from("k3"), Some(Arc::new(vec![])));
        cache.insert(Arc::from("k4"), Some(Arc::new(vec![])));
        std::thread::sleep(Duration::from_millis(50));
        cache.insert(Arc::from("k5"), Some(Arc::new(vec![])));
        assert!(
            cache.get("k2").is_some(),
            "expired k1 must be evicted before the live LRU entry k2"
        );
        assert!(cache.get("k1").is_none(), "k1 must be gone");
        assert!(cache.len() <= 4);
    }

    #[tokio::test]
    async fn duplicate_keys_collapse_to_one_request_per_key() {
        let mock = MockDimService::spawn(|path| {
            let key = path.rsplit('/').next().unwrap();
            (200, record_body(&format!("n{key}"), key.len() as i64))
        });
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let values: Vec<Option<i64>> = (0..1000).map(|row| Some(row % 3)).collect();
        let output = process_single(&processor, users_batch(values)).await;
        assert_eq!(mock.requests().len(), 3, "distinct keys only");
        let names = column_by_name(&output, "user_name")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..output.num_rows() {
            let expected = format!("n{}", row % 3);
            assert_eq!(names.value(row), expected, "row alignment must hold");
        }
    }

    #[tokio::test]
    async fn in_flight_requests_respect_concurrency_cap() {
        let mock = MockDimService::spawn(|path| {
            std::thread::sleep(Duration::from_millis(30));
            let key = path.rsplit('/').next().unwrap();
            (200, record_body(key, 1))
        });
        let processor = build_processor(base_config(
            mock.addr,
            serde_json::json!({"concurrency": 2}),
        ));
        let values: Vec<Option<i64>> = (0..12).map(Some).collect();
        process_single(&processor, users_batch(values)).await;
        let max = mock.max_in_flight();
        assert!(max <= 2, "in-flight requests exceeded the cap: {max}");
    }

    #[tokio::test]
    async fn typed_extraction_absent_path_and_bad_coercion_yield_null() {
        let mock = MockDimService::spawn(|path| {
            let key = path.rsplit('/').next().unwrap().to_string();
            if key == "1" {
                (200, record_body("alice", 30))
            } else {
                // Key 2: age is a non-coercible string, tier path absent.
                (
                    200,
                    serde_json::json!({"profile": {"name": 7, "age": "abc"}}).to_string(),
                )
            }
        });
        let mut config = base_config(mock.addr, serde_json::json!({}));
        config["columns"] = serde_json::json!([
            {"name": "user_name", "path": "profile.name", "type": "utf8"},
            {"name": "age", "path": "profile.age", "type": "int64"},
            {"name": "tier", "path": "profile.tier", "type": "utf8"},
            {"name": "score", "path": "profile.score", "type": "float64"},
            {"name": "active", "path": "profile.active", "type": "bool"}
        ]);
        let processor = build_processor(config);
        let output = process_single(&processor, users_batch(vec![Some(1), Some(2)])).await;

        let names = column_by_name(&output, "user_name")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ages = column_by_name(&output, "age")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let tiers = column_by_name(&output, "tier")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let scores = column_by_name(&output, "score")
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let active = column_by_name(&output, "active")
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        // Row 0 fully resolved.
        assert_eq!(names.value(0), "alice");
        assert_eq!(ages.value(0), 30);
        assert!(tiers.is_null(0));
        assert!(scores.is_null(0));
        assert!(active.is_null(0));
        // Row 1: number into utf8 and string into int64 are nulls.
        assert!(names.is_null(1));
        assert!(ages.is_null(1));
        assert!(tiers.is_null(1));
        assert_eq!(
            output
                .schema()
                .field_with_name("score")
                .unwrap()
                .data_type(),
            &DataType::Float64,
            "column types are fixed by configuration"
        );
    }

    #[tokio::test]
    async fn non_2xx_fails_by_default() {
        let mock = MockDimService::spawn(|_path| (500, "boom".to_string()));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let err = processor
            .process(users_batch(vec![Some(1)]))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("500"), "{err}");
    }

    #[tokio::test]
    async fn degrade_to_null_does_not_cache_the_error() {
        let mock = MockDimService::spawn(|_path| (500, "boom".to_string()));
        let processor = build_processor(base_config(
            mock.addr,
            serde_json::json!({"on_backend_error": "null"}),
        ));
        let output = process_single(&processor, users_batch(vec![Some(1)])).await;
        let names = column_by_name(&output, "user_name")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(names.is_null(0));
        // The error must not have been cached: once the backend recovers,
        // the next batch re-requests and succeeds.
        let output = process_single(&processor, users_batch(vec![Some(1)])).await;
        let names = column_by_name(&output, "user_name")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(names.is_null(0));
        assert_eq!(mock.requests().len(), 2, "error must not be cached");
    }

    #[tokio::test]
    async fn missing_key_keeps_row_with_nulls_and_discard_drops_it() {
        let mock = MockDimService::spawn(|_path| (404, "{}".to_string()));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let output = process_single(&processor, users_batch(vec![Some(1), Some(2), Some(3)])).await;
        assert_eq!(output.num_rows(), 3, "default keeps the row with nulls");

        let mock = MockDimService::spawn(|_path| (404, "{}".to_string()));
        let processor = build_processor(base_config(
            mock.addr,
            serde_json::json!({"on_missing": "discard"}),
        ));
        let output = process_single(&processor, users_batch(vec![Some(1), Some(2), Some(3)])).await;
        assert_eq!(output.num_rows(), 0, "all rows missed");
    }

    #[tokio::test]
    async fn discard_keeps_only_resolved_rows() {
        let mock = MockDimService::spawn(|path| {
            let key = path.rsplit('/').next().unwrap().to_string();
            if key == "2" {
                (404, "{}".to_string())
            } else {
                (200, record_body(&format!("n{key}"), 1))
            }
        });
        let processor = build_processor(base_config(
            mock.addr,
            serde_json::json!({"on_missing": "discard"}),
        ));
        let output = process_single(&processor, users_batch(vec![Some(1), Some(2), Some(3)])).await;
        assert_eq!(output.num_rows(), 2);
        let ids = column_by_name(&output, "user_id")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 3);
    }

    #[tokio::test]
    async fn lookup_column_colliding_with_input_column_errors() {
        let mock = MockDimService::spawn(|_path| (200, record_body("x", 1)));
        let mut config = base_config(mock.addr, serde_json::json!({}));
        config["columns"] = serde_json::json!([{"name": "user_id", "path": "profile.name"}]);
        let processor = build_processor(config);
        let err = processor
            .process(users_batch(vec![Some(1)]))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("collides"), "{err}");
    }

    #[tokio::test]
    async fn null_key_cell_follows_missing_semantics_without_request() {
        let mock = MockDimService::spawn(|path| {
            let key = path.rsplit('/').next().unwrap().to_string();
            (200, record_body(&format!("n{key}"), 1))
        });
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let output = process_single(&processor, users_batch(vec![None, Some(4)])).await;
        assert_eq!(output.num_rows(), 2);
        let names = column_by_name(&output, "user_name")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(names.is_null(0), "null key row gets null columns");
        assert_eq!(names.value(1), "n4");
        assert_eq!(
            mock.requests().len(),
            1,
            "no backend request for the null key"
        );
    }

    #[tokio::test]
    async fn encode_key_false_leaves_key_literal() {
        let mock = MockDimService::spawn(|_path| (200, record_body("x", 1)));
        let processor = build_processor(base_config(
            mock.addr,
            serde_json::json!({"backend": {"http": {
                "url": format!("http://{}/users/{{{{key}}}}", mock.addr),
                "encode_key": false
            }}}),
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "user_id",
            DataType::Utf8,
            true,
        )]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a/b"]))]).unwrap(),
        ));
        process_single(&processor, batch).await;
        assert!(
            mock.requests()[0].ends_with("/users/a/b"),
            "{:?}",
            mock.requests()[0]
        );
    }

    #[tokio::test]
    async fn headers_are_sent() {
        let mock = MockDimService::spawn(|_path| (200, record_body("x", 1)));
        let processor = build_processor(base_config(
            mock.addr,
            serde_json::json!({"backend": {"http": {
                "url": format!("http://{}/users/{{{{key}}}}", mock.addr),
                "headers": {"X-Tenant": "acme"}
            }}}),
        ));
        process_single(&processor, users_batch(vec![Some(1)])).await;
        let head = &mock.request_heads()[0];
        assert!(
            head.to_ascii_lowercase().contains("x-tenant: acme"),
            "{head}"
        );
    }

    #[test]
    fn invalid_configs_rejected() {
        let addr = "127.0.0.1:1";
        for (bad, why) in [
            (
                serde_json::json!({"columns": [{"name": "n", "path": "p"}], "backend": {"http": {"url": format!("http://{addr}/u/{{{{key}}}}")}}}),
                "missing key_columns",
            ),
            (
                serde_json::json!({"key_columns": ["k"], "backend": {"http": {"url": format!("http://{addr}/u/{{{{key}}}}")}}}),
                "missing columns",
            ),
            (
                serde_json::json!({"key_columns": ["k"], "columns": [{"name": "n", "path": "p"}]}),
                "missing backend",
            ),
            (
                serde_json::json!({"key_columns": ["k"], "columns": [{"name": "n", "path": "p"}], "backend": {"http": {}}}),
                "missing url",
            ),
            (
                serde_json::json!({"key_columns": ["k"], "columns": [{"name": "n", "path": "p"}], "backend": {"http": {"url": format!("http://{addr}/u/static")}}}),
                "url without {{key}} placeholder",
            ),
            (
                serde_json::json!({"key_columns": ["k"], "columns": [{"name": "n", "path": "p"}], "backend": {"grpc": {}}}),
                "unknown backend kind",
            ),
            (
                serde_json::json!({"key_columns": ["k"], "columns": [{"name": "n", "path": "p"}], "backend": {"http": {"url": format!("http://{addr}/u/{{{{key}}}}")}}, "concurrency": 0}),
                "zero concurrency",
            ),
        ] {
            assert!(
                LookupProcessorBuilder
                    .build(None, &Some(bad), &test_resource())
                    .is_err(),
                "config must be rejected: {why}"
            );
        }
    }

    #[tokio::test]
    async fn empty_batch_short_circuits_without_http() {
        let mock = MockDimService::spawn(|_path| (200, record_body("x", 1)));
        let processor = build_processor(base_config(mock.addr, serde_json::json!({})));
        let result = processor.process(users_batch(vec![])).await.unwrap();
        assert!(matches!(result, ProcessResult::None));
        assert_eq!(mock.requests().len(), 0);
    }
}
