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

//! Rust stream processing engine

use crate::temporary::Temporary;
use datafusion::arrow::array::{Array, ArrayRef, BinaryArray, MapArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::data_type::AsBytes;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::SystemTime;
use thiserror::Error;

pub mod buffer;
pub mod cli;
pub mod codec;
pub mod config;
pub mod engine;
pub mod input;
pub mod output;
pub mod pipeline;
pub mod processor;
pub mod stream;
pub mod temporary;

#[cfg(test)]
mod message_batch_tests;

pub const DEFAULT_BINARY_VALUE_FIELD: &str = "__value__";
pub const DEFAULT_RECORD_BATCH: usize = 8192;

/// Metadata column name prefix
pub const META_COLUMN_PREFIX: &str = "__meta_";

/// Standard metadata column names for SQL-accessible metadata
pub mod meta_columns {
    pub const SOURCE: &str = "__meta_source";
    pub const PARTITION: &str = "__meta_partition";
    pub const OFFSET: &str = "__meta_offset";
    pub const KEY: &str = "__meta_key";
    pub const TIMESTAMP: &str = "__meta_timestamp";
    pub const INGEST_TIME: &str = "__meta_ingest_time";

    /// Extended metadata column using MapArray for flexible key-value pairs
    pub const EXT: &str = "__meta_ext";
}

/// Error in the stream processing engine
#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Read error: {0}")]
    Read(String),

    #[error("Process errors: {0}")]
    Process(String),

    #[error("Connection error: {0}")]
    Connection(String),

    /// Reconnection should be attempted after a connection loss.
    #[error("Connection lost")]
    Disconnection,

    #[error("Timeout error")]
    Timeout,

    #[error("Unknown error: {0}")]
    Unknown(String),

    #[error("EOF")]
    EOF,
}

#[derive(Clone)]
pub struct Resource {
    pub temporary: HashMap<String, Arc<dyn Temporary>>,
    pub input_names: RefCell<Vec<String>>,
}

pub type Bytes = Vec<u8>;

/// Shared reference to a message batch (zero-copy)
///
/// This type alias enables zero-copy message passing by wrapping MessageBatch in Arc.
/// Multiple components can share the same message batch without cloning the underlying data.
///
/// # Example
/// ```rust,no_run
/// use arkflow_core::{MessageBatch, MessageBatchRef};
/// use std::sync::Arc;
///
/// // Create a message batch
/// let batch = MessageBatch::new_binary(vec![b"data".to_vec()]).unwrap();
///
/// // Convert to shared reference
/// let shared: MessageBatchRef = Arc::new(batch);
///
/// // Clone is cheap (just increments reference count)
/// let another_ref = shared.clone();
/// ```
pub type MessageBatchRef = Arc<MessageBatch>;

/// Result of processing a message batch
///
/// This enum represents the output of processor operations, supporting
/// single output, multiple outputs, or filtering (no output).
///
/// # Variants
///
/// * `Single` - Processor produces a single message batch
/// * `Multiple` - Processor splits/produces multiple message batches
/// * `None` - Processor filters out the message
///
/// # Example
/// ```rust,no_run
/// use arkflow_core::{MessageBatch, MessageBatchRef, ProcessResult};
/// use std::sync::Arc;
///
/// // Pass-through processor
/// fn passthrough(batch: MessageBatchRef) -> ProcessResult {
///     ProcessResult::Single(batch)
/// }
///
/// // Filter processor
/// fn filter(batch: MessageBatchRef) -> ProcessResult {
///     let should_keep = |_batch: &MessageBatchRef| true;
///     if should_keep(&batch) {
///         ProcessResult::Single(batch)
///     } else {
///         ProcessResult::None
///     }
/// }
///
/// // Split processor
/// fn split(batch: MessageBatchRef) -> ProcessResult {
///     fn split_batch(_: &MessageBatchRef, _: usize) -> Vec<MessageBatchRef> { vec![] };
///     let chunks = split_batch(&batch, 100);
///     ProcessResult::Multiple(chunks)
/// }
/// ```
#[derive(Debug)]
pub enum ProcessResult {
    /// Single message batch output
    Single(MessageBatchRef),
    /// Multiple message batches output
    Multiple(Vec<MessageBatchRef>),
    /// No output (filtered)
    None,
}

impl ProcessResult {
    /// Convert to Vec<MessageBatch> for backward compatibility
    ///
    /// This method consumes the ProcessResult and converts it to a vector
    /// of owned MessageBatch instances. This may involve cloning Arc contents
    /// if the Arc is shared.
    ///
    /// # Performance Note
    /// Prefer working with ProcessResult directly to avoid unnecessary conversions.
    pub fn into_vec(self) -> Vec<MessageBatch> {
        match self {
            ProcessResult::Single(msg) => {
                vec![Arc::try_unwrap(msg).unwrap_or_else(|m| (*m).clone())]
            }
            ProcessResult::Multiple(msgs) => msgs
                .into_iter()
                .map(|m| Arc::try_unwrap(m).unwrap_or_else(|m| (*m).clone()))
                .collect(),
            ProcessResult::None => vec![],
        }
    }

    /// Create ProcessResult from Vec<MessageBatch> for backward compatibility
    pub fn from_vec(vec: Vec<MessageBatch>) -> Self {
        match vec.len() {
            0 => ProcessResult::None,
            1 => ProcessResult::Single(Arc::new(vec.into_iter().next().unwrap())),
            _ => ProcessResult::Multiple(vec.into_iter().map(Arc::new).collect()),
        }
    }

    /// Check if result is empty (filtered out)
    pub fn is_empty(&self) -> bool {
        matches!(self, ProcessResult::None)
    }

    /// Get the number of output batches
    pub fn len(&self) -> usize {
        match self {
            ProcessResult::Single(_) => 1,
            ProcessResult::Multiple(vec) => vec.len(),
            ProcessResult::None => 0,
        }
    }
}

/// Represents a message in a stream processing engine.
#[derive(Clone, Debug)]
pub struct MessageBatch {
    record_batch: RecordBatch,
    input_name: Option<String>,
}

impl MessageBatch {
    pub fn new_binary(content: Vec<Bytes>) -> Result<Self, Error> {
        Self::new_binary_with_field_name(content, None)
    }
    pub fn new_binary_with_field_name(
        content: Vec<Bytes>,
        field_name: Option<&str>,
    ) -> Result<Self, Error> {
        let fields = vec![Field::new(
            field_name.unwrap_or(DEFAULT_BINARY_VALUE_FIELD),
            DataType::Binary,
            false,
        )];
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(content.len());

        let bytes: Vec<_> = content.iter().map(|x| x.as_bytes()).collect();

        let array = BinaryArray::from_vec(bytes);
        columns.push(Arc::new(array));

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| Error::Process(format!("Creating an Arrow record batch failed: {}", e)))?;

        Ok(Self {
            record_batch: batch,
            input_name: None,
        })
    }

    pub fn set_input_name(&mut self, input_name: Option<String>) {
        self.input_name = input_name;
    }

    pub fn get_input_name(&self) -> Option<String> {
        self.input_name.clone()
    }

    pub fn new_binary_with_origin(&self, content: Vec<Bytes>) -> Result<Self, Error> {
        let schema = self.schema();
        let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();

        fields.push(Arc::new(Field::new(
            DEFAULT_BINARY_VALUE_FIELD,
            DataType::Binary,
            false,
        )));
        let new_schema = Arc::new(Schema::new(fields));

        let mut columns: Vec<ArrayRef> = Vec::new();
        for i in 0..schema.fields().len() {
            columns.push(self.column(i).clone());
        }

        let binary_data: Vec<&[u8]> = content.iter().map(|v| v.as_slice()).collect();
        columns.push(Arc::new(BinaryArray::from(binary_data)));

        let new_msg = RecordBatch::try_new(new_schema, columns)
            .map_err(|e| Error::Process(format!("Creating an Arrow record batch failed: {}", e)))?;
        Ok(MessageBatch::new_arrow(new_msg))
    }

    pub fn filter_columns(
        &self,
        field_names_to_include: &HashSet<String>,
    ) -> Result<MessageBatch, Error> {
        let schema = self.schema();

        let cap = field_names_to_include.len();
        let mut new_columns = Vec::with_capacity(cap);
        let mut fields = Vec::with_capacity(cap);

        for (i, col) in self.columns().iter().enumerate() {
            let field = schema.field(i);
            let name = field.name();

            if field_names_to_include.contains(name.as_str()) {
                new_columns.push(col.clone());
                fields.push(field.clone());
            }
        }

        let new_schema: SchemaRef = SchemaRef::new(Schema::new(fields));
        let batch = RecordBatch::try_new(new_schema, new_columns)
            .map_err(|e| Error::Process(format!("Creating an Arrow record batch failed: {}", e)))?;
        Ok(batch.into())
    }

    pub fn from_json<T: Serialize>(value: &T) -> Result<Self, Error> {
        let content = serde_json::to_vec(value)?;
        Self::new_binary(vec![content])
    }

    pub fn new_arrow(content: RecordBatch) -> Self {
        Self {
            record_batch: content,
            input_name: None,
        }
    }

    /// Create a message from a string.
    pub fn from_string(content: &str) -> Result<Self, Error> {
        Self::new_binary(vec![content.as_bytes().to_vec()])
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.record_batch.num_rows()
    }

    pub fn to_binary(&self, name: &str) -> Result<Vec<&[u8]>, Error> {
        let Some(array_ref) = self.record_batch.column_by_name(name) else {
            return Err(Error::Process("not found column".to_string()));
        };

        let data = array_ref.to_data();

        if *data.data_type() != DataType::Binary {
            return Err(Error::Process("not support data type".to_string()));
        }

        let Some(v) = array_ref.as_any().downcast_ref::<BinaryArray>() else {
            return Err(Error::Process("not support data type".to_string()));
        };
        let vec_bytes: Vec<&[u8]> = v.iter().flatten().collect();
        Ok(vec_bytes)
    }

    /// Convert this batch into a shared reference (Arc) for zero-copy passing
    pub fn into_arc(self) -> MessageBatchRef {
        Arc::new(self)
    }
}

impl Deref for MessageBatch {
    type Target = RecordBatch;

    fn deref(&self) -> &Self::Target {
        &self.record_batch
    }
}

impl From<RecordBatch> for MessageBatch {
    fn from(batch: RecordBatch) -> Self {
        Self {
            record_batch: batch,
            input_name: None,
        }
    }
}

impl From<MessageBatch> for RecordBatch {
    fn from(batch: MessageBatch) -> Self {
        batch.record_batch
    }
}

impl TryFrom<Vec<Bytes>> for MessageBatch {
    type Error = Error;

    fn try_from(value: Vec<Bytes>) -> Result<Self, Self::Error> {
        Self::new_binary(value)
    }
}

impl TryFrom<Vec<String>> for MessageBatch {
    type Error = Error;

    fn try_from(value: Vec<String>) -> Result<Self, Self::Error> {
        Self::new_binary(value.into_iter().map(|s| s.into_bytes()).collect())
    }
}

impl TryFrom<Vec<&str>> for MessageBatch {
    type Error = Error;

    fn try_from(value: Vec<&str>) -> Result<Self, Self::Error> {
        Self::new_binary(value.into_iter().map(|s| s.as_bytes().to_vec()).collect())
    }
}

impl DerefMut for MessageBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.record_batch
    }
}

pub fn split_batch(batch_to_split: RecordBatch, size: usize) -> Vec<RecordBatch> {
    let size = size.max(1);
    let total_rows = batch_to_split.num_rows();
    if total_rows <= DEFAULT_RECORD_BATCH {
        return vec![batch_to_split];
    }

    let (chunk_size, capacity) = if size * DEFAULT_RECORD_BATCH < total_rows {
        (total_rows.div_ceil(size), size)
    } else {
        (
            DEFAULT_RECORD_BATCH,
            total_rows.div_ceil(DEFAULT_RECORD_BATCH),
        )
    };

    let mut chunks = Vec::with_capacity(capacity);
    let mut offset = 0;
    while offset < total_rows {
        let length = std::cmp::min(chunk_size, total_rows - offset);
        let slice = batch_to_split.slice(offset, length);
        chunks.push(slice);
        offset += length;
    }

    chunks
}

/// Metadata utilities for adding metadata columns to RecordBatch
///
/// This module provides helper functions to add metadata columns (with __meta_ prefix)
/// to RecordBatch instances. These columns can be accessed directly in SQL queries.
///
/// # Example
/// ```rust,no_run
/// use arkflow_core::{meta_columns, metadata};
/// use datafusion::arrow::record_batch::RecordBatch;
///
/// let batch = RecordBatch::new_empty(schema);
/// let batch_with_meta = metadata::with_source(batch, "kafka://topic-a");
/// let batch_with_meta = metadata::with_partition(batch_with_meta, 0);
/// ```
pub mod metadata {
    use super::*;
    use datafusion::arrow::array::{
        BinaryArray, StringArray, TimestampNanosecondArray, UInt32Array, UInt64Array,
    };

    /// Add source column to RecordBatch
    pub fn with_source(batch: RecordBatch, source: &str) -> Result<RecordBatch, Error> {
        add_string_column(batch, meta_columns::SOURCE, source)
    }

    /// Add partition column to RecordBatch
    pub fn with_partition(batch: RecordBatch, partition: u32) -> Result<RecordBatch, Error> {
        add_uint32_column(batch, meta_columns::PARTITION, partition)
    }

    /// Add offset column to RecordBatch
    pub fn with_offset(batch: RecordBatch, offset: u64) -> Result<RecordBatch, Error> {
        add_uint64_column(batch, meta_columns::OFFSET, offset)
    }

    /// Add key column to RecordBatch
    pub fn with_key(batch: RecordBatch, key: &[u8]) -> Result<RecordBatch, Error> {
        add_binary_column(batch, meta_columns::KEY, key)
    }

    /// Add timestamp column to RecordBatch
    pub fn with_timestamp(batch: RecordBatch, timestamp: SystemTime) -> Result<RecordBatch, Error> {
        let nanos = timestamp
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        add_timestamp_column(batch, meta_columns::TIMESTAMP, nanos)
    }

    /// Add ingest_time column to RecordBatch
    pub fn with_ingest_time(
        batch: RecordBatch,
        ingest_time: SystemTime,
    ) -> Result<RecordBatch, Error> {
        let nanos = ingest_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        add_timestamp_column(batch, meta_columns::INGEST_TIME, nanos)
    }

    // Helper functions to add scalar columns

    fn add_string_column(
        batch: RecordBatch,
        column_name: &str,
        value: &str,
    ) -> Result<RecordBatch, Error> {
        let row_count = batch.num_rows();
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        let mut columns = batch.columns().to_vec();

        fields.push(Arc::new(Field::new(column_name, DataType::Utf8, false)));
        columns.push(Arc::new(StringArray::from(vec![value; row_count])));

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns)
            .map_err(|e| Error::Process(format!("Failed to add column {}: {}", column_name, e)))
    }

    fn add_uint32_column(
        batch: RecordBatch,
        column_name: &str,
        value: u32,
    ) -> Result<RecordBatch, Error> {
        let row_count = batch.num_rows();
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        let mut columns = batch.columns().to_vec();

        fields.push(Arc::new(Field::new(column_name, DataType::UInt32, false)));
        columns.push(Arc::new(UInt32Array::from(vec![value; row_count])));

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns)
            .map_err(|e| Error::Process(format!("Failed to add column {}: {}", column_name, e)))
    }

    fn add_uint64_column(
        batch: RecordBatch,
        column_name: &str,
        value: u64,
    ) -> Result<RecordBatch, Error> {
        let row_count = batch.num_rows();
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        let mut columns = batch.columns().to_vec();

        fields.push(Arc::new(Field::new(column_name, DataType::UInt64, false)));
        columns.push(Arc::new(UInt64Array::from(vec![value; row_count])));

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns)
            .map_err(|e| Error::Process(format!("Failed to add column {}: {}", column_name, e)))
    }

    fn add_binary_column(
        batch: RecordBatch,
        column_name: &str,
        value: &[u8],
    ) -> Result<RecordBatch, Error> {
        let row_count = batch.num_rows();
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        let mut columns = batch.columns().to_vec();

        fields.push(Arc::new(Field::new(column_name, DataType::Binary, false)));
        let binary_vec: Vec<&[u8]> = vec![value; row_count];
        columns.push(Arc::new(BinaryArray::from(binary_vec)));

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns)
            .map_err(|e| Error::Process(format!("Failed to add column {}: {}", column_name, e)))
    }

    fn add_timestamp_column(
        batch: RecordBatch,
        column_name: &str,
        value_nanos: i64,
    ) -> Result<RecordBatch, Error> {
        let row_count = batch.num_rows();
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        let mut columns = batch.columns().to_vec();

        fields.push(Arc::new(Field::new(
            column_name,
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        )));
        columns.push(Arc::new(TimestampNanosecondArray::from(vec![
            value_nanos;
            row_count
        ])));

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns)
            .map_err(|e| Error::Process(format!("Failed to add column {}: {}", column_name, e)))
    }

    /// Add extended metadata column (MapArray) to RecordBatch
    ///
    /// This adds a `__meta_ext` column containing a Map<String, String> with custom metadata.
    /// All rows will have the same metadata values (scalar metadata).
    ///
    /// # Example
    /// ```rust,no_run
    /// use arkflow_core::{metadata, meta_columns};
    /// use std::collections::HashMap;
    ///
    /// let mut ext_meta = HashMap::new();
    /// ext_meta.insert("tag".to_string(), "production".to_string());
    /// ext_meta.insert("version".to_string(), "1.0".to_string());
    ///
    /// let batch_with_ext = metadata::with_ext_metadata(batch, &ext_meta)?;
    /// ```
    pub fn with_ext_metadata(
        batch: RecordBatch,
        metadata: &HashMap<String, String>,
    ) -> Result<RecordBatch, Error> {
        let row_count = batch.num_rows();
        if row_count == 0 {
            return Ok(batch);
        }

        // Create MapArray with the same metadata for all rows
        let keys: Vec<&str> = metadata.keys().map(|k| k.as_str()).collect();
        let values: Vec<&str> = metadata.values().map(|v| v.as_str()).collect();
        let entries_per_row = keys.len();

        // Build keys and values arrays (repeated for each row)
        let mut all_keys = Vec::with_capacity(row_count * entries_per_row);
        let mut all_values = Vec::with_capacity(row_count * entries_per_row);
        let mut offsets = vec![0i32];

        for row in 0..row_count {
            for &key in &keys {
                all_keys.push(key);
            }
            for &value in &values {
                all_values.push(value);
            }
            offsets.push(((row + 1) * entries_per_row) as i32);
        }

        let keys_array = StringArray::from(all_keys);
        let values_array = StringArray::from(all_values);

        // Create StructArray for map entries
        let struct_array = datafusion::arrow::array::StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(keys_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::Utf8, false)),
                Arc::new(values_array) as ArrayRef,
            ),
        ]);

        // Create the map field
        let map_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Utf8, false)),
                    Arc::new(Field::new("value", DataType::Utf8, false)),
                ]
                .into(),
            ),
            false,
        ));

        // Convert offsets to OffsetBuffer
        let offsets_buffer = datafusion::arrow::buffer::OffsetBuffer::new(offsets.into());

        let map_array = MapArray::new(map_field, offsets_buffer, struct_array, None, false);

        // Add the map column to the batch
        add_map_column(batch, meta_columns::EXT, map_array)
    }

    /// Add extended metadata column with different metadata per row
    ///
    /// Each row can have different metadata keys and values.
    ///
    /// # Example
    /// ```rust,no_run
    /// use arkflow_core::metadata;
    /// use std::collections::HashMap;
    ///
    /// let mut row1_meta = HashMap::new();
    /// row1_meta.insert("tag".to_string(), "prod".to_string());
    ///
    /// let mut row2_meta = HashMap::new();
    /// row2_meta.insert("tag".to_string(), "dev".to_string());
    /// row2_meta.insert("debug".to_string(), "true".to_string());
    ///
    /// let batch_with_ext = metadata::with_ext_metadata_per_row(batch, &[row1_meta, row2_meta])?;
    /// ```
    pub fn with_ext_metadata_per_row(
        batch: RecordBatch,
        metadata_list: &[HashMap<String, String>],
    ) -> Result<RecordBatch, Error> {
        let row_count = batch.num_rows();
        if row_count == 0 || metadata_list.is_empty() {
            return Ok(batch);
        }

        if metadata_list.len() != row_count {
            return Err(Error::Process(format!(
                "Metadata list length ({}) must match batch row count ({})",
                metadata_list.len(),
                row_count
            )));
        }

        // Build keys and values arrays
        let mut all_keys = Vec::new();
        let mut all_values = Vec::new();
        let mut offsets = vec![0i32];

        for metadata in metadata_list {
            for (key, value) in metadata {
                all_keys.push(key.as_str());
                all_values.push(value.as_str());
            }
            offsets.push(all_keys.len() as i32);
        }

        let keys_array = StringArray::from(all_keys);
        let values_array = StringArray::from(all_values);

        let struct_array = datafusion::arrow::array::StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(keys_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::Utf8, false)),
                Arc::new(values_array) as ArrayRef,
            ),
        ]);

        // Create the map field
        let map_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Utf8, false)),
                    Arc::new(Field::new("value", DataType::Utf8, false)),
                ]
                .into(),
            ),
            false,
        ));

        // Convert offsets to OffsetBuffer
        let offsets_buffer = datafusion::arrow::buffer::OffsetBuffer::new(offsets.into());

        let map_array = MapArray::new(map_field, offsets_buffer, struct_array, None, false);

        add_map_column(batch, meta_columns::EXT, map_array)
    }

    /// Add a MapArray column to the batch
    fn add_map_column(
        batch: RecordBatch,
        column_name: &str,
        map_array: MapArray,
    ) -> Result<RecordBatch, Error> {
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        let mut columns = batch.columns().to_vec();

        // Add the map field
        fields.push(Arc::new(Field::new(
            column_name,
            map_array.data_type().clone(),
            false,
        )));
        columns.push(Arc::new(map_array));

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns)
            .map_err(|e| Error::Process(format!("Failed to add column {}: {}", column_name, e)))
    }
}

#[cfg(test)]
mod metadata_tests {
    use super::*;
    use datafusion::arrow::array::StringArray;

    #[test]
    fn test_metadata_with_source() {
        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["test"]))]).unwrap();

        let batch_with_source = metadata::with_source(batch, "kafka://topic-a").unwrap();

        assert!(batch_with_source
            .column_by_name(meta_columns::SOURCE)
            .is_some());
        assert_eq!(batch_with_source.num_columns(), 2);
    }

    #[test]
    fn test_metadata_with_partition() {
        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["test"]))]).unwrap();

        let batch_with_partition = metadata::with_partition(batch, 0).unwrap();

        assert!(batch_with_partition
            .column_by_name(meta_columns::PARTITION)
            .is_some());
        assert_eq!(batch_with_partition.num_columns(), 2);
    }

    #[test]
    fn test_metadata_chain() {
        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["test"]))]).unwrap();

        let batch_with_meta = metadata::with_source(batch, "kafka://topic-a")
            .and_then(|b| metadata::with_partition(b, 0))
            .and_then(|b| metadata::with_offset(b, 123))
            .unwrap();

        assert!(batch_with_meta
            .column_by_name(meta_columns::SOURCE)
            .is_some());
        assert!(batch_with_meta
            .column_by_name(meta_columns::PARTITION)
            .is_some());
        assert!(batch_with_meta
            .column_by_name(meta_columns::OFFSET)
            .is_some());
        assert_eq!(batch_with_meta.num_columns(), 4); // data + 3 metadata columns
    }

    #[test]
    fn test_metadata_in_sql() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["Alice"])),
            ],
        )
        .unwrap();

        // Add metadata
        let batch_with_meta = metadata::with_source(batch, "kafka://users")
            .and_then(|b| metadata::with_partition(b, 0))
            .unwrap();

        // Can be queried in SQL as:
        // SELECT id, name, __meta_source, __meta_partition FROM flow
        assert!(batch_with_meta.column_by_name("__meta_source").is_some());
        assert!(batch_with_meta.column_by_name("__meta_partition").is_some());
    }

    #[test]
    fn test_ext_metadata_scalar() {
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["test1", "test2", "test3"]))],
        )
        .unwrap();

        let mut ext_meta = HashMap::new();
        ext_meta.insert("tag".to_string(), "production".to_string());
        ext_meta.insert("version".to_string(), "1.0.0".to_string());

        let batch_with_ext = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        // Verify __meta_ext column exists
        assert!(batch_with_ext.column_by_name(meta_columns::EXT).is_some());
        assert_eq!(batch_with_ext.num_columns(), 2); // data + __meta_ext
        assert_eq!(batch_with_ext.num_rows(), 3);
    }

    #[test]
    fn test_ext_metadata_per_row() {
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["test1", "test2"]))],
        )
        .unwrap();

        let mut row1_meta = HashMap::new();
        row1_meta.insert("tag".to_string(), "prod".to_string());
        row1_meta.insert("region".to_string(), "us-east".to_string());

        let mut row2_meta = HashMap::new();
        row2_meta.insert("tag".to_string(), "dev".to_string());
        row2_meta.insert("debug".to_string(), "true".to_string());

        let batch_with_ext =
            metadata::with_ext_metadata_per_row(batch, &[row1_meta, row2_meta]).unwrap();

        // Verify __meta_ext column exists
        assert!(batch_with_ext.column_by_name(meta_columns::EXT).is_some());
        assert_eq!(batch_with_ext.num_columns(), 2); // data + __meta_ext
        assert_eq!(batch_with_ext.num_rows(), 2);
    }

    #[test]
    fn test_mixed_metadata() {
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["test"]))]).unwrap();

        // Add standard metadata columns
        let batch = metadata::with_source(batch, "kafka://topic-a").unwrap();
        let batch = metadata::with_partition(batch, 0).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("custom_field".to_string(), "custom_value".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        // Verify all columns exist
        assert!(batch.column_by_name(meta_columns::SOURCE).is_some());
        assert!(batch.column_by_name(meta_columns::PARTITION).is_some());
        assert!(batch.column_by_name(meta_columns::EXT).is_some());
        assert_eq!(batch.num_columns(), 4); // data + 3 metadata columns
    }

    #[test]
    fn test_ext_metadata_empty() {
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["test"]))]).unwrap();

        let empty_meta = HashMap::new();
        let batch_with_ext = metadata::with_ext_metadata(batch, &empty_meta).unwrap();

        // Empty metadata should still add the column
        assert!(batch_with_ext.column_by_name(meta_columns::EXT).is_some());
    }

    #[test]
    fn test_ext_metadata_mismatch_length() {
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["test1", "test2"]))],
        )
        .unwrap();

        let mut meta = HashMap::new();
        meta.insert("key".to_string(), "value".to_string());

        // Only 1 metadata for 2 rows should fail
        let result = metadata::with_ext_metadata_per_row(batch, &[meta.clone()]);
        assert!(result.is_err());
    }
}
