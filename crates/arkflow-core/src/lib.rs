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
use datafusion::arrow::array::{Array, ArrayRef, BinaryArray, MapArray};
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
    /// use arkflow_core::{metadata, meta_columns, MessageBatch};
    /// use std::collections::HashMap;
    ///
    /// // Create a message batch
    /// let batch = MessageBatch::new_binary(vec![b"data".to_vec()]).unwrap().into();
    ///
    /// // Create extended metadata
    /// let mut ext_meta = HashMap::new();
    /// ext_meta.insert("tag".to_string(), "production".to_string());
    /// ext_meta.insert("version".to_string(), "1.0".to_string());
    ///
    /// // Add extended metadata to batch
    /// let batch_with_ext = metadata::with_ext_metadata(batch, &ext_meta)?;
    /// # Ok::<(), arkflow_core::Error>(())
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
        // Collect items to maintain key-value pairing
        let items: Vec<(&str, &str)> = metadata
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let keys: Vec<&str> = items.iter().map(|(k, _)| *k).collect();
        let values: Vec<&str> = items.iter().map(|(_, v)| *v).collect();
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

    #[tokio::test]
    async fn test_metadata_sql_query_with_datafusion() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        // Create a batch with metadata
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://users").unwrap();
        let batch = metadata::with_partition(batch, 0).unwrap();
        let batch = metadata::with_offset(batch, 100).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("env".to_string(), "production".to_string());
        ext_meta.insert("region".to_string(), "us-west".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        // Create DataFusion context and register the batch
        let ctx = SessionContext::new();
        ctx.register_batch("flow", batch).unwrap();

        // Test 1: Select metadata columns
        let sql = "SELECT id, name, __meta_source, __meta_partition, __meta_offset FROM flow";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 5);

        // Test 2: Filter by metadata
        let sql = "SELECT id, name FROM flow WHERE __meta_partition = 0 AND __meta_offset >= 100";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3); // All 3 rows match

        // Test 3: Aggregate with metadata
        let sql = "SELECT __meta_source, COUNT(*) as cnt FROM flow GROUP BY __meta_source";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn test_metadata_sql_with_different_ext_metadata() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        // Create batches with different extended metadata per row
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                1, 2, 3,
            ]))],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://orders").unwrap();
        let batch = metadata::with_partition(batch, 0).unwrap();

        // Add different extended metadata per row
        let row1_meta = HashMap::from([("env".to_string(), "prod".to_string())]);
        let row2_meta = HashMap::from([
            ("env".to_string(), "staging".to_string()),
            ("debug".to_string(), "true".to_string()),
        ]);
        let row3_meta = HashMap::from([("env".to_string(), "dev".to_string())]);

        let batch =
            metadata::with_ext_metadata_per_row(batch, &[row1_meta, row2_meta, row3_meta]).unwrap();

        // Verify the batch has extended metadata column
        assert!(batch.column_by_name("__meta_ext").is_some());
        assert_eq!(batch.num_rows(), 3);

        // Create DataFusion context and register the batch
        let ctx = SessionContext::new();
        ctx.register_batch("orders", batch).unwrap();

        // Query core metadata
        let sql = "SELECT id, __meta_source, __meta_partition FROM orders";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);
    }

    #[tokio::test]
    async fn test_metadata_sql_all_types() {
        use datafusion::prelude::*;

        // Test all metadata types together
        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, false)]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["test", "test2"]))],
        )
        .unwrap();

        // Add all types of core metadata
        let batch = metadata::with_source(batch, "kafka://test").unwrap();
        let batch = metadata::with_partition(batch, 5).unwrap();
        let batch = metadata::with_offset(batch, 9999).unwrap();
        let batch = metadata::with_key(batch, b"test-key").unwrap();
        let batch = metadata::with_timestamp(batch, SystemTime::now()).unwrap();
        let batch = metadata::with_ingest_time(batch, SystemTime::now()).unwrap();

        // Verify all columns exist
        assert!(batch.column_by_name(meta_columns::SOURCE).is_some());
        assert!(batch.column_by_name(meta_columns::PARTITION).is_some());
        assert!(batch.column_by_name(meta_columns::OFFSET).is_some());
        assert!(batch.column_by_name(meta_columns::KEY).is_some());
        assert!(batch.column_by_name(meta_columns::TIMESTAMP).is_some());
        assert!(batch.column_by_name(meta_columns::INGEST_TIME).is_some());

        assert_eq!(batch.num_columns(), 7); // data + 6 metadata columns

        // Create DataFusion context and verify SQL query works
        let ctx = SessionContext::new();
        ctx.register_batch("test_data", batch).unwrap();

        let sql = "SELECT data, __meta_source, __meta_partition, __meta_offset
                   FROM test_data
                   WHERE __meta_partition = 5";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_metadata_sql_order_by() {
        use datafusion::prelude::*;

        // Create a batch with multiple rows and metadata
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    1, 2, 3, 4, 5,
                ])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    10, 20, 30, 40, 50,
                ])),
            ],
        )
        .unwrap();

        // Add metadata with different offsets
        let mut batches = Vec::new();
        for i in 0..5 {
            let single_batch = batch.slice(i, 1);
            let single_batch = metadata::with_source(single_batch, "kafka://test").unwrap();
            let single_batch = metadata::with_partition(single_batch, (i % 2) as u32).unwrap();
            let single_batch = metadata::with_offset(single_batch, 100 + i as u64).unwrap();
            batches.push(single_batch);
        }

        // Combine batches (simplified - just use the first batch with varying metadata)
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Int32, false),
            ])),
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let batch = metadata::with_source(batch, "kafka://test").unwrap();
        let batch = metadata::with_partition(batch, 1).unwrap();
        let batch = metadata::with_offset(batch, 200).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("test_table", batch).unwrap();

        // ORDER BY metadata column
        let sql = "SELECT id, value, __meta_offset FROM test_table ORDER BY __meta_offset DESC";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_metadata_sql_aggregations() {
        use datafusion::prelude::*;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    1, 2, 3, 4, 5,
                ])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    100, 200, 150, 250, 300,
                ])),
            ],
        )
        .unwrap();

        // Add different partitions
        let batch = metadata::with_source(batch, "kafka://orders").unwrap();
        let batch = metadata::with_partition(batch, 2).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("orders", batch).unwrap();

        // Test various aggregations
        let sql = "SELECT
                   __meta_source,
                   __meta_partition,
                   COUNT(*) as count,
                   SUM(amount) as total,
                   AVG(amount) as avg_amount,
                   MIN(amount) as min_amount,
                   MAX(amount) as max_amount
                   FROM orders
                   GROUP BY __meta_source, __meta_partition";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 7);
    }

    #[tokio::test]
    async fn test_metadata_sql_case_when() {
        use datafusion::prelude::*;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    50, 150, 250,
                ])),
            ],
        )
        .unwrap();

        let batch = metadata::with_source(batch, "kafka://sales").unwrap();
        let batch = metadata::with_partition(batch, 0).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("sales", batch).unwrap();

        // CASE WHEN with metadata
        let sql = "SELECT
                   id,
                   amount,
                   CASE
                     WHEN __meta_partition = 0 THEN 'primary'
                     ELSE 'secondary'
                   END as partition_type
                   FROM sales
                   ORDER BY id";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3);
        assert_eq!(result[0].num_columns(), 3);
    }

    #[tokio::test]
    async fn test_metadata_sql_complex_where() {
        use datafusion::prelude::*;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("status", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    1, 2, 3, 4, 5,
                ])),
                Arc::new(StringArray::from(vec![
                    "active", "pending", "active", "done", "active",
                ])),
            ],
        )
        .unwrap();

        let batch = metadata::with_source(batch, "kafka://events").unwrap();
        let batch = metadata::with_partition(batch, 1).unwrap();
        let batch = metadata::with_offset(batch, 100).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("events", batch).unwrap();

        // Complex WHERE with metadata and data columns
        let sql = "SELECT id, status, __meta_source
                   FROM events
                   WHERE status = 'active'
                   AND __meta_partition = 1
                   AND __meta_offset >= 100
                   ORDER BY id";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3); // 3 active records
    }

    #[tokio::test]
    async fn test_metadata_sql_having() {
        use datafusion::prelude::*;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    1, 2, 3, 4, 5,
                ])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    100, 200, 150, 250, 300,
                ])),
            ],
        )
        .unwrap();

        let batch = metadata::with_source(batch, "kafka://products").unwrap();
        let batch = metadata::with_partition(batch, 0).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("products", batch).unwrap();

        // HAVING clause with metadata - filter by total amount
        let sql = "SELECT
                   __meta_partition,
                   COUNT(*) as count,
                   SUM(amount) as total
                   FROM products
                   GROUP BY __meta_partition
                   HAVING SUM(amount) > 500";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        // Total amount = 100 + 200 + 150 + 250 + 300 = 1000, which is > 500
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);
    }

    #[tokio::test]
    async fn test_metadata_sql_multiple_sources() {
        use datafusion::prelude::*;

        // Create two different batches from different sources
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![10, 20])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![3, 4])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![30, 40])),
            ],
        )
        .unwrap();

        let batch1 = metadata::with_source(batch1, "kafka://topic-a").unwrap();
        let batch1 = metadata::with_partition(batch1, 0).unwrap();

        let batch2 = metadata::with_source(batch2, "kafka://topic-b").unwrap();
        let batch2 = metadata::with_partition(batch2, 1).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("stream1", batch1).unwrap();
        ctx.register_batch("stream2", batch2).unwrap();

        // UNION ALL with metadata
        let sql = "SELECT id, value, __meta_source, __meta_partition
                   FROM stream1
                   UNION ALL
                   SELECT id, value, __meta_source, __meta_partition
                   FROM stream2
                   ORDER BY __meta_source, id";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 4);
    }

    #[tokio::test]
    async fn test_metadata_sql_with_timestamp() {
        use datafusion::prelude::*;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                1, 2, 3,
            ]))],
        )
        .unwrap();

        let batch = metadata::with_source(batch, "kafka://events").unwrap();
        let batch = metadata::with_timestamp(batch, SystemTime::now()).unwrap();
        let batch = metadata::with_ingest_time(batch, SystemTime::now()).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("timed_events", batch).unwrap();

        // Query with timestamp metadata
        let sql = "SELECT id, __meta_timestamp, __meta_ingest_time
                   FROM timed_events
                   ORDER BY id";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3);
        assert_eq!(result[0].num_columns(), 3);
    }

    #[tokio::test]
    async fn test_metadata_sql_join_on_metadata() {
        use datafusion::prelude::*;

        // Create two batches that can be joined on metadata
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, false),
        ]));

        let schema2 = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema1,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![100, 200])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema2,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![101, 102])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            ],
        )
        .unwrap();

        // Add same partition metadata to both
        let batch1 = metadata::with_source(batch1, "kafka://orders").unwrap();
        let batch1 = metadata::with_partition(batch1, 0).unwrap();

        let batch2 = metadata::with_source(batch2, "kafka://users").unwrap();
        let batch2 = metadata::with_partition(batch2, 0).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("orders", batch1).unwrap();
        ctx.register_batch("users", batch2).unwrap();

        // JOIN on metadata column
        let sql = "SELECT o.order_id, o.amount, u.name
                   FROM orders o
                   INNER JOIN users u
                   ON o.__meta_partition = u.__meta_partition";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        // Cross join: 2 orders x 2 users = 4 rows
        assert_eq!(result[0].num_rows(), 4);
    }

    #[tokio::test]
    async fn test_metadata_sql_distinct() {
        use datafusion::prelude::*;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                1, 2, 3, 4, 5,
            ]))],
        )
        .unwrap();

        let batch = metadata::with_source(batch, "kafka://test").unwrap();
        let batch = metadata::with_partition(batch, 1).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("distinct_test", batch).unwrap();

        // DISTINCT with metadata
        let sql = "SELECT DISTINCT __meta_source, __meta_partition FROM distinct_test";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1); // Only 1 unique combination
    }

    #[tokio::test]
    async fn test_metadata_sql_limit_offset() {
        use datafusion::prelude::*;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                1, 2, 3, 4, 5,
            ]))],
        )
        .unwrap();

        let batch = metadata::with_source(batch, "kafka://test").unwrap();
        let batch = metadata::with_offset(batch, 100).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("limit_test", batch).unwrap();

        // LIMIT and OFFSET
        let sql = "SELECT id, __meta_offset FROM limit_test ORDER BY id LIMIT 3 OFFSET 1";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_column_exists() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                1, 2, 3,
            ]))],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://test").unwrap();
        let batch = metadata::with_partition(batch, 0).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("environment".to_string(), "production".to_string());
        ext_meta.insert("version".to_string(), "2.0".to_string());
        ext_meta.insert("region".to_string(), "us-west".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("with_ext", batch).unwrap();

        // Test 1: Verify __meta_ext column exists by selecting it
        let sql = "SELECT id, __meta_ext FROM with_ext";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);

        // Test 2: Check if column is in schema
        let sql = "SELECT * FROM with_ext LIMIT 1";
        let df = ctx.sql(sql).await.unwrap();
        let df_schema = df.schema();

        // Check if metadata columns exist in the schema
        assert!(df_schema.field_with_name(None, "__meta_ext").is_ok());
        assert!(df_schema.field_with_name(None, "__meta_source").is_ok());
        assert!(df_schema.field_with_name(None, "__meta_partition").is_ok());
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_in_result() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![100, 200])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://orders").unwrap();
        let batch = metadata::with_partition(batch, 1).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("priority".to_string(), "high".to_string());
        ext_meta.insert("tag".to_string(), "v1".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("orders", batch).unwrap();

        // Query all columns including __meta_ext
        let sql = "SELECT * FROM orders";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        // Should have: id, value, __meta_source, __meta_partition, __meta_ext
        assert_eq!(batch.num_columns(), 5);
        assert_eq!(batch.num_rows(), 2);

        // Verify __meta_ext column exists
        assert!(batch.column_by_name("__meta_ext").is_some());
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_per_row_queries() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    50, 150, 250,
                ])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://sales").unwrap();
        let batch = metadata::with_partition(batch, 0).unwrap();

        // Add different extended metadata per row
        let mut ext_meta1 = HashMap::new();
        ext_meta1.insert("category".to_string(), "electronics".to_string());

        let mut ext_meta2 = HashMap::new();
        ext_meta2.insert("category".to_string(), "books".to_string());
        ext_meta2.insert("discount".to_string(), "true".to_string());

        let mut ext_meta3 = HashMap::new();
        ext_meta3.insert("category".to_string(), "electronics".to_string());
        ext_meta3.insert("premium".to_string(), "true".to_string());

        let batch =
            metadata::with_ext_metadata_per_row(batch, &[ext_meta1, ext_meta2, ext_meta3]).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("sales", batch).unwrap();

        // Query with core metadata filtering
        let sql = "SELECT id, amount, __meta_ext
                   FROM sales
                   WHERE __meta_partition = 0
                   ORDER BY id";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);

        // Verify __meta_ext column exists and contains data
        assert!(batch.column_by_name("__meta_ext").is_some());
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_with_core_filters() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("status", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec![
                    "active", "pending", "active", "done",
                ])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://events").unwrap();
        let batch = metadata::with_partition(batch, 2).unwrap();
        let batch = metadata::with_offset(batch, 1000).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("service".to_string(), "api-gateway".to_string());
        ext_meta.insert("team".to_string(), "platform".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("events", batch).unwrap();

        // Filter by core metadata, select __meta_ext
        let sql = "SELECT id, status, __meta_partition, __meta_offset, __meta_ext
                   FROM events
                   WHERE status = 'active' AND __meta_partition = 2
                   ORDER BY id";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2); // 2 active records
        assert_eq!(batch.num_columns(), 5);

        // Verify all columns including __meta_ext exist
        assert!(batch.column_by_name("id").is_some());
        assert!(batch.column_by_name("status").is_some());
        assert!(batch.column_by_name("__meta_partition").is_some());
        assert!(batch.column_by_name("__meta_offset").is_some());
        assert!(batch.column_by_name("__meta_ext").is_some());
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_aggregation() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    10, 20, 30, 40,
                ])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://transactions").unwrap();
        let batch = metadata::with_partition(batch, 3).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("currency".to_string(), "USD".to_string());
        ext_meta.insert("type".to_string(), "payment".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("transactions", batch).unwrap();

        // Aggregate with __meta_ext in SELECT
        let sql = "SELECT
                   __meta_source,
                   __meta_partition,
                   COUNT(*) as count,
                   SUM(amount) as total
                   FROM transactions
                   GROUP BY __meta_source, __meta_partition";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 4);
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_join() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        // Create two batches with extended metadata
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, false),
        ]));

        let schema2 = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema1,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![100, 200])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema2,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![101, 102])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            ],
        )
        .unwrap();

        // Add core and extended metadata to both batches
        let mut ext_meta1 = HashMap::new();
        ext_meta1.insert("table".to_string(), "orders".to_string());
        let batch1 = metadata::with_source(batch1, "kafka://orders").unwrap();
        let batch1 = metadata::with_partition(batch1, 0).unwrap();
        let batch1 = metadata::with_ext_metadata(batch1, &ext_meta1).unwrap();

        let mut ext_meta2 = HashMap::new();
        ext_meta2.insert("table".to_string(), "users".to_string());
        let batch2 = metadata::with_source(batch2, "kafka://users").unwrap();
        let batch2 = metadata::with_partition(batch2, 0).unwrap();
        let batch2 = metadata::with_ext_metadata(batch2, &ext_meta2).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("orders", batch1).unwrap();
        ctx.register_batch("users", batch2).unwrap();

        // JOIN on partition and select __meta_ext from both tables
        let sql = "SELECT
                   o.order_id,
                   o.amount,
                   o.__meta_ext as order_meta,
                   u.name,
                   u.__meta_ext as user_meta
                   FROM orders o
                   INNER JOIN users u
                   ON o.__meta_partition = u.__meta_partition";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        // Cross join: 2 orders x 2 users = 4 rows
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 5);
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_count() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                1, 2, 3, 4, 5,
            ]))],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://items").unwrap();
        let batch = metadata::with_partition(batch, 1).unwrap();

        // Add extended metadata with multiple fields
        let mut ext_meta = HashMap::new();
        ext_meta.insert("field1".to_string(), "value1".to_string());
        ext_meta.insert("field2".to_string(), "value2".to_string());
        ext_meta.insert("field3".to_string(), "value3".to_string());
        ext_meta.insert("field4".to_string(), "value4".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("items", batch).unwrap();

        // Count rows with extended metadata
        let sql = "SELECT COUNT(*) as total_count FROM items";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);

        // Verify count - DataFusion returns Int64Array for COUNT(*)
        let count_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(count_array.value(0), 5);
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_types() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                1, 2,
            ]))],
        )
        .unwrap();

        // Add all core metadata types
        let batch = metadata::with_source(batch, "kafka://typed").unwrap();
        let batch = metadata::with_partition(batch, 5).unwrap();
        let batch = metadata::with_offset(batch, 12345).unwrap();
        let batch = metadata::with_key(batch, b"test-key").unwrap();
        let batch = metadata::with_timestamp(batch, SystemTime::now()).unwrap();
        let batch = metadata::with_ingest_time(batch, SystemTime::now()).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("string_field".to_string(), "string_value".to_string());
        ext_meta.insert("number_as_string".to_string(), "12345".to_string());
        ext_meta.insert("json_field".to_string(), r#"{"key":"value"}"#.to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("typed_test", batch).unwrap();

        // Query all columns
        let sql = "SELECT * FROM typed_test";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        // id + 6 core metadata + 1 ext metadata = 8 columns
        assert_eq!(batch.num_columns(), 8);
        assert_eq!(batch.num_rows(), 2);

        // Verify all metadata columns exist
        assert!(batch.column_by_name("__meta_source").is_some());
        assert!(batch.column_by_name("__meta_partition").is_some());
        assert!(batch.column_by_name("__meta_offset").is_some());
        assert!(batch.column_by_name("__meta_key").is_some());
        assert!(batch.column_by_name("__meta_timestamp").is_some());
        assert!(batch.column_by_name("__meta_ingest_time").is_some());
        assert!(batch.column_by_name("__meta_ext").is_some());
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_filter_by_column_presence() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    10, 20, 30, 40,
                ])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://test").unwrap();
        let batch = metadata::with_partition(batch, 1).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("priority".to_string(), "high".to_string());
        ext_meta.insert("verified".to_string(), "true".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("test_table", batch).unwrap();

        // Filter by core metadata and select __meta_ext
        // Note: We filter by core metadata columns, not directly by MapArray contents
        let sql = "SELECT id, value, __meta_ext
                   FROM test_table
                   WHERE __meta_partition = 1
                   ORDER BY id";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 3);
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_with_case_expression() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("status", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["active", "pending", "active"])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://items").unwrap();
        let batch = metadata::with_partition(batch, 2).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("region".to_string(), "us-east".to_string());
        ext_meta.insert("env".to_string(), "prod".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("items", batch).unwrap();

        // Use CASE expression based on core metadata, include __meta_ext in results
        let sql = "SELECT
                   id,
                   status,
                   CASE
                     WHEN __meta_partition = 2 THEN 'partition-2'
                     ELSE 'other'
                   END as partition_label,
                   __meta_ext
                   FROM items
                   WHERE status = 'active'
                   ORDER BY id";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2); // 2 active records
        assert_eq!(batch.num_columns(), 4);
    }

    #[tokio::test]
    async fn test_metadata_sql_access_ext_metadata_in_batch() {
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    100, 200, 300,
                ])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://orders").unwrap();
        let batch = metadata::with_partition(batch, 0).unwrap();

        // Add extended metadata with specific keys
        let mut ext_meta = HashMap::new();
        ext_meta.insert("priority".to_string(), "high".to_string());
        ext_meta.insert("category".to_string(), "electronics".to_string());
        ext_meta.insert("discount".to_string(), "10".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        // Demonstrate accessing __meta_ext column directly from RecordBatch
        if let Some(ext_col) = batch.column_by_name("__meta_ext") {
            // Verify it's a MapArray
            let map_array = ext_col.as_any().downcast_ref::<MapArray>();
            assert!(map_array.is_some(), "__meta_ext should be a MapArray");

            if let Some(map_array) = map_array {
                // Check map properties
                assert_eq!(map_array.len(), 3); // 3 rows
            }
        } else {
            panic!("__meta_ext column should exist");
        }

        // Now use in SQL query
        use datafusion::prelude::*;
        let ctx = SessionContext::new();
        ctx.register_batch("orders", batch).unwrap();

        let sql = "SELECT id, amount, __meta_ext FROM orders";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3);
        assert!(batch.column_by_name("__meta_ext").is_some());
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_combined_filters() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, false),
            Field::new("status", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    1, 2, 3, 4, 5,
                ])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    50, 150, 250, 350, 450,
                ])),
                Arc::new(StringArray::from(vec![
                    "new",
                    "new",
                    "processed",
                    "processed",
                    "done",
                ])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://transactions").unwrap();
        let batch = metadata::with_partition(batch, 3).unwrap();
        let batch = metadata::with_offset(batch, 500).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("currency".to_string(), "USD".to_string());
        ext_meta.insert("type".to_string(), "payment".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("transactions", batch).unwrap();

        // Complex filter combining data columns and core metadata
        // __meta_ext is included in results but filtering is done on other columns
        let sql = "SELECT
                   id,
                   amount,
                   status,
                   __meta_partition,
                   __meta_offset,
                   __meta_ext
                   FROM transactions
                   WHERE amount >= 100
                   AND status IN ('new', 'processed')
                   AND __meta_partition = 3
                   AND __meta_offset >= 500
                   ORDER BY amount";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3); // 3 rows match the criteria
        assert_eq!(batch.num_columns(), 6);

        // Verify __meta_ext exists in results
        assert!(batch.column_by_name("__meta_ext").is_some());
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_subquery() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    100, 200, 300,
                ])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://metrics").unwrap();
        let batch = metadata::with_partition(batch, 5).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("unit".to_string(), "bytes".to_string());
        ext_meta.insert("source".to_string(), "sensor-1".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("metrics", batch).unwrap();

        // Use subquery with core metadata, include __meta_ext in final result
        let sql = "SELECT * FROM (
                   SELECT id, value, __meta_partition, __meta_ext
                   FROM metrics
                   WHERE __meta_partition = 5
                 ) AS filtered
                 WHERE value >= 150
                 ORDER BY value";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2); // 2 rows with value >= 150
        assert_eq!(batch.num_columns(), 4);
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_with_union() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let schema2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema1,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![10, 20])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema2,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![3, 4])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![30, 40])),
            ],
        )
        .unwrap();

        // Add core and extended metadata to batch1
        let mut ext_meta1 = HashMap::new();
        ext_meta1.insert("source_type".to_string(), "primary".to_string());
        let batch1 = metadata::with_source(batch1, "kafka://stream1").unwrap();
        let batch1 = metadata::with_partition(batch1, 0).unwrap();
        let batch1 = metadata::with_ext_metadata(batch1, &ext_meta1).unwrap();

        // Add core and extended metadata to batch2
        let mut ext_meta2 = HashMap::new();
        ext_meta2.insert("source_type".to_string(), "secondary".to_string());
        let batch2 = metadata::with_source(batch2, "kafka://stream2").unwrap();
        let batch2 = metadata::with_partition(batch2, 1).unwrap();
        let batch2 = metadata::with_ext_metadata(batch2, &ext_meta2).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("stream1", batch1).unwrap();
        ctx.register_batch("stream2", batch2).unwrap();

        // UNION ALL with __meta_ext, filter on core metadata
        let sql = "SELECT id, value, __meta_source, __meta_ext
                   FROM stream1
                   WHERE __meta_partition = 0
                   UNION ALL
                   SELECT id, value, __meta_source, __meta_ext
                   FROM stream2
                   WHERE __meta_partition = 1
                   ORDER BY value";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 4);
        assert!(batch.column_by_name("__meta_ext").is_some());
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_with_window_functions() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    100, 200, 300, 400,
                ])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://sales").unwrap();
        let batch = metadata::with_partition(batch, 1).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("region".to_string(), "north".to_string());
        ext_meta.insert("team".to_string(), "sales-alpha".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("sales", batch).unwrap();

        // Window function with __meta_ext in SELECT
        let sql = "SELECT
                   id,
                   amount,
                   __meta_ext,
                   ROW_NUMBER() OVER (ORDER BY amount) as row_num,
                   SUM(amount) OVER (ORDER BY amount) as running_total
                   FROM sales
                   WHERE __meta_partition = 1
                   ORDER BY amount";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        // DataFusion may return multiple batches
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);

        // Check first batch
        let batch = &result[0];
        assert!(batch.num_rows() > 0);
        assert_eq!(batch.num_columns(), 5);
    }

    #[tokio::test]
    async fn test_metadata_sql_filter_rows_with_ext_metadata() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        // Create rows with different extended metadata
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["A", "B", "A"])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://products").unwrap();
        let batch = metadata::with_partition(batch, 0).unwrap();

        // Add different extended metadata per row
        let mut ext_meta1 = HashMap::new();
        ext_meta1.insert("flag".to_string(), "important".to_string());

        let mut ext_meta2 = HashMap::new();
        ext_meta2.insert("flag".to_string(), "normal".to_string());

        let mut ext_meta3 = HashMap::new();
        ext_meta3.insert("flag".to_string(), "important".to_string());
        ext_meta3.insert("featured".to_string(), "true".to_string());

        let batch =
            metadata::with_ext_metadata_per_row(batch, &[ext_meta1, ext_meta2, ext_meta3]).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("products", batch).unwrap();

        // Filter by data column and select __meta_ext
        let sql = "SELECT id, category, __meta_ext
                   FROM products
                   WHERE category = 'A'
                   ORDER BY id";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2); // 2 rows with category = 'A'
        assert_eq!(batch.num_columns(), 3);

        // Verify rows with __meta_ext exist
        assert!(batch.column_by_name("__meta_ext").is_some());
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_filtering_limitations() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![100, 200])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://test").unwrap();
        let batch = metadata::with_partition(batch, 0).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("key1".to_string(), "value1".to_string());
        ext_meta.insert("key2".to_string(), "value2".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("test_data", batch).unwrap();

        // Test 1: Can select __meta_ext
        let sql = "SELECT id, __meta_ext FROM test_data";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();
        assert_eq!(result[0].num_rows(), 2);

        // Test 2: Can filter by core metadata while selecting __meta_ext
        let sql = "SELECT id, __meta_ext FROM test_data WHERE __meta_partition = 0";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();
        assert_eq!(result[0].num_rows(), 2);

        // Test 3: Verify __meta_ext column structure
        let batch = &result[0];
        if let Some(ext_col) = batch.column_by_name("__meta_ext") {
            let map_array = ext_col.as_any().downcast_ref::<MapArray>().unwrap();
            // MapArray has structure: offsets, keys, values
            assert_eq!(map_array.len(), 2);
        }
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_where_is_not_null() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    100, 200, 300,
                ])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://test").unwrap();
        let batch = metadata::with_partition(batch, 1).unwrap();

        // Add extended metadata to all rows
        let mut ext_meta = HashMap::new();
        ext_meta.insert("env".to_string(), "prod".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("test_table", batch).unwrap();

        // Filter: __meta_ext IS NOT NULL
        let sql = "SELECT id, value, __meta_ext
                   FROM test_table
                   WHERE __meta_ext IS NOT NULL";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        // All rows have __meta_ext
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_filter_by_combined_condition() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("priority", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    1, 2, 3, 4, 5,
                ])),
                Arc::new(StringArray::from(vec![
                    "active", "pending", "active", "done", "active",
                ])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    1, 2, 1, 3, 2,
                ])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://tasks").unwrap();
        let batch = metadata::with_partition(batch, 2).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("team".to_string(), "backend".to_string());
        ext_meta.insert("sprint".to_string(), "23".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("tasks", batch).unwrap();

        // Complex WHERE: combine data columns, core metadata, and __meta_ext presence
        let sql = "SELECT id, status, priority, __meta_ext
                   FROM tasks
                   WHERE status = 'active'
                   AND priority = 1
                   AND __meta_partition = 2
                   AND __meta_ext IS NOT NULL
                   ORDER BY id";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2); // 2 active tasks with priority 1
        assert_eq!(batch.num_columns(), 4);
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_in_subquery_filter() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    50, 150, 250, 350,
                ])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://orders").unwrap();
        let batch = metadata::with_partition(batch, 0).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("region".to_string(), "us-west".to_string());
        ext_meta.insert("version".to_string(), "v2".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("orders", batch).unwrap();

        // Use __meta_ext in subquery WHERE clause
        let sql = "SELECT * FROM (
                   SELECT id, amount, __meta_ext
                   FROM orders
                   WHERE __meta_ext IS NOT NULL
                 ) AS with_meta
                 WHERE amount >= 100
                 ORDER BY amount";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3); // 3 rows with amount >= 100
        assert_eq!(batch.num_columns(), 3);
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_where_with_or() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://items").unwrap();
        let batch = metadata::with_partition(batch, 1).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("type".to_string(), "premium".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("items", batch).unwrap();

        // WHERE with OR: __meta_ext IS NOT NULL OR other conditions
        let sql = "SELECT id, value, __meta_ext
                   FROM items
                   WHERE (__meta_ext IS NOT NULL AND value >= 20)
                      OR __meta_partition = 0
                   ORDER BY id";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        // First condition matches rows 2,3 (value >= 20), partition=0 doesn't match any
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_where_with_exists() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["A", "B", "A"])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://products").unwrap();
        let batch = metadata::with_partition(batch, 0).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("active".to_string(), "true".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("products", batch).unwrap();

        // Use __meta_ext in WHERE clause (check if not null)
        let sql = "SELECT id, category, __meta_ext
                   FROM products
                   WHERE __meta_ext IS NOT NULL
                   AND category = 'A'
                   ORDER BY id";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2); // 2 rows with category = 'A'
        assert_eq!(batch.num_columns(), 3);
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_where_in_having() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    100, 200, 300, 400,
                ])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://sales").unwrap();
        let batch = metadata::with_partition(batch, 1).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("region".to_string(), "north".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("sales", batch).unwrap();

        // Use __meta_ext in HAVING clause
        let sql = "SELECT
                   __meta_partition,
                   COUNT(*) as count,
                   SUM(amount) as total
                   FROM sales
                   WHERE __meta_ext IS NOT NULL
                   GROUP BY __meta_partition
                   HAVING SUM(amount) > 500";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_with_case_in_where() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://metrics").unwrap();
        let batch = metadata::with_partition(batch, 5).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("unit".to_string(), "bytes".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("metrics", batch).unwrap();

        // CASE in WHERE clause with __meta_ext
        let sql = "SELECT id, value, __meta_ext
                   FROM metrics
                   WHERE CASE
                     WHEN __meta_partition = 5 THEN true
                     ELSE false
                   END = true
                   AND __meta_ext IS NOT NULL
                   ORDER BY id";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_where_between_and_like() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "David"])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    85, 90, 95, 80,
                ])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://users").unwrap();
        let batch = metadata::with_partition(batch, 0).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("role".to_string(), "admin".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("users", batch).unwrap();

        // WHERE with BETWEEN, LIKE, and __meta_ext
        let sql = "SELECT id, name, score, __meta_ext
                   FROM users
                   WHERE score BETWEEN 80 AND 95
                   AND name LIKE 'A%'
                   AND __meta_ext IS NOT NULL
                   ORDER BY score";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 1); // Only Alice (score 85, name starts with 'A')
        assert_eq!(batch.num_columns(), 4);
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_where_with_in_clause() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("status", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    1, 2, 3, 4, 5,
                ])),
                Arc::new(StringArray::from(vec![
                    "active", "pending", "done", "active", "pending",
                ])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://jobs").unwrap();
        let batch = metadata::with_partition(batch, 3).unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("queue".to_string(), "high-priority".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("jobs", batch).unwrap();

        // WHERE with IN clause and __meta_ext
        let sql = "SELECT id, status, __meta_ext
                   FROM jobs
                   WHERE status IN ('active', 'done')
                   AND __meta_partition IN (0, 3)
                   AND __meta_ext IS NOT NULL
                   ORDER BY id";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        // 3 active/done jobs (id 1, 3, 4)
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);
    }

    #[tokio::test]
    async fn test_metadata_sql_ext_metadata_join_with_where() {
        use datafusion::prelude::*;
        use std::collections::HashMap;

        let schema1 = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, false),
        ]));

        let schema2 = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int32, false),
            Field::new("limit", DataType::Int32, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema1,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![100, 200])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema2,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![10, 20])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![500, 1000])),
            ],
        )
        .unwrap();

        // Add core and extended metadata to batch1
        let mut ext_meta1 = HashMap::new();
        ext_meta1.insert("source".to_string(), "api".to_string());
        let batch1 = metadata::with_source(batch1, "kafka://orders").unwrap();
        let batch1 = metadata::with_partition(batch1, 0).unwrap();
        let batch1 = metadata::with_ext_metadata(batch1, &ext_meta1).unwrap();

        // Add core and extended metadata to batch2
        let mut ext_meta2 = HashMap::new();
        ext_meta2.insert("type".to_string(), "premium".to_string());
        let batch2 = metadata::with_source(batch2, "kafka://users").unwrap();
        let batch2 = metadata::with_partition(batch2, 0).unwrap();
        let batch2 = metadata::with_ext_metadata(batch2, &ext_meta2).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("orders", batch1).unwrap();
        ctx.register_batch("users", batch2).unwrap();

        // JOIN with WHERE clause filtering on __meta_ext
        let sql = "SELECT o.order_id, o.amount, o.__meta_ext as order_meta
                   FROM orders o
                   INNER JOIN users u ON o.__meta_partition = u.__meta_partition
                   WHERE o.__meta_ext IS NOT NULL
                   AND u.__meta_ext IS NOT NULL
                   ORDER BY o.order_id";
        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 4); // Cross join: 2 x 2 = 4
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn test_access_ext_metadata_map_structure() {
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                1, 2, 3,
            ]))],
        )
        .unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("env".to_string(), "production".to_string());
        ext_meta.insert("version".to_string(), "2.0".to_string());
        ext_meta.insert("region".to_string(), "us-west".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        // Access the MapArray column
        let ext_col = batch.column_by_name("__meta_ext").unwrap();
        let map_array = ext_col.as_any().downcast_ref::<MapArray>().unwrap();

        // Verify MapArray structure
        assert_eq!(map_array.len(), 3); // 3 rows

        // Check offsets (n_rows + 1 offsets)
        let offsets = map_array.offsets();
        assert_eq!(offsets.len(), 4); // 3 rows + 1
        assert_eq!(offsets[0], 0); // First offset is always 0

        // Each row has 3 key-value pairs (offsets are cumulative)
        assert_eq!(offsets[1] - offsets[0], 3); // Row 0 has 3 entries
        assert_eq!(offsets[2] - offsets[1], 3); // Row 1 has 3 entries
        assert_eq!(offsets[3] - offsets[2], 3); // Row 2 has 3 entries
        assert_eq!(offsets[3], 9); // Total of 9 key-value pairs across all rows
    }

    #[test]
    fn test_access_ext_metadata_keys_and_values() {
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                1,
            ]))],
        )
        .unwrap();

        // Add extended metadata with multiple keys
        let mut ext_meta = HashMap::new();
        ext_meta.insert("key1".to_string(), "value1".to_string());
        ext_meta.insert("key2".to_string(), "value2".to_string());
        ext_meta.insert("key3".to_string(), "value3".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        // Access MapArray and its internal structure
        let ext_col = batch.column_by_name("__meta_ext").unwrap();
        let map_array = ext_col.as_any().downcast_ref::<MapArray>().unwrap();

        // Get the keys and values arrays (StructArray)
        let entries = map_array.entries();
        let keys_col = entries.column(0); // First column is keys
        let values_col = entries.column(1); // Second column is values

        let keys_array = keys_col.as_any().downcast_ref::<StringArray>().unwrap();
        let values_array = values_col.as_any().downcast_ref::<StringArray>().unwrap();

        // Verify we have 3 key-value pairs
        assert_eq!(keys_array.len(), 3);
        assert_eq!(values_array.len(), 3);

        // Build a HashMap from the array to verify key-value pairs (order-independent)
        let mut kv_map = HashMap::new();
        for i in 0..keys_array.len() {
            let key = keys_array.value(i);
            let value = values_array.value(i);
            kv_map.insert(key.to_string(), value.to_string());
        }

        // Verify all key-value pairs are present and correct
        assert_eq!(kv_map.get("key1"), Some(&"value1".to_string()));
        assert_eq!(kv_map.get("key2"), Some(&"value2".to_string()));
        assert_eq!(kv_map.get("key3"), Some(&"value3".to_string()));
    }

    #[test]
    fn test_access_ext_metadata_per_row() {
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                1, 2, 3,
            ]))],
        )
        .unwrap();

        // Add different extended metadata per row
        let mut meta1 = HashMap::new();
        meta1.insert("type".to_string(), "A".to_string());

        let mut meta2 = HashMap::new();
        meta2.insert("type".to_string(), "B".to_string());
        meta2.insert("extra".to_string(), "true".to_string());

        let mut meta3 = HashMap::new();
        meta3.insert("type".to_string(), "C".to_string());

        let batch = metadata::with_ext_metadata_per_row(batch, &[meta1, meta2, meta3]).unwrap();

        // Access MapArray
        let ext_col = batch.column_by_name("__meta_ext").unwrap();
        let map_array = ext_col.as_any().downcast_ref::<MapArray>().unwrap();

        let offsets = map_array.offsets();

        // Row 0: 1 key-value pair
        assert_eq!(offsets[1] - offsets[0], 1);

        // Row 1: 2 key-value pairs
        assert_eq!(offsets[2] - offsets[1], 2);

        // Row 2: 1 key-value pair
        assert_eq!(offsets[3] - offsets[2], 1);
    }

    #[test]
    fn test_access_ext_metadata_extract_value_by_key() {
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                1,
            ]))],
        )
        .unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("priority".to_string(), "high".to_string());
        ext_meta.insert("status".to_string(), "active".to_string());
        ext_meta.insert("count".to_string(), "42".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        // Access and extract values
        let ext_col = batch.column_by_name("__meta_ext").unwrap();
        let map_array = ext_col.as_any().downcast_ref::<MapArray>().unwrap();

        let entries = map_array.entries();
        let keys_array = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values_array = entries
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // Find and extract specific key-value pairs
        let mut found_priority = false;
        let mut found_count = false;

        for i in 0..keys_array.len() {
            let key = keys_array.value(i);
            let value = values_array.value(i);

            match key {
                "priority" => {
                    assert_eq!(value, "high");
                    found_priority = true;
                }
                "count" => {
                    assert_eq!(value, "42");
                    found_count = true;
                }
                _ => {}
            }
        }

        assert!(found_priority, "Should find priority key");
        assert!(found_count, "Should find count key");
    }

    #[test]
    fn test_access_ext_metadata_filter_by_map_content() {
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        // Add extended metadata with different "region" values per row
        let mut meta1 = HashMap::new();
        meta1.insert("region".to_string(), "us-west".to_string());

        let mut meta2 = HashMap::new();
        meta2.insert("region".to_string(), "us-east".to_string());

        let mut meta3 = HashMap::new();
        meta3.insert("region".to_string(), "us-west".to_string());

        let batch = metadata::with_ext_metadata_per_row(batch, &[meta1, meta2, meta3]).unwrap();

        // Filter rows where region == "us-west"
        let ext_col = batch.column_by_name("__meta_ext").unwrap();
        let map_array = ext_col.as_any().downcast_ref::<MapArray>().unwrap();

        let entries = map_array.entries();
        let keys_array = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values_array = entries
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let offsets = map_array.offsets();
        let mut matching_rows = Vec::new();

        for row_idx in 0..map_array.len() {
            let start = offsets[row_idx] as usize;
            let end = offsets[row_idx + 1] as usize;

            for i in start..end {
                let key = keys_array.value(i);
                let value = values_array.value(i);

                if key == "region" && value == "us-west" {
                    matching_rows.push(row_idx);
                    break;
                }
            }
        }

        assert_eq!(matching_rows, vec![0, 2]); // Rows 0 and 2 have region "us-west"
    }

    #[test]
    fn test_access_ext_metadata_empty_map() {
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                1, 2,
            ]))],
        )
        .unwrap();

        // Add empty extended metadata
        let empty_meta = HashMap::new();
        let batch = metadata::with_ext_metadata(batch, &empty_meta).unwrap();

        // Access MapArray
        let ext_col = batch.column_by_name("__meta_ext").unwrap();
        let map_array = ext_col.as_any().downcast_ref::<MapArray>().unwrap();

        // Verify empty maps
        assert_eq!(map_array.len(), 2);

        let offsets = map_array.offsets();
        // Both rows should have 0 entries
        assert_eq!(offsets[1] - offsets[0], 0);
        assert_eq!(offsets[2] - offsets[1], 0);
    }

    #[test]
    fn test_access_ext_metadata_transform_to_hashmap() {
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                1, 2,
            ]))],
        )
        .unwrap();

        // Add different extended metadata per row
        let mut meta1 = HashMap::new();
        meta1.insert("a".to_string(), "1".to_string());
        meta1.insert("b".to_string(), "2".to_string());

        let mut meta2 = HashMap::new();
        meta2.insert("c".to_string(), "3".to_string());

        let batch = metadata::with_ext_metadata_per_row(batch, &[meta1, meta2]).unwrap();

        // Transform each row's MapArray to HashMap
        let ext_col = batch.column_by_name("__meta_ext").unwrap();
        let map_array = ext_col.as_any().downcast_ref::<MapArray>().unwrap();

        let entries = map_array.entries();
        let keys_array = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values_array = entries
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let offsets = map_array.offsets();
        let mut row_maps: Vec<HashMap<String, String>> = Vec::new();

        for row_idx in 0..map_array.len() {
            let mut row_map = HashMap::new();
            let start = offsets[row_idx] as usize;
            let end = offsets[row_idx + 1] as usize;

            for i in start..end {
                let key = keys_array.value(i).to_string();
                let value = values_array.value(i).to_string();
                row_map.insert(key, value);
            }

            row_maps.push(row_map);
        }

        // Verify transformed data
        assert_eq!(row_maps.len(), 2);
        assert_eq!(row_maps[0].len(), 2); // Row 0 has 2 keys
        assert_eq!(row_maps[0].get("a"), Some(&"1".to_string()));
        assert_eq!(row_maps[0].get("b"), Some(&"2".to_string()));
        assert_eq!(row_maps[1].len(), 1); // Row 1 has 1 key
        assert_eq!(row_maps[1].get("c"), Some(&"3".to_string()));
    }

    #[test]
    fn test_access_ext_metadata_with_data_types() {
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                1,
            ]))],
        )
        .unwrap();

        // Add extended metadata with various data types (as strings)
        let mut ext_meta = HashMap::new();
        ext_meta.insert("string_val".to_string(), "hello".to_string());
        ext_meta.insert("number_as_string".to_string(), "12345".to_string());
        ext_meta.insert("bool_as_string".to_string(), "true".to_string());
        ext_meta.insert(
            "json_as_string".to_string(),
            r#"{"key":"value"}"#.to_string(),
        );
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        // Access and verify different types
        let ext_col = batch.column_by_name("__meta_ext").unwrap();
        let map_array = ext_col.as_any().downcast_ref::<MapArray>().unwrap();

        let entries = map_array.entries();
        let keys_array = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values_array = entries
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let mut found_types: HashMap<&str, &str> = HashMap::new();

        for i in 0..keys_array.len() {
            let key = keys_array.value(i);
            let value = values_array.value(i);
            found_types.insert(key, value);
        }

        assert_eq!(found_types.get("string_val"), Some(&"hello"));
        assert_eq!(found_types.get("number_as_string"), Some(&"12345"));
        assert_eq!(found_types.get("bool_as_string"), Some(&"true"));
        assert_eq!(
            found_types.get("json_as_string"),
            Some(&r#"{"key":"value"}"#)
        );
    }

    #[test]
    fn test_access_ext_metadata_check_key_exists() {
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                1, 2,
            ]))],
        )
        .unwrap();

        // Row 1 has "special_key", row 2 doesn't
        let mut meta1 = HashMap::new();
        meta1.insert("special_key".to_string(), "present".to_string());
        meta1.insert("other".to_string(), "value".to_string());

        let mut meta2 = HashMap::new();
        meta2.insert("other".to_string(), "value".to_string());

        let batch = metadata::with_ext_metadata_per_row(batch, &[meta1, meta2]).unwrap();

        // Check which rows have "special_key"
        let ext_col = batch.column_by_name("__meta_ext").unwrap();
        let map_array = ext_col.as_any().downcast_ref::<MapArray>().unwrap();

        let entries = map_array.entries();
        let keys_array = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let offsets = map_array.offsets();

        let mut has_special_key = vec![false; 2];

        for row_idx in 0..map_array.len() {
            let start = offsets[row_idx] as usize;
            let end = offsets[row_idx + 1] as usize;

            for i in start..end {
                if keys_array.value(i) == "special_key" {
                    has_special_key[row_idx] = true;
                    break;
                }
            }
        }

        assert_eq!(has_special_key, vec![true, false]); // Only row 0 has the key
    }

    #[test]
    fn test_access_ext_metadata_aggregate_values() {
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                1, 2, 3,
            ]))],
        )
        .unwrap();

        // Add extended metadata with numeric values as strings
        let mut meta1 = HashMap::new();
        meta1.insert("count".to_string(), "10".to_string());

        let mut meta2 = HashMap::new();
        meta2.insert("count".to_string(), "20".to_string());

        let mut meta3 = HashMap::new();
        meta3.insert("count".to_string(), "30".to_string());

        let batch = metadata::with_ext_metadata_per_row(batch, &[meta1, meta2, meta3]).unwrap();

        // Aggregate "count" values across rows
        let ext_col = batch.column_by_name("__meta_ext").unwrap();
        let map_array = ext_col.as_any().downcast_ref::<MapArray>().unwrap();

        let entries = map_array.entries();
        let keys_array = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values_array = entries
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let offsets = map_array.offsets();
        let mut total_count = 0;

        for row_idx in 0..map_array.len() {
            let start = offsets[row_idx] as usize;
            let end = offsets[row_idx + 1] as usize;

            for i in start..end {
                if keys_array.value(i) == "count" {
                    let value = values_array.value(i);
                    if let Ok(num) = value.parse::<i32>() {
                        total_count += num;
                    }
                    break;
                }
            }
        }

        assert_eq!(total_count, 60); // 10 + 20 + 30
    }

    #[test]
    fn test_access_ext_metadata_merge_multiple_maps() {
        use std::collections::HashMap;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![100, 200])),
            ],
        )
        .unwrap();

        // Add core metadata
        let batch = metadata::with_source(batch, "kafka://test").unwrap();

        // Add extended metadata
        let mut ext_meta = HashMap::new();
        ext_meta.insert("ext1".to_string(), "a".to_string());
        ext_meta.insert("ext2".to_string(), "b".to_string());
        let batch = metadata::with_ext_metadata(batch, &ext_meta).unwrap();

        // Access both core and extended metadata
        let source_col = batch.column_by_name("__meta_source").unwrap();
        let source_array = source_col.as_any().downcast_ref::<StringArray>().unwrap();

        let ext_col = batch.column_by_name("__meta_ext").unwrap();
        let map_array = ext_col.as_any().downcast_ref::<MapArray>().unwrap();

        // Verify both are accessible
        assert_eq!(source_array.len(), 2);
        assert_eq!(source_array.value(0), "kafka://test");
        assert_eq!(map_array.len(), 2);
    }

    #[test]
    fn test_ext_metadata_scalar() {
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
