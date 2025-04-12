//! Rust stream processing engine

use datafusion::arrow::array::{Array, ArrayRef, BinaryArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::data_type::AsBytes;
use serde::Serialize;
use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use thiserror::Error;

pub mod buffer;
pub mod cli;
pub mod config;
pub mod engine;
pub mod input;
pub mod output;
pub mod pipeline;
pub mod processor;

pub mod stream;

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

pub type Bytes = Vec<u8>;

/// Represents a message in a stream processing engine.
#[derive(Clone, Debug)]
pub struct MessageBatch(RecordBatch);

impl MessageBatch {
    pub fn new_binary(content: Vec<Bytes>) -> Result<Self, Error> {
        let fields = vec![Field::new(
            DEFAULT_BINARY_VALUE_FIELD,
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

        Ok(Self(batch))
    }
    /// Creates a new MessageBatch by appending a binary column to the current batch.
    ///
    /// The method clones the existing schema and columns from the MessageBatch, then adds an additional
    /// binary column constructed from the provided vector of byte arrays. The new binary column is assigned
    /// the default field name defined by `DEFAULT_BINARY_VALUE_FIELD`. It returns a new MessageBatch instance
    /// that includes both the original columns and the newly appended binary column, or an Error if the
    /// construction of the underlying RecordBatch fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Arc;
    /// use arrow::datatypes::{Field, DataType, Schema};
    /// use arrow::array::{ArrayRef, Int32Array};
    /// use arrow::record_batch::RecordBatch;
    /// use arkflow_core::{MessageBatch, Bytes, DEFAULT_BINARY_VALUE_FIELD, Error};
    ///
    /// // Create a dummy record batch with a single integer column.
    /// let schema = Arc::new(Schema::new(vec![
    ///     Arc::new(Field::new("col1", DataType::Int32, false))
    /// ]));
    /// let int_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
    /// let record_batch = RecordBatch::try_new(schema.clone(), vec![int_array]).unwrap();
    /// let msg_batch = MessageBatch::new_arrow(record_batch);
    ///
    /// // Append a binary column using a vector of byte arrays.
    /// let binary_values: Vec<Bytes> = vec![vec![65, 66], vec![67, 68], vec![69, 70]];
    /// let new_msg_batch = msg_batch.new_binary_with_origin(binary_values).unwrap();
    ///
    /// // The new MessageBatch should have one more column than the original.
    /// assert_eq!(new_msg_batch.num_columns(), msg_batch.num_columns() + 1);
    /// ```
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

    /// Filters the columns of the MessageBatch to include only those with names present in the specified set.
    ///
    /// This method iterates over the underlying record batch and selects columns whose field names are contained in the
    /// provided HashSet. A new MessageBatch is constructed with a schema that reflects only the filtered columns. An error
    /// is returned if the creation of the Arrow record batch fails.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::HashSet;
    ///
    /// // Assume `message_batch` is an existing MessageBatch with fields "id", "name", and "value".
    /// let mut message_batch = /* initialization of MessageBatch */;
    ///
    /// let mut fields_to_include = HashSet::new();
    /// fields_to_include.insert("id".to_string());
    /// fields_to_include.insert("value".to_string());
    ///
    /// let filtered_batch = message_batch.filter_columns(&fields_to_include);
    /// assert!(filtered_batch.is_ok());
    ///
    /// let filtered_batch = filtered_batch.unwrap();
    /// // Verify that the filtered batch's schema contains only the specified fields.
    /// assert_eq!(filtered_batch.schema().fields().len(), 2);
    /// ```
    pub fn filter_columns(
        &mut self,
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

    /// Creates a MessageBatch from a serializable value by converting it to JSON bytes.
    ///
    /// This function serializes the provided value using `serde_json::to_vec` and then wraps
    /// the resulting JSON byte vector into a new binary MessageBatch via `new_binary`.
    /// Any errors during serialization or batch creation are propagated.
    ///
    /// # Arguments
    ///
    /// * `value` - A reference to a value that implements the `Serialize` trait.
    ///
    /// # Returns
    ///
    /// A `Result` containing the constructed MessageBatch or an `Error` if the conversion fails.
    ///
    /// # Examples
    ///
    /// ```
    /// use serde::Serialize;
    /// 
    /// #[derive(Serialize)]
    /// struct TestData {
    ///     name: String,
    ///     value: i32,
    /// }
    /// 
    /// let data = TestData { name: "example".to_string(), value: 42 };
    /// let batch = MessageBatch::from_json(&data).unwrap();
    /// // The `batch` now contains a binary representation of the JSON-serialized data.
    /// ```
    pub fn from_json<T: Serialize>(value: &T) -> Result<Self, Error> {
        let content = serde_json::to_vec(value)?;
        Ok(Self::new_binary(vec![content])?)
    }

    pub fn new_arrow(content: RecordBatch) -> Self {
        Self(content)
    }

    /// Create a message from a string.
    pub fn from_string(content: &str) -> Result<Self, Error> {
        Self::new_binary(vec![content.as_bytes().to_vec()])
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.0.num_rows()
    }

    pub fn to_binary(&self, name: &str) -> Result<Vec<&[u8]>, Error> {
        let Some(array_ref) = self.0.column_by_name(name) else {
            return Err(Error::Process("not found column".to_string()));
        };

        let data = array_ref.to_data();

        if *data.data_type() != DataType::Binary {
            return Err(Error::Process("not support data type".to_string()));
        }

        let Some(v) = array_ref.as_any().downcast_ref::<BinaryArray>() else {
            return Err(Error::Process("not support data type".to_string()));
        };
        let mut vec_bytes = vec![];
        for x in v {
            if let Some(data) = x {
                vec_bytes.push(data)
            }
        }
        Ok(vec_bytes)
    }
}

impl Deref for MessageBatch {
    type Target = RecordBatch;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<RecordBatch> for MessageBatch {
    fn from(batch: RecordBatch) -> Self {
        Self(batch)
    }
}

impl From<MessageBatch> for RecordBatch {
    fn from(batch: MessageBatch) -> Self {
        batch.0
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
        &mut self.0
    }
}

pub const DEFAULT_BINARY_VALUE_FIELD: &str = "__value__";
