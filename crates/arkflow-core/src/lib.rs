//! Rust stream processing engine

use datafusion::arrow::array::{ArrayRef, BinaryArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use serde::Serialize;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use thiserror::Error;

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

    #[error("Arrow error: {0}")]
    Arrow(String),

    #[error("Unknown error: {0}")]
    Unknown(String),

    #[error("EOF")]
    EOF,
}

pub type Bytes = Vec<u8>;

/// Represents a message in a stream processing engine.

#[derive(Clone, Debug)]
pub struct MessageBatch(RecordBatch);

#[derive(Clone, Debug)]
pub enum MessageType {
    Binary,
    Arrow,
}

impl MessageBatch {
    pub fn new_binary(content: Vec<Bytes>) -> Self {
        let fields = vec![Field::new("value", DataType::Binary, false)];
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(content.len());

        for x in content {
            let array = BinaryArray::from_vec(vec![&x]);
            columns.push(Arc::new(array))
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| Error::Process(format!("创建Arrow记录批次失败: {}", e)))
            .unwrap();

        Self(batch)
    }
    pub fn from_json<T: Serialize>(value: &T) -> Result<Self, Error> {
        let content = serde_json::to_vec(value)?;
        Ok(Self::new_binary(vec![content]))
    }
    pub fn new_arrow(content: RecordBatch) -> Self {
        Self(content)
    }

    /// Create a message from a string.
    pub fn from_string(content: &str) -> Self {
        Self::new_binary(vec![content.as_bytes().to_vec()])
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn len(&self) -> usize {
        self.0.num_rows()
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

impl From<Vec<Bytes>> for MessageBatch {
    fn from(content: Vec<Bytes>) -> Self {
        Self::new_binary(content)
    }
}
impl From<Vec<String>> for MessageBatch {
    fn from(content: Vec<String>) -> Self {
        Self::new_binary(content.into_iter().map(|s| s.into_bytes()).collect())
    }
}
impl From<Vec<&str>> for MessageBatch {
    fn from(content: Vec<&str>) -> Self {
        Self::new_binary(content.into_iter().map(|s| s.as_bytes().to_vec()).collect())
    }
}

impl DerefMut for MessageBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
