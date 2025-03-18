//! Rust stream processing engine

use datafusion::arrow::record_batch::RecordBatch;
use serde::Serialize;
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

    #[error("Unknown error: {0}")]
    Unknown(String),

    #[error("EOF")]
    EOF,
}

pub type Bytes = Vec<u8>;

/// Represents a message in a stream processing engine.

#[derive(Clone, Debug)]
pub struct MessageBatch {
    /// Message content
    pub content: Content,
}

#[derive(Clone, Debug)]
pub enum Content {
    Arrow(RecordBatch),
    Binary(Vec<Bytes>),
}

impl MessageBatch {
    pub fn new_binary(content: Vec<Bytes>) -> Self {
        Self {
            content: Content::Binary(content),
        }
    }
    pub fn from_json<T: Serialize>(value: &T) -> Result<Self, Error> {
        let content = serde_json::to_vec(value)?;
        Ok(Self::new_binary(vec![content]))
    }
    pub fn new_arrow(content: RecordBatch) -> Self {
        Self {
            content: Content::Arrow(content),
        }
    }

    /// Create a message from a string.
    pub fn from_string(content: &str) -> Self {
        Self::new_binary(vec![content.as_bytes().to_vec()])
    }

    /// Parse the message content into a string.
    pub fn as_string(&self) -> Result<Vec<String>, Error> {
        match &self.content {
            Content::Arrow(_) => Err(Error::Process("无法解析为JSON".to_string())),
            Content::Binary(v) => {
                let x: Result<Vec<String>, Error> = v
                    .iter()
                    .map(|v| {
                        String::from_utf8(v.clone())
                            .map_err(|_| Error::Process("无法解析为字符串".to_string()))
                    })
                    .collect();
                Ok(x?)
            }
        }
    }
    /// Get the binary content of the message.
    pub fn as_binary(&self) -> &Vec<Bytes> {
        match &self.content {
            Content::Arrow(_) => panic!("Cannot get binary content from Arrow message"),
            Content::Binary(v) => v,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn len(&self) -> usize {
        match &self.content {
            Content::Arrow(v) => v.num_rows(),
            Content::Binary(v) => v.len(),
        }
    }
}
