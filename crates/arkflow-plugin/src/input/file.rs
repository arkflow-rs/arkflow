//! File Input Component
//!
//! Read data from the file system

use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

use arkflow_core::input::{register_input_builder, Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{Error, MessageBatch};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInputConfig {
    /// Input file path
    pub path: String,
    /// Whether to close after reading is complete
    pub close_on_eof: Option<bool>,
    /// Whether to start reading from the beginning of the file (otherwise start from the end)
    pub start_from_beginning: Option<bool>,
}

/// File input component
pub struct FileInput {
    config: FileInputConfig,
    reader: Arc<Mutex<Option<BufReader<File>>>>,
    connected: AtomicBool,
    eof_reached: AtomicBool,
}

impl FileInput {
    /// Create a new file input component
    pub fn new(config: FileInputConfig) -> Result<Self, Error> {
        Ok(Self {
            config,
            reader: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
            eof_reached: AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl Input for FileInput {
    async fn connect(&self) -> Result<(), Error> {
        let path = Path::new(&self.config.path);

        // Open the file
        let file = File::open(path).map_err(|e| {
            Error::Connection(format!("Unable to open file {}: {}", self.config.path, e))
        })?;

        let mut reader = BufReader::new(file);

        // If it is not read from the beginning, it moves to the end of the file
        if !self.config.start_from_beginning.unwrap_or(true) {
            reader
                .seek(SeekFrom::End(0))
                .map_err(|e| Error::Process(format!("Unable to seek to end of file: {}", e)))?;
        }

        let reader_arc = self.reader.clone();
        reader_arc.lock().await.replace(reader);
        self.connected.store(true, Ordering::SeqCst);
        self.eof_reached.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        let reader_arc = self.reader.clone();
        let mut reader_mutex = reader_arc.lock().await;
        if !self.connected.load(Ordering::SeqCst) || reader_mutex.is_none() {
            return Err(Error::Connection("The input is not connected".to_string()));
        }

        if self.eof_reached.load(Ordering::SeqCst) && self.config.close_on_eof.unwrap_or(true) {
            return Err(Error::EOF);
        }

        let bytes_read;
        let mut line = String::new();
        {
            let reader_mutex = reader_mutex.as_mut();
            if reader_mutex.is_none() {
                return Err(Error::Connection("The input is not connected".to_string()));
            }

            let reader = reader_mutex.unwrap();
            bytes_read = reader.read_line(&mut line).map_err(Error::Io)?;
        }

        if bytes_read == 0 {
            self.eof_reached.store(true, Ordering::SeqCst);

            if self.config.close_on_eof.unwrap_or(true) {
                return Err(Error::EOF);
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            return Err(Error::Process("Wait for new data".to_string()));
        }

        // Remove the trailing line break
        if line.ends_with('\n') {
            line.pop();
            if line.ends_with('\r') {
                line.pop();
            }
        }

        Ok((MessageBatch::from_string(&line), Arc::new(NoopAck)))
    }

    async fn close(&self) -> Result<(), Error> {
        self.connected.store(false, Ordering::SeqCst);
        let reader_arc = self.reader.clone();
        let mut reader_mutex = reader_arc.lock().await;
        *reader_mutex = None;
        Ok(())
    }
}

pub(crate) struct FileInputBuilder;
impl InputBuilder for FileInputBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Input>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "File input configuration is missing".to_string(),
            ));
        }

        let config: FileInputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(FileInput::new(config)?))
    }
}

pub fn init() {
    register_input_builder("file", Arc::new(FileInputBuilder));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_file_input_new() {
        let config = FileInputConfig {
            path: "test.txt".to_string(),
            close_on_eof: Some(true),
            start_from_beginning: Some(true),
        };
        let input = FileInput::new(config);
        assert!(input.is_ok());
    }

    #[tokio::test]
    async fn test_file_input_connect_file_not_exists() {
        let config = FileInputConfig {
            path: "non_existent_file.txt".to_string(),
            close_on_eof: Some(true),
            start_from_beginning: Some(true),
        };
        let input = FileInput::new(config).unwrap();
        let result = input.connect().await;
        assert!(result.is_err());
        match result {
            Err(Error::Connection(_)) => {} // Expected error type
            _ => panic!("Expected Connection error"),
        }
    }

    #[tokio::test]
    async fn test_file_input_read_without_connect() {
        let config = FileInputConfig {
            path: "test.txt".to_string(),
            close_on_eof: Some(true),
            start_from_beginning: Some(true),
        };
        let input = FileInput::new(config).unwrap();
        let result = input.read().await;
        assert!(result.is_err());
        match result {
            Err(Error::Connection(_)) => {} // Expected error type
            _ => panic!("Expected Connection error"),
        }
    }

    #[tokio::test]
    async fn test_file_input_read_from_beginning() {
        // Create temporary directory and file
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        let file_path_str = file_path.to_str().unwrap();

        // Write test data
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "line1").unwrap();
        writeln!(file, "line2").unwrap();
        writeln!(file, "line3").unwrap();
        file.flush().unwrap();

        // Configure to read from the beginning of the file
        let config = FileInputConfig {
            path: file_path_str.to_string(),
            close_on_eof: Some(true),
            start_from_beginning: Some(true),
        };
        let input = FileInput::new(config).unwrap();

        // Connect and read
        assert!(input.connect().await.is_ok());

        // Read the first line
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["line1"]);
        ack.ack().await;

        // Read the second line
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["line2"]);
        ack.ack().await;

        // Read the third line
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["line3"]);
        ack.ack().await;

        // End of file, should return Done error
        let result = input.read().await;
        assert!(matches!(result, Err(Error::EOF)));

        // Close the connection
        assert!(input.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_file_input_read_from_end() {
        // Create temporary directory and file
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        let file_path_str = file_path.to_str().unwrap();

        // Write test data
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "line1").unwrap();
        writeln!(file, "line2").unwrap();
        file.flush().unwrap();

        // Configure to read from the end of the file
        let config = FileInputConfig {
            path: file_path_str.to_string(),
            close_on_eof: Some(true),
            start_from_beginning: Some(false),
        };
        let input = FileInput::new(config).unwrap();

        // Connect
        assert!(input.connect().await.is_ok());

        // Reading from the end, should have no data, return Done error
        let result = input.read().await;
        assert!(matches!(result, Err(Error::EOF)));

        // Append new data
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(&file_path)
            .unwrap();
        writeln!(file, "line3").unwrap();
        file.flush().unwrap();

        // Reconnect
        assert!(input.close().await.is_ok());
        assert!(input.connect().await.is_ok());

        // Now should be able to read the newly added line
        let result = input.read().await;
        assert!(matches!(result, Err(Error::EOF)));

        // Close the connection
        assert!(input.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_file_input_close_on_eof_false() {
        // Create temporary directory and file
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        let file_path_str = file_path.to_str().unwrap();

        // Write test data
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "line1").unwrap();
        file.flush().unwrap();

        // Configure not to close after reading is complete
        let config = FileInputConfig {
            path: file_path_str.to_string(),
            close_on_eof: Some(false),
            start_from_beginning: Some(true),
        };
        let input = FileInput::new(config).unwrap();

        // Connect and read
        assert!(input.connect().await.is_ok());

        // Read the first line
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["line1"]);
        ack.ack().await;

        // End of file, but don't close, should return Processing error
        let result = input.read().await;
        assert!(matches!(result, Err(Error::Process(_))));

        // Append new data
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(&file_path)
            .unwrap();
        writeln!(file, "line2").unwrap();
        file.flush().unwrap();

        // Should be able to read the newly added line
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["line2"]);
        ack.ack().await;

        // Close the connection
        assert!(input.close().await.is_ok());
    }
}
