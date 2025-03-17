//! File output component
//!
//! Output the processed data to a file

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatch};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

/// File output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileOutputConfig {
    /// Output file path
    pub path: String,
    /// Whether to add a newline after each message
    pub append_newline: Option<bool>,
    /// Whether to append to the end of the file (instead of overwriting)
    pub append: Option<bool>,
}

/// File output component
pub struct FileOutput {
    config: FileOutputConfig,
    writer: Arc<Mutex<Option<File>>>,
    connected: AtomicBool,
}

impl FileOutput {
    /// Create a new file output component
    pub fn new(config: FileOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            writer: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl Output for FileOutput {
    async fn connect(&self) -> Result<(), Error> {
        let path = Path::new(&self.config.path);

        // Make sure the directory exists
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(Error::Io)?
            }
        }
        let append = self.config.append.unwrap_or(true);
        // Open the file
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(append)
            .truncate(!append)
            .open(path)
            .map_err(Error::Io)?;
        let writer_arc = self.writer.clone();
        let mut writer_arc_guard = writer_arc.lock().await;
        writer_arc_guard.replace(file);
        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn write(&self, msg: &MessageBatch) -> Result<(), Error> {
        let writer_arc = self.writer.clone();
        let writer_arc_guard = writer_arc.lock().await;
        if !self.connected.load(Ordering::SeqCst) || writer_arc_guard.is_none() {
            return Err(Error::Connection("The output is not connected".to_string()));
        }

        let content = msg.as_string()?;
        let writer = writer_arc_guard.as_ref();
        let mut file =
            writer.ok_or(Error::Connection("The output is not connected".to_string()))?;

        for x in content {
            if self.config.append_newline.unwrap_or(true) {
                writeln!(file, "{}", x).map_err(Error::Io)?
            } else {
                write!(file, "{}", x).map_err(Error::Io)?
            }
        }

        file.flush().map_err(Error::Io)?;
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        self.connected.store(false, Ordering::SeqCst);
        let writer_arc = self.writer.clone();
        let mut writer_arc_mutex_guard = writer_arc.lock().await;
        *writer_arc_mutex_guard = None;
        Ok(())
    }
}

pub(crate) struct FileOutputBuilder;
impl OutputBuilder for FileOutputBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "File output configuration is missing".to_string(),
            ));
        }

        let config: FileOutputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(FileOutput::new(config)?))
    }
}

pub fn init() {
    register_output_builder("file", Arc::new(FileOutputBuilder));
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::fs::read_to_string;

    /// Test creating new file and writing content
    #[tokio::test]
    async fn test_create_and_write_to_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");

        let config = FileOutputConfig {
            path: file_path.to_str().unwrap().to_string(),
            append_newline: Some(true),
            append: Some(false),
        };

        let output = FileOutput::new(config).unwrap();
        output.connect().await.unwrap();

        let msg = MessageBatch::from_string("test content");
        output.write(&msg).await.unwrap();
        output.close().await.unwrap();

        let content = read_to_string(&file_path).await.unwrap();
        assert_eq!(content, "test content\n");
    }

    /// Test appending to existing file
    #[tokio::test]
    async fn test_append_mode() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("append.txt");

        let config = FileOutputConfig {
            path: file_path.to_str().unwrap().to_string(),
            append_newline: Some(false),
            append: Some(true),
        };

        let output = FileOutput::new(config).unwrap();
        output.connect().await.unwrap();

        // First write
        output
            .write(&MessageBatch::from_string("first"))
            .await
            .unwrap();
        // Second write
        output
            .write(&MessageBatch::from_string("second"))
            .await
            .unwrap();
        output.close().await.unwrap();

        let content = read_to_string(&file_path).await.unwrap();
        assert_eq!(content, "firstsecond");
    }

    /// Test newline configuration handling
    #[tokio::test]
    async fn test_newline_configuration() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("newline.txt");

        let config = FileOutputConfig {
            path: file_path.to_str().unwrap().to_string(),
            append_newline: Some(false),
            append: Some(false),
        };

        let output = FileOutput::new(config).unwrap();
        output.connect().await.unwrap();
        output
            .write(&MessageBatch::from_string("no_newline"))
            .await
            .unwrap();
        output.close().await.unwrap();

        let content = read_to_string(&file_path).await.unwrap();
        assert_eq!(content, "no_newline");
    }

    /// Test error handling for invalid directory
    #[tokio::test]
    async fn test_invalid_directory() {
        let config = FileOutputConfig {
            path: "/invalid/path/test.txt".to_string(),
            append_newline: Some(true),
            append: Some(false),
        };

        let output = FileOutput::new(config).unwrap();
        let result = output.connect().await;
        assert!(matches!(result, Err(Error::Io(_))));
    }
}
