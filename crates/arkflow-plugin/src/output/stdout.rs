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

//! Standard output components
//!
//! Outputs the processed data to standard output

use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatch};
use async_trait::async_trait;
use datafusion::arrow;
use serde::{Deserialize, Serialize};
use std::io::{self, Stdout, Write};
use std::string::String;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Standard output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StdoutOutputConfig {
    /// Whether to add a line break after each message
    pub append_newline: Option<bool>,
}

/// Standard output components
struct StdoutOutput<T> {
    config: StdoutOutputConfig,
    writer: Mutex<T>,
}

impl<T: StdWriter> StdoutOutput<T> {
    /// Create a new standard output component
    pub fn new(config: StdoutOutputConfig, writer: T) -> Result<Self, Error> {
        Ok(Self {
            config,
            writer: Mutex::new(writer),
        })
    }
}

#[async_trait]
impl<T> Output for StdoutOutput<T>
where
    T: StdWriter,
{
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn write(&self, msg: MessageBatch) -> Result<(), Error> {
        self.arrow_stdout(msg).await
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}
impl<T: StdWriter> StdoutOutput<T> {
    async fn arrow_stdout(&self, message_batch: MessageBatch) -> Result<(), Error> {
        let mut writer_std = self.writer.lock().await;

        // Use Arrow's JSON serialization functionality
        let mut buf = Vec::new();
        let mut writer = arrow::json::ArrayWriter::new(&mut buf);
        writer
            .write(&message_batch)
            .map_err(|e| Error::Process(format!("Arrow JSON serialization error: {}", e)))?;
        writer
            .finish()
            .map_err(|e| Error::Process(format!("Arrow JSON serialization error: {}", e)))?;
        let s = String::from_utf8_lossy(&buf);

        if self.config.append_newline.unwrap_or(true) {
            writeln!(writer_std, "{}", s).map_err(Error::Io)?
        } else {
            write!(writer_std, "{}", s).map_err(Error::Io)?
        }

        writer_std.flush().map_err(Error::Io)?;
        Ok(())
    }
}

pub(crate) struct StdoutOutputBuilder;
impl OutputBuilder for StdoutOutputBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Stdout output configuration is missing".to_string(),
            ));
        }
        let config: StdoutOutputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(StdoutOutput::new(config, io::stdout())?))
    }
}

pub fn init() {
    register_output_builder("stdout", Arc::new(StdoutOutputBuilder));
}

trait StdWriter: Write + Send + Sync {}

impl StdWriter for Stdout {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // Mock writer for testing
    struct MockWriter(Cursor<Vec<u8>>);

    impl MockWriter {
        fn new() -> Self {
            Self(Cursor::new(Vec::new()))
        }

        fn get_output(&self) -> String {
            String::from_utf8_lossy(&self.0.get_ref()).to_string()
        }
    }

    impl Write for MockWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.0.flush()
        }
    }

    impl StdWriter for MockWriter {}

    /// Test basic functionality of StdoutOutput
    #[tokio::test]
    async fn test_basic_functionality() {
        let config = StdoutOutputConfig {
            append_newline: Some(true),
        };
        let output = StdoutOutput::new(config, MockWriter::new()).unwrap();

        // Test connect
        assert!(output.connect().await.is_ok());

        // Test write with simple text
        let msg = MessageBatch::from_string("test message").unwrap();
        assert!(output.write(msg).await.is_ok());

        // Test close
        assert!(output.close().await.is_ok());
    }

    /// Test handling of different data types (Arrow and Binary)
    #[tokio::test]
    async fn test_data_type_handling() {
        let config = StdoutOutputConfig {
            append_newline: Some(true),
        };
        let output = StdoutOutput::new(config, MockWriter::new()).unwrap();

        // Test binary data
        let binary_msg = MessageBatch::from_string("binary test").unwrap();
        assert!(output.write(binary_msg).await.is_ok());

        // Test Arrow data (would need more complex setup)
        // TODO: Add Arrow data type test cases
    }
}
