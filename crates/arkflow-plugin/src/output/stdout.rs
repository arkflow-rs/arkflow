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

use arkflow_core::codec::Codec;
use arkflow_core::component::{register_output_metadata, ComponentMetadata};
use arkflow_core::error_helpers::parse_config;
use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatchRef, Resource};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::io::{self, BufWriter, Write};
use std::string::String;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Standard output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StdoutOutputConfig {
    /// Whether to add a line break after each message
    pub append_newline: Option<bool>,
}

/// Buffered writer handle. `writeln!` on a plain `Stdout` goes through a
/// `LineWriter` that flushes (one syscall) per line; wrapping in `BufWriter`
/// keeps a whole batch's lines in the buffer so the explicit flush at the
/// end of `write` is the only trip to the underlying writer.
type BufferedStdWriter = BufWriter<Box<dyn Write + Send>>;

/// Standard output components
struct StdoutOutput {
    config: StdoutOutputConfig,
    writer: Mutex<BufferedStdWriter>,
    codec: Option<Arc<dyn Codec>>,
}

impl StdoutOutput {
    /// Create a new standard output component
    pub fn new(
        config: StdoutOutputConfig,
        writer: BufferedStdWriter,
        codec: Option<Arc<dyn Codec>>,
    ) -> Result<Self, Error> {
        Ok(Self {
            config,
            writer: Mutex::new(writer),
            codec,
        })
    }
}

#[async_trait]
impl Output for StdoutOutput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn write(&self, msg: MessageBatchRef) -> Result<(), Error> {
        // Apply codec encoding if configured
        let payloads = crate::output::codec_helper::apply_codec_encode(&msg, &self.codec).await?;

        // Write each payload to stdout; the buffer coalesces the batch's
        // lines and the flush below is the single per-batch write.
        let mut writer_std = self.writer.lock().await;
        for payload in payloads {
            let s = String::from_utf8_lossy(&payload);

            if self.config.append_newline.unwrap_or(true) {
                writeln!(writer_std, "{}", s).map_err(Error::Io)?
            } else {
                write!(writer_std, "{}", s).map_err(Error::Io)?
            }
        }

        writer_std.flush().map_err(Error::Io)?;
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

struct StdoutOutputBuilder;

impl OutputBuilder for StdoutOutputBuilder {
    fn build(
        &self,
        _name: Option<&str>,
        config: &Option<serde_json::Value>,
        codec: Option<Arc<dyn Codec>>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        let config: StdoutOutputConfig = parse_config(config, "StdoutOutput input")?;
        Ok(Arc::new(StdoutOutput::new(
            config,
            BufWriter::new(Box::new(io::stdout())),
            codec,
        )?))
    }
}

pub fn init() -> Result<(), Error> {
    register_output_builder("stdout", Arc::new(StdoutOutputBuilder))?;
    register_output_metadata(ComponentMetadata::with_schema(
        "stdout",
        "Writes each message to the console. Useful for debugging and demos.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "append_newline": {"type": "boolean", "default": true, "description": "Append a newline after each message."}
            }
        }),
    ).with_optional().with_example(serde_json::json!({"append_newline": true})))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::MessageBatch;
    use std::io::Cursor;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Mock writer for testing
    struct MockWriter(Cursor<Vec<u8>>);

    impl MockWriter {
        fn new() -> Self {
            Self(Cursor::new(Vec::new()))
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

    fn mock_output(append_newline: bool) -> StdoutOutput {
        StdoutOutput::new(
            StdoutOutputConfig {
                append_newline: Some(append_newline),
            },
            BufWriter::new(Box::new(MockWriter::new())),
            None,
        )
        .unwrap()
    }

    /// Shared state of a [`CountingWriter`]: how many write calls reached the
    /// underlying layer, and every byte those calls carried.
    #[derive(Default)]
    struct WriteTally {
        writes: AtomicUsize,
        bytes: std::sync::Mutex<Vec<u8>>,
    }

    impl WriteTally {
        fn snapshot(&self) -> (usize, Vec<u8>) {
            (
                self.writes.load(Ordering::SeqCst),
                self.bytes.lock().unwrap().clone(),
            )
        }
    }

    /// Underlying writer that counts its write calls: the batch-coalescing
    /// guarantee is one write per batch, not one per line.
    struct CountingWriter(Arc<WriteTally>);

    impl Write for CountingWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.writes.fetch_add(1, Ordering::SeqCst);
            self.0.bytes.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Test basic functionality of StdoutOutput
    #[tokio::test]
    async fn test_basic_functionality() {
        let output = mock_output(true);

        // Test connect
        assert!(output.connect().await.is_ok());

        // Test write with simple text
        let msg = Arc::new(MessageBatch::from_string("test message").unwrap());
        assert!(output.write(msg).await.is_ok());

        // Test close
        assert!(output.close().await.is_ok());
    }

    /// Test handling of different data types (Arrow and Binary)
    #[tokio::test]
    async fn test_data_type_handling() {
        let output = mock_output(true);

        // Test binary data
        let binary_msg = Arc::new(MessageBatch::from_string("binary test").unwrap());
        assert!(output.write(binary_msg).await.is_ok());

        // Test Arrow data (would need more complex setup)
        // TODO: Add Arrow data type test cases
    }

    /// A batch's many rows coalesce into a single write at the underlying
    /// writer: without the `BufWriter` wrapper each `writeln!` reached the
    /// line-buffered stdout as its own write.
    #[tokio::test]
    async fn test_batch_multiple_rows_flush_once() {
        let tally = Arc::new(WriteTally::default());
        let output = StdoutOutput::new(
            StdoutOutputConfig {
                append_newline: Some(true),
            },
            BufWriter::new(Box::new(CountingWriter(tally.clone()))),
            None,
        )
        .unwrap();

        let rows: Vec<String> = (0..200).map(|i| format!("row-{i}")).collect();
        let batch =
            MessageBatch::new_binary(rows.clone().into_iter().map(String::into_bytes).collect())
                .unwrap();
        output.write(Arc::new(batch)).await.unwrap();

        let (writes, bytes) = tally.snapshot();
        let observed = String::from_utf8(bytes).unwrap();
        let expected: String = rows
            .iter()
            .map(|row| format!("{row}\n"))
            .collect::<Vec<_>>()
            .join("");
        assert_eq!(observed, expected);
        assert_eq!(
            writes, 1,
            "one batch of 200 short rows must reach the underlying writer as a single write"
        );
    }

    /// Without newlines the batch still lands as one buffered write.
    #[tokio::test]
    async fn test_batch_without_newline_still_coalesces() {
        let tally = Arc::new(WriteTally::default());
        let output = StdoutOutput::new(
            StdoutOutputConfig {
                append_newline: Some(false),
            },
            BufWriter::new(Box::new(CountingWriter(tally.clone()))),
            None,
        )
        .unwrap();

        let rows: Vec<String> = (0..100).map(|i| format!("row-{i};")).collect();
        let batch =
            MessageBatch::new_binary(rows.clone().into_iter().map(String::into_bytes).collect())
                .unwrap();
        output.write(Arc::new(batch)).await.unwrap();

        let (writes, bytes) = tally.snapshot();
        let observed = String::from_utf8(bytes).unwrap();
        let expected: String = rows.concat();
        assert_eq!(observed, expected);
        assert_eq!(writes, 1);
    }
}
