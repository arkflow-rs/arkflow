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

//! Stream configuration — the legacy YAML stream schema.
//!
//! The linear-pipeline executor that used to live here is retired: streams
//! compile to JobSpecs (`executor::stream_compiler`) and execute on the
//! unified kernel (`executor::task::run_graph`). This module keeps the
//! configuration type as the compiler's input and the control plane's
//! resource identity.

use crate::wal::WalConfig;
use crate::Error;

/// Stream configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StreamConfig {
    /// Stable logical identifier used by the control plane. Older
    /// configurations may omit this field and receive a deterministic
    /// `stream-<index>` identifier at runtime.
    #[serde(default)]
    pub id: Option<String>,
    pub input: crate::input::InputConfig,
    pub pipeline: crate::pipeline::PipelineConfig,
    pub output: crate::output::OutputConfig,
    pub error_output: Option<crate::output::OutputConfig>,
    pub buffer: Option<crate::buffer::BufferConfig>,
    pub durability: Option<WalConfig>,
    pub temporary: Option<Vec<crate::temporary::TemporaryConfig>>,
}

impl StreamConfig {
    /// Return the configured ID, or the deterministic compatibility ID for a
    /// legacy configuration that does not contain one.
    pub fn effective_id(&self, index: usize) -> String {
        self.id.clone().unwrap_or_else(|| format!("stream-{index}"))
    }

    /// Validate the configured ID when present. IDs are intentionally kept
    /// URL-safe because they are also used as control-plane resource names.
    pub fn validate_id(&self, index: usize) -> Result<(), Error> {
        let Some(id) = &self.id else {
            return Ok(());
        };

        if id.is_empty()
            || !id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(Error::Config(format!(
                "Invalid stream id '{}' at index {}; use non-empty letters, numbers, '-' or '_'",
                id, index
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream(id: Option<&str>) -> StreamConfig {
        StreamConfig {
            id: id.map(str::to_owned),
            input: crate::input::InputConfig {
                input_type: "generate".into(),
                name: None,
                codec: None,
                config: None,
            },
            pipeline: crate::pipeline::PipelineConfig {
                thread_num: 1,
                processors: vec![],
            },
            output: crate::output::OutputConfig {
                output_type: "drop".into(),
                name: None,
                codec: None,
                config: None,
            },
            error_output: None,
            buffer: None,
            durability: None,
            temporary: None,
        }
    }

    #[test]
    fn effective_id_falls_back_to_index() {
        assert_eq!(stream(None).effective_id(2), "stream-2");
        assert_eq!(stream(Some("orders")).effective_id(0), "orders");
    }

    #[test]
    fn validate_id_rejects_unsafe_ids() {
        assert!(stream(Some("orders")).validate_id(0).is_ok());
        assert!(stream(Some("")).validate_id(0).is_err());
        assert!(stream(Some("bad id")).validate_id(0).is_err());
        assert!(stream(None).validate_id(0).is_ok());
    }
}
