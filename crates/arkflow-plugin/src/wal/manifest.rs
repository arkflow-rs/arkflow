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

//! Manifest object for the object-store WAL backend (D4 + D6).
//!
//! The manifest is a single small JSON object overwritten via PUT on every
//! rotation / cursor-flush / truncation. Object-store PUT is atomic
//! per-object, so a reader always sees either the old or the new manifest
//! — never a torn one.
//!
//! Recovery *also* enumerates the actual segment objects on the store
//! (LIST) and unions that set with the manifest, so a manifest that's
//! lagging behind (because the last PUT crashed before the manifest was
//! rewritten) doesn't lose entries.

use serde::{Deserialize, Serialize};

/// Compression algorithm used for a sealed segment. Persisted in the
/// manifest so recovery knows how to decode the bytes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum SegmentCompression {
    /// No compression.
    #[default]
    None,
    /// Zstandard.
    Zstd,
    /// LZ4.
    Lz4,
}

/// The manifest object. Public for round-trip and tests; the rest of the
/// crate reads it via `serde_json::from_slice`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Manifest {
    /// Schema version. Bumped only on incompatible changes; recovery
    /// treats an unknown `version` as an empty manifest.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Stable per-pod identifier (sanity check on recovery — the segment
    /// set in the bucket must match the configured `node_id`).
    pub node_id: String,
    /// Stable per-stream identifier.
    pub stream_id: String,
    /// Committed watermark: the highest sequence whose downstream ack has
    /// been flushed to the manifest.
    pub cursor: u64,
    /// Highest sequence number *sealed* (across sealed segments and the
    /// active segment). Anything above this but still in the in-memory
    /// pending buffer is at risk on node loss (D4's loss window).
    pub max_sealed_seq: u64,
    /// Currently-open segment filename (D4). `None` on a fresh bucket.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_segment: Option<String>,
    /// Compression algorithm of the active segment, if any. Recorded so
    /// recovery knows how to decode (task 4.8).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_segment_compression: Option<SegmentCompression>,
    /// All sealed-but-not-yet-truncated segments. Entries are filenames
    /// relative to the `segments/` prefix.
    #[serde(default)]
    pub sealed_segments: Vec<String>,
}

fn default_version() -> u32 {
    // Bump to 2 when reading manifests that don't yet have
    // `active_segment_compression`. Old manifests (v1) are forward-
    // compatible because the field is optional with default `None`.
    2
}

impl Manifest {
    pub(crate) fn fresh(node_id: String, stream_id: String) -> Self {
        Self {
            version: default_version(),
            node_id,
            stream_id,
            cursor: 0,
            max_sealed_seq: 0,
            active_segment: None,
            active_segment_compression: None,
            sealed_segments: Vec::new(),
        }
    }

    pub(crate) fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    pub(crate) fn from_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_manifest_serialises() {
        let m = Manifest::fresh("pod-a".into(), "main".into());
        let bytes = m.to_json().unwrap();
        let back = Manifest::from_json(&bytes).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn missing_active_segment_defaults_to_none() {
        let json = r#"{
            "version": 1,
            "node_id": "a",
            "stream_id": "b",
            "cursor": 0,
            "max_sealed_seq": 0,
            "sealed_segments": []
        }"#;
        let m = Manifest::from_json(json.as_bytes()).unwrap();
        assert_eq!(m.active_segment, None);
    }
}
