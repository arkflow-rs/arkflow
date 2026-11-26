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
use std::collections::BTreeMap;

/// Per-segment metadata persisted in the manifest so retention and recovery
/// can evaluate sequence bounds, byte budgets, and segment ages without
/// downloading the segment object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SegmentMeta {
    /// First sequence number contained in the segment.
    pub first_seq: u64,
    /// Last sequence number contained in the segment.
    pub last_seq: u64,
    /// Compressed object size in bytes stored on the object store.
    pub size_bytes: u64,
    /// Unix epoch timestamp in milliseconds when the segment was sealed.
    pub sealed_at_ms: u64,
}

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
    /// refuses a manifest version this build does not understand rather
    /// than resuming from cursor semantics that may have changed.
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
    /// Per-segment metadata keyed by segment filename (`"00000001.wal"`).
    /// Additive v2 field so legacy v1/v2 manifests deserialize cleanly.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub segment_meta: BTreeMap<String, SegmentMeta>,
    /// Segments deregistered from `sealed_segments` and queued for object-store
    /// `DELETE`. Persisted before deletion so crashes remain self-healing.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_deletes: Vec<String>,
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
            segment_meta: BTreeMap::new(),
            pending_deletes: Vec::new(),
        }
    }

    pub(crate) fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    pub(crate) fn from_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let manifest: Self = serde_json::from_slice(bytes)?;
        // v1 lacks `active_segment_compression`; v2 adds it. A newer writer
        // may change cursor semantics, so refuse it instead of resuming
        // acknowledged progress from a position this build misreads.
        if !(1..=2).contains(&manifest.version) {
            return Err(<serde_json::Error as serde::de::Error>::custom(format!(
                "unsupported WAL manifest version {} (this build reads versions 1..=2)",
                manifest.version
            )));
        }
        Ok(manifest)
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
    fn unknown_version_is_rejected_before_cursor_use() {
        let json = r#"{
            "version": 3,
            "node_id": "a",
            "stream_id": "b",
            "cursor": 42,
            "max_sealed_seq": 42,
            "sealed_segments": []
        }"#;
        let error = Manifest::from_json(json.as_bytes()).unwrap_err();
        assert!(error.to_string().contains("unsupported"), "{error}");
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
        assert!(m.segment_meta.is_empty());
        assert!(m.pending_deletes.is_empty());
    }

    #[test]
    fn segment_meta_and_pending_deletes_round_trip() {
        let mut m = Manifest::fresh("pod-a".into(), "main".into());
        m.sealed_segments.push("00000001.wal".into());
        m.segment_meta.insert(
            "00000001.wal".into(),
            SegmentMeta {
                first_seq: 1,
                last_seq: 4,
                size_bytes: 512,
                sealed_at_ms: 1_700_000_000_000,
            },
        );
        m.pending_deletes.push("00000000.wal".into());

        let bytes = m.to_json().unwrap();
        let back = Manifest::from_json(&bytes).unwrap();
        assert_eq!(m, back);
        assert_eq!(back.version, 2);
    }

    #[test]
    fn legacy_v2_manifest_without_retention_fields_loads() {
        let json = r#"{
            "version": 2,
            "node_id": "pod-a",
            "stream_id": "main",
            "cursor": 10,
            "max_sealed_seq": 12,
            "active_segment": "00000002.wal",
            "sealed_segments": ["00000001.wal"]
        }"#;
        let m = Manifest::from_json(json.as_bytes()).unwrap();
        assert_eq!(m.cursor, 10);
        assert_eq!(m.max_sealed_seq, 12);
        assert!(m.segment_meta.is_empty());
        assert!(m.pending_deletes.is_empty());
    }
}
