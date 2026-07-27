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

//! `WalBackend` selection (local `redb` vs. opt-in remote backends).
//!
//! The full config shape for the object-store backend lives here so the
//! `WalBackend::ObjectStore` variant can be deserialized by core without
//! `object_store` as a dependency. The `S3WalStoreBuilder` in
//! `arkflow-plugin` reads these fields and turns them into an `S3Store`.

use serde::{Deserialize, Serialize};

use crate::wal::SyncPolicy;

/// Backend selection for the WAL.
///
/// The default (and legacy) shape is `Local` (when `backend:` is omitted or
/// `null`). `ObjectStore` requires the `S3WalStoreBuilder` (or equivalent) to
/// be registered at startup; if it is not, `Wal::open` returns a `Config`
/// error.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WalBackend {
    /// Embedded `redb` database on local disk (default).
    Local {
        /// Directory in which to store the WAL database file.
        path: String,
        /// Sync (fsync) policy for appends. Defaults to `GroupCommit`.
        #[serde(default)]
        sync: SyncPolicy,
    },
    /// S3-compatible object storage backend, supplied by an `arkflow-plugin`
    /// builder registered as `"object_store"`.
    ObjectStore(ObjectStoreWalConfig),
}

impl WalBackend {
    /// Short name identifying the backend kind. Used for diagnostics and to
    /// validate `cfg.backend` against a registered builder.
    pub fn kind(&self) -> &'static str {
        match self {
            WalBackend::Local { .. } => "local",
            WalBackend::ObjectStore(_) => "object_store",
        }
    }
}

/// Configuration for the object-store WAL backend. The full surface is
/// defined in core so the YAML config deserializes without an
/// `object_store` dependency; the S3 builder in `arkflow-plugin` reads these
/// fields and constructs the client.
///
/// # `node_id` stability contract (D2)
///
/// `node_id` is the WAL's namespace inside the shared bucket. Recovery
/// uses it to find *its own* previous WAL on restart — so it MUST be
/// stable across restarts of the same logical node (pod). **Two nodes
/// with the same `node_id` will silently corrupt each other's WAL**;
/// the change does not enforce uniqueness (no leader election /
/// distributed lock is introduced — see design D2).
///
/// In Kubernetes, prefer one of:
///
/// - **`StatefulSet` ordinal** — `${POD_NAME}` from the downward API
///   carries the StatefulSet ordinal, so each replica has a stable,
///   unique id for its lifetime.
/// - **ConfigMap / env injection** — assign each pod a fixed id via
///   env (`ARKFLOW_NODE_ID`) injected from a ConfigMap or set in the
///   pod spec; stable as long as the pod spec doesn't change.
/// - **DO NOT use `${HOSTNAME}` from a regular `Deployment`** — on
///   reschedule the hostname may change (it isn't guaranteed stable),
///   which makes the prior WAL unrecoverable.
///
/// Outside Kubernetes, any stable per-process identifier (a UUID
/// persisted to a separate, durable location) is acceptable. Auto-
/// generating the id and storing it *on the same bucket* is
/// bootstrapping-circular, so the change makes the id explicit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectStoreWalConfig {
    /// Stable per-pod identifier. See the type-level doc above.
    pub node_id: String,
    /// Stable per-stream identifier (multiple streams can share a bucket
    /// without colliding on segment keys). Must be unique within a node.
    pub stream_id: String,
    /// Root prefix for this WAL inside the bucket. Defaults to `"arkflow/wal"`.
    #[serde(default = "default_prefix")]
    pub prefix: String,
    /// S3 connection settings.
    pub s3: ObjectStoreS3Config,
    /// Segment batching parameters (forwarded to the S3 builder).
    #[serde(default)]
    pub segment: SegmentConfig,
    /// Cursor flush parameters.
    #[serde(default)]
    pub cursor: CursorFlushConfig,
    /// Sync policy. Forced to `GroupCommit`/`Periodic` at the store level;
    /// `PerEntry` is rejected at config-load time.
    #[serde(default)]
    pub sync: SyncPolicy,
}

fn default_prefix() -> String {
    "arkflow/wal".to_string()
}

/// S3 (or S3-compatible) connection settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectStoreS3Config {
    /// Bucket name.
    pub bucket: String,
    /// AWS region (or compatible region string).
    #[serde(default)]
    pub region: Option<String>,
    /// Custom endpoint URL (for MinIO, LocalStack, etc.).
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Access key. May be omitted when the environment / instance profile
    /// supplies credentials.
    #[serde(default)]
    pub access_key_id: Option<String>,
    /// Secret key.
    #[serde(default)]
    pub secret_access_key: Option<String>,
    /// Allow plain HTTP (required when `endpoint` is `http://`).
    #[serde(default)]
    pub allow_http: bool,
}

/// Segment batching parameters (D4 of the change's design).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentConfig {
    /// Maximum entries per sealed segment.
    #[serde(default = "default_segment_max_entries")]
    pub max_entries: usize,
    /// Maximum bytes per sealed segment.
    #[serde(default = "default_segment_max_bytes")]
    pub max_bytes: usize,
    /// Maximum time before sealing an in-memory segment.
    #[serde(default = "default_segment_flush_interval", with = "duration_serde")]
    pub flush_interval: Duration,
}

fn default_segment_max_entries() -> usize {
    1000
}
fn default_segment_max_bytes() -> usize {
    1024 * 1024
}
fn default_segment_flush_interval() -> Duration {
    Duration::from_secs(1)
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            max_entries: default_segment_max_entries(),
            max_bytes: default_segment_max_bytes(),
            flush_interval: default_segment_flush_interval(),
        }
    }
}

/// Cursor flush parameters (D6).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorFlushConfig {
    /// Maximum cursor advances to coalesce into a single manifest PUT.
    #[serde(default = "default_cursor_max_entries")]
    pub max_entries: usize,
    /// Maximum time before flushing a pending cursor advance.
    #[serde(default = "default_cursor_interval", with = "duration_serde")]
    pub interval: Duration,
}

fn default_cursor_max_entries() -> usize {
    1000
}
fn default_cursor_interval() -> Duration {
    Duration::from_secs(1)
}

impl Default for CursorFlushConfig {
    fn default() -> Self {
        Self {
            max_entries: default_cursor_max_entries(),
            interval: default_cursor_interval(),
        }
    }
}

// --- duration serde (humantime-ish string form: "1s", "500ms") ---

pub mod duration_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format!("{}ms", d.as_millis()))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Str(String),
            Int(u64),
        }
        match Raw::deserialize(d)? {
            Raw::Int(ms) => Ok(Duration::from_millis(ms)),
            Raw::Str(s) => parse_human(&s).ok_or_else(|| {
                serde::de::Error::custom(format!("invalid duration: {}", s))
            }),
        }
    }

    fn parse_human(s: &str) -> Option<Duration> {
        let s = s.trim();
        if let Some(rest) = s.strip_suffix("ms") {
            return rest.parse::<u64>().ok().map(Duration::from_millis);
        }
        if let Some(rest) = s.strip_suffix('s') {
            if let Ok(secs) = rest.parse::<f64>() {
                return Some(Duration::from_secs_f64(secs));
            }
        }
        if let Some(rest) = s.strip_suffix('m') {
            if let Ok(mins) = rest.parse::<u64>() {
                return Some(Duration::from_secs(mins * 60));
            }
        }
        s.parse::<u64>().ok().map(Duration::from_millis)
    }
}

use std::time::Duration;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalConfig;

    #[test]
    fn local_default_sync_is_group_commit() {
        let yaml = r#"
            type: local
            path: /tmp/wal
        "#;
        let b: WalBackend = serde_yaml::from_str(yaml).unwrap();
        match b {
            WalBackend::Local { path, sync } => {
                assert_eq!(path, "/tmp/wal");
                assert_eq!(sync, SyncPolicy::GroupCommit);
            }
            _ => panic!("wrong backend"),
        }
    }

    #[test]
    fn object_store_yaml_round_trip() {
        let yaml = r#"
            type: object_store
            node_id: pod-a
            stream_id: main
            s3:
              bucket: my-bucket
              region: us-east-1
        "#;
        let b: WalBackend = serde_yaml::from_str(yaml).unwrap();
        match b {
            WalBackend::ObjectStore(o) => {
                assert_eq!(o.node_id, "pod-a");
                assert_eq!(o.stream_id, "main");
                assert_eq!(o.prefix, "arkflow/wal");
                assert_eq!(o.s3.bucket, "my-bucket");
            }
            _ => panic!("wrong backend"),
        }
    }

    #[test]
    fn object_store_defaults_segment_and_cursor() {
        let yaml = r#"
            type: object_store
            node_id: a
            stream_id: b
            s3:
              bucket: b
        "#;
        let b: WalBackend = serde_yaml::from_str(yaml).unwrap();
        let o = match b {
            WalBackend::ObjectStore(o) => o,
            _ => panic!(),
        };
        assert_eq!(o.segment.max_entries, 1000);
        assert_eq!(o.segment.max_bytes, 1024 * 1024);
        assert_eq!(o.segment.flush_interval, Duration::from_secs(1));
        assert_eq!(o.cursor.max_entries, 1000);
        assert_eq!(o.cursor.interval, Duration::from_secs(1));
        assert_eq!(o.sync, SyncPolicy::GroupCommit);
    }

    /// D8: per_entry on object_store is rejected at config-load time.
    #[test]
    fn per_entry_on_object_store_is_rejected() {
        let yaml = r#"
            type: object_store
            node_id: a
            stream_id: b
            sync: per_entry
            s3:
              bucket: b
        "#;
        let b: WalBackend = serde_yaml::from_str(yaml).unwrap();
        let cfg = WalConfig {
            enabled: true,
            path: String::new(),
            sync: SyncPolicy::default(),
            backend: Some(b),
        };
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("per_entry"),
            "expected per_entry rejection, got: {}",
            err
        );
    }

    /// D2: empty node_id is rejected at config-load time.
    #[test]
    fn empty_node_id_on_object_store_is_rejected() {
        let mut o = ObjectStoreWalConfig {
            node_id: "   ".into(),
            stream_id: "s".into(),
            prefix: default_prefix(),
            s3: ObjectStoreS3Config {
                bucket: "b".into(),
                region: None,
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
                allow_http: false,
            },
            segment: SegmentConfig::default(),
            cursor: CursorFlushConfig::default(),
            sync: SyncPolicy::GroupCommit,
        };
        let cfg = WalConfig {
            enabled: true,
            path: String::new(),
            sync: SyncPolicy::default(),
            backend: Some(WalBackend::ObjectStore(o.clone())),
        };
        assert!(cfg.validate().is_err());
        o.node_id = "pod-a".into();
        o.stream_id = "".into();
        let cfg = WalConfig {
            enabled: true,
            path: String::new(),
            sync: SyncPolicy::default(),
            backend: Some(WalBackend::ObjectStore(o)),
        };
        assert!(cfg.validate().is_err());
    }

    /// Local backend is unaffected by the object-store-only validation.
    #[test]
    fn local_backend_always_validates() {
        let cfg = WalConfig::local(true, "/tmp/wal".into(), SyncPolicy::PerEntry);
        cfg.validate().unwrap();
    }
}