//! Keyed state backend contracts and the initial embedded `redb` backend.

use crate::Error;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const STATE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("job_state");

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateEntry {
    pub namespace: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    #[serde(default)]
    pub expires_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateSnapshot {
    pub format_version: u32,
    pub entries: Vec<StateEntry>,
    pub checksum: u64,
}

impl StateSnapshot {
    pub fn new(format_version: u32, entries: Vec<StateEntry>) -> Self {
        let checksum = checksum_entries(&entries);
        Self {
            format_version,
            entries,
            checksum,
        }
    }

    pub fn verify(&self) -> bool {
        self.checksum == checksum_entries(&self.entries)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateMetrics {
    pub keys: u64,
    pub bytes: u64,
}

pub trait StateBackend: Send + Sync {
    fn format_version(&self) -> u32;
    fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    fn put(&self, namespace: &str, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.put_with_ttl(namespace, key, value, None, now_ms())
    }
    fn put_with_ttl(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
        now_ms: u64,
    ) -> Result<(), Error>;
    fn update_i64(&self, namespace: &str, key: &[u8], delta: i64) -> Result<i64, Error>;
    fn delete(&self, namespace: &str, key: &[u8]) -> Result<bool, Error>;
    fn purge_expired(&self, now_ms: u64) -> Result<u64, Error>;
    fn scan(&self, namespace: &str) -> Result<Vec<StateEntry>, Error>;
    fn snapshot(&self) -> Result<StateSnapshot, Error> {
        self.snapshot_at(now_ms())
    }
    fn snapshot_at(&self, now_ms: u64) -> Result<StateSnapshot, Error>;
    fn restore(&self, snapshot: &StateSnapshot) -> Result<(), Error>;
    fn metrics(&self) -> Result<StateMetrics, Error>;
    fn close(&self) -> Result<(), Error>;
}

pub struct KeyedCounter {
    backend: std::sync::Arc<dyn StateBackend>,
    namespace: String,
}

impl KeyedCounter {
    pub fn new(backend: std::sync::Arc<dyn StateBackend>, namespace: impl Into<String>) -> Self {
        Self {
            backend,
            namespace: namespace.into(),
        }
    }

    pub fn add(&self, key: &[u8], delta: i64) -> Result<i64, Error> {
        self.backend.update_i64(&self.namespace, key, delta)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<i64>, Error> {
        self.backend
            .get(&self.namespace, key)?
            .map(|value| serde_json::from_slice(&value).map_err(Error::Serialization))
            .transpose()
    }
}

pub struct WindowAccumulator {
    counter: KeyedCounter,
}

impl WindowAccumulator {
    pub fn new(backend: std::sync::Arc<dyn StateBackend>, operator: &str) -> Self {
        Self {
            counter: KeyedCounter::new(backend, format!("window:{operator}")),
        }
    }

    pub fn add(
        &self,
        key: &[u8],
        window_start_ms: i64,
        window_end_ms: i64,
        delta: i64,
    ) -> Result<i64, Error> {
        let mut state_key = window_start_ms.to_be_bytes().to_vec();
        state_key.extend_from_slice(&window_end_ms.to_be_bytes());
        state_key.extend_from_slice(key);
        self.counter.add(&state_key, delta)
    }
}

pub struct RedbStateBackend {
    db: Database,
    root: PathBuf,
    format_version: u32,
    max_bytes: Option<u64>,
    keys: AtomicU64,
    bytes: AtomicU64,
}

impl RedbStateBackend {
    pub fn open(root: impl AsRef<Path>, format_version: u32) -> Result<Self, Error> {
        if format_version == 0 {
            return Err(Error::Config(
                "state format_version must be positive".into(),
            ));
        }
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)
            .map_err(|error| Error::Process(format!("create state directory: {error}")))?;
        let db = Database::create(root.join("state.redb"))
            .map_err(|error| Error::Process(format!("open state database: {error}")))?;
        let backend = Self {
            db,
            root,
            format_version,
            max_bytes: None,
            keys: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        };
        let metrics = backend.metrics()?;
        backend.keys.store(metrics.keys, Ordering::Relaxed);
        backend.bytes.store(metrics.bytes, Ordering::Relaxed);
        Ok(backend)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }

    fn storage_key(namespace: &str, key: &[u8]) -> String {
        format!("{namespace}\0{}", hex_encode(key))
    }

    fn parse_key(storage_key: &str) -> Result<(&str, Vec<u8>), Error> {
        let Some((namespace, key)) = storage_key.split_once('\0') else {
            return Err(Error::Process("invalid state key".into()));
        };
        Ok((namespace, hex_decode(key)?))
    }
}

impl StateBackend for RedbStateBackend {
    fn format_version(&self) -> u32 {
        self.format_version
    }

    fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let tx = self
            .db
            .begin_read()
            .map_err(|error| Error::Process(format!("state read: {error}")))?;
        let table = match tx.open_table(STATE_TABLE) {
            Ok(table) => table,
            Err(_) => return Ok(None),
        };
        let value = table
            .get(Self::storage_key(namespace, key).as_str())
            .map_err(|error| Error::Process(format!("state get: {error}")))
            .and_then(|value| value.map(|value| decode_value(value.value())).transpose())?;
        if let Some(value) = value {
            if value
                .expires_at_ms
                .is_some_and(|expires| expires <= now_ms())
            {
                drop(table);
                drop(tx);
                self.delete(namespace, key)?;
                return Ok(None);
            }
            Ok(Some(value.value))
        } else {
            Ok(None)
        }
    }

    fn put_with_ttl(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
        now_ms: u64,
    ) -> Result<(), Error> {
        let storage_key = Self::storage_key(namespace, key);
        let previous = self.get(namespace, key)?.map(|value| value.len() as u64);
        if let Some(max_bytes) = self.max_bytes {
            let next_bytes = self
                .bytes
                .load(Ordering::Relaxed)
                .saturating_sub(previous.unwrap_or(0))
                .saturating_add(value.len() as u64);
            if next_bytes > max_bytes {
                return Err(Error::Process(format!(
                    "state budget exceeded: {next_bytes} > {max_bytes} bytes"
                )));
            }
        }
        let encoded = encode_value(value, ttl_ms.map(|ttl| now_ms.saturating_add(ttl)))?;
        let tx = self
            .db
            .begin_write()
            .map_err(|error| Error::Process(format!("state write: {error}")))?;
        {
            let mut table = tx
                .open_table(STATE_TABLE)
                .map_err(|error| Error::Process(format!("state table: {error}")))?;
            table
                .insert(storage_key.as_str(), encoded.as_slice())
                .map_err(|error| Error::Process(format!("state put: {error}")))?;
        }
        tx.commit()
            .map_err(|error| Error::Process(format!("state commit: {error}")))?;
        if previous.is_none() {
            self.keys.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(previous) = previous {
            self.bytes.fetch_sub(previous, Ordering::Relaxed);
        }
        self.bytes.fetch_add(value.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    fn update_i64(&self, namespace: &str, key: &[u8], delta: i64) -> Result<i64, Error> {
        let storage_key = Self::storage_key(namespace, key);
        let tx = self
            .db
            .begin_write()
            .map_err(|error| Error::Process(format!("state write: {error}")))?;
        let (previous_bytes, next, next_bytes) = {
            let mut table = tx
                .open_table(STATE_TABLE)
                .map_err(|error| Error::Process(format!("state table: {error}")))?;
            let previous = table
                .get(storage_key.as_str())
                .map_err(|error| Error::Process(format!("state get: {error}")))?
                .map(|value| decode_value(value.value()))
                .transpose()?;
            let previous_bytes = previous.as_ref().map(|value| value.value.len() as u64);
            let previous_is_live = previous.as_ref().is_some_and(|value| {
                !value
                    .expires_at_ms
                    .is_some_and(|expires| expires <= now_ms())
            });
            let current = previous
                .filter(|value| {
                    !value
                        .expires_at_ms
                        .is_some_and(|expires| expires <= now_ms())
                })
                .map(|value| serde_json::from_slice::<i64>(&value.value))
                .transpose()?
                .unwrap_or_default();
            let next = current.saturating_add(delta);
            let next_value = serde_json::to_vec(&next)?;
            if let Some(max_bytes) = self.max_bytes {
                let accounted_previous = if previous_is_live {
                    previous_bytes.unwrap_or_default()
                } else {
                    0
                };
                let next_total = self
                    .bytes
                    .load(Ordering::Relaxed)
                    .saturating_sub(accounted_previous)
                    .saturating_add(next_value.len() as u64);
                if next_total > max_bytes {
                    return Err(Error::Process(format!(
                        "state budget exceeded: {next_total} > {max_bytes} bytes"
                    )));
                }
            }
            let encoded = encode_value(&next_value, None)?;
            table
                .insert(storage_key.as_str(), encoded.as_slice())
                .map_err(|error| Error::Process(format!("state put: {error}")))?;
            (previous_bytes, next, next_value.len() as u64)
        };
        tx.commit()
            .map_err(|error| Error::Process(format!("state commit: {error}")))?;
        if let Some(previous) = previous_bytes {
            self.bytes.fetch_sub(previous, Ordering::Relaxed);
        } else {
            self.keys.fetch_add(1, Ordering::Relaxed);
        }
        self.bytes.fetch_add(next_bytes, Ordering::Relaxed);
        Ok(next)
    }

    fn delete(&self, namespace: &str, key: &[u8]) -> Result<bool, Error> {
        let storage_key = Self::storage_key(namespace, key);
        let tx = self
            .db
            .begin_write()
            .map_err(|error| Error::Process(format!("state write: {error}")))?;
        let previous = {
            let mut table = tx
                .open_table(STATE_TABLE)
                .map_err(|error| Error::Process(format!("state table: {error}")))?;
            let removed = table
                .remove(storage_key.as_str())
                .map_err(|error| Error::Process(format!("state delete: {error}")))?
                .map(|value| decode_value(value.value()).map(|decoded| decoded.value.len() as u64))
                .transpose()?;
            removed
        };
        tx.commit()
            .map_err(|error| Error::Process(format!("state commit: {error}")))?;
        if let Some(bytes) = previous {
            self.keys.fetch_sub(1, Ordering::Relaxed);
            self.bytes.fetch_sub(bytes, Ordering::Relaxed);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn purge_expired(&self, now_ms: u64) -> Result<u64, Error> {
        let tx = self
            .db
            .begin_read()
            .map_err(|error| Error::Process(format!("state read: {error}")))?;
        let mut expired = Vec::new();
        if let Ok(table) = tx.open_table(STATE_TABLE) {
            for item in table
                .iter()
                .map_err(|error| Error::Process(format!("state scan: {error}")))?
            {
                let (key, value) =
                    item.map_err(|error| Error::Process(format!("state scan: {error}")))?;
                if decode_value(value.value())?
                    .expires_at_ms
                    .is_some_and(|expires| expires <= now_ms)
                {
                    expired.push(key.value().to_owned());
                }
            }
        }
        drop(tx);
        let mut removed = 0;
        for storage_key in expired {
            let (namespace, key) = Self::parse_key(&storage_key)?;
            if self.delete(namespace, &key)? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn scan(&self, namespace: &str) -> Result<Vec<StateEntry>, Error> {
        let tx = self
            .db
            .begin_read()
            .map_err(|error| Error::Process(format!("state read: {error}")))?;
        let Ok(table) = tx.open_table(STATE_TABLE) else {
            return Ok(Vec::new());
        };
        let mut entries = Vec::new();
        for item in table
            .iter()
            .map_err(|error| Error::Process(format!("state scan: {error}")))?
        {
            let (storage_key, value) =
                item.map_err(|error| Error::Process(format!("state scan: {error}")))?;
            let (entry_namespace, key) = Self::parse_key(storage_key.value())?;
            if entry_namespace == namespace {
                let decoded = decode_value(value.value())?;
                if decoded
                    .expires_at_ms
                    .is_some_and(|expires| expires <= now_ms())
                {
                    continue;
                }
                entries.push(StateEntry {
                    namespace: entry_namespace.to_owned(),
                    key,
                    value: decoded.value,
                    expires_at_ms: decoded.expires_at_ms,
                });
            }
        }
        Ok(entries)
    }

    fn snapshot_at(&self, now_ms: u64) -> Result<StateSnapshot, Error> {
        let tx = self
            .db
            .begin_read()
            .map_err(|error| Error::Process(format!("state read: {error}")))?;
        let mut entries = Vec::new();
        if let Ok(table) = tx.open_table(STATE_TABLE) {
            for item in table
                .iter()
                .map_err(|error| Error::Process(format!("state snapshot: {error}")))?
            {
                let (storage_key, value) =
                    item.map_err(|error| Error::Process(format!("state snapshot: {error}")))?;
                let (namespace, key) = Self::parse_key(storage_key.value())?;
                let decoded = decode_value(value.value())?;
                if decoded
                    .expires_at_ms
                    .is_some_and(|expires| expires <= now_ms)
                {
                    continue;
                }
                entries.push(StateEntry {
                    namespace: namespace.to_owned(),
                    key,
                    value: decoded.value,
                    expires_at_ms: decoded.expires_at_ms,
                });
            }
        }
        Ok(StateSnapshot::new(self.format_version, entries))
    }

    fn restore(&self, snapshot: &StateSnapshot) -> Result<(), Error> {
        if snapshot.format_version != self.format_version {
            return Err(Error::Config(format!(
                "state format {} is incompatible with backend format {}",
                snapshot.format_version, self.format_version
            )));
        }
        if !snapshot.verify() {
            return Err(Error::Process("state snapshot checksum mismatch".into()));
        }
        let tx = self
            .db
            .begin_write()
            .map_err(|error| Error::Process(format!("state write: {error}")))?;
        {
            let mut table = tx
                .open_table(STATE_TABLE)
                .map_err(|error| Error::Process(format!("state table: {error}")))?;
            let existing_keys: Vec<String> = table
                .iter()
                .map_err(|error| Error::Process(format!("state restore scan: {error}")))?
                .map(|item| {
                    item.map(|(key, _)| key.value().to_owned())
                        .map_err(|error| Error::Process(format!("state restore scan: {error}")))
                })
                .collect::<Result<_, _>>()?;
            for key in existing_keys {
                table
                    .remove(key.as_str())
                    .map_err(|error| Error::Process(format!("state restore clear: {error}")))?;
            }
            for entry in &snapshot.entries {
                let key = Self::storage_key(&entry.namespace, &entry.key);
                if entry
                    .expires_at_ms
                    .is_some_and(|expires| expires <= now_ms())
                {
                    continue;
                }
                let encoded = encode_value(&entry.value, entry.expires_at_ms)?;
                table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(|error| Error::Process(format!("state restore: {error}")))?;
            }
        }
        tx.commit()
            .map_err(|error| Error::Process(format!("state commit: {error}")))?;
        let metrics = self.metrics()?;
        self.keys.store(metrics.keys, Ordering::Relaxed);
        self.bytes.store(metrics.bytes, Ordering::Relaxed);
        Ok(())
    }

    fn metrics(&self) -> Result<StateMetrics, Error> {
        let tx = self
            .db
            .begin_read()
            .map_err(|error| Error::Process(format!("state read: {error}")))?;
        let mut metrics = StateMetrics::default();
        if let Ok(table) = tx.open_table(STATE_TABLE) {
            for item in table
                .iter()
                .map_err(|error| Error::Process(format!("state metrics: {error}")))?
            {
                let (_, value) =
                    item.map_err(|error| Error::Process(format!("state metrics: {error}")))?;
                metrics.keys += 1;
                metrics.bytes += decode_value(value.value())?.value.len() as u64;
            }
        }
        Ok(metrics)
    }

    fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredStateValue {
    expires_at_ms: Option<u64>,
    value: Vec<u8>,
}

fn encode_value(value: &[u8], expires_at_ms: Option<u64>) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(&StoredStateValue {
        expires_at_ms,
        value: value.to_vec(),
    })
    .map_err(Error::Serialization)
}

fn decode_value(value: &[u8]) -> Result<StoredStateValue, Error> {
    serde_json::from_slice(value).map_err(Error::Serialization)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn checksum_entries(entries: &[StateEntry]) -> u64 {
    let mut checksum = 0xcbf29ce484222325u64;
    for entry in entries {
        for byte in entry
            .namespace
            .as_bytes()
            .iter()
            .chain(entry.key.iter())
            .chain(entry.value.iter())
        {
            checksum ^= u64::from(*byte);
            checksum = checksum.wrapping_mul(0x100000001b3);
        }
        for byte in entry.expires_at_ms.unwrap_or_default().to_le_bytes() {
            checksum ^= u64::from(byte);
            checksum = checksum.wrapping_mul(0x100000001b3);
        }
    }
    checksum
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hex_decode(value: &str) -> Result<Vec<u8>, Error> {
    if !value.len().is_multiple_of(2) {
        return Err(Error::Process("invalid state key encoding".into()));
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16)
                .map_err(|error| Error::Process(format!("invalid state key encoding: {error}")))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redb_backend_round_trips_and_snapshots_state() {
        let dir = tempfile::tempdir().unwrap();
        let backend = RedbStateBackend::open(dir.path(), 1).unwrap();
        backend.put("orders", b"a", b"1").unwrap();
        backend.put("orders", b"b", b"2").unwrap();
        backend.put("other", b"a", b"3").unwrap();
        assert_eq!(backend.get("orders", b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(backend.scan("orders").unwrap().len(), 2);
        let snapshot = backend.snapshot().unwrap();
        assert!(snapshot.verify());
        assert_eq!(backend.metrics().unwrap().keys, 3);
        assert!(backend.delete("other", b"a").unwrap());
        backend.restore(&snapshot).unwrap();
        assert_eq!(backend.metrics().unwrap().keys, 3);
    }

    #[test]
    fn rejects_incompatible_or_corrupt_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        let backend = RedbStateBackend::open(dir.path(), 1).unwrap();
        let mut snapshot = StateSnapshot::new(
            2,
            vec![StateEntry {
                namespace: "x".into(),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                expires_at_ms: None,
            }],
        );
        assert!(backend.restore(&snapshot).is_err());
        snapshot.format_version = 1;
        snapshot.checksum = 0;
        assert!(backend.restore(&snapshot).is_err());
    }

    #[test]
    fn enforces_ttl_and_state_budget() {
        let dir = tempfile::tempdir().unwrap();
        let backend = RedbStateBackend::open(dir.path(), 1)
            .unwrap()
            .with_max_bytes(2);
        backend
            .put_with_ttl("orders", b"a", b"1", Some(10), 100)
            .unwrap();
        assert!(backend.put("orders", b"b", b"22").is_err());
        assert_eq!(backend.purge_expired(109).unwrap(), 0);
        assert_eq!(backend.purge_expired(110).unwrap(), 1);
    }

    #[test]
    fn enforces_state_budget_for_keyed_counter_updates() {
        let dir = tempfile::tempdir().unwrap();
        let backend: std::sync::Arc<dyn StateBackend> = std::sync::Arc::new(
            RedbStateBackend::open(dir.path(), 1)
                .unwrap()
                .with_max_bytes(2),
        );
        let counter = KeyedCounter::new(backend, "aggregate");
        assert_eq!(counter.add(b"a", 1).unwrap(), 1);
        assert!(counter.add(b"b", 22).is_err());
        assert_eq!(counter.get(b"a").unwrap(), Some(1));
        assert_eq!(counter.get(b"b").unwrap(), None);
    }

    #[test]
    fn reopens_disk_backed_state_after_backend_drop() {
        let dir = tempfile::tempdir().unwrap();
        {
            let backend = RedbStateBackend::open(dir.path(), 1).unwrap();
            backend.put("orders", b"a", b"persisted").unwrap();
        }
        let reopened = RedbStateBackend::open(dir.path(), 1).unwrap();
        assert_eq!(
            reopened.get("orders", b"a").unwrap(),
            Some(b"persisted".to_vec())
        );
    }

    #[test]
    fn keyed_aggregate_and_window_use_isolated_state() {
        let dir = tempfile::tempdir().unwrap();
        let backend: std::sync::Arc<dyn StateBackend> =
            std::sync::Arc::new(RedbStateBackend::open(dir.path(), 1).unwrap());
        let counter = KeyedCounter::new(backend.clone(), "aggregate");
        assert_eq!(counter.add(b"a", 2).unwrap(), 2);
        assert_eq!(counter.add(b"a", 3).unwrap(), 5);
        assert_eq!(counter.get(b"b").unwrap(), None);
        let windows = WindowAccumulator::new(backend, "aggregate");
        assert_eq!(windows.add(b"a", 0, 1_000, 1).unwrap(), 1);
        assert_eq!(windows.add(b"a", 0, 1_000, 2).unwrap(), 3);
    }

    #[test]
    fn snapshots_filter_expired_entries_and_restore_ttl_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let backend = RedbStateBackend::open(dir.path(), 1).unwrap();
        let base = now_ms().saturating_add(100_000);
        backend
            .put_with_ttl("orders", b"live", b"1", Some(1_000), base)
            .unwrap();
        backend
            .put_with_ttl("orders", b"expired", b"2", Some(10), base)
            .unwrap();
        let snapshot = backend.snapshot_at(base + 100).unwrap();
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].expires_at_ms, Some(base + 1_000));
        let restored_dir = tempfile::tempdir().unwrap();
        let restored = RedbStateBackend::open(restored_dir.path(), 1).unwrap();
        restored.restore(&snapshot).unwrap();
        assert_eq!(
            restored.get("orders", b"live").unwrap(),
            Some(b"1".to_vec())
        );
        assert_eq!(restored.snapshot_at(base + 1_001).unwrap().entries.len(), 0);
    }

    #[test]
    fn scans_filter_expired_entries() {
        let dir = tempfile::tempdir().unwrap();
        let backend = RedbStateBackend::open(dir.path(), 1).unwrap();
        backend.put("orders", b"live", b"1").unwrap();
        backend
            .put_with_ttl("orders", b"expired", b"2", Some(1), 0)
            .unwrap();
        let entries = backend.scan("orders").unwrap();
        assert_eq!(backend.scan("orders").unwrap().len(), 1);
        assert_eq!(entries[0].key, b"live");
    }

    #[test]
    fn keyed_counter_updates_are_atomic_under_concurrency() {
        let dir = tempfile::tempdir().unwrap();
        let backend: std::sync::Arc<dyn StateBackend> =
            std::sync::Arc::new(RedbStateBackend::open(dir.path(), 1).unwrap());
        let mut workers = Vec::new();
        for _ in 0..8 {
            let backend = backend.clone();
            workers.push(std::thread::spawn(move || {
                let counter = KeyedCounter::new(backend, "aggregate");
                for _ in 0..25 {
                    counter.add(b"same-key", 1).unwrap();
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        let counter = KeyedCounter::new(backend, "aggregate");
        assert_eq!(counter.get(b"same-key").unwrap(), Some(200));
    }
}
