//! Keyed state backend contracts and the initial embedded `redb` backend.

use crate::Error;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
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
    /// Read the complete stored value, including expiration metadata.  The
    /// default implementation keeps third-party backends source-compatible;
    /// durable backends should override it so compensating state rollback can
    /// restore TTL exactly.
    fn get_entry(&self, namespace: &str, key: &[u8]) -> Result<Option<StateEntry>, Error> {
        self.get(namespace, key).map(|value| {
            value.map(|value| StateEntry {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                value,
                expires_at_ms: None,
            })
        })
    }
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
    fn update_i64_with_ttl(
        &self,
        namespace: &str,
        key: &[u8],
        delta: i64,
        ttl_ms: Option<u64>,
    ) -> Result<i64, Error> {
        let _ = ttl_ms;
        self.update_i64(namespace, key, delta)
    }
    fn delete(&self, namespace: &str, key: &[u8]) -> Result<bool, Error>;
    /// Restore one exact entry captured by [`StateBackend::get_entry`].
    /// Backends with TTL support should preserve the absolute expiration.
    fn restore_entry(
        &self,
        namespace: &str,
        key: &[u8],
        entry: Option<&StateEntry>,
    ) -> Result<(), Error> {
        let now = now_ms();
        match entry {
            Some(entry) if entry.expires_at_ms.is_some_and(|expires| expires <= now) => {
                self.delete(namespace, key)?;
                Ok(())
            }
            Some(entry) => self.put_with_ttl(
                namespace,
                key,
                &entry.value,
                entry
                    .expires_at_ms
                    .map(|expires| expires.saturating_sub(now)),
                now,
            ),
            None => {
                self.delete(namespace, key)?;
                Ok(())
            }
        }
    }
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

/// Process-local state backend used by local streams that declare a stateful
/// operator but do not configure a durable Job backend. It has the same
/// snapshot/restore contract as the embedded backend, so the execution graph
/// does not need a second window implementation just for local mode.
pub struct InMemoryStateBackend {
    format_version: u32,
    entries: RwLock<BTreeMap<(String, Vec<u8>), StateEntry>>,
}

impl InMemoryStateBackend {
    pub fn new(format_version: u32) -> Result<Self, Error> {
        if format_version == 0 {
            return Err(Error::Config(
                "state format_version must be positive".into(),
            ));
        }
        Ok(Self {
            format_version,
            entries: RwLock::new(BTreeMap::new()),
        })
    }

    fn is_live(entry: &StateEntry, now_ms: u64) -> bool {
        !entry
            .expires_at_ms
            .is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
    }
}

impl StateBackend for InMemoryStateBackend {
    fn format_version(&self) -> u32 {
        self.format_version
    }

    fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let entries = self.entries.read().unwrap();
        Ok(entries
            .get(&(namespace.to_owned(), key.to_vec()))
            .filter(|entry| Self::is_live(entry, now_ms()))
            .map(|entry| entry.value.clone()))
    }

    fn get_entry(&self, namespace: &str, key: &[u8]) -> Result<Option<StateEntry>, Error> {
        let entries = self.entries.read().unwrap();
        Ok(entries
            .get(&(namespace.to_owned(), key.to_vec()))
            .filter(|entry| Self::is_live(entry, now_ms()))
            .cloned())
    }

    fn restore_entry(
        &self,
        namespace: &str,
        key: &[u8],
        entry: Option<&StateEntry>,
    ) -> Result<(), Error> {
        let mut entries = self.entries.write().unwrap();
        match entry {
            Some(entry) if !Self::is_live(entry, now_ms()) => {
                entries.remove(&(namespace.to_owned(), key.to_vec()));
            }
            Some(entry) => {
                entries.insert(
                    (namespace.to_owned(), key.to_vec()),
                    StateEntry {
                        namespace: namespace.to_owned(),
                        key: key.to_vec(),
                        ..entry.clone()
                    },
                );
            }
            None => {
                entries.remove(&(namespace.to_owned(), key.to_vec()));
            }
        }
        Ok(())
    }

    fn put_with_ttl(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
        now_ms: u64,
    ) -> Result<(), Error> {
        self.entries.write().unwrap().insert(
            (namespace.to_owned(), key.to_vec()),
            StateEntry {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                value: value.to_vec(),
                expires_at_ms: ttl_ms.map(|ttl| now_ms.saturating_add(ttl)),
            },
        );
        Ok(())
    }

    fn update_i64_with_ttl(
        &self,
        namespace: &str,
        key: &[u8],
        delta: i64,
        ttl_ms: Option<u64>,
    ) -> Result<i64, Error> {
        let now = now_ms();
        let map_key = (namespace.to_owned(), key.to_vec());
        let mut entries = self.entries.write().unwrap();
        let current = entries
            .get(&map_key)
            .filter(|entry| Self::is_live(entry, now))
            .map(|entry| serde_json::from_slice::<i64>(&entry.value))
            .transpose()?
            .unwrap_or_default();
        let next = current.saturating_add(delta);
        let value = serde_json::to_vec(&next)?;
        entries.insert(
            map_key,
            StateEntry {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                value,
                expires_at_ms: ttl_ms.map(|ttl| now.saturating_add(ttl)),
            },
        );
        Ok(next)
    }

    fn update_i64(&self, namespace: &str, key: &[u8], delta: i64) -> Result<i64, Error> {
        self.update_i64_with_ttl(namespace, key, delta, None)
    }

    fn delete(&self, namespace: &str, key: &[u8]) -> Result<bool, Error> {
        Ok(self
            .entries
            .write()
            .unwrap()
            .remove(&(namespace.to_owned(), key.to_vec()))
            .is_some())
    }

    fn purge_expired(&self, now_ms: u64) -> Result<u64, Error> {
        let mut entries = self.entries.write().unwrap();
        let before = entries.len();
        entries.retain(|_, entry| Self::is_live(entry, now_ms));
        Ok((before - entries.len()) as u64)
    }

    fn scan(&self, namespace: &str) -> Result<Vec<StateEntry>, Error> {
        let now = now_ms();
        Ok(self
            .entries
            .read()
            .unwrap()
            .values()
            .filter(|entry| entry.namespace == namespace && Self::is_live(entry, now))
            .cloned()
            .collect())
    }

    fn snapshot_at(&self, now_ms: u64) -> Result<StateSnapshot, Error> {
        let entries = self
            .entries
            .read()
            .unwrap()
            .values()
            .filter(|entry| Self::is_live(entry, now_ms))
            .cloned()
            .collect();
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
        let now = now_ms();
        let mut entries = self.entries.write().unwrap();
        entries.clear();
        for entry in &snapshot.entries {
            if Self::is_live(entry, now) {
                entries.insert((entry.namespace.clone(), entry.key.clone()), entry.clone());
            }
        }
        Ok(())
    }

    fn metrics(&self) -> Result<StateMetrics, Error> {
        let now = now_ms();
        let entries = self.entries.read().unwrap();
        Ok(StateMetrics {
            keys: entries
                .values()
                .filter(|entry| Self::is_live(entry, now))
                .count() as u64,
            bytes: entries
                .values()
                .filter(|entry| Self::is_live(entry, now))
                .map(|entry| entry.value.len() as u64)
                .sum(),
        })
    }

    fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

pub struct KeyedCounter {
    backend: std::sync::Arc<dyn StateBackend>,
    namespace: String,
    ttl_ms: Option<u64>,
}

impl KeyedCounter {
    pub fn new(backend: std::sync::Arc<dyn StateBackend>, namespace: impl Into<String>) -> Self {
        Self {
            backend,
            namespace: namespace.into(),
            ttl_ms: None,
        }
    }

    pub fn with_ttl(
        backend: std::sync::Arc<dyn StateBackend>,
        namespace: impl Into<String>,
        ttl_ms: Option<u64>,
    ) -> Self {
        Self {
            backend,
            namespace: namespace.into(),
            ttl_ms,
        }
    }

    pub fn add(&self, key: &[u8], delta: i64) -> Result<i64, Error> {
        self.backend
            .update_i64_with_ttl(&self.namespace, key, delta, self.ttl_ms)
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
    /// Physical row count / value bytes of the state table, maintained
    /// incrementally by every mutation. Write paths never rescan the table.
    keys: AtomicU64,
    bytes: AtomicU64,
    /// Write counter for the amortized expired-entry purge cadence.
    writes: AtomicU64,
    /// Serialize the read/check/write/update sequence used by
    /// `put_with_ttl`, so concurrent writers cannot all pass the same stale
    /// byte-budget check.
    write_lock: Mutex<()>,
}

/// How many writes between amortized expired-entry purges. Reads already
/// hide expired values, so the purge is space reclamation only.
const PURGE_INTERVAL_WRITES: u64 = 4096;

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
            writes: AtomicU64::new(0),
            write_lock: Mutex::new(()),
        };
        let metrics = backend.physical_metrics()?;
        backend.keys.store(metrics.keys, Ordering::Relaxed);
        backend.bytes.store(metrics.bytes, Ordering::Relaxed);
        Ok(backend)
    }

    /// Size of the physical table (expired-but-unpurged rows included). Used
    /// once at `open` to seed the incremental counters and to reconcile them
    /// after a mutation whose exact size the caller measured; the counters are
    /// observability only and never gate a write, because a configured byte
    /// budget measures the table inside its own write transaction.
    fn physical_metrics(&self) -> Result<StateMetrics, Error> {
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
                metrics.bytes += decode_value(value.value())?.value.len() as u64;
                metrics.keys += 1;
            }
        }
        Ok(metrics)
    }

    /// Remove expired rows inside an open write transaction. Returns the
    /// value bytes and row count they freed so callers keep the incremental
    /// counters consistent. Reads already hide expired values, so this is
    /// space reclamation rather than a visibility fix.
    fn remove_expired_in_table(
        table: &mut redb::Table<'_, &str, &[u8]>,
        now_ms: u64,
    ) -> Result<(u64, u64), Error> {
        let mut expired = Vec::new();
        for item in table
            .iter()
            .map_err(|error| Error::Process(format!("state scan: {error}")))?
        {
            let (key, value) =
                item.map_err(|error| Error::Process(format!("state scan: {error}")))?;
            let decoded = decode_value(value.value())?;
            if decoded
                .expires_at_ms
                .is_some_and(|expires| expires <= now_ms)
            {
                expired.push((key.value().to_owned(), decoded.value.len() as u64));
            }
        }
        let mut bytes = 0_u64;
        let keys = expired.len() as u64;
        for (key, len) in expired {
            table
                .remove(key.as_str())
                .map_err(|error| Error::Process(format!("state purge: {error}")))?;
            bytes += len;
        }
        Ok((bytes, keys))
    }

    /// Classify a previous row for accounting: `(bytes, live)`. A live row's
    /// bytes leave the table when it is replaced; a missing or expired row is
    /// logically absent (its bytes, if any, belong to the purge).
    fn previous_row(previous: &Option<StoredStateValue>, now_ms: u64) -> (u64, bool) {
        match previous {
            Some(entry) if !entry.expires_at_ms.is_some_and(|expires| expires <= now_ms) => {
                (entry.value.len() as u64, true)
            }
            _ => (0, false),
        }
    }

    /// Decrement a tracked counter by at most its current value. The counters
    /// are observability only — the configured byte budget measures the table
    /// exactly — so a counter that is momentarily below the amount being
    /// released must saturate at zero rather than wrap to `u64::MAX` and
    /// poison every later report.
    fn release_bytes(counter: &AtomicU64, amount: u64) {
        if amount == 0 {
            return;
        }
        let mut current = counter.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_sub(amount);
            if next == current {
                return;
            }
            match counter.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    /// Row-count counterpart of [`Self::release_bytes`].
    fn release_keys(counter: &AtomicU64, amount: u64) {
        Self::release_bytes(counter, amount);
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

    fn get_entry(&self, namespace: &str, key: &[u8]) -> Result<Option<StateEntry>, Error> {
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
        let Some(value) = value else {
            return Ok(None);
        };
        if value
            .expires_at_ms
            .is_some_and(|expires| expires <= now_ms())
        {
            drop(table);
            drop(tx);
            self.delete(namespace, key)?;
            return Ok(None);
        }
        Ok(Some(StateEntry {
            namespace: namespace.to_owned(),
            key: key.to_vec(),
            value: value.value,
            expires_at_ms: value.expires_at_ms,
        }))
    }

    fn restore_entry(
        &self,
        namespace: &str,
        key: &[u8],
        entry: Option<&StateEntry>,
    ) -> Result<(), Error> {
        let Some(entry) = entry else {
            self.delete(namespace, key)?;
            return Ok(());
        };
        if entry
            .expires_at_ms
            .is_some_and(|expires| expires <= now_ms())
        {
            self.delete(namespace, key)?;
            return Ok(());
        }
        let storage_key = Self::storage_key(namespace, key);
        let encoded = encode_value(&entry.value, entry.expires_at_ms)?;
        let _write_guard = self.write_lock.lock().unwrap();
        let tx = self
            .db
            .begin_write()
            .map_err(|error| Error::Process(format!("state write: {error}")))?;
        let previous_bytes = {
            let mut table = tx
                .open_table(STATE_TABLE)
                .map_err(|error| Error::Process(format!("state table: {error}")))?;
            let previous = table
                .get(storage_key.as_str())
                .map_err(|error| Error::Process(format!("state get: {error}")))?
                .map(|value| decode_value(value.value()))
                .transpose()?;
            let (bytes, had_live) = Self::previous_row(&previous, now_ms());
            table
                .insert(storage_key.as_str(), encoded.as_slice())
                .map_err(|error| Error::Process(format!("state restore: {error}")))?;
            (bytes, had_live)
        };
        tx.commit()
            .map_err(|error| Error::Process(format!("state commit: {error}")))?;
        // Adjust the tracked counters for exactly the row this restore touched:
        // a row that physically existed is replaced by the restore, so the key
        // count is unchanged even when its TTL had already passed.
        let (previous_bytes, had_live) = previous_bytes;
        let _ = had_live;
        if previous_bytes > 0 {
            Self::release_bytes(&self.bytes, previous_bytes);
        }
        self.bytes
            .fetch_add(entry.value.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    fn put_with_ttl(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
        now_ms: u64,
    ) -> Result<(), Error> {
        let _write_guard = self.write_lock.lock().unwrap();
        let storage_key = Self::storage_key(namespace, key);
        let encoded = encode_value(value, ttl_ms.map(|ttl| now_ms.saturating_add(ttl)))?;
        let tx = self
            .db
            .begin_write()
            .map_err(|error| Error::Process(format!("state write: {error}")))?;
        let (previous_bytes, had_previous, freed_bytes, freed_keys, budget_delta) = {
            let mut table = tx
                .open_table(STATE_TABLE)
                .map_err(|error| Error::Process(format!("state table: {error}")))?;
            let mut freed_bytes = 0_u64;
            let mut freed_keys = 0_u64;
            // A configured byte budget must reclaim expired bytes before the
            // check (the exact contract of the budget tests); without a
            // budget the purge is amortized space reclamation.
            if self.max_bytes.is_some()
                || self.writes.fetch_add(1, Ordering::Relaxed).is_multiple_of(PURGE_INTERVAL_WRITES)
            {
                let (bytes, keys) = Self::remove_expired_in_table(&mut table, now_ms)?;
                freed_bytes += bytes;
                freed_keys += keys;
            }
            let previous = table
                .get(storage_key.as_str())
                .map_err(|error| Error::Process(format!("state get: {error}")))?
                .map(|value| decode_value(value.value()))
                .transpose()?;
            let (previous_bytes, had_previous) = Self::previous_row(&previous, now_ms);
            let present_but_expired = previous.is_some() && !had_previous;
            if present_but_expired {
                // The row exists but its TTL has passed, so it is not a live
                // previous value. Purge it instead of overwriting it: the
                // overwrite would leave the key counter incrementing for a row
                // that was already physically present, and the purge also
                // credits the reclaimed bytes.
                let (bytes, keys) = Self::remove_expired_in_table(&mut table, now_ms)?;
                freed_bytes = freed_bytes.saturating_add(bytes);
                freed_keys = freed_keys.saturating_add(keys);
            }
            // The budget is checked against the table's exact size inside this
            // write transaction: the tracked counters are observability only
            // and must not gate a write, because a counter that ever wrapped or
            // drifted would reject every future write. `max_bytes` is the one
            // configuration that needs an exact answer, so it pays for the
            // scan and reconciles the counter to the measurement afterwards.
            let budget_delta = if self.max_bytes.is_some() {
                let mut live_bytes = 0_u64;
                let mut live_keys = 0_u64;
                for item in table
                    .iter()
                    .map_err(|error| Error::Process(format!("state scan: {error}")))?
                {
                    let (stored_key, stored_value) =
                        item.map_err(|error| Error::Process(format!("state scan: {error}")))?;
                    if stored_key.value() == storage_key.as_str() {
                        continue;
                    }
                    let decoded = decode_value(stored_value.value())?;
                    if decoded
                        .expires_at_ms
                        .is_some_and(|expires| expires <= now_ms)
                    {
                        continue;
                    }
                    live_bytes = live_bytes.saturating_add(decoded.value.len() as u64);
                    live_keys = live_keys.saturating_add(1);
                }
                let next_bytes = live_bytes.saturating_add(value.len() as u64);
                if let Some(max_bytes) = self.max_bytes {
                    if next_bytes > max_bytes {
                        return Err(Error::Process(format!(
                            "state budget exceeded: {next_bytes} > {max_bytes} bytes"
                        )));
                    }
                }
                Some((next_bytes, live_keys.saturating_add(1)))
            } else {
                None
            };
            table
                .insert(storage_key.as_str(), encoded.as_slice())
                .map_err(|error| Error::Process(format!("state put: {error}")))?;
            (
                previous_bytes,
                had_previous,
                freed_bytes,
                freed_keys,
                budget_delta,
            )
        };
        tx.commit()
            .map_err(|error| Error::Process(format!("state commit: {error}")))?;
        match budget_delta {
            // The budget scan measured the table exactly: adopt the
            // measurement so the counters cannot stay wedged or drifted.
            Some((next_bytes, next_keys)) => {
                self.bytes.store(next_bytes, Ordering::Relaxed);
                self.keys.store(next_keys, Ordering::Relaxed);
            }
            // Incremental accounting instead of a full-table metrics rescan.
            None => {
                if had_previous {
                    Self::release_bytes(&self.bytes, previous_bytes);
                } else {
                    self.keys.fetch_add(1, Ordering::Relaxed);
                }
                self.bytes.fetch_add(value.len() as u64, Ordering::Relaxed);
                Self::release_bytes(&self.bytes, freed_bytes);
                Self::release_keys(&self.keys, freed_keys);
            }
        }
        Ok(())
    }

    fn update_i64(&self, namespace: &str, key: &[u8], delta: i64) -> Result<i64, Error> {
        self.update_i64_with_ttl(namespace, key, delta, None)
    }

    fn update_i64_with_ttl(
        &self,
        namespace: &str,
        key: &[u8],
        delta: i64,
        ttl_ms: Option<u64>,
    ) -> Result<i64, Error> {
        let _write_guard = self.write_lock.lock().unwrap();
        let storage_key = Self::storage_key(namespace, key);
        let current_time_ms = now_ms();
        let tx = self
            .db
            .begin_write()
            .map_err(|error| Error::Process(format!("state write: {error}")))?;
        let (
            previous_bytes,
            had_previous,
            next,
            next_len,
            freed_bytes,
            freed_keys,
            budget_delta,
        ) = {
            let mut table = tx
                .open_table(STATE_TABLE)
                .map_err(|error| Error::Process(format!("state table: {error}")))?;
            let mut freed_bytes = 0_u64;
            let mut freed_keys = 0_u64;
            // A configured byte budget must reclaim expired bytes before the
            // check; without a budget the purge is amortized space
            // reclamation (reads hide expired values either way).
            if self.max_bytes.is_some()
                || self.writes.fetch_add(1, Ordering::Relaxed).is_multiple_of(PURGE_INTERVAL_WRITES)
            {
                let (bytes, keys) = Self::remove_expired_in_table(&mut table, current_time_ms)?;
                freed_bytes += bytes;
                freed_keys += keys;
            }
            let previous = table
                .get(storage_key.as_str())
                .map_err(|error| Error::Process(format!("state get: {error}")))?
                .map(|value| decode_value(value.value()))
                .transpose()?;
            let (previous_bytes, had_previous) = Self::previous_row(&previous, current_time_ms);
            if previous.is_some() && !had_previous {
                // Same rule as `put_with_ttl`: an expired row is purged rather
                // than overwritten, so the key counter does not increment for a
                // row that was already present.
                let (bytes, keys) =
                    Self::remove_expired_in_table(&mut table, current_time_ms)?;
                freed_bytes = freed_bytes.saturating_add(bytes);
                freed_keys = freed_keys.saturating_add(keys);
            }
            let current = previous
                .filter(|value| {
                    !value
                        .expires_at_ms
                        .is_some_and(|expires| expires <= current_time_ms)
                })
                .map(|value| serde_json::from_slice::<i64>(&value.value))
                .transpose()?
                .unwrap_or_default();
            let next = current.saturating_add(delta);
            let next_value = serde_json::to_vec(&next)?;
            // The budget is measured inside the write transaction for the same
            // reason as `put_with_ttl`: the tracked counters are observability
            // only, and a drifted counter must not reject a legal write.
            let budget_delta = if self.max_bytes.is_some() {
                let mut live_bytes = 0_u64;
                let mut live_keys = 0_u64;
                for item in table
                    .iter()
                    .map_err(|error| Error::Process(format!("state scan: {error}")))?
                {
                    let (stored_key, stored_value) =
                        item.map_err(|error| Error::Process(format!("state scan: {error}")))?;
                    if stored_key.value() == storage_key.as_str() {
                        continue;
                    }
                    let decoded = decode_value(stored_value.value())?;
                    if decoded
                        .expires_at_ms
                        .is_some_and(|expires| expires <= current_time_ms)
                    {
                        continue;
                    }
                    live_bytes = live_bytes.saturating_add(decoded.value.len() as u64);
                    live_keys = live_keys.saturating_add(1);
                }
                let next_total = live_bytes.saturating_add(next_value.len() as u64);
                if let Some(max_bytes) = self.max_bytes {
                    if next_total > max_bytes {
                        return Err(Error::Process(format!(
                            "state budget exceeded: {next_total} > {max_bytes} bytes"
                        )));
                    }
                }
                Some((next_total, live_keys.saturating_add(1)))
            } else {
                None
            };
            let encoded = encode_value(
                &next_value,
                ttl_ms.map(|ttl| current_time_ms.saturating_add(ttl)),
            )?;
            table
                .insert(storage_key.as_str(), encoded.as_slice())
                .map_err(|error| Error::Process(format!("state put: {error}")))?;
            (
                previous_bytes,
                had_previous,
                next,
                next_value.len() as u64,
                freed_bytes,
                freed_keys,
                budget_delta,
            )
        };
        tx.commit()
            .map_err(|error| Error::Process(format!("state commit: {error}")))?;
        match budget_delta {
            Some((next_total, next_keys)) => {
                self.bytes.store(next_total, Ordering::Relaxed);
                self.keys.store(next_keys, Ordering::Relaxed);
            }
            None => {
                if had_previous {
                    Self::release_bytes(&self.bytes, previous_bytes);
                } else {
                    self.keys.fetch_add(1, Ordering::Relaxed);
                }
                self.bytes.fetch_add(next_len, Ordering::Relaxed);
                Self::release_bytes(&self.bytes, freed_bytes);
                Self::release_keys(&self.keys, freed_keys);
            }
        }
        Ok(next)
    }

    fn delete(&self, namespace: &str, key: &[u8]) -> Result<bool, Error> {
        let _write_guard = self.write_lock.lock().unwrap();
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
        if let Some(previous_bytes) = previous {
            Self::release_keys(&self.keys, 1);
            Self::release_bytes(&self.bytes, previous_bytes);
        }
        Ok(previous.is_some())
    }

    fn purge_expired(&self, now_ms: u64) -> Result<u64, Error> {
        let _write_guard = self.write_lock.lock().unwrap();
        let tx = self
            .db
            .begin_write()
            .map_err(|error| Error::Process(format!("state write: {error}")))?;
        let removed = {
            let mut table = tx
                .open_table(STATE_TABLE)
                .map_err(|error| Error::Process(format!("state table: {error}")))?;
            let mut expired = Vec::new();
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
            let mut removed = 0_u64;
            let mut freed_bytes = 0_u64;
            for key in expired {
                let freed = table
                    .remove(key.as_str())
                    .map_err(|error| Error::Process(format!("state purge: {error}")))?
                    .map(|guard| {
                        decode_value(guard.value())
                            .map(|decoded| decoded.value.len() as u64)
                            .unwrap_or_default()
                    })
                    .unwrap_or_default();
                freed_bytes = freed_bytes.saturating_add(freed);
                removed = removed.saturating_add(1);
            }
            (removed, freed_bytes)
        };
        tx.commit()
            .map_err(|error| Error::Process(format!("state commit: {error}")))?;
        // Counters are released only after the commit: a failed commit leaves
        // the rows in the table, so decrementing before it would under-report.
        let (removed, freed_bytes) = removed;
        Self::release_keys(&self.keys, removed);
        Self::release_bytes(&self.bytes, freed_bytes);
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
        let now = now_ms();
        let mut live_entries = BTreeMap::<String, &StateEntry>::new();
        for entry in &snapshot.entries {
            if entry
                .expires_at_ms
                .is_some_and(|expires| expires <= now)
            {
                continue;
            }
            live_entries.insert(Self::storage_key(&entry.namespace, &entry.key), entry);
        }
        let restored_bytes = live_entries
            .values()
            .map(|entry| entry.value.len() as u64)
            .sum::<u64>();
        if let Some(max_bytes) = self.max_bytes {
            if restored_bytes > max_bytes {
                return Err(Error::Process(format!(
                    "state restore exceeds budget: {restored_bytes} > {max_bytes} bytes"
                )));
            }
        }
        let _write_guard = self.write_lock.lock().unwrap();
        let tx = self
            .db
            .begin_write()
            .map_err(|error| Error::Process(format!("state write: {error}")))?;
        let restored = {
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
            for (key, entry) in &live_entries {
                let encoded = encode_value(&entry.value, entry.expires_at_ms)?;
                table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(|error| Error::Process(format!("state restore: {error}")))?;
            }
            (live_entries.len() as u64, restored_bytes)
        };
        tx.commit()
            .map_err(|error| Error::Process(format!("state commit: {error}")))?;
        // A restore replaces the whole table, so the exact post-restore size is
        // the count of live snapshot entries — no rescan needed, and the
        // counters cannot keep a stale size from before the restore.
        let (restored_keys, restored_bytes) = restored;
        self.keys.store(restored_keys, Ordering::Relaxed);
        self.bytes.store(restored_bytes, Ordering::Relaxed);
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
                let decoded = decode_value(value.value())?;
                if decoded
                    .expires_at_ms
                    .is_some_and(|expires| expires <= now_ms())
                {
                    continue;
                }
                metrics.keys += 1;
                metrics.bytes += decoded.value.len() as u64;
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

pub(crate) fn now_ms() -> u64 {
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
        let base = now_ms();
        backend
            .put_with_ttl("orders", b"a", b"1", Some(10_000), base)
            .unwrap();
        // Exactly reaching the configured live-byte budget is valid.
        backend.put("orders", b"b", b"1").unwrap();
        assert_eq!(backend.metrics().unwrap().bytes, 2);
        assert_eq!(backend.get("orders", b"b").unwrap(), Some(b"1".to_vec()));
        // The next write exceeds the budget and must not partially commit.
        assert!(backend.put("orders", b"b", b"22").is_err());
        assert_eq!(backend.get("orders", b"b").unwrap(), Some(b"1".to_vec()));
        assert_eq!(backend.purge_expired(base + 9_999).unwrap(), 0);
        assert_eq!(backend.purge_expired(base + 10_000).unwrap(), 1);
    }

    #[test]
    fn restore_rejects_a_snapshot_over_the_state_budget_without_clearing_state() {
        let dir = tempfile::tempdir().unwrap();
        let backend = RedbStateBackend::open(dir.path(), 1)
            .unwrap()
            .with_max_bytes(2);
        backend.put("orders", b"existing", b"ok").unwrap();
        let snapshot = StateSnapshot::new(
            1,
            vec![StateEntry {
                namespace: "orders".into(),
                key: b"restored".to_vec(),
                value: b"too-large".to_vec(),
                expires_at_ms: None,
            }],
        );

        let error = backend.restore(&snapshot).unwrap_err();
        assert!(error.to_string().contains("state restore exceeds budget"));
        assert_eq!(
            backend.get("orders", b"existing").unwrap(),
            Some(b"ok".to_vec())
        );
        assert_eq!(backend.get("orders", b"restored").unwrap(), None);
    }

    /// Regression: the byte counters are observability only. They used to gate
    /// the budget check, and a compensation that resynchronized them from a
    /// scan that hides expired rows left them below the bytes a later purge
    /// released — `AtomicU64::fetch_sub` then wrapped to ~`u64::MAX` and every
    /// subsequent write failed `state budget exceeded` for the process
    /// lifetime.
    #[test]
    fn expired_purge_after_a_resync_does_not_wedge_the_budget() {
        let dir = tempfile::tempdir().unwrap();
        let backend = RedbStateBackend::open(dir.path(), 1)
            .unwrap()
            .with_max_bytes(1_000);
        let base = now_ms();
        // A large TTL row fills most of the budget.
        backend
            .put_with_ttl("orders", b"a", &vec![b'x'; 900], Some(1), base)
            .unwrap();
        // Let it expire without a TTL-aware reader touching it, then run a
        // state-journal compensation (`restore_entry` with the entry it
        // replaced).
        backend
            .restore_entry("orders", b"a", Some(&StateEntry {
                namespace: "orders".into(),
                key: b"a".to_vec(),
                value: vec![b'x'; 900],
                expires_at_ms: Some(base + 1),
            }))
            .unwrap();
        // The next write purges the expired row. The budget measures the table
        // exactly, so a legal write must still succeed.
        backend.put("orders", b"b", b"22").unwrap();
        assert_eq!(backend.get("orders", b"b").unwrap(), Some(b"22".to_vec()));
        let metrics = backend.metrics().unwrap();
        assert_eq!(metrics.keys, 1);
        assert_eq!(metrics.bytes, 2);
    }

    /// Regression: overwriting an expired-but-unpurged row must not count as a
    /// new key. The old code took the "no live previous row" branch and
    /// incremented the key counter for a row that was already present.
    #[test]
    fn overwriting_an_expired_row_does_not_inflate_the_key_count() {
        let dir = tempfile::tempdir().unwrap();
        let backend = RedbStateBackend::open(dir.path(), 1).unwrap();
        // A past base keeps the TTL live relative to wall-clock `metrics`.
        let base = now_ms().saturating_sub(1_000);
        backend
            .put_with_ttl("orders", b"a", b"1", Some(10_000), base)
            .unwrap();
        assert_eq!(backend.metrics().unwrap().keys, 1);
        // Overwrite after the TTL passed but before any purge ran.
        backend
            .put_with_ttl("orders", b"a", b"2", Some(10_000), base + 11)
            .unwrap();
        assert_eq!(
            backend.metrics().unwrap().keys,
            1,
            "the row was physically present, so the key count must not grow"
        );
        assert_eq!(backend.get("orders", b"a").unwrap(), Some(b"2".to_vec()));
    }

    /// The purge releases counters only after its transaction commits.
    #[test]
    fn purge_and_delete_release_counters_without_underflow() {
        let dir = tempfile::tempdir().unwrap();
        let backend = RedbStateBackend::open(dir.path(), 1).unwrap();
        let base = now_ms();
        backend
            .put_with_ttl("orders", b"a", b"1234", Some(1), base)
            .unwrap();
        backend.put("orders", b"b", b"12").unwrap();
        assert_eq!(backend.purge_expired(base + 2).unwrap(), 1);
        assert_eq!(backend.metrics().unwrap().keys, 1);
        assert_eq!(backend.metrics().unwrap().bytes, 2);
        assert!(backend.delete("orders", b"b").unwrap());
        assert!(!backend.delete("orders", b"b").unwrap());
        assert_eq!(backend.metrics().unwrap().keys, 0);
        assert_eq!(backend.metrics().unwrap().bytes, 0);
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
    fn keyed_counter_reclaims_expired_bytes_before_budget_check() {
        let dir = tempfile::tempdir().unwrap();
        let backend: std::sync::Arc<dyn StateBackend> = std::sync::Arc::new(
            RedbStateBackend::open(dir.path(), 1)
                .unwrap()
                .with_max_bytes(2),
        );
        backend
            .put_with_ttl("aggregate", b"expired", b"x", Some(1), 0)
            .unwrap();
        backend
            .put_with_ttl("aggregate", b"expired-2", b"x", Some(1), 0)
            .unwrap();
        let counter = KeyedCounter::new(backend, "aggregate");
        assert_eq!(counter.add(b"live", 1).unwrap(), 1);
    }

    #[test]
    fn keyed_counter_applies_configured_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let backend: std::sync::Arc<dyn StateBackend> =
            std::sync::Arc::new(RedbStateBackend::open(dir.path(), 1).unwrap());
        let counter = KeyedCounter::with_ttl(backend.clone(), "aggregate", Some(0));
        assert_eq!(counter.add(b"expired", 1).unwrap(), 1);
        assert!(backend
            .snapshot_at(now_ms().saturating_add(1))
            .unwrap()
            .entries
            .is_empty());
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
