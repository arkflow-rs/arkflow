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

//! Pluggable storage backend for the WAL (`WalStore` trait + builder registry).
//!
//! `Wal` itself owns the batching layer (pending buffer, flusher task, cursor
//! atomic). The storage layer is delegated to a [`WalStore`] so the same engine
//! can target a local embedded `redb` (the default) or an S3-compatible object
//! store (provided by `arkflow-plugin`) without changing the surrounding
//! pipeline.
//!
//! Backends are registered via [`register_wal_store_builder`] (mirroring the
//! input/output/processor/buffer/codec registries) and selected by
//! `WalConfig.backend_kind()`. The local `redb` builder is registered
//! on-demand by [`ensure_local_store_registered`]; plugin-provided backends
//! (e.g. the `s3` builder) register themselves at `init()` time.

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, RwLock};

use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use redb::{Database, ReadableTable, TableDefinition};

use crate::wal::WalConfig;
use crate::{Error, MessageBatch, MessageBatchRef};

// ---------- redb table definitions (shared with `Wal`'s pending-buffer flush) ----------

/// Table mapping sequence number → serialized message.
const ENTRIES: TableDefinition<u64, &[u8]> = TableDefinition::new("entries");
/// Single-row metadata table.
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");
/// Meta key holding the highest fully acknowledged sequence (the watermark).
const CURSOR_KEY: &str = "cursor";

// ---------- serialization (Arrow IPC with input-name prefix) ----------
//
// Reused by every backend (local redb + S3 segments) so entries written by one
// backend can be read by the same code on recovery.

/// Serialize a `MessageBatch` to bytes: a length-prefixed optional input name
/// followed by the Arrow IPC stream of the record batch (which preserves the
/// schema, data, and `__meta_*` metadata columns).
pub fn serialize(msg: &MessageBatch) -> Result<Vec<u8>, Error> {
    let record: &RecordBatch = msg.deref();
    let input_name = msg.get_input_name();

    let mut out = Vec::new();
    match &input_name {
        Some(name) => {
            let len = u32::try_from(name.len())
                .map_err(|_| Error::Process("input name too long".into()))?;
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(name.as_bytes());
        }
        None => out.extend_from_slice(&0u32.to_be_bytes()),
    }

    let mut writer =
        StreamWriter::try_new(&mut out, record.schema().as_ref()).map_err(|e| {
            Error::Process(format!("Failed to start Arrow IPC writer: {}", e))
        })?;
    writer
        .write(record)
        .map_err(|e| Error::Process(format!("Failed to write Arrow IPC: {}", e)))?;
    writer
        .finish()
        .map_err(|e| Error::Process(format!("Failed to finish Arrow IPC: {}", e)))?;
    Ok(out)
}

/// Deserialize bytes produced by [`serialize`] back into a `MessageBatch`.
pub fn deserialize(bytes: &[u8]) -> Result<MessageBatch, Error> {
    use std::io::{Cursor, Read};

    let mut cursor = Cursor::new(bytes);
    let mut len_buf = [0u8; 4];
    cursor
        .read_exact(&mut len_buf)
        .map_err(|e| Error::Process(format!("WAL entry truncated (name len): {}", e)))?;
    let name_len = u32::from_be_bytes(len_buf) as usize;
    let input_name = if name_len > 0 {
        let mut nb = vec![0u8; name_len];
        cursor
            .read_exact(&mut nb)
            .map_err(|e| Error::Process(format!("WAL entry truncated (name): {}", e)))?;
        Some(
            String::from_utf8(nb)
                .map_err(|e| Error::Process(format!("WAL entry has invalid utf8 name: {}", e)))?,
        )
    } else {
        None
    };

    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| Error::Process(format!("Failed to start Arrow IPC reader: {}", e)))?;
    let record_batch = reader
        .next()
        .ok_or_else(|| Error::Process("WAL entry has no record batch".into()))?
        .map_err(|e| Error::Process(format!("Failed to read Arrow IPC: {}", e)))?;

    let mut mb = MessageBatch::new_arrow(record_batch);
    mb.set_input_name(input_name);
    Ok(mb)
}

/// Pluggable storage backend for the WAL.
///
/// A `WalStore` owns its own medium (`redb::Database`, S3 client, …). The
/// `Wal` batching layer stages appends into `pending`, then drains them as a
/// `Vec<(u64, Vec<u8>)>` to `append_batch`. Cursor advancement and recovery
/// reads are direct.
///
/// All methods are synchronous so `Wal` can drive them from both sync
/// (`Wal::open`, `stream::run`'s recovery `?`) and async contexts (`Wal::append`'s
/// per-entry commit, flusher task). The async contract is preserved by
/// `Wal`'s wrappers, which use `tokio::task::spawn_blocking` for redb's
/// blocking calls and `await` directly for S3's async client.
pub trait WalStore: Send + Sync + 'static {
    /// Short name identifying this store kind (`"local"`, `"s3"`, …).
    /// Diagnostics only — the dispatcher uses `WalConfig.backend_kind()`.
    fn kind(&self) -> &'static str;

    /// Persist a batch of `(seq, payload)` entries. Implementations are free
    /// to coalesce within the batch.
    fn append_batch(&self, entries: Vec<(u64, Vec<u8>)>) -> Result<(), Error>;

    /// Advance the committed cursor to `seq` (monotonic). Implementations
    /// should no-op when `seq <= current_cursor`.
    fn advance_cursor(&self, seq: u64) -> Result<(), Error>;

    /// Read all entries with sequence strictly greater than the committed
    /// cursor, in ascending order. Used by recovery replay.
    fn read_after_cursor(&self) -> Result<Vec<(u64, MessageBatchRef)>, Error>;

    /// Current committed watermark (highest acked sequence, 0 if none).
    fn cursor(&self) -> u64;

    /// Hint for the first sequence number to assign on a fresh open.
    /// Defaults to `cursor() + 1`, which is safe for any backend (sealed
    /// entries above the cursor are still on the store and the recovery
    /// `read_after_cursor` will surface them). Backends with a more
    /// precise derivation (e.g. local `redb`, which tracks the max key
    /// directly) override this to avoid assigning seqs that already
    /// exist on disk.
    fn next_seq_hint(&self) -> u64 {
        self.cursor().saturating_add(1)
    }

    /// Flush any in-flight writes and release held resources. After this
    /// returns the store must be safe to drop. For `redb`, this is implicit
    /// via `Database::drop` (fcntl flock release); object stores await their
    /// last PUT in a blocking fashion.
    fn close(&self) -> Result<(), Error>;
}

/// Builder for a [`WalStore`] instance.
///
/// Mirrors the `InputBuilder` / `OutputBuilder` pattern. The plugin crate
/// implements one builder per backend kind and registers it via
/// [`register_wal_store_builder`] at `init()` time.
pub trait WalStoreBuilder: Send + Sync + 'static {
    /// Construct a `WalStore` from the full `WalConfig`. The backend kind
    /// selector has already been validated by [`build_wal_store`] before this
    /// is called.
    fn build(&self, cfg: &WalConfig) -> Result<Arc<dyn WalStore>, Error>;

    /// Short name identifying the backend kind (e.g. `"local"`, `"s3"`). Must
    /// match `WalConfig.backend_kind()` for dispatch.
    fn kind(&self) -> &'static str;
}

// ---- registry ----

lazy_static::lazy_static! {
    static ref WAL_STORE_BUILDERS: RwLock<HashMap<&'static str, Arc<dyn WalStoreBuilder>>> =
        RwLock::new(HashMap::new());
}

/// Register a `WalStoreBuilder` under `name`. Re-registering the same name
/// returns `Error::Config` (mirrors `register_input_builder` etc.).
pub fn register_wal_store_builder(
    name: &'static str,
    builder: Arc<dyn WalStoreBuilder>,
) -> Result<(), Error> {
    let mut map = WAL_STORE_BUILDERS.write().map_err(|_| {
        Error::Process("WAL store builder registry poisoned".into())
    })?;
    if map.contains_key(name) {
        return Err(Error::Config(format!(
            "WAL store type already registered: {}",
            name
        )));
    }
    map.insert(name, builder);
    Ok(())
}

/// Look up a registered builder by name. Used by [`build_wal_store`].
pub fn lookup_wal_store_builder(name: &str) -> Option<Arc<dyn WalStoreBuilder>> {
    WAL_STORE_BUILDERS.read().ok()?.get(name).cloned()
}

/// Build a `WalStore` for the given config.
///
/// Dispatches on `cfg.backend_kind()`. Returns `Error::Config` if the selected
/// kind has no registered builder. The local builder is auto-registered via
/// [`ensure_local_store_registered`] so legacy configs without a registered
/// plugin still resolve.
pub fn build_wal_store(cfg: &WalConfig) -> Result<Arc<dyn WalStore>, Error> {
    ensure_local_store_registered()?;
    let kind = cfg.backend_kind();
    let builder = lookup_wal_store_builder(kind).ok_or_else(|| {
        Error::Config(format!(
            "no WAL store builder registered for kind `{}`; \
             ensure the relevant plugin's `init()` has run",
            kind
        ))
    })?;
    builder.build(cfg)
}

/// Number of registered WAL store builders. Test-only.
pub fn registered_wal_store_count() -> usize {
    WAL_STORE_BUILDERS.read().map(|m| m.len()).unwrap_or(0)
}

// ---------- Local (redb) backend ----------

/// Embedded `redb` backend. The default when no `backend` key is present or
/// when `backend.type: local` is selected. Implements [`WalStore`].
///
/// `redb::Database` is internally `Arc`; on drop, the fcntl flock on the
/// database file is released, which is what `Wal::open` relies on for
/// reopen-after-close semantics.
pub struct RedbStore {
    db: Database,
}

impl RedbStore {
    /// Open (or create) a redb database at `<dir>/wal.redb`.
    pub fn open(path: &std::path::Path) -> Result<Self, Error> {
        std::fs::create_dir_all(path).map_err(|e| {
            Error::Process(format!("Failed to create WAL directory: {}", e))
        })?;
        let db_path = path.join("wal.redb");
        let db = Database::create(&db_path)
            .map_err(|e| Error::Process(format!("Failed to open WAL database: {}", e)))?;
        Ok(Self { db })
    }

    /// Highest sequence number present in the entries table, or 0 if empty.
    /// Used by `Wal::open` to derive `next_seq` (max + 1).
    pub fn max_seq(&self) -> Result<u64, Error> {
        let tx = self
            .db
            .begin_read()
            .map_err(|e| Error::Process(format!("WAL read failed: {}", e)))?;
        Ok(tx
            .open_table(ENTRIES)
            .ok()
            .and_then(|t| t.last().ok().flatten().map(|(k, _)| k.value()))
            .unwrap_or(0))
    }
}

impl WalStore for RedbStore {
    fn kind(&self) -> &'static str {
        "local"
    }

    fn append_batch(&self, entries: Vec<(u64, Vec<u8>)>) -> Result<(), Error> {
        // redb is synchronous; calls are brief (single fsync) and inline is
        // safe in the multi-thread runtime that arkflow ships with. `Wal`
        // wraps this in `spawn_blocking` for the async per-entry path so the
        // executor isn't blocked.
        let tx = self
            .db
            .begin_write()
            .map_err(|e| Error::Process(format!("WAL write failed: {}", e)))?;
        {
            let mut table = tx
                .open_table(ENTRIES)
                .map_err(|e| Error::Process(format!("WAL entries open failed: {}", e)))?;
            for (seq, bytes) in &entries {
                table
                    .insert(*seq, bytes.as_slice())
                    .map_err(|e| Error::Process(format!("WAL insert failed: {}", e)))?;
            }
        }
        tx.commit()
            .map_err(|e| Error::Process(format!("WAL commit failed: {}", e)))?;
        Ok(())
    }

    fn advance_cursor(&self, seq: u64) -> Result<(), Error> {
        let tx = self
            .db
            .begin_write()
            .map_err(|e| Error::Process(format!("WAL write failed: {}", e)))?;
        {
            let mut meta = tx
                .open_table(META)
                .map_err(|e| Error::Process(format!("WAL meta open failed: {}", e)))?;
            let current = meta
                .get(CURSOR_KEY)
                .map_err(|e| Error::Process(format!("WAL meta read failed: {}", e)))?
                .map(|g| g.value())
                .unwrap_or(0);
            if seq > current {
                meta.insert(CURSOR_KEY, seq).map_err(|e| {
                    Error::Process(format!("WAL meta write failed: {}", e))
                })?;
            }
        }
        tx.commit()
            .map_err(|e| Error::Process(format!("WAL commit failed: {}", e)))?;
        Ok(())
    }

    fn read_after_cursor(&self) -> Result<Vec<(u64, MessageBatchRef)>, Error> {
        let tx = self
            .db
            .begin_read()
            .map_err(|e| Error::Process(format!("WAL read failed: {}", e)))?;
        let cursor = tx
            .open_table(META)
            .ok()
            .and_then(|t| t.get(CURSOR_KEY).ok().flatten().map(|g| g.value()))
            .unwrap_or(0);
        let Some(entries) = tx.open_table(ENTRIES).ok() else {
            return Ok(Vec::new());
        };

        let mut out = Vec::new();
        let range = entries
            .iter()
            .map_err(|e| Error::Process(format!("WAL iter failed: {}", e)))?;
        for item in range {
            let (k, v) = item.map_err(|e| Error::Process(format!("WAL iter failed: {}", e)))?;
            let seq = k.value();
            if seq <= cursor {
                continue;
            }
            let msg = Arc::new(deserialize(v.value())?);
            out.push((seq, msg));
        }
        Ok(out)
    }

    fn cursor(&self) -> u64 {
        let tx = match self.db.begin_read() {
            Ok(tx) => tx,
            Err(_) => return 0,
        };
        tx.open_table(META)
            .ok()
            .and_then(|t| t.get(CURSOR_KEY).ok().flatten().map(|g| g.value()))
            .unwrap_or(0)
    }

    fn next_seq_hint(&self) -> u64 {
        // For local redb we know the exact max seq. Anything simpler would
        // under-count after a restart where the cursor advanced past the
        // tail of the table.
        self.max_seq()
            .map(|m| m.saturating_add(1))
            .unwrap_or(1)
            .max(1)
    }

    fn close(&self) -> Result<(), Error> {
        // redb drops implicitly when the last `Arc<RedbStore>` is released,
        // releasing the fcntl flock. `close` is a no-op here.
        Ok(())
    }
}

/// Builder for the local `redb` backend. Registered under the name `"local"`.
pub struct LocalStoreBuilder;

impl WalStoreBuilder for LocalStoreBuilder {
    fn build(&self, cfg: &WalConfig) -> Result<Arc<dyn WalStore>, Error> {
        // The dispatcher routes by `cfg.backend_kind()`; we only land here
        // for `local`. For the legacy flat shape, `WalConfig.local_path()`
        // returns `Some(&cfg.path)`.
        let path = cfg.local_path().ok_or_else(|| {
            Error::Config("LocalStoreBuilder requires a local `path`".into())
        })?;
        let store = RedbStore::open(std::path::Path::new(path))?;
        Ok(Arc::new(store))
    }

    fn kind(&self) -> &'static str {
        "local"
    }
}

/// Register the local `redb` builder. Idempotent — repeated calls are a
/// no-op (the registry rejects duplicates, which is fine on repeat). Safe
/// under concurrent test invocations.
pub fn ensure_local_store_registered() -> Result<(), Error> {
    let mut map = WAL_STORE_BUILDERS.write().map_err(|_| {
        Error::Process("WAL store builder registry poisoned".into())
    })?;
    if map.contains_key("local") {
        return Ok(());
    }
    map.insert("local", Arc::new(LocalStoreBuilder));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::SyncPolicy;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    fn sample_batch(input_name: Option<&str>) -> MessageBatch {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("data", DataType::Utf8, false),
            Field::new("__meta_offset", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
                std::sync::Arc::new(Int64Array::from(vec![Some(10), Some(20)])),
            ],
        )
        .unwrap();
        let mut mb = MessageBatch::new_arrow(batch);
        mb.set_input_name(input_name.map(|s| s.to_string()));
        mb
    }

    #[test]
    fn roundtrip_preserves_schema_data_and_metadata() {
        let mb = sample_batch(Some("kafka"));
        let bytes = serialize(&mb).unwrap();
        let back = deserialize(&bytes).unwrap();
        assert_eq!(back.get_input_name().as_deref(), Some("kafka"));
        let rb_in: &RecordBatch = mb.deref();
        let rb_out: &RecordBatch = back.deref();
        assert_eq!(rb_in.schema(), rb_out.schema());
        assert_eq!(rb_in.num_rows(), rb_out.num_rows());
    }

    #[test]
    fn redb_store_round_trip_through_trait() {
        let dir = std::env::temp_dir().join(format!(
            "arkflow-wal-store-trait-test-{}-{}",
            std::process::id(),
            AtomicU64::new(0).fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = WalConfig::local(true, dir.to_string_lossy().to_string(), SyncPolicy::PerEntry);
        let store = LocalStoreBuilder.build(&cfg).unwrap();
        assert_eq!(store.kind(), "local");

        let payload = serialize(&sample_batch(Some("kafka"))).unwrap();
        store
            .append_batch(vec![(1, payload.clone()), (2, payload.clone())])
            .unwrap();

        // Nothing acked → both entries are replay candidates.
        let replayed = store.read_after_cursor().unwrap();
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].0, 1);
        assert_eq!(replayed[1].0, 2);

        // Advance cursor to 1 → only seq 2 is replay candidate.
        store.advance_cursor(1).unwrap();
        let replayed = store.read_after_cursor().unwrap();
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].0, 2);

        // cursor() reflects the advance.
        assert_eq!(store.cursor(), 1);

        // Close is a no-op for redb (drops on last `Arc`).
        store.close().unwrap();
    }
}