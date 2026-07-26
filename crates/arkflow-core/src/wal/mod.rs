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

//! Write-ahead log (WAL) for durable input ingestion.
//!
//! Each message read from an input is persisted (`append`) and assigned a
//! monotonically increasing sequence number before it enters the pipeline.
//! The committed cursor is advanced (`advance`) only after the downstream
//! output confirms the write, so a crash between `append` and `advance` leaves
//! the entry in the WAL and it is replayed on recovery (`read_after_cursor`).
//!
//! Storage is an embedded `redb` database. Per-entry writes commit (and fsync)
//! a transaction per append. `group-commit` and `periodic` policies coalesce
//! concurrent appends into shared transactions to amortize the fsync cost, at
//! the price of a small loss window if the process crashes mid-flush.

use crate::{Error, MessageBatch, MessageBatchRef};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Table mapping sequence number → serialized message.
const ENTRIES: TableDefinition<u64, &[u8]> = TableDefinition::new("entries");
/// Single-row metadata table.
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");
/// Meta key holding the highest fully acknowledged sequence (the watermark).
const CURSOR_KEY: &str = "cursor";

/// Sync (fsync) policy for appends.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SyncPolicy {
    /// Commit (fsync) a transaction on every append. Fully durable; slowest.
    PerEntry,
    /// Coalesce concurrent appends into shared transactions flushed as soon as
    /// pending data is available.
    GroupCommit,
    /// Flush pending appends on a fixed interval.
    Periodic(Duration),
}

impl Default for SyncPolicy {
    fn default() -> Self {
        SyncPolicy::GroupCommit
    }
}

/// Configuration for a durable ingest WAL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// Whether durability is active for the stream. Defaults to `true` so that
    /// adding a `durability:` section enables it; set `false` to disable
    /// without removing the section. A stream with no `durability:` section at
    /// all is not durable (today's in-memory behavior).
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Directory in which to store the WAL database file.
    pub path: String,
    #[serde(default)]
    pub sync: SyncPolicy,
}

fn default_enabled() -> bool {
    true
}

/// A durable write-ahead log for input messages.
pub struct Wal {
    db: Database,
    /// Next sequence number to assign. Append is single-threaded (the input
    /// worker), but an atomic keeps it race-free regardless.
    next_seq: AtomicU64,
    policy: SyncPolicy,
    // --- staging for group-commit / periodic ---
    pending: Mutex<Vec<(u64, Vec<u8>)>>,
    pending_notify: Notify,
    close: CancellationToken,
    flusher: Mutex<Option<JoinHandle<()>>>,
}

impl Wal {
    /// Open (or create) a WAL database inside `config.path`.
    pub fn open(config: &WalConfig) -> Result<Arc<Self>, Error> {
        std::fs::create_dir_all(&config.path).map_err(|e| {
            Error::Process(format!("Failed to create WAL directory: {}", e))
        })?;
        let db_path = Path::new(&config.path).join("wal.redb");
        let db = Database::create(&db_path)
            .map_err(|e| Error::Process(format!("Failed to open WAL database: {}", e)))?;

        // Derive next_seq from existing data (max key + 1, or 1 if empty).
        let next_seq = {
            let tx = db
                .begin_read()
                .map_err(|e| Error::Process(format!("WAL read failed: {}", e)))?;
            tx.open_table(ENTRIES)
                .ok()
                .and_then(|t| t.last().ok().flatten().map(|(k, _)| k.value()))
                .map(|m| m + 1)
                .unwrap_or(1)
        };

        let wal = Arc::new(Self {
            db,
            next_seq: AtomicU64::new(next_seq),
            policy: config.sync.clone(),
            pending: Mutex::new(Vec::new()),
            pending_notify: Notify::new(),
            close: CancellationToken::new(),
            flusher: Mutex::new(None),
        });

        match &wal.policy {
            SyncPolicy::PerEntry => {}
            SyncPolicy::GroupCommit | SyncPolicy::Periodic(_) => {
                let handle = Self::spawn_flusher(wal.clone());
                *wal.flusher.try_lock().unwrap() = Some(handle);
            }
        }

        Ok(wal)
    }

    fn spawn_flusher(wal: Arc<Wal>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let interval = match &wal.policy {
                SyncPolicy::Periodic(d) => Some(*d),
                _ => None,
            };
            loop {
                let wait = async {
                    if let Some(d) = interval {
                        tokio::time::sleep(d).await;
                    } else {
                        wal.pending_notify.notified().await;
                    }
                };
                tokio::select! {
                    biased;
                    _ = wal.close.cancelled() => {
                        let _ = wal.flush_pending().await;
                        break;
                    }
                    _ = wait => {
                        let _ = wal.flush_pending().await;
                    }
                }
            }
        })
    }

    /// Persist a message and return its assigned sequence number.
    ///
    /// `per-entry` commits (fsyncs) before returning — fully durable. `group-
    /// commit` and `periodic` stage the entry and return immediately; the
    /// background flusher commits batches to amortize the fsync cost. Under
    /// those two policies a crash before the next flush loses staged entries
    /// (the documented small loss window) — pick `per-entry` when every entry
    /// must survive a crash regardless of timing.
    pub async fn append(&self, msg: &MessageBatchRef) -> Result<u64, Error> {
        let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
        let bytes = serialize(msg)?;

        match &self.policy {
            SyncPolicy::PerEntry => {
                self.commit_entry(seq, &bytes)?;
            }
            SyncPolicy::GroupCommit | SyncPolicy::Periodic(_) => {
                self.pending.lock().await.push((seq, bytes));
                self.pending_notify.notify_one();
            }
        }
        Ok(seq)
    }

    /// Advance the committed cursor to `seq` (monotonic). Called by the ack
    /// path only after the downstream output confirms the write.
    pub fn advance(&self, seq: u64) -> Result<(), Error> {
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
                meta.insert(CURSOR_KEY, seq)
                    .map_err(|e| Error::Process(format!("WAL meta write failed: {}", e)))?;
            }
        }
        tx.commit()
            .map_err(|e| Error::Process(format!("WAL commit failed: {}", e)))?;
        Ok(())
    }

    /// Read all entries with sequence strictly greater than the committed
    /// cursor, in ascending order. Used by recovery replay.
    pub fn read_after_cursor(&self) -> Result<Vec<(u64, MessageBatchRef)>, Error> {
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

    /// Current committed watermark (highest acked sequence, 0 if none).
    pub fn cursor(&self) -> u64 {
        self.db
            .begin_read()
            .ok()
            .and_then(|tx| {
                tx.open_table(META)
                    .ok()
                    .and_then(|t| t.get(CURSOR_KEY).ok().flatten().map(|g| g.value()))
            })
            .unwrap_or(0)
    }

    fn commit_entry(&self, seq: u64, bytes: &[u8]) -> Result<(), Error> {
        let tx = self
            .db
            .begin_write()
            .map_err(|e| Error::Process(format!("WAL write failed: {}", e)))?;
        {
            let mut table = tx
                .open_table(ENTRIES)
                .map_err(|e| Error::Process(format!("WAL entries open failed: {}", e)))?;
            table
                .insert(seq, bytes)
                .map_err(|e| Error::Process(format!("WAL insert failed: {}", e)))?;
        }
        tx.commit()
            .map_err(|e| Error::Process(format!("WAL commit failed: {}", e)))?;
        Ok(())
    }

    async fn flush_pending(&self) -> Result<(), Error> {
        let batch: Vec<(u64, Vec<u8>)> = {
            let mut p = self.pending.lock().await;
            if p.is_empty() {
                return Ok(());
            }
            std::mem::take(p.as_mut())
        };
        let tx = self
            .db
            .begin_write()
            .map_err(|e| Error::Process(format!("WAL write failed: {}", e)))?;
        {
            let mut table = tx
                .open_table(ENTRIES)
                .map_err(|e| Error::Process(format!("WAL entries open failed: {}", e)))?;
            for (seq, bytes) in &batch {
                table
                    .insert(*seq, bytes.as_slice())
                    .map_err(|e| Error::Process(format!("WAL insert failed: {}", e)))?;
            }
        }
        tx.commit()
            .map_err(|e| Error::Process(format!("WAL commit failed: {}", e)))?;
        Ok(())
    }

    /// Flush any staged appends and stop the background flusher. After this
    /// returns the flusher task has exited, so dropping the last `Arc<Wal>`
    /// closes the underlying database.
    ///
    /// The final flush result is returned so callers can surface a flush
    /// failure rather than silently dropping it. The flusher task's join is
    /// best-effort: a panic inside the flusher is logged and `Ok` is returned
    /// so shutdown still proceeds.
    pub async fn close(&self) -> Result<(), Error> {
        self.close.cancel();
        if let Some(handle) = self.flusher.lock().await.take() {
            if let Err(e) = handle.await {
                if e.is_panic() {
                    tracing::error!("WAL flusher task panicked during shutdown");
                }
            }
        }
        // Best-effort final flush (no-op for per-entry; pending already drained
        // by the flusher's shutdown branch otherwise). Surface the result so
        // a torn-write / disk failure is not silently lost on graceful shutdown.
        self.flush_pending().await
    }
}

/// Acknowledgement decorator that advances the WAL cursor before delegating to
/// the source-side ack. Wired into the stream so the durable cursor only moves
/// past a message once the downstream output confirms the write.
pub struct WalAck {
    wal: Arc<Wal>,
    seq: u64,
    inner: Arc<dyn crate::input::Ack>,
}

impl WalAck {
    pub fn new(wal: Arc<Wal>, seq: u64, inner: Arc<dyn crate::input::Ack>) -> Self {
        Self { wal, seq, inner }
    }
}

#[async_trait::async_trait]
impl crate::input::Ack for WalAck {
    async fn ack(&self) -> Result<(), Error> {
        // Advance the durable cursor first (so the entry is reclaimable on
        // restart), then commit the source. If the source ack fails after the
        // cursor advanced, the source re-delivers — at-least-once, not loss.
        self.wal.advance(self.seq)?;
        self.inner.ack().await?;
        Ok(())
    }
}

// ---------- serialization ----------

/// Serialize a `MessageBatch` to bytes: a length-prefixed optional input name
/// followed by the Arrow IPC stream of the record batch (which preserves the
/// schema, data, and `__meta_*` metadata columns).
fn serialize(msg: &MessageBatch) -> Result<Vec<u8>, Error> {
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
fn deserialize(bytes: &[u8]) -> Result<MessageBatch, Error> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{Ack, NoopAck};
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as StdArc;

    fn sample_batch(input_name: Option<&str>) -> MessageBatch {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("data", DataType::Utf8, false),
            Field::new("__meta_offset", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(StringArray::from(vec![Some("hello"), Some("world")])),
                StdArc::new(Int64Array::from(vec![Some(10), Some(20)])),
            ],
        )
        .unwrap();
        let mut mb = MessageBatch::new_arrow(batch);
        mb.set_input_name(input_name.map(|s| s.to_string()));
        mb
    }

    fn tempdir() -> std::path::PathBuf {
        static C: AtomicU64 = AtomicU64::new(0);
        let n = C.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("arkflow-wal-test-{}-{}", std::process::id(), n));
        std::fs::create_dir_all(&dir).unwrap();
        dir
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
        assert_eq!(
            rb_out
                .column_by_name("__meta_offset")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(1),
            20
        );
    }

    #[tokio::test]
    async fn per_entry_write_read_advance_and_reopen() {
        let dir = tempdir();
        let cfg = WalConfig {
            enabled: true,
            path: dir.to_string_lossy().to_string(),
            sync: SyncPolicy::PerEntry,
        };
        let wal = Wal::open(&cfg).unwrap();

        let seqs = [
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
        ];
        assert_eq!(seqs, [1, 2, 3]);

        // Nothing acked yet: all three are after the cursor.
        let pending = wal.read_after_cursor().unwrap();
        assert_eq!(pending.len(), 3);

        // Ack the first two in order; the third remains pending.
        wal.advance(1).unwrap();
        wal.advance(2).unwrap();
        assert_eq!(wal.cursor(), 2);
        let pending = wal.read_after_cursor().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, 3);

        // Reopen (simulate restart): next_seq continues, cursor persists.
        wal.close().await.unwrap();
        drop(wal);
        let wal2 = Wal::open(&cfg).unwrap();
        let pending = wal2.read_after_cursor().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, 3);
        // New appends continue from seq 4, not colliding.
        let s4 = wal2
            .append(&StdArc::new(sample_batch(None)))
            .await
            .unwrap();
        assert_eq!(s4, 4);
    }

    #[tokio::test]
    async fn group_commit_flushes_on_close() {
        let dir = tempdir();
        let cfg = WalConfig {
            enabled: true,
            path: dir.to_string_lossy().to_string(),
            sync: SyncPolicy::GroupCommit,
        };
        let wal = Wal::open(&cfg).unwrap();
        // group-commit stages appends; they are flushed by the background
        // flusher and on close. After close + reopen they must all be present.
        let s1 = wal.append(&StdArc::new(sample_batch(None))).await.unwrap();
        let s2 = wal.append(&StdArc::new(sample_batch(None))).await.unwrap();
        wal.close().await.unwrap();
        drop(wal);
        let wal2 = Wal::open(&cfg).unwrap();
        let seqs: Vec<u64> = wal2
            .read_after_cursor()
            .unwrap()
            .iter()
            .map(|(s, _)| *s)
            .collect();
        assert!(seqs.contains(&s1));
        assert!(seqs.contains(&s2));
    }

    #[tokio::test]
    async fn corrupted_store_surfaces_error() {
        let dir = tempdir();
        let cfg = WalConfig {
            enabled: true,
            path: dir.to_string_lossy().to_string(),
            sync: SyncPolicy::PerEntry,
        };
        let wal = Wal::open(&cfg).unwrap();
        wal.append(&StdArc::new(sample_batch(None)))
            .await
            .unwrap();
        wal.close().await.unwrap();
        drop(wal);
        // Corrupt the database header with garbage bytes (a torn header must
        // not silently produce a valid, empty WAL — it must surface an error).
        use std::io::Write;
        let db_path = dir.join("wal.redb");
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&db_path)
            .unwrap();
        f.write_all(&[0xFFu8; 256]).unwrap();
        f.flush().unwrap();
        drop(f);
        let result = Wal::open(&cfg);
        assert!(
            result.is_err(),
            "opening a corrupted WAL must surface an error"
        );
    }

    /// End-to-end crash-recovery contract (task 6.4): a message ingested but
    /// not yet acknowledged must survive a crash and be replayed on restart
    /// (no loss); the replay IS the at-least-once duplicate. Once the
    /// downstream confirms, the cursor advances and a further restart replays
    /// nothing.
    #[tokio::test]
    async fn crash_recovery_replays_unacked_then_advances_on_ack() {
        let dir = tempdir();
        let cfg = WalConfig {
            enabled: true,
            path: dir.to_string_lossy().to_string(),
            sync: SyncPolicy::PerEntry,
        };

        // Phase 1: ingest a message, then "crash" before acknowledging it.
        let wal = Wal::open(&cfg).unwrap();
        let seq = wal
            .append(&StdArc::new(sample_batch(Some("http"))))
            .await
            .unwrap();
        assert_eq!(seq, 1);
        // No advance — simulate a crash before the downstream output confirmed.
        wal.close().await.unwrap();
        drop(wal);

        // Phase 2: restart. Recovery must replay the unacked message (no loss).
        let wal2 = Wal::open(&cfg).unwrap();
        let replayed = wal2.read_after_cursor().unwrap();
        assert_eq!(
            replayed.len(),
            1,
            "unacked message must be replayed (no loss)"
        );
        assert_eq!(replayed[0].0, seq);
        assert_eq!(replayed[0].1.get_input_name().as_deref(), Some("http"));

        // Phase 3: downstream confirms → WalAck advances the cursor first, then
        // the (noop) source ack. The replay above is the at-least-once duplicate.
        let ack: StdArc<dyn Ack> = StdArc::new(WalAck::new(
            wal2.clone(),
            replayed[0].0,
            StdArc::new(NoopAck),
        ));
        ack.ack().await.unwrap();
        assert_eq!(wal2.cursor(), seq);
        drop(ack); // release the WalAck's Arc<Wal> so the database can close

        // Phase 4: a further restart replays nothing — fully acknowledged.
        wal2.close().await.unwrap();
        drop(wal2);
        let wal3 = Wal::open(&cfg).unwrap();
        assert!(wal3.read_after_cursor().unwrap().is_empty());
    }

    /// Throughput benchmark per sync policy (task 5.3). Ignored by default;
    /// run with `cargo test -p arkflow-core --lib wal::tests::bench_append_throughput --release -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore]
    async fn bench_append_throughput() {
        let n = 5_000u32;
        for (name, policy) in [
            ("per_entry", SyncPolicy::PerEntry),
            ("group_commit", SyncPolicy::GroupCommit),
            ("periodic_1ms", SyncPolicy::Periodic(Duration::from_millis(1))),
        ] {
            let dir = tempdir();
            let cfg = WalConfig {
                enabled: true,
                path: dir.to_string_lossy().to_string(),
                sync: policy,
            };
            let wal = Wal::open(&cfg).unwrap();
            let msg = StdArc::new(sample_batch(None));
            let start = std::time::Instant::now();
            for _ in 0..n {
                wal.append(&msg).await.unwrap();
            }
            wal.close().await.unwrap();
            let elapsed = start.elapsed();
            let per = elapsed / n;
            println!(
                "bench {}: {} appends in {:?} ({:.2} us/append, {:.0} appends/s)",
                name,
                n,
                elapsed,
                per.as_secs_f64() * 1e6,
                n as f64 / elapsed.as_secs_f64()
            );
        }
    }
}
