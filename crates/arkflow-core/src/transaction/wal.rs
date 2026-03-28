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

//! Write-Ahead Log (WAL) for transaction durability
//!
//! The WAL provides durability guarantees for transactions by appending
//! transaction records to a log before committing them.

use crate::Error;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::RwLock;

use super::types::TransactionRecord;

/// WAL configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// Directory to store WAL files
    pub wal_dir: String,

    /// Maximum WAL file size before rotation
    pub max_file_size: u64,

    /// Whether to sync on every write (safer but slower)
    pub sync_on_write: bool,

    /// Whether to compress WAL entries
    pub compression: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            wal_dir: "/var/lib/arkflow/wal".to_string(),
            max_file_size: 1024 * 1024 * 1024, // 1GB
            sync_on_write: true,
            compression: true,
        }
    }
}

/// WAL entry wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalEntry {
    /// Transaction record
    record: TransactionRecord,

    /// Checksum for integrity verification
    checksum: u64,
}

impl WalEntry {
    fn new(record: TransactionRecord) -> Self {
        // Simple checksum (in production, use CRC32)
        let serialized = bincode::serialize(&record).unwrap_or_default();
        let checksum = serialized
            .iter()
            .fold(0u64, |acc, &b| acc.wrapping_mul(31).wrapping_add(b as u64));

        Self { record, checksum }
    }

    fn verify(&self) -> bool {
        let serialized = bincode::serialize(&self.record).unwrap_or_default();
        let checksum = serialized
            .iter()
            .fold(0u64, |acc, &b| acc.wrapping_mul(31).wrapping_add(b as u64));
        checksum == self.checksum
    }
}

/// Write-Ahead Log trait
#[async_trait]
pub trait WriteAheadLog: Send + Sync {
    /// Append a transaction record to the WAL
    async fn append(&self, record: &TransactionRecord) -> Result<(), Error>;

    /// Recover uncommitted transactions from WAL
    async fn recover(&self) -> Result<Vec<TransactionRecord>, Error>;

    /// Truncate the WAL (remove old entries)
    async fn truncate(&self, retain_last_n: usize) -> Result<(), Error>;
}

/// File-based WAL implementation
pub struct FileWal {
    config: WalConfig,
    current_file: Arc<RwLock<Option<File>>>,
    current_size: Arc<RwLock<u64>>,
    wal_dir: PathBuf,
}

impl FileWal {
    /// Create a new file-based WAL
    pub fn new(config: WalConfig) -> Result<Self, Error> {
        let wal_dir = PathBuf::from(&config.wal_dir);

        // Create WAL directory if it doesn't exist
        std::fs::create_dir_all(&wal_dir)
            .map_err(|e| Error::Read(format!("Failed to create WAL directory: {}", e)))?;

        Ok(Self {
            config,
            current_file: Arc::new(RwLock::new(None)),
            current_size: Arc::new(RwLock::new(0)),
            wal_dir,
        })
    }

    /// Get the current WAL file path
    fn wal_file_path(&self) -> PathBuf {
        self.wal_dir.join("wal.log")
    }

    /// Ensure WAL file is open
    async fn ensure_file_open(&self) -> Result<(), Error> {
        let mut file_guard = self.current_file.write().await;
        if file_guard.is_some() {
            return Ok(());
        }

        let path = self.wal_file_path();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| Error::Read(format!("Failed to open WAL file: {}", e)))?;

        // Get current file size
        let metadata = file
            .metadata()
            .await
            .map_err(|e| Error::Read(format!("Failed to get WAL metadata: {}", e)))?;
        *self.current_size.write().await = metadata.len();

        *file_guard = Some(file);
        Ok(())
    }
}

#[async_trait]
impl WriteAheadLog for FileWal {
    async fn append(&self, record: &TransactionRecord) -> Result<(), Error> {
        self.ensure_file_open().await?;

        // Create WAL entry
        let entry = WalEntry::new(record.clone());

        // Serialize
        let serialized = bincode::serialize(&entry)
            .map_err(|e| Error::Process(format!("Failed to serialize WAL entry: {}", e)))?;

        // Write length prefix (4 bytes)
        let len = serialized.len() as u32;
        let mut file_guard = self.current_file.write().await;
        let file = file_guard.as_mut().unwrap();

        file.write_u32(len)
            .await
            .map_err(|e| Error::Read(format!("Failed to write WAL length: {}", e)))?;

        // Write data
        file.write_all(&serialized)
            .await
            .map_err(|e| Error::Read(format!("Failed to write WAL data: {}", e)))?;

        // Optionally sync
        if self.config.sync_on_write {
            file.sync_all()
                .await
                .map_err(|e| Error::Read(format!("Failed to sync WAL: {}", e)))?;
        }

        // Update size
        let mut size = self.current_size.write().await;
        *size += 4 + serialized.len() as u64;

        Ok(())
    }

    async fn recover(&self) -> Result<Vec<TransactionRecord>, Error> {
        let path = self.wal_file_path();

        // Check if WAL file exists
        if !path.exists() {
            return Ok(Vec::new());
        }

        // Open file for reading
        let file = File::open(&path)
            .await
            .map_err(|e| Error::Read(format!("Failed to open WAL for recovery: {}", e)))?;

        let mut reader = BufReader::new(file);
        let mut records = Vec::new();

        loop {
            // Read length prefix
            let len = match reader.read_u32().await {
                Ok(l) => l,
                Err(_) => break, // EOF or corrupted
            };

            // Prevent unreasonably large allocations
            if len > 10 * 1024 * 1024 {
                return Err(Error::Process(format!(
                    "WAL entry too large: {} bytes",
                    len
                )));
            }

            // Read entry data
            let mut buffer = vec![0u8; len as usize];
            if (reader.read_exact(&mut buffer).await).is_err() {
                break;
            }

            // Deserialize
            let entry: WalEntry = bincode::deserialize(&buffer)
                .map_err(|e| Error::Process(format!("Failed to deserialize WAL entry: {}", e)))?;

            // Verify checksum
            if !entry.verify() {
                return Err(Error::Process("WAL entry checksum mismatch".to_string()));
            }

            // Only keep non-terminal transactions
            if !entry.record.is_terminal() {
                records.push(entry.record);
            }
        }

        tracing::info!("Recovered {} transactions from WAL", records.len());
        Ok(records)
    }

    async fn truncate(&self, retain_last_n: usize) -> Result<(), Error> {
        // Recover all records
        let all_records = self.recover().await?;

        if all_records.len() <= retain_last_n {
            return Ok(());
        }

        // Keep only the last N records
        let retained: Vec<_> = all_records.into_iter().rev().take(retain_last_n).collect();

        // Rewrite WAL file
        let path = self.wal_file_path();

        // Close current file
        *self.current_file.write().await = None;
        *self.current_size.write().await = 0;

        // Create new file
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await
            .map_err(|e| Error::Read(format!("Failed to recreate WAL: {}", e)))?;

        // Write retained records (in original order)
        for record in retained.into_iter().rev() {
            let entry = WalEntry::new(record);
            let serialized = bincode::serialize(&entry)
                .map_err(|e| Error::Process(format!("Failed to serialize: {}", e)))?;

            let len = serialized.len() as u32;
            file.write_u32(len)
                .await
                .map_err(|e| Error::Read(format!("Failed to write length: {}", e)))?;
            file.write_all(&serialized)
                .await
                .map_err(|e| Error::Read(format!("Failed to write data: {}", e)))?;
        }

        file.sync_all()
            .await
            .map_err(|e| Error::Read(format!("Failed to sync WAL: {}", e)))?;

        tracing::info!("Truncated WAL, retained {} records", retain_last_n);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::types::TransactionState;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_wal_entry_checksum() {
        let record = TransactionRecord::new(1, vec![10, 20]);
        let entry = WalEntry::new(record);

        assert!(entry.verify());
    }

    #[tokio::test]
    async fn test_wal_append_and_recover() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            wal_dir: temp_dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };

        let wal = FileWal::new(config).unwrap();

        // Append some records
        let mut record1 = TransactionRecord::new(1, vec![10]);
        record1.transition_to(TransactionState::Prepared);
        wal.append(&record1).await.unwrap();

        let mut record2 = TransactionRecord::new(2, vec![20]);
        record2.transition_to(TransactionState::Prepared);
        wal.append(&record2).await.unwrap();

        // Recover
        let recovered = wal.recover().await.unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].id, 1);
        assert_eq!(recovered[1].id, 2);
    }

    #[tokio::test]
    async fn test_wal_truncate() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            wal_dir: temp_dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };

        let wal = FileWal::new(config).unwrap();

        // Append 5 records
        for i in 1..=5 {
            let mut record = TransactionRecord::new(i, vec![i * 10]);
            record.transition_to(TransactionState::Prepared);
            wal.append(&record).await.unwrap();
        }

        // Truncate to keep last 2
        wal.truncate(2).await.unwrap();

        // Recover should only get 2 records
        let recovered = wal.recover().await.unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].id, 4);
        assert_eq!(recovered[1].id, 5);
    }

    #[tokio::test]
    async fn test_wal_no_file() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            wal_dir: temp_dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };

        let wal = FileWal::new(config).unwrap();
        let recovered = wal.recover().await.unwrap();

        assert_eq!(recovered.len(), 0);
    }
}
