//! Checkpoint and savepoint contracts for the Job runtime.

use crate::job::{JobId, JobVersion};
use crate::state::StateSnapshot;
use crate::Error;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourcePosition {
    pub partition: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointBarrier {
    pub checkpoint_id: String,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCheckpointAck {
    pub task_id: String,
    pub attempt_id: String,
    pub partition: u32,
    pub state: StateSnapshot,
    pub source_positions: Vec<SourcePosition>,
    pub watermark_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointManifest {
    pub checkpoint_id: String,
    pub job_id: JobId,
    pub job_version: JobVersion,
    pub generation: u64,
    pub task_attempts: Vec<TaskAttemptSnapshot>,
    pub source_positions: Vec<SourcePosition>,
    pub watermarks_ms: BTreeMap<u32, i64>,
    pub in_flight_barrier: CheckpointBarrier,
    pub state_snapshots: Vec<StateSnapshotRef>,
    pub format_version: u32,
    pub checksum: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskAttemptSnapshot {
    pub task_id: String,
    pub attempt_id: String,
    pub node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateSnapshotRef {
    pub task_id: String,
    pub uri: String,
    pub checksum: u64,
    pub bytes: u64,
}

impl CheckpointManifest {
    pub fn verify(&self) -> bool {
        self.checksum == manifest_checksum(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Expired,
}

pub trait CheckpointStore: Send + Sync {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), Error>;
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error>;
    fn delete(&self, key: &str) -> Result<(), Error>;
}

#[derive(Debug, Clone)]
pub struct FileCheckpointStore {
    root: PathBuf,
}

impl FileCheckpointStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, Error> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)
            .map_err(|error| Error::Process(format!("create checkpoint store: {error}")))?;
        Ok(Self { root })
    }

    fn path_for(&self, key: &str) -> Result<PathBuf, Error> {
        if key.is_empty() || key.contains("..") || key.starts_with('/') {
            return Err(Error::Config("invalid checkpoint store key".into()));
        }
        Ok(self.root.join(key))
    }
}

impl CheckpointStore for FileCheckpointStore {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), Error> {
        let path = self.path_for(key)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| Error::Process(format!("create checkpoint prefix: {error}")))?;
        }
        std::fs::write(path, bytes)
            .map_err(|error| Error::Process(format!("write checkpoint object: {error}")))
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let path = self.path_for(key)?;
        match std::fs::read(path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(Error::Process(format!("read checkpoint object: {error}"))),
        }
    }

    fn delete(&self, key: &str) -> Result<(), Error> {
        let path = self.path_for(key)?;
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Error::Process(format!("delete checkpoint object: {error}"))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryArtifactKind {
    Checkpoint,
    Savepoint,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryArtifact {
    pub id: String,
    pub kind: RecoveryArtifactKind,
    pub manifest_key: String,
    pub job_version: JobVersion,
    pub format_version: u32,
    pub created_at_ms: u64,
    pub status: CheckpointStatus,
}

pub struct CheckpointRepository<S> {
    store: S,
}

impl<S: CheckpointStore> CheckpointRepository<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub fn write_checkpoint(
        &self,
        manifest: &CheckpointManifest,
    ) -> Result<RecoveryArtifact, Error> {
        if !manifest.verify() {
            return Err(Error::Process("cannot persist invalid checkpoint".into()));
        }
        let key = format!("checkpoints/{}/manifest.json", manifest.checkpoint_id);
        let bytes = serde_json::to_vec(manifest)?;
        self.store.put(&key, &bytes)?;
        Ok(RecoveryArtifact {
            id: manifest.checkpoint_id.clone(),
            kind: RecoveryArtifactKind::Checkpoint,
            manifest_key: key,
            job_version: manifest.job_version,
            format_version: manifest.format_version,
            created_at_ms: now_ms(),
            status: CheckpointStatus::Completed,
        })
    }

    pub fn read_manifest(&self, artifact: &RecoveryArtifact) -> Result<CheckpointManifest, Error> {
        let bytes = self.store.get(&artifact.manifest_key)?.ok_or_else(|| {
            Error::Process(format!("missing recovery artifact '{}'", artifact.id))
        })?;
        let manifest: CheckpointManifest = serde_json::from_slice(&bytes)?;
        if !manifest.verify() {
            return Err(Error::Process(format!(
                "recovery artifact '{}' checksum mismatch",
                artifact.id
            )));
        }
        Ok(manifest)
    }

    pub fn write_state_snapshot(
        &self,
        checkpoint_id: &str,
        snapshot: &StateSnapshot,
    ) -> Result<StateSnapshotRef, Error> {
        if !snapshot.verify() {
            return Err(Error::Process(
                "cannot persist invalid state snapshot".into(),
            ));
        }
        let bytes = serde_json::to_vec(snapshot)?;
        let key = format!(
            "checkpoints/{checkpoint_id}/state-{}.json",
            snapshot_checksum(snapshot)
        );
        self.store.put(&key, &bytes)?;
        Ok(StateSnapshotRef {
            task_id: String::new(),
            uri: key,
            checksum: snapshot_checksum(snapshot),
            bytes: bytes.len() as u64,
        })
    }

    pub fn read_state_snapshot(
        &self,
        reference: &StateSnapshotRef,
    ) -> Result<StateSnapshot, Error> {
        let bytes = self
            .store
            .get(&reference.uri)?
            .ok_or_else(|| Error::Process(format!("missing state snapshot '{}'", reference.uri)))?;
        let snapshot: StateSnapshot = serde_json::from_slice(&bytes)?;
        if !snapshot.verify() || snapshot_checksum(&snapshot) != reference.checksum {
            return Err(Error::Process("state snapshot checksum mismatch".into()));
        }
        Ok(snapshot)
    }

    pub fn create_savepoint(
        &self,
        manifest: &CheckpointManifest,
    ) -> Result<RecoveryArtifact, Error> {
        if !manifest.verify() {
            return Err(Error::Process("cannot persist invalid savepoint".into()));
        }
        let key = format!("savepoints/{}/manifest.json", manifest.checkpoint_id);
        self.store.put(&key, &serde_json::to_vec(manifest)?)?;
        Ok(RecoveryArtifact {
            id: manifest.checkpoint_id.clone(),
            kind: RecoveryArtifactKind::Savepoint,
            manifest_key: key,
            job_version: manifest.job_version,
            format_version: manifest.format_version,
            created_at_ms: now_ms(),
            status: CheckpointStatus::Completed,
        })
    }

    pub fn delete(&self, artifact: &RecoveryArtifact) -> Result<(), Error> {
        self.store.delete(&artifact.manifest_key)
    }
}

#[derive(Debug, Clone, Default)]
pub struct CheckpointCatalog {
    artifacts: Vec<RecoveryArtifact>,
}

impl CheckpointCatalog {
    pub fn record(&mut self, artifact: RecoveryArtifact) {
        self.artifacts
            .retain(|existing| existing.id != artifact.id || existing.kind != artifact.kind);
        self.artifacts.push(artifact);
        self.artifacts
            .sort_by_key(|artifact| artifact.created_at_ms);
    }

    pub fn latest_valid(
        &self,
        job_version: JobVersion,
        format_version: u32,
    ) -> Option<&RecoveryArtifact> {
        self.artifacts.iter().rev().find(|artifact| {
            artifact.kind == RecoveryArtifactKind::Checkpoint
                && artifact.status == CheckpointStatus::Completed
                && artifact.job_version == job_version
                && artifact.format_version == format_version
        })
    }

    pub fn retain_checkpoints(&mut self, retention: usize) -> Vec<RecoveryArtifact> {
        let mut removed = Vec::new();
        let checkpoint_ids: Vec<String> = self
            .artifacts
            .iter()
            .filter(|artifact| artifact.kind == RecoveryArtifactKind::Checkpoint)
            .map(|artifact| artifact.id.clone())
            .collect();
        let remove_count = checkpoint_ids.len().saturating_sub(retention);
        for id in checkpoint_ids.into_iter().take(remove_count) {
            if let Some(index) = self.artifacts.iter().position(|artifact| artifact.id == id) {
                removed.push(self.artifacts.remove(index));
            }
        }
        removed
    }

    pub fn artifacts(&self) -> &[RecoveryArtifact] {
        &self.artifacts
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryPlan {
    pub checkpoint_id: String,
    pub source_positions: Vec<SourcePosition>,
    pub watermarks_ms: BTreeMap<u32, i64>,
    pub task_attempts: Vec<TaskAttemptSnapshot>,
}

impl RecoveryPlan {
    pub fn from_manifest(manifest: &CheckpointManifest) -> Result<Self, Error> {
        if !manifest.verify() {
            return Err(Error::Process(
                "cannot recover from invalid checkpoint".into(),
            ));
        }
        Ok(Self {
            checkpoint_id: manifest.checkpoint_id.clone(),
            source_positions: manifest.source_positions.clone(),
            watermarks_ms: manifest.watermarks_ms.clone(),
            task_attempts: manifest.task_attempts.clone(),
        })
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointCoordinator {
    job_id: JobId,
    job_version: JobVersion,
    generation: u64,
    format_version: u32,
    participants: BTreeSet<String>,
    acknowledgements: BTreeMap<String, TaskCheckpointAck>,
    status: CheckpointStatus,
    barrier: Option<CheckpointBarrier>,
}

impl CheckpointCoordinator {
    pub fn new(
        job_id: JobId,
        job_version: JobVersion,
        generation: u64,
        format_version: u32,
        participants: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            job_id,
            job_version,
            generation,
            format_version,
            participants: participants.into_iter().collect(),
            acknowledgements: BTreeMap::new(),
            status: CheckpointStatus::Pending,
            barrier: None,
        }
    }

    pub fn start(&mut self, checkpoint_id: impl Into<String>) -> Result<CheckpointBarrier, Error> {
        if self.status != CheckpointStatus::Pending {
            return Err(Error::Process("checkpoint is already started".into()));
        }
        let barrier = CheckpointBarrier {
            checkpoint_id: checkpoint_id.into(),
            generation: self.generation,
        };
        self.barrier = Some(barrier.clone());
        self.status = CheckpointStatus::InProgress;
        Ok(barrier)
    }

    pub fn acknowledge(&mut self, ack: TaskCheckpointAck) -> Result<bool, Error> {
        if self.status != CheckpointStatus::InProgress {
            return Err(Error::Process("checkpoint is not in progress".into()));
        }
        if !self.participants.contains(&ack.task_id) {
            return Err(Error::Config(format!(
                "checkpoint acknowledgement from unknown task '{}'",
                ack.task_id
            )));
        }
        if ack.state.format_version != self.format_version || !ack.state.verify() {
            return Err(Error::Process(format!(
                "invalid state snapshot from task '{}'",
                ack.task_id
            )));
        }
        self.acknowledgements.insert(ack.task_id.clone(), ack);
        Ok(self.acknowledgements.len() == self.participants.len())
    }

    pub fn complete(
        &mut self,
        task_attempts: Vec<TaskAttemptSnapshot>,
        state_snapshots: Vec<StateSnapshotRef>,
    ) -> Result<CheckpointManifest, Error> {
        if self.status != CheckpointStatus::InProgress
            || self.acknowledgements.len() != self.participants.len()
        {
            return Err(Error::Process(
                "checkpoint cannot complete before all task acknowledgements".into(),
            ));
        }
        let barrier = self
            .barrier
            .clone()
            .ok_or_else(|| Error::Process("checkpoint barrier is missing".into()))?;
        let mut source_positions = Vec::new();
        let mut watermarks_ms = BTreeMap::new();
        for ack in self.acknowledgements.values() {
            source_positions.extend(ack.source_positions.clone());
            if let Some(watermark) = ack.watermark_ms {
                watermarks_ms.insert(ack.partition, watermark);
            }
        }
        let mut manifest = CheckpointManifest {
            checkpoint_id: barrier.checkpoint_id.clone(),
            job_id: self.job_id.clone(),
            job_version: self.job_version,
            generation: self.generation,
            task_attempts,
            source_positions,
            watermarks_ms,
            in_flight_barrier: barrier,
            state_snapshots,
            format_version: self.format_version,
            checksum: 0,
        };
        manifest.checksum = manifest_checksum(&manifest);
        self.status = CheckpointStatus::Completed;
        Ok(manifest)
    }

    pub fn status(&self) -> CheckpointStatus {
        self.status
    }
}

fn manifest_checksum(manifest: &CheckpointManifest) -> u64 {
    let encoded = serde_json::to_vec(&(
        &manifest.checkpoint_id,
        &manifest.job_id,
        manifest.job_version,
        manifest.generation,
        &manifest.task_attempts,
        &manifest.source_positions,
        &manifest.watermarks_ms,
        &manifest.in_flight_barrier,
        &manifest.state_snapshots,
        manifest.format_version,
    ))
    .unwrap_or_default();
    encoded.iter().fold(0xcbf29ce484222325u64, |hash, byte| {
        hash.wrapping_mul(0x100000001b3) ^ u64::from(*byte)
    })
}

fn snapshot_checksum(snapshot: &StateSnapshot) -> u64 {
    snapshot.checksum
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::{JobId, JobVersion};

    fn state() -> StateSnapshot {
        StateSnapshot::new(
            1,
            vec![crate::state::StateEntry {
                namespace: "orders".into(),
                key: b"a".to_vec(),
                value: b"1".to_vec(),
                expires_at_ms: None,
            }],
        )
    }

    #[test]
    fn completes_only_after_all_tasks_acknowledge() {
        let mut coordinator = CheckpointCoordinator::new(
            JobId::new("orders").unwrap(),
            JobVersion(1),
            7,
            1,
            ["task-0".into(), "task-1".into()],
        );
        coordinator.start("cp-1").unwrap();
        let ack = |task_id: &str| TaskCheckpointAck {
            task_id: task_id.into(),
            attempt_id: format!("{task_id}-attempt"),
            partition: if task_id == "task-0" { 0 } else { 1 },
            state: state(),
            source_positions: vec![SourcePosition {
                partition: 0,
                offset: 10,
            }],
            watermark_ms: Some(100),
        };
        assert!(!coordinator.acknowledge(ack("task-0")).unwrap());
        assert!(coordinator.acknowledge(ack("task-1")).unwrap());
        let manifest = coordinator
            .complete(
                vec![TaskAttemptSnapshot {
                    task_id: "task-0".into(),
                    attempt_id: "task-0-attempt".into(),
                    node_id: "node-a".into(),
                }],
                vec![StateSnapshotRef {
                    task_id: "task-0".into(),
                    uri: "s3://bucket/cp-1/task-0".into(),
                    checksum: 1,
                    bytes: 10,
                }],
            )
            .unwrap();
        assert!(manifest.verify());
        assert_eq!(manifest.watermarks_ms.len(), 2);
        assert_eq!(coordinator.status(), CheckpointStatus::Completed);
    }

    #[test]
    fn rejects_unknown_task_acknowledgement() {
        let mut coordinator = CheckpointCoordinator::new(
            JobId::new("orders").unwrap(),
            JobVersion(1),
            1,
            1,
            ["task-0".into()],
        );
        coordinator.start("cp-1").unwrap();
        let result = coordinator.acknowledge(TaskCheckpointAck {
            task_id: "unknown".into(),
            attempt_id: "attempt".into(),
            partition: 0,
            state: state(),
            source_positions: vec![],
            watermark_ms: None,
        });
        assert!(result.is_err());
    }

    #[test]
    fn persists_savepoints_and_selects_latest_valid_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let repository = CheckpointRepository::new(FileCheckpointStore::new(dir.path()).unwrap());
        let mut manifest = CheckpointManifest {
            checkpoint_id: "cp-1".into(),
            job_id: JobId::new("orders").unwrap(),
            job_version: JobVersion(1),
            generation: 1,
            task_attempts: vec![],
            source_positions: vec![SourcePosition {
                partition: 0,
                offset: 10,
            }],
            watermarks_ms: BTreeMap::new(),
            in_flight_barrier: CheckpointBarrier {
                checkpoint_id: "cp-1".into(),
                generation: 1,
            },
            state_snapshots: vec![],
            format_version: 1,
            checksum: 0,
        };
        manifest.checksum = manifest_checksum(&manifest);
        let checkpoint = repository.write_checkpoint(&manifest).unwrap();
        let savepoint = repository.create_savepoint(&manifest).unwrap();
        assert_eq!(repository.read_manifest(&checkpoint).unwrap(), manifest);
        assert_eq!(
            RecoveryPlan::from_manifest(&manifest)
                .unwrap()
                .checkpoint_id,
            "cp-1"
        );

        let mut catalog = CheckpointCatalog::default();
        catalog.record(checkpoint.clone());
        catalog.record(savepoint.clone());
        assert!(catalog.latest_valid(JobVersion(1), 1).is_some());
        assert_eq!(catalog.retain_checkpoints(0).len(), 1);
        repository.delete(&savepoint).unwrap();
    }

    #[test]
    fn uploads_and_downloads_state_snapshot_with_checksum_validation() {
        let dir = tempfile::tempdir().unwrap();
        let repository = CheckpointRepository::new(FileCheckpointStore::new(dir.path()).unwrap());
        let snapshot = state();
        let reference = repository.write_state_snapshot("cp-1", &snapshot).unwrap();
        assert_eq!(
            repository.read_state_snapshot(&reference).unwrap(),
            snapshot
        );
        let mut corrupted = reference.clone();
        corrupted.checksum += 1;
        assert!(repository.read_state_snapshot(&corrupted).is_err());
    }

    #[test]
    fn missing_object_and_invalid_manifest_are_rejected_deterministically() {
        let dir = tempfile::tempdir().unwrap();
        let repository = CheckpointRepository::new(FileCheckpointStore::new(dir.path()).unwrap());
        let artifact = RecoveryArtifact {
            id: "cp-missing".into(),
            kind: RecoveryArtifactKind::Checkpoint,
            manifest_key: "checkpoints/cp-missing/manifest.json".into(),
            job_version: JobVersion(1),
            format_version: 1,
            created_at_ms: 0,
            status: CheckpointStatus::Completed,
        };
        assert!(repository.read_manifest(&artifact).is_err());
        let mut snapshot = state();
        snapshot.checksum ^= 1;
        assert!(repository
            .write_state_snapshot("cp-bad", &snapshot)
            .is_err());
    }
}
