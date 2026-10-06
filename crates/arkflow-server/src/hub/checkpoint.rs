//! Distributed checkpoint orchestration and retention.

use super::error::HubError;
use super::wire::{HubOperation, HubOperationState, NodeConnectionState};
use super::{now_ms, Hub};
use crate::agent::delete_checkpoint_artifact;
use crate::storage::{JobCheckpointRecord, JobRecord};
use std::collections::BTreeSet;

pub(crate) fn recovery_record_is_compatible(
    spec: &arkflow_core::job::JobSpec,
    record: &JobCheckpointRecord,
) -> bool {
    // The same version-direction rule the shared recovery evaluator applies
    // for the Agent and the repository: an equal state format permits a
    // TARGET VERSION UPGRADE (a savepoint written by an older Job version
    // restoring into the new one); downgrades and format changes have no
    // migration path and stay rejected on both sides.
    record.format_version == job_state_format_version(spec) && record.job_version <= spec.version.0
}

pub(crate) fn job_state_format_version(spec: &arkflow_core::job::JobSpec) -> u32 {
    spec.state
        .as_ref()
        .map(|state| state.format_version)
        .unwrap_or(1)
}

impl Hub {
    pub async fn record_job_checkpoint(
        &self,
        record: JobCheckpointRecord,
    ) -> Result<Option<JobRecord>, HubError> {
        let record_for_dispatch = record.clone();
        // A checkpoint produced by a different Job deployment (version) must
        // never repoint the live record's recovery pointer: the artifact row
        // is kept for audit, but recovery keeps its current selection.
        let version_matches = self
            .job(&record.job_id)
            .await
            .map(|job| job.is_some_and(|job| job.version == record.job_version))?;
        if let Some(storage) = &self.storage {
            storage
                .upsert_job_checkpoint(record.clone())
                .await
                .map_err(HubError::from)?;
            let job = if version_matches {
                storage
                    .update_job(
                        &record.job_id,
                        None,
                        None,
                        None,
                        None,
                        Some(record.checkpoint_id.clone()),
                        None,
                    )
                    .await
                    .map_err(HubError::from)?
            } else {
                self.job(&record.job_id).await?
            };
            if let Some(job) = &job {
                self.jobs
                    .write()
                    .await
                    .insert(job.job_id.clone(), job.clone());
                self.job_checkpoints
                    .write()
                    .await
                    .entry(record.job_id.clone())
                    .or_default()
                    .retain(|existing| existing.checkpoint_id != record.checkpoint_id);
                self.job_checkpoints
                    .write()
                    .await
                    .entry(record.job_id.clone())
                    .or_default()
                    .push(record.clone());
                self.dispatch_job_artifact(job, &record_for_dispatch)
                    .await?;
                self.enforce_checkpoint_retention_for_job(&record.job_id)
                    .await?;
            }
            return Ok(job);
        }
        let mut jobs = self.jobs.write().await;
        let Some(job) = jobs.get_mut(&record.job_id) else {
            return Ok(None);
        };
        if version_matches {
            job.checkpoint_id = Some(record.checkpoint_id.clone());
        }
        job.updated_at_ms = now_ms();
        let result = job.clone();
        drop(jobs);
        self.job_checkpoints
            .write()
            .await
            .entry(record.job_id.clone())
            .or_default()
            .retain(|existing| existing.checkpoint_id != record.checkpoint_id);
        self.job_checkpoints
            .write()
            .await
            .entry(record.job_id.clone())
            .or_default()
            .push(record);
        self.dispatch_job_artifact(&result, &record_for_dispatch)
            .await?;
        self.enforce_checkpoint_retention_for_job(&record_for_dispatch.job_id)
            .await?;
        Ok(Some(result))
    }

    async fn dispatch_job_artifact(
        &self,
        job: &JobRecord,
        record: &JobCheckpointRecord,
    ) -> Result<usize, HubError> {
        let spec: arkflow_core::job::JobSpec = serde_json::from_str(&job.spec_json)
            .map_err(|error| HubError::Invalid(format!("invalid persisted Job spec: {error}")))?;
        let plan = arkflow_core::job::JobPlan::compile(spec.clone())
            .map_err(|error| HubError::Invalid(error.to_string()))?;
        let candidates = if job.node_ids.is_empty() {
            self.operations
                .read()
                .await
                .values()
                .filter(|operation| {
                    operation.resource_id == job.job_id
                        && operation.operation == "job_start"
                        && operation.generation == job.generation
                        && matches!(
                            operation.state,
                            HubOperationState::Queued
                                | HubOperationState::Dispatched
                                | HubOperationState::Acknowledged
                                | HubOperationState::Running
                                | HubOperationState::Succeeded
                        )
                })
                .map(|operation| operation.node_id.clone())
                .collect::<BTreeSet<_>>()
        } else {
            job.node_ids.clone().into_iter().collect::<BTreeSet<_>>()
        };
        // Order the checkpoint targets in the placement's dispatch order so
        // the re-derived split round-robin produces the same task→node
        // mapping as the live placement (the command payload carries the
        // assignments; Agents ignore them today, but they must not lie).
        let candidates = if job.node_ids.is_empty() {
            self.retained_targets_in_dispatch_order(&job.job_id, &candidates)
                .await
        } else {
            candidates.into_iter().collect()
        };
        let nodes = self.nodes.read().await;
        let targets = candidates
            .into_iter()
            .filter(|node_id| {
                nodes.get(node_id).is_some_and(|node| {
                    node.resource.state == NodeConnectionState::Online
                        && node.resource.lease_expires_at_ms > now_ms()
                })
            })
            .collect::<Vec<_>>();
        drop(nodes);
        let assignments = plan
            .assignments_for_nodes(&targets, job.generation)
            .map_err(|error| HubError::Invalid(error.to_string()))?;
        let operation = if record.kind == "savepoint" {
            "job_savepoint"
        } else {
            "job_checkpoint"
        };
        let mut dispatched = 0;
        for node_id in targets {
            let node_assignments = assignments
                .iter()
                .filter(|assignment| assignment.node_id == node_id)
                .cloned()
                .collect::<Vec<_>>();
            if node_assignments.is_empty() {
                continue;
            }
            self.enqueue_with_metadata(
                node_id,
                operation.into(),
                job.job_id.clone(),
                None,
                Some(serde_json::json!({
                    "job_id": job.job_id,
                    "plan": plan,
                    "assignments": node_assignments,
                    "checkpoint_id": record.checkpoint_id,
                })),
                job.generation,
                None,
                None,
                None,
                None,
                None,
            )
            .await?;
            dispatched += 1;
        }
        Ok(dispatched)
    }

    pub async fn complete_job_checkpoint(
        &self,
        job_id: &str,
        checkpoint_id: &str,
        status: &str,
        manifest_uri: Option<String>,
    ) -> Result<(), HubError> {
        let kind = if checkpoint_id.starts_with("savepoint-") {
            "savepoint"
        } else {
            "checkpoint"
        };
        let mut record = self
            .job_checkpoints
            .read()
            .await
            .get(job_id)
            .and_then(|records| {
                records
                    .iter()
                    .find(|record| record.checkpoint_id == checkpoint_id)
                    .cloned()
            });
        if record.is_none() {
            if let Some(storage) = &self.storage {
                record = storage
                    .list_job_checkpoints(job_id)
                    .await
                    .map_err(HubError::from)?
                    .into_iter()
                    .find(|record| record.checkpoint_id == checkpoint_id);
            }
        }
        let record = if let Some(mut record) = record {
            record.status = status.into();
            record.manifest_uri = manifest_uri;
            record.updated_at_ms = now_ms();
            record
        } else {
            let job = self.job(job_id).await?;
            let (job_version, format_version) = job
                .as_ref()
                .and_then(|job| {
                    serde_json::from_str::<arkflow_core::job::JobSpec>(&job.spec_json)
                        .ok()
                        .map(|spec| (spec.version.0, job_state_format_version(&spec)))
                })
                .unwrap_or((0, 1));
            JobCheckpointRecord {
                job_id: job_id.into(),
                job_version,
                checkpoint_id: checkpoint_id.into(),
                kind: kind.into(),
                status: status.into(),
                manifest_uri,
                format_version,
                created_at_ms: now_ms(),
                updated_at_ms: now_ms(),
            }
        };
        if let Some(storage) = &self.storage {
            storage
                .upsert_job_checkpoint(record.clone())
                .await
                .map_err(HubError::from)?;
        }
        let mut records = self.job_checkpoints.write().await;
        records
            .entry(job_id.into())
            .or_default()
            .retain(|existing| existing.checkpoint_id != checkpoint_id);
        records.entry(job_id.into()).or_default().push(record);
        drop(records);
        self.enforce_checkpoint_retention_for_job(job_id).await?;
        Ok(())
    }

    pub async fn job_checkpoints(
        &self,
        job_id: &str,
    ) -> Result<Vec<JobCheckpointRecord>, HubError> {
        let Some(storage) = &self.storage else {
            return Ok(self
                .job_checkpoints
                .read()
                .await
                .get(job_id)
                .cloned()
                .unwrap_or_default());
        };
        storage
            .list_job_checkpoints(job_id)
            .await
            .map_err(HubError::from)
    }

    /// Enqueue periodic checkpoints for running Jobs whose configured interval
    /// has elapsed. Scheduling lives in the Hub so the normal checkpoint
    /// aggregation and fencing path is used for every Agent.
    pub async fn schedule_periodic_checkpoints(&self) -> Result<usize, HubError> {
        let now = now_ms();
        let mut scheduled = 0;
        for job in self.jobs().await? {
            if job.desired_state != "running" {
                continue;
            }
            let spec: arkflow_core::job::JobSpec =
                serde_json::from_str(&job.spec_json).map_err(|error| {
                    HubError::Invalid(format!("invalid persisted Job spec: {error}"))
                })?;
            let Some(checkpoint) = spec.checkpoint.as_ref() else {
                continue;
            };
            if checkpoint.interval_ms == 0 {
                continue;
            }
            let records = self.job_checkpoints(&job.job_id).await?;
            let last_attempt = records.iter().map(|record| record.created_at_ms).max();
            if last_attempt
                .is_some_and(|created| now.saturating_sub(created) < checkpoint.interval_ms)
            {
                continue;
            }
            let checkpoint_id = format!("checkpoint-{}-{}-{}", job.job_id, job.generation, now);
            let record = JobCheckpointRecord {
                job_id: job.job_id.clone(),
                job_version: spec.version.0,
                checkpoint_id,
                kind: "checkpoint".into(),
                status: "pending".into(),
                manifest_uri: None,
                format_version: job_state_format_version(&spec),
                created_at_ms: now,
                updated_at_ms: now,
            };
            self.record_job_checkpoint(record).await?;
            scheduled += 1;
        }
        Ok(scheduled)
    }

    /// Reclaim pending/failed checkpoint attempt records older than the
    /// retention window. Completed records are governed by the per-Job
    /// checkpoint retention policy; pending/failed rows used to accumulate
    /// forever whenever an Agent could not finish a round.
    pub async fn prune_stale_checkpoint_records(&self) -> Result<(), HubError> {
        const RETENTION_MS: i64 = 24 * 60 * 60 * 1000;
        let cutoff = now_ms() as i64 - RETENTION_MS;
        if let Some(storage) = &self.storage {
            storage
                .prune_job_checkpoint_records(cutoff)
                .await
                .map_err(HubError::from)?;
        }
        let stale: Vec<(String, String)> = self
            .job_checkpoints
            .read()
            .await
            .iter()
            .flat_map(|(job_id, records)| {
                records
                    .iter()
                    .filter(|record| {
                        (record.updated_at_ms as i64) < cutoff
                            && matches!(record.status.as_str(), "pending" | "failed")
                    })
                    .map(|record| (job_id.clone(), record.checkpoint_id.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        if stale.is_empty() {
            return Ok(());
        }
        let mut checkpoints = self.job_checkpoints.write().await;
        for (job_id, checkpoint_id) in stale {
            if let Some(records) = checkpoints.get_mut(&job_id) {
                records.retain(|record| record.checkpoint_id != checkpoint_id);
            }
        }
        Ok(())
    }

    async fn enforce_checkpoint_retention(
        &self,
        job: &JobRecord,
        spec: &arkflow_core::job::JobSpec,
    ) -> Result<(), HubError> {
        let retention = spec
            .checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.retention as usize)
            .unwrap_or(0);
        if retention == 0 {
            return Ok(());
        }
        let mut completed = self
            .job_checkpoints(&job.job_id)
            .await?
            .into_iter()
            .filter(|record| record.kind == "checkpoint" && record.status == "completed")
            .collect::<Vec<_>>();
        completed.sort_by(|left, right| {
            right
                .created_at_ms
                .cmp(&left.created_at_ms)
                .then_with(|| right.checkpoint_id.cmp(&left.checkpoint_id))
        });
        // An active upgrade orchestration restores from its own savepoint on
        // rollback; retention must never delete an artifact it references.
        let pinned = self.pinned_job_upgrade_savepoints(&job.job_id).await;
        for record in completed
            .into_iter()
            .skip(retention)
            .filter(|record| !pinned.contains(&record.checkpoint_id))
        {
            let artifact = arkflow_core::checkpoint::RecoveryArtifact {
                id: record.checkpoint_id.clone(),
                kind: arkflow_core::checkpoint::RecoveryArtifactKind::Checkpoint,
                manifest_key: arkflow_core::checkpoint::recovery_manifest_key(
                    arkflow_core::checkpoint::RecoveryArtifactKind::Checkpoint,
                    &record.checkpoint_id,
                ),
                job_version: spec.version,
                format_version: record.format_version,
                created_at_ms: record.created_at_ms,
                status: arkflow_core::checkpoint::CheckpointStatus::Completed,
            };
            // The artifact delete performs blocking object-store I/O; keep it
            // off the async runtime's worker threads (this runs inside
            // reconciliation and request handling).
            let spec_for_delete = spec.clone();
            tokio::task::spawn_blocking(move || {
                delete_checkpoint_artifact(&spec_for_delete, &artifact).map_err(HubError::Invalid)
            })
            .await
            .map_err(|error| {
                HubError::Invalid(format!("checkpoint retention task failed: {error}"))
            })??;
            if let Some(storage) = &self.storage {
                storage
                    .delete_job_checkpoint(&record.job_id, &record.checkpoint_id)
                    .await
                    .map_err(HubError::from)?;
            } else {
                self.job_checkpoints
                    .write()
                    .await
                    .entry(record.job_id.clone())
                    .or_default()
                    .retain(|candidate| candidate.checkpoint_id != record.checkpoint_id);
            }
        }
        Ok(())
    }

    async fn enforce_checkpoint_retention_for_job(&self, job_id: &str) -> Result<(), HubError> {
        let Some(job) = self.job(job_id).await? else {
            return Ok(());
        };
        let spec: arkflow_core::job::JobSpec = serde_json::from_str(&job.spec_json)
            .map_err(|error| HubError::Invalid(format!("invalid persisted Job spec: {error}")))?;
        self.enforce_checkpoint_retention(&job, &spec).await
    }

    /// Resolve the complete node/task scope for one checkpoint round. The
    /// checkpoint operation list contains only nodes that were online when
    /// dispatch ran, so it cannot be used as the expected set by itself: an
    /// offline node would disappear and a partial artifact could be sealed.
    /// Prefer the Job's explicit placement, otherwise retain the nodes from
    /// the generation's active start assignments.
    pub(crate) async fn checkpoint_scope(
        &self,
        operation: &HubOperation,
        fallback_nodes: &BTreeSet<String>,
    ) -> Result<(BTreeSet<String>, Vec<String>), HubError> {
        let Some(job) = self.job(&operation.resource_id).await? else {
            return Ok((fallback_nodes.clone(), Vec::new()));
        };
        let spec: arkflow_core::job::JobSpec = serde_json::from_str(&job.spec_json)
            .map_err(|error| HubError::Invalid(format!("invalid persisted Job spec: {error}")))?;
        let plan = arkflow_core::job::JobPlan::compile(spec)
            .map_err(|error| HubError::Invalid(error.to_string()))?;
        let candidates = if job.node_ids.is_empty() {
            let nodes = self
                .operations
                .read()
                .await
                .values()
                .filter(|candidate| {
                    candidate.resource_id == operation.resource_id
                        && candidate.operation == "job_start"
                        && candidate.generation == operation.generation
                        && !matches!(
                            candidate.state,
                            HubOperationState::Failed
                                | HubOperationState::TimedOut
                                | HubOperationState::NodeUnavailable
                                | HubOperationState::Cancelled
                                | HubOperationState::Superseded
                        )
                })
                .map(|candidate| candidate.node_id.clone())
                .collect::<BTreeSet<_>>();
            if nodes.is_empty() {
                fallback_nodes.iter().cloned().collect::<Vec<_>>()
            } else {
                nodes.into_iter().collect::<Vec<_>>()
            }
        } else {
            job.node_ids.clone()
        };
        let assignments = plan
            .assignments_for_nodes(&candidates, operation.generation)
            .map_err(|error| HubError::Invalid(error.to_string()))?;
        let expected_nodes = assignments
            .iter()
            .map(|assignment| assignment.node_id.clone())
            .collect::<BTreeSet<_>>();
        let planned_task_ids = plan
            .tasks
            .iter()
            .map(|task| task.id.clone())
            .collect::<Vec<_>>();
        Ok((expected_nodes, planned_task_ids))
    }
}
