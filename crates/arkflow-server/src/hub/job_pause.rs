//! Durable Job pause/resume state machine.
//!
//! Pausing is a *durable quiesce*: the Hub captures a recovery artifact at
//! the pause generation, then stops the Job, and reports `paused` only once
//! every target node observed the stop. Resuming bumps the generation and
//! lets the ordinary recovery start restore from that artifact. The kernel
//! and the Agent protocol are untouched — the pause state lives in
//! `cp_jobs` and is driven by the leader's reconcile tick, mirroring the
//! atomic-upgrade orchestration.
//!
//! See `openspec/changes/add-job-pause-resume/` and the
//! `job-pause-resume` capability spec.

use super::error::HubError;
use super::wire::{AgentOperation, HubEvent, HubOperationState};
use super::{now_ms, Hub};
use crate::storage::{AuditRecord, JobCheckpointRecord, JobPauseUpdate, JobRecord};
use arkflow_core::control::ControlEvent;

pub(crate) const PAUSE_CAPTURING: &str = "capturing";
pub(crate) const PAUSE_STOPPING: &str = "stopping";
pub(crate) const PAUSE_PAUSED: &str = "paused";
pub(crate) const PAUSE_RESUMING: &str = "resuming";

/// A capture round this old is treated as failed; the pause aborts.
pub(crate) const PAUSE_CAPTURE_TIMEOUT_MS: u64 = 5 * 60 * 1_000;
/// Bounded capture re-dispatch budget (Hub restart / timeout recovery).
pub(crate) const MAX_PAUSE_CAPTURE_RETRIES: u32 = 3;

impl Hub {
    fn require_durable_storage(&self) -> Result<&crate::storage::StorageActor, HubError> {
        self.storage
            .as_ref()
            .ok_or(HubError::PauseRequiresDurableStorage)
    }

    pub(crate) fn record_pause_outcome(&self, outcome: &str) {
        self.command_metrics.record_pause_outcome(outcome);
    }

    async fn cache_job(&self, job: &JobRecord) {
        self.jobs
            .write()
            .await
            .insert(job.job_id.clone(), job.clone());
    }

    fn parse_spec(job: &JobRecord) -> Result<arkflow_core::job::JobSpec, HubError> {
        serde_json::from_str(&job.spec_json)
            .map_err(|error| HubError::Invalid(format!("invalid persisted Job spec: {error}")))
    }

    /// Artifact kind the Job's recovery policy will accept on resume.
    fn pause_artifact_kind(spec: &arkflow_core::job::JobSpec) -> Result<&'static str, HubError> {
        match spec.recovery {
            arkflow_core::job::RecoveryPolicy::LatestCheckpoint => Ok("checkpoint"),
            arkflow_core::job::RecoveryPolicy::LatestSavepoint => Ok("savepoint"),
            arkflow_core::job::RecoveryPolicy::Fail => Err(HubError::PauseRequiresRecoveryPolicy),
        }
    }

    /// Record the audit trail for a pause/resume mutation. Secrets and Job
    /// configuration bodies never enter the record.
    pub(crate) async fn record_job_pause_audit(
        &self,
        action: &str,
        job_id: &str,
        actor: &str,
        outcome: &str,
        failure_code: Option<&str>,
        message: String,
    ) {
        let Some(storage) = self.storage.as_ref() else {
            return;
        };
        let _ = storage
            .record_audit(AuditRecord {
                event_id: 0,
                actor: Some(actor.to_owned()),
                action: action.to_owned(),
                resource_type: "job".into(),
                resource_id: Some(job_id.to_owned()),
                node_id: None,
                stream_id: None,
                correlation_id: None,
                outcome: outcome.to_owned(),
                failure_code: failure_code.map(str::to_owned),
                message: Some(message),
                occurred_at_ms: now_ms(),
            })
            .await;
    }

    fn broadcast_pause_event(
        &self,
        event_type: &str,
        job_id: &str,
        message: String,
        actor: Option<&str>,
    ) {
        let _ = self.updates.send(HubEvent {
            event_id: None,
            node_id: "hub".into(),
            event: ControlEvent {
                occurred_at_ms: now_ms(),
                event_type: event_type.into(),
                stream_id: None,
                outcome: "accepted".into(),
                message: Some(message),
                operation_id: Some(job_id.to_owned()),
                correlation_id: None,
                actor: actor.map(str::to_owned),
            },
        });
    }

    /// Begin a pause for a converged running Job. Persists the intent and
    /// dispatches the capture round; returns the Job as recorded after the
    /// intent write.
    pub async fn pause_job(&self, job_id: &str, actor: &str) -> Result<JobRecord, HubError> {
        let storage = self.require_durable_storage()?.clone();
        let current = self.job(job_id).await?.ok_or(HubError::NotFound)?;
        if let Some(phase) = current.pause_phase.as_deref() {
            return Err(if phase == PAUSE_PAUSED {
                HubError::JobAlreadyPaused
            } else {
                HubError::PauseInProgress
            });
        }
        if self.active_job_upgrade_for(job_id).await.is_some() {
            return Err(HubError::OrchestrationInProgress);
        }
        if current.desired_state != "running"
            || current.observed_state != "running"
            || current.convergence != "converged"
        {
            return Err(HubError::JobNotRunning);
        }
        let spec = Self::parse_spec(&current)?;
        let kind = Self::pause_artifact_kind(&spec)?;
        let capture = spec.checkpoint.is_some();
        let phase = if capture {
            PAUSE_CAPTURING
        } else {
            PAUSE_STOPPING
        };
        let artifact_id =
            capture.then(|| format!("{kind}-pause-g{}-{}", current.generation, now_ms()));
        let update = JobPauseUpdate::begin(
            current.generation,
            phase,
            artifact_id.clone(),
            current.checkpoint_id.clone(),
            now_ms(),
            actor,
        );
        let job = storage
            .update_job_pause(job_id, update)
            .await
            .map_err(HubError::from)?
            .ok_or(HubError::NotFound)?;
        self.cache_job(&job).await;
        self.record_pause_outcome("requested");
        self.record_job_pause_audit(
            "job.pause",
            job_id,
            actor,
            "accepted",
            None,
            format!(
                "pause accepted, generation={}, phase={}, artifact={}",
                job.generation,
                phase,
                artifact_id.as_deref().unwrap_or("none")
            ),
        )
        .await;
        self.broadcast_pause_event(
            "job.pause_requested",
            job_id,
            format!("pause requested at generation {}", job.generation),
            Some(actor),
        );
        if let Some(artifact_id) = artifact_id {
            let record = JobCheckpointRecord {
                job_id: job_id.to_owned(),
                job_version: job.version,
                checkpoint_id: artifact_id,
                kind: kind.into(),
                status: "pending".into(),
                manifest_uri: None,
                format_version: super::checkpoint::job_state_format_version(&spec),
                created_at_ms: now_ms(),
                updated_at_ms: now_ms(),
            };
            // Moves the recovery pointer to the pending artifact and
            // dispatches the capture round to the placed nodes. The durable
            // intent is already committed, so a dispatch failure must not
            // surface as a rejection: the reconcile tick re-issues the
            // capture under the bounded retry budget.
            if let Err(error) = self.record_job_checkpoint(record).await {
                tracing::warn!(
                    job_id,
                    %error,
                    "pause capture dispatch failed; deferred to the reconcile tick"
                );
            }
        } else {
            // No artifact to capture: dispatch the stop directly. Same rule:
            // post-commit dispatch failures are retryable by the reconcile
            // tick, not client-facing rejections.
            if let Err(error) = self.reconcile_job(&job).await {
                tracing::warn!(
                    job_id,
                    %error,
                    "pause stop dispatch failed; deferred to the reconcile tick"
                );
            }
        }
        self.job(job_id).await?.ok_or(HubError::NotFound)
    }

    /// Resume a paused Job: bump the generation, keep the recovery pointer
    /// on the pause artifact, and let reconciliation start the Job.
    pub async fn resume_job(&self, job_id: &str, actor: &str) -> Result<JobRecord, HubError> {
        let storage = self.require_durable_storage()?.clone();
        let current = self.job(job_id).await?.ok_or(HubError::NotFound)?;
        match current.pause_phase.as_deref() {
            Some(PAUSE_PAUSED) => {}
            Some(_) => return Err(HubError::PauseInProgress),
            None => return Err(HubError::PauseNotPaused),
        }
        if self.active_job_upgrade_for(job_id).await.is_some() {
            return Err(HubError::OrchestrationInProgress);
        }
        let update = JobPauseUpdate {
            expected_generation: current.generation,
            expected_phase: Some(PAUSE_PAUSED.into()),
            desired_state: Some("running".into()),
            phase: Some(PAUSE_RESUMING.into()),
            bump_generation: true,
            ..JobPauseUpdate::default()
        };
        let job = storage
            .update_job_pause(job_id, update)
            .await
            .map_err(HubError::from)?
            .ok_or(HubError::NotFound)?;
        self.cache_job(&job).await;
        self.record_pause_outcome("resumed");
        self.record_job_pause_audit(
            "job.resume",
            job_id,
            actor,
            "accepted",
            None,
            format!(
                "resume accepted, generation={}, artifact={}",
                job.generation,
                job.checkpoint_id.as_deref().unwrap_or("none")
            ),
        )
        .await;
        self.broadcast_pause_event(
            "job.resume_requested",
            job_id,
            format!("resume requested at generation {}", job.generation),
            Some(actor),
        );
        // Post-commit dispatch: the phase change is durable, so a dispatch
        // failure is deferred to the reconcile tick instead of surfacing as
        // a rejection for a mutation the Hub already accepted.
        if let Err(error) = self.reconcile_job(&job).await {
            tracing::warn!(
                job_id,
                %error,
                "post-commit pause dispatch failed; deferred to the reconcile tick"
            );
        }
        self.job(job_id).await?.ok_or(HubError::NotFound)
    }

    /// Stop a paused (or pausing) Job: clear the pause bookkeeping and move
    /// the desired state to `stopped` in one fenced write.
    pub async fn stop_paused_job(&self, job_id: &str, actor: &str) -> Result<JobRecord, HubError> {
        let storage = self.require_durable_storage()?.clone();
        let current = self.job(job_id).await?.ok_or(HubError::NotFound)?;
        let Some(phase) = current.pause_phase.clone() else {
            return Err(HubError::PauseNotPaused);
        };
        // A stop during the capture phase abandons the in-flight artifact:
        // leaving its pending record behind would pin the recovery pointer to
        // an artifact that never completes, so a later start could not select
        // any completed artifact for the same Job. Delete it and restore the
        // pre-pause pointer instead.
        let mut abandoned_artifact = None;
        if let Some(artifact_id) = current.pause_artifact_id.as_deref() {
            let records = self.job_checkpoints(job_id).await?;
            if records
                .iter()
                .find(|record| record.checkpoint_id == artifact_id)
                .is_some_and(|record| record.status != "completed")
            {
                abandoned_artifact = Some(artifact_id.to_owned());
            }
        }
        let mut update = JobPauseUpdate {
            expected_generation: current.generation,
            expected_phase: Some(phase),
            desired_state: Some("stopped".into()),
            clear_pause: true,
            bump_generation: true,
            ..JobPauseUpdate::default()
        };
        if abandoned_artifact.is_some() {
            update.restore_pointer = true;
            update.restore_checkpoint_id = current.pause_prev_checkpoint_id.clone();
        }
        let job = storage
            .update_job_pause(job_id, update)
            .await
            .map_err(HubError::from)?
            .ok_or(HubError::NotFound)?;
        self.cache_job(&job).await;
        if let Some(artifact_id) = abandoned_artifact {
            if let Err(error) = storage.delete_job_checkpoint(job_id, &artifact_id).await {
                tracing::warn!(
                    job_id,
                    artifact_id = %artifact_id,
                    %error,
                    "failed to delete an abandoned pause capture record"
                );
            }
            self.job_checkpoints
                .write()
                .await
                .entry(job_id.to_owned())
                .or_default()
                .retain(|record| record.checkpoint_id != artifact_id);
        }
        self.record_job_pause_audit(
            "job.stop",
            job_id,
            actor,
            "accepted",
            None,
            format!(
                "paused job stopped by operator, generation={}",
                job.generation
            ),
        )
        .await;
        // Post-commit dispatch: the phase change is durable, so a dispatch
        // failure is deferred to the reconcile tick instead of surfacing as
        // a rejection for a mutation the Hub already accepted.
        if let Err(error) = self.reconcile_job(&job).await {
            tracing::warn!(
                job_id,
                %error,
                "post-commit pause dispatch failed; deferred to the reconcile tick"
            );
        }
        self.job(job_id).await?.ok_or(HubError::NotFound)
    }

    /// Artifacts a non-terminal pause references: checkpoint retention must
    /// not delete them before resume converges.
    pub(crate) async fn pinned_job_pause_artifacts(&self, job_id: &str) -> Vec<String> {
        let Some(job) = self.job(job_id).await.ok().flatten() else {
            return Vec::new();
        };
        if job.pause_phase.is_none() {
            return Vec::new();
        }
        let mut pinned = Vec::new();
        if let Some(artifact) = job.pause_artifact_id {
            pinned.push(artifact);
        }
        if let Some(pointer) = job.checkpoint_id {
            if !pinned.contains(&pointer) {
                pinned.push(pointer);
            }
        }
        pinned
    }

    /// True while a capture round for this Job generation is still in flight.
    async fn pause_capture_in_flight(&self, job: &JobRecord, artifact_id: &str) -> bool {
        let operations = self.operations.read().await;
        operations.values().any(|operation| {
            operation.resource_id == job.job_id
                && operation.generation == job.generation
                && matches!(
                    operation.operation,
                    AgentOperation::JobCheckpoint | AgentOperation::JobSavepoint
                )
                && matches!(
                    operation.state,
                    HubOperationState::Queued
                        | HubOperationState::Dispatched
                        | HubOperationState::Acknowledged
                        | HubOperationState::Running
                )
                && operation
                    .checkpoint_id
                    .as_deref()
                    .is_none_or(|id| id == artifact_id)
        })
    }

    /// Advance every non-terminal pause. Called once per leader reconcile
    /// tick; each transition is a phase-guarded write, so concurrent ticks
    /// and operator actions cannot double-apply a step.
    pub async fn reconcile_job_pauses(&self) -> Result<usize, HubError> {
        if self.storage.is_none() {
            return Ok(0);
        }
        let jobs = self.jobs().await?;
        let mut advanced = 0;
        for job in jobs {
            if job.pause_phase.is_none() {
                continue;
            }
            match self.reconcile_job_pause(&job).await {
                Ok(true) => advanced += 1,
                Ok(false) => {}
                Err(error @ (HubError::StorageUnavailable | HubError::Storage(_))) => {
                    return Err(error)
                }
                Err(HubError::NotFound) => {}
                Err(error) => {
                    tracing::warn!(
                        job_id = %job.job_id,
                        error = %error,
                        "skipping Job pause reconciliation this tick"
                    );
                }
            }
        }
        Ok(advanced)
    }

    async fn reconcile_job_pause(&self, job: &JobRecord) -> Result<bool, HubError> {
        let Some(phase) = job.pause_phase.as_deref() else {
            return Ok(false);
        };
        match phase {
            PAUSE_CAPTURING => self.advance_pause_capture(job).await,
            PAUSE_STOPPING => {
                if job.observed_state == "stopped" {
                    self.advance_pause_phase(job, PAUSE_STOPPING, PAUSE_PAUSED)
                        .await
                } else {
                    // Re-dispatch (idempotent) so offline nodes that came
                    // back converge; the dispatch-skip suppresses repeats.
                    self.reconcile_job(job).await?;
                    Ok(false)
                }
            }
            PAUSE_PAUSED => Ok(false),
            PAUSE_RESUMING => {
                if job.desired_state == "running"
                    && job.observed_state == "running"
                    && job.convergence == "converged"
                {
                    let storage = self.require_durable_storage()?.clone();
                    let update = JobPauseUpdate {
                        expected_generation: job.generation,
                        expected_phase: Some(PAUSE_RESUMING.into()),
                        clear_pause: true,
                        ..JobPauseUpdate::default()
                    };
                    let cleared = storage
                        .update_job_pause(&job.job_id, update)
                        .await
                        .map_err(HubError::from)?;
                    if let Some(cleared) = cleared {
                        self.cache_job(&cleared).await;
                        self.record_job_pause_audit(
                            "job.resume",
                            &job.job_id,
                            job.paused_by.as_deref().unwrap_or("hub"),
                            "accepted",
                            None,
                            format!("resume converged at generation {}", cleared.generation),
                        )
                        .await;
                        self.broadcast_pause_event(
                            "job.resumed",
                            &job.job_id,
                            format!("resumed at generation {}", cleared.generation),
                            None,
                        );
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            _ => Ok(false),
        }
    }

    /// Advance the capture phase: wait for the artifact, stop, abort, or
    /// re-dispatch a bounded fresh capture round after a Hub restart.
    async fn advance_pause_capture(&self, job: &JobRecord) -> Result<bool, HubError> {
        let Some(artifact_id) = job.pause_artifact_id.clone() else {
            return self.abort_pause(job, "pause_artifact_missing").await;
        };
        let records = self.job_checkpoints(&job.job_id).await?;
        let record = records
            .iter()
            .find(|record| record.checkpoint_id == artifact_id);
        match record.map(|record| record.status.as_str()) {
            Some("completed") => {
                self.advance_pause_phase(job, PAUSE_CAPTURING, PAUSE_STOPPING)
                    .await
            }
            Some("failed") => self.abort_pause(job, "pause_savepoint_failed").await,
            _ => {
                let deadline = job
                    .paused_at_ms
                    .map(|start| start.saturating_add(PAUSE_CAPTURE_TIMEOUT_MS))
                    .unwrap_or_else(|| now_ms().saturating_add(PAUSE_CAPTURE_TIMEOUT_MS));
                if now_ms() >= deadline {
                    return self.abort_pause(job, "pause_savepoint_timeout").await;
                }
                if self.pause_capture_in_flight(job, &artifact_id).await {
                    return Ok(false);
                }
                // No operation is in flight (typically after a Hub restart
                // settled the previous round as timed out): re-dispatch a
                // fresh capture round under a bounded budget.
                if job.pause_retry_count >= MAX_PAUSE_CAPTURE_RETRIES {
                    return self.abort_pause(job, "pause_savepoint_failed").await;
                }
                self.retry_pause_capture(job).await
            }
        }
    }

    async fn retry_pause_capture(&self, job: &JobRecord) -> Result<bool, HubError> {
        let storage = self.require_durable_storage()?.clone();
        let spec = Self::parse_spec(job)?;
        let kind = Self::pause_artifact_kind(&spec)?;
        let artifact_id = format!("{kind}-pause-g{}-{}", job.generation, now_ms());
        let update = JobPauseUpdate {
            expected_generation: job.generation,
            expected_phase: Some(PAUSE_CAPTURING.into()),
            artifact_id: Some(artifact_id.clone()),
            retry_count: Some(job.pause_retry_count.saturating_add(1)),
            ..JobPauseUpdate::default()
        };
        let updated = storage
            .update_job_pause(&job.job_id, update)
            .await
            .map_err(HubError::from)?;
        let Some(updated) = updated else {
            return Ok(false);
        };
        self.cache_job(&updated).await;
        let record = JobCheckpointRecord {
            job_id: updated.job_id.clone(),
            job_version: updated.version,
            checkpoint_id: artifact_id,
            kind: kind.into(),
            status: "pending".into(),
            manifest_uri: None,
            format_version: super::checkpoint::job_state_format_version(&spec),
            created_at_ms: now_ms(),
            updated_at_ms: now_ms(),
        };
        self.record_job_checkpoint(record).await?;
        self.broadcast_pause_event(
            "job.pause_retrying",
            &updated.job_id,
            format!(
                "capture retry {}/{} at generation {}",
                updated.pause_retry_count, MAX_PAUSE_CAPTURE_RETRIES, updated.generation
            ),
            updated.paused_by.as_deref(),
        );
        Ok(true)
    }

    async fn advance_pause_phase(
        &self,
        job: &JobRecord,
        from_phase: &str,
        to_phase: &str,
    ) -> Result<bool, HubError> {
        let storage = self.require_durable_storage()?.clone();
        let update = JobPauseUpdate {
            expected_generation: job.generation,
            expected_phase: Some(from_phase.into()),
            phase: Some(to_phase.into()),
            ..JobPauseUpdate::default()
        };
        let Some(updated) = storage
            .update_job_pause(&job.job_id, update)
            .await
            .map_err(HubError::from)?
        else {
            return Ok(false);
        };
        self.cache_job(&updated).await;
        if to_phase == PAUSE_STOPPING {
            // Capture is durable: tell the Job to stop (idempotent).
            self.reconcile_job(&updated).await?;
        } else if to_phase == PAUSE_PAUSED {
            self.record_job_pause_audit(
                "job.pause",
                &updated.job_id,
                updated.paused_by.as_deref().unwrap_or("hub"),
                "accepted",
                None,
                format!(
                    "pause converged at generation {}, artifact={}",
                    updated.generation,
                    updated.pause_artifact_id.as_deref().unwrap_or("none")
                ),
            )
            .await;
            self.broadcast_pause_event(
                "job.paused",
                &updated.job_id,
                format!(
                    "paused at generation {}, artifact={}",
                    updated.generation,
                    updated.pause_artifact_id.as_deref().unwrap_or("none")
                ),
                updated.paused_by.as_deref(),
            );
            self.record_pause_outcome("paused");
            // The phase write set `reconciling`; a paused Job's convergence
            // is phase-defined, so settle it now instead of waiting for an
            // observation the stopped Agent will never send again.
            let _ = self
                .observe_job(&updated.job_id, updated.generation, "stopped", None, None)
                .await;
        }
        Ok(true)
    }

    /// Abort a capture that failed or timed out: return to the pre-pause
    /// running state, restore the pre-pause recovery pointer, and leave the
    /// old generation untouched.
    async fn abort_pause(&self, job: &JobRecord, failure_code: &str) -> Result<bool, HubError> {
        let storage = self.require_durable_storage()?.clone();
        let Some(phase) = job.pause_phase.as_deref() else {
            return Ok(false);
        };
        let update = JobPauseUpdate {
            expected_generation: job.generation,
            expected_phase: Some(phase.into()),
            desired_state: Some("running".into()),
            clear_pause: true,
            restore_pointer: true,
            restore_checkpoint_id: job.pause_prev_checkpoint_id.clone(),
            last_error: Some(failure_code.into()),
            ..JobPauseUpdate::default()
        };
        let Some(updated) = storage
            .update_job_pause(&job.job_id, update)
            .await
            .map_err(HubError::from)?
        else {
            return Ok(false);
        };
        self.cache_job(&updated).await;
        self.record_pause_outcome("failed");
        self.record_job_pause_audit(
            "job.pause",
            &updated.job_id,
            job.paused_by.as_deref().unwrap_or("hub"),
            "failed",
            Some(failure_code),
            format!(
                "pause aborted at generation {}: {failure_code}",
                updated.generation
            ),
        )
        .await;
        self.broadcast_pause_event(
            "job.pause_failed",
            &updated.job_id,
            format!("pause aborted: {failure_code}"),
            job.paused_by.as_deref(),
        );
        // Back to running: recompute convergence from the observation that
        // was current before the pause (the running Agent will not re-report).
        let _ = self
            .observe_job(
                &updated.job_id,
                updated.generation,
                &updated.observed_state,
                None,
                None,
            )
            .await;
        // Observation writes clear `last_error`; the abort's stable failure
        // code is the operator's only signal, so restore it afterwards.
        if let Some(restored) = storage
            .update_job(
                &updated.job_id,
                None,
                None,
                None,
                None,
                None,
                Some(failure_code.into()),
            )
            .await
            .map_err(HubError::from)?
        {
            self.cache_job(&restored).await;
        }
        Ok(true)
    }
}
