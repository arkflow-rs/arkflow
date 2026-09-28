//! Atomic job-upgrade orchestration: savepoint → version commit → recovery
//! start, supervised behind one API call with per-phase deadlines.
//!
//! The orchestration chains existing primitives only — the ordinary savepoint
//! dispatch path, the generation-fenced Job write, and normal reconciliation
//! (which is the new generation's start mechanism and already fences
//! historical generations). Old-generation safety rests on two verified
//! properties: savepoint barriers do not stop data flow, and the recovery
//! pointer moves at checkpoint completion while the Job version still
//! matches, so the fenced commit write never writes (or regresses) the
//! pointer itself.

use super::*;

/// Phase-name constants. Stored as plain strings, matching the rollout
/// state convention.
pub(crate) mod phase {
    pub const PENDING: &str = "pending";
    pub const SAVING_SAVEPOINT: &str = "saving_savepoint";
    pub const COMMITTING_VERSION: &str = "committing_version";
    pub const VERIFYING: &str = "verifying";
    pub const ROLLING_BACK: &str = "rolling_back";
    pub const SUCCEEDED: &str = "succeeded";
    pub const ABORTED: &str = "aborted";
    pub const FAILED: &str = "failed";
    pub const ROLLED_BACK: &str = "rolled_back";
    pub const CANCELLED: &str = "cancelled";
    pub const PAUSED: &str = "paused";
}

pub(crate) const SAVEPOINT_PHASE_TIMEOUT_MS: u64 = 5 * 60_000;
pub(crate) const COMMIT_PHASE_TIMEOUT_MS: u64 = 60_000;
pub(crate) const DEFAULT_VERIFY_TIMEOUT_MS: u64 = 10 * 60_000;
pub(crate) const MAX_SAVEPOINT_RETRIES: u32 = 2;
const HUB_NODE_ID_FOR_UPGRADE_EVENTS: &str = "hub";

fn verify_timeout_ms(record: &JobUpgradeRecord) -> u64 {
    if record.verify_timeout_ms > 0 {
        record.verify_timeout_ms
    } else {
        DEFAULT_VERIFY_TIMEOUT_MS
    }
}

/// Deadline a phase entry arms, by phase name.
fn phase_timeout_ms(record: &JobUpgradeRecord, phase: &str) -> u64 {
    match phase {
        phase::COMMITTING_VERSION => COMMIT_PHASE_TIMEOUT_MS,
        phase::VERIFYING | phase::ROLLING_BACK => verify_timeout_ms(record),
        _ => SAVEPOINT_PHASE_TIMEOUT_MS,
    }
}

impl Hub {
    // ------------------------------------------------------------------
    // Creation and reads
    // ------------------------------------------------------------------

    /// Create an atomic upgrade orchestration for a running Job. Guards
    /// mirror the stopped-mode upgrade handler (spec identity, version
    /// monotonicity, compile + deep validation, state-format compatibility)
    /// plus the atomic-mode preconditions: the Job must be running, and no
    /// other non-terminal orchestration may own it.
    pub async fn create_job_upgrade(
        &self,
        job_id: &str,
        spec: &mut arkflow_core::job::JobSpec,
        expected_generation: u64,
        verify_timeout_ms: u64,
        actor: Option<String>,
        correlation_id: Option<String>,
    ) -> Result<JobUpgradeRecord, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        let Some(current) = self.job(job_id).await? else {
            return Err(HubError::Invalid("job not found".into()));
        };
        if current.generation != expected_generation {
            return Err(HubError::GenerationConflict {
                expected: expected_generation,
                current: current.generation,
            });
        }
        if current.desired_state != "running" {
            return Err(HubError::Invalid(
                "the atomic upgrade mode requires a running Job".into(),
            ));
        }
        if spec.id.as_str() != job_id {
            return Err(HubError::Invalid(
                "upgrade spec id must match the Job id".into(),
            ));
        }
        if spec.version.0 <= current.version {
            return Err(HubError::Invalid(
                "upgrade version must be greater than the current version".into(),
            ));
        }
        spec.validate()
            .and_then(|_| arkflow_core::job::JobPlan::compile(spec.clone()).map(|_| ()))
            .map_err(|error| HubError::Invalid(error.to_string()))?;
        crate::deep_validate_job(spec).map_err(HubError::Invalid)?;
        // The savepoint this orchestration takes is produced by the CURRENT
        // spec, so its state format is the current spec's format; recovery
        // into the target version then follows the same equal-format rule
        // the shared compatibility evaluator applies.
        let current_spec: arkflow_core::job::JobSpec =
            serde_json::from_str(&current.spec_json).map_err(|error| {
                HubError::Invalid(format!("invalid persisted Job spec: {error}"))
            })?;
        if crate::hub::checkpoint::job_state_format_version(&current_spec)
            != crate::hub::checkpoint::job_state_format_version(spec)
        {
            return Err(HubError::Invalid(
                "the target spec state format is incompatible with the running Job".into(),
            ));
        }
        // Exclusivity: at most one non-terminal orchestration per Job. The
        // storage query is authoritative (covers rows created before a
        // restart repopulated the cache).
        let active = storage.recover_job_upgrades().await?;
        if active
            .iter()
            .any(|record| record.job_id == job_id && !record.phase_is_terminal())
        {
            return Err(HubError::OrchestrationInProgress);
        }
        spec.recovery = arkflow_core::job::RecoveryPolicy::LatestSavepoint;
        let target_spec_json = serde_json::to_string(spec)
            .map_err(|error| HubError::Invalid(format!("invalid Job spec: {error}")))?;
        let now = now_ms();
        let mut record = JobUpgradeRecord {
            upgrade_id: format!("job-upgrade-{}", HUB_SEQUENCE.fetch_add(1, Ordering::Relaxed)),
            job_id: job_id.to_owned(),
            from_version: current.version,
            to_version: spec.version.0,
            phase: phase::SAVING_SAVEPOINT.into(),
            savepoint_id: None,
            target_spec_json,
            phase_deadline_at_ms: now + SAVEPOINT_PHASE_TIMEOUT_MS,
            savepoint_retries: 0,
            verify_timeout_ms,
            actor: actor.clone(),
            correlation_id: correlation_id.clone(),
            last_error: None,
            paused_from: None,
            created_at_ms: now,
            updated_at_ms: now,
        };
        // Skip straight past the savepoint phase when the Job has no state
        // to carry: `requires_state` is false, so no artifact can be taken
        // or restored, and the commit is the whole upgrade.
        if !spec.requires_state() {
            record.phase = phase::COMMITTING_VERSION.into();
            record.phase_deadline_at_ms = now + COMMIT_PHASE_TIMEOUT_MS;
        }
        storage.upsert_job_upgrade(record.clone()).await?;
        storage
            .record_audit(crate::storage::AuditRecord {
                event_id: 0,
                actor,
                action: "job.upgrade.atomic.initiate".into(),
                resource_type: "job".into(),
                resource_id: Some(job_id.to_owned()),
                node_id: None,
                stream_id: None,
                correlation_id,
                outcome: "accepted".into(),
                failure_code: None,
                message: Some(format!(
                    "atomic upgrade to version {} (from {})",
                    record.to_version, record.from_version
                )),
                occurred_at_ms: now,
            })
            .await
            .map(|_| ())
            .map_err(HubError::from)?;
        self.job_upgrades
            .write()
            .await
            .insert(record.upgrade_id.clone(), record.clone());
        self.emit_job_upgrade_event(&record, "initiated", format!(
            "atomic upgrade started: {} -> {}",
            record.from_version, record.to_version
        )).await;
        Ok(record)
    }

    pub async fn job_upgrade(
        &self,
        upgrade_id: &str,
    ) -> Result<Option<JobUpgradeRecord>, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage
            .get_job_upgrade(upgrade_id.to_owned())
            .await
            .map_err(HubError::from)
    }

    pub async fn job_upgrades(&self, job_id: &str) -> Result<Vec<JobUpgradeRecord>, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage
            .list_job_upgrades(job_id.to_owned())
            .await
            .map_err(HubError::from)
    }

    /// The non-terminal orchestration owning a Job, if any (cache read; the
    /// cache is repopulated at boot recovery and maintained on every
    /// transition).
    pub(crate) async fn active_job_upgrade_for(
        &self,
        job_id: &str,
    ) -> Option<JobUpgradeRecord> {
        self.job_upgrades
            .read()
            .await
            .values()
            .find(|record| record.job_id == job_id && !record.phase_is_terminal())
            .cloned()
    }

    /// True while an active orchestration owns the Job's dispatch state and
    /// the general reconciler must defer (see the reconciliation fence).
    /// The observation phases (verification, rollback) fall through:
    /// ordinary reconciliation IS the start mechanism there — for the new
    /// generation and for a restored previous one alike.
    pub(crate) async fn job_upgrade_fences_reconciliation(&self, job_id: &str) -> bool {
        match self.active_job_upgrade_for(job_id).await {
            Some(record) => {
                record.phase != phase::VERIFYING && record.phase != phase::ROLLING_BACK
            }
            None => false,
        }
    }

    /// Savepoint artifacts an active orchestration references; checkpoint
    /// retention must not delete them (the rollback path restores from the
    /// same cut).
    pub(crate) async fn pinned_job_upgrade_savepoints(&self, job_id: &str) -> Vec<String> {
        self.job_upgrades
            .read()
            .await
            .values()
            .filter(|record| {
                record.job_id == job_id
                    && !record.phase_is_terminal()
                    && record.savepoint_id.is_some()
            })
            .filter_map(|record| record.savepoint_id.clone())
            .collect()
    }

    // ------------------------------------------------------------------
    // Operator actions
    // ------------------------------------------------------------------

    pub async fn act_job_upgrade(
        &self,
        upgrade_id: &str,
        action: &str,
        actor: Option<String>,
        correlation_id: Option<String>,
    ) -> Result<JobUpgradeRecord, HubError> {
        let Some(mut record) = self.job_upgrade(upgrade_id).await? else {
            return Err(HubError::Invalid("job upgrade not found".into()));
        };
        if record.phase_is_terminal() {
            return Err(HubError::Invalid("job upgrade is already terminal".into()));
        }
        match action {
            "pause" => {
                if record.phase == phase::PAUSED {
                    return Err(HubError::Invalid("job upgrade is already paused".into()));
                }
                record.paused_from = Some(record.phase.clone());
                record.phase = phase::PAUSED.into();
                self.finish_job_upgrade_transition(&mut record, None).await?;
                self.audit_job_upgrade_action(&record, "job.upgrade.atomic.pause", actor, correlation_id, "accepted")
                    .await?;
                Ok(record)
            }
            "resume" => {
                if record.phase != phase::PAUSED {
                    return Err(HubError::Invalid("only a paused job upgrade can resume".into()));
                }
                let resumed = record
                    .paused_from
                    .clone()
                    .unwrap_or_else(|| phase::SAVING_SAVEPOINT.into());
                record.phase = resumed;
                record.paused_from = None;
                // A long pause may have outlived the phase deadline: re-arm
                // it so resume does not immediately time the phase out.
                record.phase_deadline_at_ms =
                    now_ms() + phase_timeout_ms(&record, &record.phase);
                self.finish_job_upgrade_transition(&mut record, None).await?;
                self.audit_job_upgrade_action(&record, "job.upgrade.atomic.resume", actor, correlation_id, "accepted")
                    .await?;
                Ok(record)
            }
            "cancel" => {
                record.phase = phase::CANCELLED.into();
                self.finish_job_upgrade_transition(
                    &mut record,
                    Some("cancelled by operator"),
                )
                .await?;
                self.audit_job_upgrade_action(&record, "job.upgrade.atomic.cancel", actor, correlation_id, "accepted")
                    .await?;
                Ok(record)
            }
            "rollback" => {
                if record.phase != phase::VERIFYING
                    && record.paused_from.as_deref() != Some(phase::VERIFYING)
                {
                    return Err(HubError::Invalid(
                        "rollback is available once the new version is being verified".into(),
                    ));
                }
                record.phase = phase::ROLLING_BACK.into();
                record.paused_from = None;
                record.phase_deadline_at_ms = now_ms() + verify_timeout_ms(&record);
                self.finish_job_upgrade_transition(&mut record, None).await?;
                self.audit_job_upgrade_action(&record, "job.upgrade.atomic.rollback", actor, correlation_id, "accepted")
                    .await?;
                Ok(record)
            }
            _ => Err(HubError::Invalid(
                "action must be pause, resume, cancel, or rollback".into(),
            )),
        }
    }

    // ------------------------------------------------------------------
    // The tick
    // ------------------------------------------------------------------

    /// Advance every non-terminal orchestration one step. Runs on the leader
    /// reconcile tick, before `reconcile_jobs`, so a savepoint that completes
    /// here commits in the same tick and the (unfenced) job reconciler starts
    /// the new generation immediately — the cutover window is not stretched
    /// by a tick of idle.
    pub async fn reconcile_job_upgrades(&self) -> Result<usize, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        let active = storage.recover_job_upgrades().await?;
        let mut changes = 0;
        for mut record in active {
            let before = record.phase.clone();
            match record.phase.as_str() {
                phase::PAUSED => continue,
                phase::SAVING_SAVEPOINT | phase::PENDING => {
                    self.step_savepoint_phase(storage, &mut record).await?
                }
                phase::COMMITTING_VERSION => self.step_commit_phase(&mut record).await?,
                phase::VERIFYING => self.step_verify_phase(&mut record).await?,
                phase::ROLLING_BACK => self.step_rollback_phase(&mut record).await?,
                _ => {}
            }
            if record.phase != before {
                changes += 1;
            }
        }
        Ok(changes)
    }

    // ------------------------------------------------------------------
    // Phases
    // ------------------------------------------------------------------

    /// Savepoint phase: dispatch a fresh savepoint round when none is in
    /// flight, poll it otherwise. The old generation keeps running through
    /// every outcome here; a failed round only costs a retry.
    async fn step_savepoint_phase(
        &self,
        storage: &StorageActor,
        record: &mut JobUpgradeRecord,
    ) -> Result<(), HubError> {
        let now = now_ms();
        if record.savepoint_id.is_none() {
            if record.savepoint_retries > MAX_SAVEPOINT_RETRIES {
                record.phase = phase::ABORTED.into();
                return self
                    .finish_job_upgrade_transition(
                        record,
                        Some("savepoint retries exhausted; the Job is unchanged"),
                    )
                    .await;
            }
            let Some(job) = self.job(&record.job_id).await? else {
                record.phase = phase::ABORTED.into();
                return self
                    .finish_job_upgrade_transition(record, Some("job disappeared"))
                    .await;
            };
            if job.version != record.from_version || job.desired_state != "running" {
                record.phase = phase::ABORTED.into();
                return self
                    .finish_job_upgrade_transition(
                        record,
                        Some(&format!(
                            "job changed before the savepoint (version {}, desired {})",
                            job.version, job.desired_state
                        )),
                    )
                    .await;
            }
            let spec: arkflow_core::job::JobSpec = serde_json::from_str(&job.spec_json)
                .map_err(|error| HubError::Invalid(format!("invalid persisted Job spec: {error}")))?;
            let checkpoint_id = format!(
                "savepoint-{}-{}-{}",
                record.job_id, job.generation, now
            );
            let checkpoint = JobCheckpointRecord {
                job_id: record.job_id.clone(),
                job_version: job.version,
                checkpoint_id: checkpoint_id.clone(),
                kind: "savepoint".into(),
                status: "pending".into(),
                manifest_uri: None,
                format_version: crate::hub::checkpoint::job_state_format_version(&spec),
                created_at_ms: now,
                updated_at_ms: now,
            };
            // Records and dispatches the savepoint round at the Job's current
            // generation; the version still matches, so completing it later
            // moves the recovery pointer to exactly this artifact.
            self.record_job_checkpoint(checkpoint).await?;
            record.savepoint_id = Some(checkpoint_id);
            record.updated_at_ms = now;
            storage.upsert_job_upgrade(record.clone()).await?;
            self.job_upgrades
                .write()
                .await
                .insert(record.upgrade_id.clone(), record.clone());
            return Ok(());
        }
        let savepoint_id = record.savepoint_id.clone().expect("checked above");
        let status = self
            .job_checkpoints(&record.job_id)
            .await?
            .into_iter()
            .find(|checkpoint| checkpoint.checkpoint_id == savepoint_id)
            .map(|checkpoint| checkpoint.status);
        match status.as_deref() {
            Some("completed") => {
                record.phase = phase::COMMITTING_VERSION.into();
                record.phase_deadline_at_ms = now + COMMIT_PHASE_TIMEOUT_MS;
                self.finish_job_upgrade_transition(record, None).await?;
                // Commit in the same tick the savepoint completed: the
                // cutover window ends at the next reconcile_jobs pass, not a
                // tick later.
                self.step_commit_phase(record).await
            }
            Some("failed") => {
                record.savepoint_retries += 1;
                if record.savepoint_retries > MAX_SAVEPOINT_RETRIES {
                    record.phase = phase::ABORTED.into();
                    return self
                        .finish_job_upgrade_transition(
                            record,
                            Some("savepoint rounds failed; the Job is unchanged"),
                        )
                        .await;
                }
                record.savepoint_id = None;
                record.updated_at_ms = now;
                storage.upsert_job_upgrade(record.clone()).await?;
                self.job_upgrades
                    .write()
                    .await
                    .insert(record.upgrade_id.clone(), record.clone());
                Ok(())
            }
            _ => {
                if now > record.phase_deadline_at_ms {
                    record.phase = phase::ABORTED.into();
                    return self
                        .finish_job_upgrade_transition(
                            record,
                            Some("savepoint phase deadline exceeded; the Job is unchanged"),
                        )
                        .await;
                }
                Ok(())
            }
        }
    }

    /// Commit phase: ONE generation-fenced write carrying the new spec and
    /// `desired_state = running`. The write never touches the recovery
    /// pointer — it already references the completed savepoint (moved at
    /// completion while the version still matched), and the fenced-update
    /// column set preserves it. A generation conflict is interpreted against
    /// observable state: an already-applied commit advances, anything else
    /// aborts. Idempotency is judged by what the Job record says, never by
    /// trusting the phase row.
    async fn step_commit_phase(&self, record: &mut JobUpgradeRecord) -> Result<(), HubError> {
        let now = now_ms();
        let Some(job) = self.job(&record.job_id).await? else {
            record.phase = phase::ABORTED.into();
            return self
                .finish_job_upgrade_transition(record, Some("job disappeared"))
                .await;
        };
        let already_committed =
            job.version == record.to_version && job.desired_state == "running";
        if already_committed {
            record.phase = phase::VERIFYING.into();
            record.phase_deadline_at_ms = now + verify_timeout_ms(record);
            return self.finish_job_upgrade_transition(record, None).await;
        }
        if now > record.phase_deadline_at_ms {
            record.phase = phase::ABORTED.into();
            return self
                .finish_job_upgrade_transition(
                    record,
                    Some("commit phase deadline exceeded; the Job is unchanged"),
                )
                .await;
        }
        if job.version != record.from_version {
            record.phase = phase::ABORTED.into();
            return self
                .finish_job_upgrade_transition(
                    record,
                    Some(&format!(
                        "job version moved to {} while the orchestration expected {}",
                        job.version, record.from_version
                    )),
                )
                .await;
        }
        let upgraded = JobRecord {
            job_id: record.job_id.clone(),
            version: record.to_version,
            spec_json: record.target_spec_json.clone(),
            desired_state: "running".into(),
            observed_state: "stopped".into(),
            convergence: "pending_recovery".into(),
            generation: job.generation,
            node_ids: job.node_ids.clone(),
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: now,
        };
        match self
            .update_job_with_expected_generation(upgraded, job.generation)
            .await
        {
            Ok(_) => {
                record.phase = phase::VERIFYING.into();
                record.phase_deadline_at_ms = now + verify_timeout_ms(record);
                self.finish_job_upgrade_transition(record, None).await
            }
            Err(HubError::GenerationConflict { .. }) => {
                // Re-read and interpret: the conflict either is our own
                // already-applied write (crash between write and phase
                // update) or someone else's change we must not overwrite.
                let Some(fresh) = self.job(&record.job_id).await? else {
                    record.phase = phase::ABORTED.into();
                    return self
                        .finish_job_upgrade_transition(record, Some("job disappeared"))
                        .await;
                };
                if fresh.version == record.to_version && fresh.desired_state == "running" {
                    record.phase = phase::VERIFYING.into();
                    record.phase_deadline_at_ms = now + verify_timeout_ms(record);
                    self.finish_job_upgrade_transition(record, None).await
                } else {
                    record.phase = phase::ABORTED.into();
                    self.finish_job_upgrade_transition(
                        record,
                        Some("commit lost a generation race; the newer Job state is preserved"),
                    )
                    .await
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Verification phase: passive observation while normal reconciliation
    /// starts the new generation (the fence is off in this phase, so
    /// `reconcile_jobs` assembles the recovery start and fences the old
    /// generation's nodes itself).
    async fn step_verify_phase(&self, record: &mut JobUpgradeRecord) -> Result<(), HubError> {
        let now = now_ms();
        let Some(job) = self.job(&record.job_id).await? else {
            record.phase = phase::FAILED.into();
            return self
                .finish_job_upgrade_transition(record, Some("job disappeared"))
                .await;
        };
        if job.version == record.to_version && job.observed_state == "running" {
            record.phase = phase::SUCCEEDED.into();
            return self
                .finish_job_upgrade_transition(record, Some("verified running at the target version"))
                .await;
        }
        if job.version != record.to_version {
            // The Job moved underneath the orchestration (a manual rollback
            // or another writer). Nothing to verify and nothing safe to do.
            record.phase = phase::FAILED.into();
            return self
                .finish_job_upgrade_transition(
                    record,
                    Some(&format!(
                        "job version moved to {} while verifying {}",
                        job.version, record.to_version
                    )),
                )
                .await;
        }
        if now > record.phase_deadline_at_ms {
            // Reconciliation could not converge the new generation within
            // the deadline: restore the previous version from the same cut.
            record.phase = phase::ROLLING_BACK.into();
            record.phase_deadline_at_ms = now + verify_timeout_ms(record);
            return self.finish_job_upgrade_transition(
                record,
                Some("verification deadline exceeded; rolling back to the previous version"),
            )
            .await;
        }
        Ok(())
    }

    /// Rollback phase: apply the previous-version restore (idempotently —
    /// observable state decides whether it already landed), then observe the
    /// Job running again at the previous version. Failure here is terminal:
    /// the Job is left stopped with its recovery pointer intact for the
    /// operator, and the savepoint stays pinned until the row is terminal.
    async fn step_rollback_phase(&self, record: &mut JobUpgradeRecord) -> Result<(), HubError> {
        let now = now_ms();
        let Some(job) = self.job(&record.job_id).await? else {
            record.phase = phase::FAILED.into();
            return self
                .finish_job_upgrade_transition(record, Some("job disappeared"))
                .await;
        };
        let restore_applied = job.version == record.from_version && job.desired_state == "running";
        if restore_applied {
            if job.observed_state == "running" {
                record.phase = phase::ROLLED_BACK.into();
                return self
                    .finish_job_upgrade_transition(
                        record,
                        Some("restored and running at the previous version"),
                    )
                    .await;
            }
            if now > record.phase_deadline_at_ms {
                record.phase = phase::FAILED.into();
                return self
                    .finish_job_upgrade_transition(
                        record,
                        Some("rollback verification deadline exceeded; the Job is stopped with its recovery pointer intact"),
                    )
                    .await;
            }
            return Ok(());
        }
        if job.version != record.to_version {
            record.phase = phase::FAILED.into();
            return self
                .finish_job_upgrade_transition(
                    record,
                    Some(&format!(
                        "job version moved to {} during rollback",
                        job.version
                    )),
                )
                .await;
        }
        let Some(previous) = self
            .job_versions(&record.job_id)
            .await?
            .into_iter()
            .find(|version| version.version == record.from_version)
        else {
            record.phase = phase::FAILED.into();
            return self
                .finish_job_upgrade_transition(
                    record,
                    Some("previous Job version is not available for rollback"),
                )
                .await;
        };
        let mut restored_spec: arkflow_core::job::JobSpec =
            serde_json::from_str(&previous.spec_json).map_err(|error| {
                HubError::Invalid(format!("invalid persisted Job spec: {error}"))
            })?;
        restored_spec.recovery = arkflow_core::job::RecoveryPolicy::LatestSavepoint;
        // The artifact compatibility rule the manual rollback applies, against
        // the exact artifact this orchestration's savepoint produced.
        if let Some(savepoint_id) = record.savepoint_id.as_deref() {
            let compatible = self
                .job_checkpoints(&record.job_id)
                .await?
                .into_iter()
                .find(|checkpoint| checkpoint.checkpoint_id == savepoint_id)
                .is_some_and(|checkpoint| {
                    crate::hub::checkpoint::recovery_record_is_compatible(&restored_spec, &checkpoint)
                });
            if !compatible {
                record.phase = phase::FAILED.into();
                return self
                    .finish_job_upgrade_transition(
                        record,
                        Some("the savepoint is incompatible with the previous Job version"),
                    )
                    .await;
            }
        }
        restored_spec
            .validate()
            .and_then(|_| {
                arkflow_core::job::JobPlan::compile(restored_spec.clone()).map(|_| ())
            })
            .map_err(|error| HubError::Invalid(error.to_string()))?;
        let restored_spec_json = serde_json::to_string(&restored_spec)
            .map_err(|error| HubError::Invalid(format!("invalid Job spec: {error}")))?;
        let restored = JobRecord {
            job_id: record.job_id.clone(),
            version: record.from_version,
            spec_json: restored_spec_json,
            desired_state: "running".into(),
            observed_state: "stopped".into(),
            convergence: "pending_recovery".into(),
            generation: job.generation,
            node_ids: job.node_ids.clone(),
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: now,
        };
        match self
            .update_job_with_expected_generation(restored, job.generation)
            .await
        {
            Ok(_) | Err(HubError::GenerationConflict { .. }) => {
                // On conflict the observable-state check on the next tick
                // decides applied-vs-failed; nothing else to do here.
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    // ------------------------------------------------------------------
    // Persistence, events, audit
    // ------------------------------------------------------------------

    /// Persist a phase transition: durable upsert, cache refresh, and an
    /// event (phase transitions are what operators watch on the SSE stream).
    async fn finish_job_upgrade_transition(
        &self,
        record: &mut JobUpgradeRecord,
        message: Option<&str>,
    ) -> Result<(), HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        record.updated_at_ms = now_ms();
        if record.phase_is_terminal() {
            record.paused_from = None;
        }
        storage.upsert_job_upgrade(record.clone()).await?;
        self.job_upgrades
            .write()
            .await
            .insert(record.upgrade_id.clone(), record.clone());
        self.emit_job_upgrade_event(
            record,
            &record.phase,
            message
                .map(|message| message.to_owned())
                .unwrap_or_else(|| format!("phase -> {}", record.phase)),
        )
        .await;
        Ok(())
    }

    async fn emit_job_upgrade_event(&self, record: &JobUpgradeRecord, outcome: &str, message: String) {
        let event = ControlEvent {
            occurred_at_ms: now_ms(),
            event_type: "job.upgrade".into(),
            stream_id: None,
            outcome: outcome.into(),
            message: Some(bounded_text(&message, 512)),
            operation_id: Some(record.upgrade_id.clone()),
            correlation_id: record.correlation_id.clone(),
            actor: Some("hub".into()),
        };
        let hub_event = HubEvent {
            event_id: None,
            node_id: HUB_NODE_ID_FOR_UPGRADE_EVENTS.into(),
            event,
        };
        let mut events = self.events.write().await;
        if events.len() >= MAX_EVENTS {
            events.pop_front();
        }
        events.push_back(hub_event.clone());
        drop(events);
        let _ = self.updates.send(hub_event);
    }

    async fn audit_job_upgrade_action(
        &self,
        record: &JobUpgradeRecord,
        action: &str,
        actor: Option<String>,
        correlation_id: Option<String>,
        outcome: &str,
    ) -> Result<(), HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage
            .record_audit(crate::storage::AuditRecord {
                event_id: 0,
                actor,
                action: action.into(),
                resource_type: "job".into(),
                resource_id: Some(record.job_id.clone()),
                node_id: None,
                stream_id: None,
                correlation_id,
                outcome: outcome.into(),
                failure_code: None,
                message: Some(format!(
                    "{} (upgrade {}, phase {})",
                    action, record.upgrade_id, record.phase
                )),
                occurred_at_ms: now_ms(),
            })
            .await
            .map(|_| ())
            .map_err(HubError::from)
    }

    /// Boot recovery for orchestration rows: repopulate the fence/pin cache
    /// from durable state. Re-entry itself happens on the next reconcile
    /// tick (`reconcile_job_upgrades`), which resumes each phase
    /// idempotently from observable state.
    pub(crate) async fn recover_job_upgrade_cache(&self) -> Result<(), HubError> {
        let Some(storage) = self.storage.as_ref() else {
            return Ok(());
        };
        let active = storage.recover_job_upgrades().await?;
        let mut upgrades = self.job_upgrades.write().await;
        for record in active {
            upgrades.insert(record.upgrade_id.clone(), record);
        }
        Ok(())
    }

    /// Terminal-row history bound, run on the retention sweep.
    pub async fn prune_job_upgrade_history(&self) -> Result<usize, HubError> {
        let Some(storage) = self.storage.as_ref() else {
            return Ok(0);
        };
        const RETENTION_MS: i64 = 30 * 24 * 60 * 60 * 1000;
        const MAX_RETAINED: i64 = 4096;
        storage
            .prune_job_upgrades(now_ms() as i64 - RETENTION_MS, MAX_RETAINED)
            .await
            .map_err(HubError::from)
    }
}
