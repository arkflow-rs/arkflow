//! Job record store: CRUD, versioned updates, desired-state edits.

use super::*;

impl Hub {
    pub async fn jobs(&self) -> Result<Vec<JobRecord>, HubError> {
        if let Some(storage) = &self.storage {
            return storage.list_jobs().await.map_err(HubError::from);
        }
        Ok(self.jobs.read().await.values().cloned().collect())
    }

    pub async fn job(&self, job_id: &str) -> Result<Option<JobRecord>, HubError> {
        if let Some(storage) = &self.storage {
            return storage.get_job(job_id).await.map_err(HubError::from);
        }
        Ok(self.jobs.read().await.get(job_id).cloned())
    }

    pub async fn upsert_job(&self, mut job: JobRecord) -> Result<JobRecord, HubError> {
        // A pinned placement has nothing to relocate to: rebalancing is a
        // scheduler decision over unpinned Jobs, so an explicit pin combined
        // with the auto policy is a configuration contradiction, not a
        // silent no-op.
        if let Ok(spec) = serde_json::from_str::<arkflow_core::job::JobSpec>(&job.spec_json) {
            if spec
                .rebalance
                .is_some_and(|policy| policy.mode == arkflow_core::job::RebalanceMode::Auto)
                && !job.node_ids.is_empty()
            {
                return Err(HubError::Invalid(
                    "rebalance policy 'auto' requires an unpinned placement: remove node_ids"
                        .into(),
                ));
            }
        }
        let version_record = serde_json::from_str::<arkflow_core::job::JobSpec>(&job.spec_json)
            .ok()
            .and_then(|spec| {
                arkflow_core::job::JobPlan::compile(spec)
                    .ok()
                    .and_then(|plan| serde_json::to_string(&plan).ok())
                    .map(|plan_json| JobVersionRecord {
                        job_id: job.job_id.clone(),
                        version: job.version,
                        spec_json: job.spec_json.clone(),
                        plan_json,
                        created_at_ms: now_ms(),
                    })
            });
        if let Some(storage) = &self.storage {
            job = storage.upsert_job(job).await.map_err(HubError::from)?;
            if let Some(record) = version_record.clone() {
                storage
                    .upsert_job_version(record)
                    .await
                    .map_err(HubError::from)?;
            }
        } else {
            let mut jobs = self.jobs.write().await;
            job.generation = jobs
                .get(&job.job_id)
                .map(|current| current.generation.saturating_add(1))
                .unwrap_or_else(|| job.generation.max(1));
            jobs.insert(job.job_id.clone(), job.clone());
        }
        if let Some(record) = version_record {
            let mut versions = self.job_versions.write().await;
            let entries = versions.entry(record.job_id.clone()).or_default();
            entries.retain(|existing| existing.version != record.version);
            entries.push(record);
            entries.sort_by_key(|entry| std::cmp::Reverse(entry.version));
        }
        if self.storage.is_some() {
            self.jobs
                .write()
                .await
                .insert(job.job_id.clone(), job.clone());
        }
        if job.desired_state != "stopped" {
            self.reconcile_job(&job).await?;
        }
        Ok(job)
    }

    /// Generation-fenced Job record replacement for upgrade and rollback.
    /// The handlers read the Job, await several round trips and then write;
    /// the fence makes a concurrent desired-state change (or reconciler
    /// write) that bumped the generation surface as a conflict instead of
    /// being silently overwritten by the older read.
    pub async fn update_job_with_expected_generation(
        &self,
        job: JobRecord,
        expected_generation: u64,
    ) -> Result<JobRecord, HubError> {
        let version_record = serde_json::from_str::<arkflow_core::job::JobSpec>(&job.spec_json)
            .ok()
            .and_then(|spec| {
                arkflow_core::job::JobPlan::compile(spec)
                    .ok()
                    .and_then(|plan| serde_json::to_string(&plan).ok())
                    .map(|plan_json| JobVersionRecord {
                        job_id: job.job_id.clone(),
                        version: job.version,
                        spec_json: job.spec_json.clone(),
                        plan_json,
                        created_at_ms: now_ms(),
                    })
            });
        let updated = if let Some(storage) = &self.storage {
            let updated = storage
                .update_job_with_expected_generation(job.clone(), expected_generation)
                .await
                .map_err(HubError::from)?;
            if let Some(record) = version_record.clone() {
                storage
                    .upsert_job_version(record)
                    .await
                    .map_err(HubError::from)?;
            }
            self.jobs
                .write()
                .await
                .insert(updated.job_id.clone(), updated.clone());
            updated
        } else {
            let mut jobs = self.jobs.write().await;
            match jobs.get(&job.job_id) {
                Some(current) if current.generation == expected_generation => {
                    // Same rule as the storage backend: the recovery pointer
                    // belongs to the checkpoint path, which moves it without
                    // bumping the generation, so this write must not copy the
                    // caller's earlier read back over it.
                    let stored_checkpoint = current.checkpoint_id.clone();
                    let mut updated = job;
                    updated.generation = expected_generation.saturating_add(1);
                    if stored_checkpoint.is_some() {
                        updated.checkpoint_id = stored_checkpoint;
                    }
                    jobs.insert(updated.job_id.clone(), updated.clone());
                    updated
                }
                Some(current) => {
                    return Err(HubError::from(StorageError::GenerationConflict {
                        expected: expected_generation,
                        current: current.generation,
                    }));
                }
                None => {
                    return Err(HubError::from(StorageError::GenerationConflict {
                        expected: expected_generation,
                        current: 0,
                    }));
                }
            }
        };
        if let Some(record) = version_record {
            let mut versions = self.job_versions.write().await;
            let entries = versions.entry(record.job_id.clone()).or_default();
            entries.retain(|existing| existing.version != record.version);
            entries.push(record);
            entries.sort_by_key(|entry| std::cmp::Reverse(entry.version));
        }
        Ok(updated)
    }

    pub async fn job_versions(&self, job_id: &str) -> Result<Vec<JobVersionRecord>, HubError> {
        if let Some(storage) = &self.storage {
            let versions = storage
                .list_job_versions(job_id)
                .await
                .map_err(HubError::from)?;
            if !versions.is_empty() {
                return Ok(versions);
            }
        }
        Ok(self
            .job_versions
            .read()
            .await
            .get(job_id)
            .cloned()
            .unwrap_or_default())
    }

    pub async fn update_job(
        &self,
        job_id: &str,
        desired_state: Option<&str>,
        generation: Option<u64>,
    ) -> Result<Option<JobRecord>, HubError> {
        let updated = if let Some(storage) = &self.storage {
            storage
                .update_job(
                    job_id,
                    desired_state.map(str::to_owned),
                    None,
                    None,
                    generation,
                    None,
                    None,
                )
                .await
                .map_err(HubError::from)?
        } else {
            let mut jobs = self.jobs.write().await;
            let Some(job) = jobs.get_mut(job_id) else {
                return Ok(None);
            };
            if let Some(desired_state) = desired_state {
                job.desired_state = desired_state.into();
            }
            if let Some(generation) = generation {
                job.generation = generation;
            }
            job.updated_at_ms = now_ms();
            Some(job.clone())
        };
        if let Some(job) = &updated {
            self.jobs
                .write()
                .await
                .insert(job.job_id.clone(), job.clone());
            self.reconcile_job(job).await?;
        }
        Ok(updated)
    }

    pub async fn update_job_desired_state(
        &self,
        job_id: &str,
        desired_state: &str,
        expected_generation: u64,
    ) -> Result<Option<JobRecord>, HubError> {
        let updated = if let Some(storage) = &self.storage {
            storage
                .update_job_desired_state(job_id, desired_state, expected_generation)
                .await
                .map_err(HubError::from)?
        } else {
            let mut jobs = self.jobs.write().await;
            let Some(job) = jobs.get_mut(job_id) else {
                return Ok(None);
            };
            if job.generation != expected_generation {
                return Err(HubError::GenerationConflict {
                    expected: expected_generation,
                    current: job.generation,
                });
            }
            job.desired_state = desired_state.into();
            job.convergence = "reconciling".into();
            job.generation = expected_generation.saturating_add(1);
            job.updated_at_ms = now_ms();
            Some(job.clone())
        };
        if let Some(job) = &updated {
            self.jobs
                .write()
                .await
                .insert(job.job_id.clone(), job.clone());
            self.reconcile_job(job).await?;
        }
        Ok(updated)
    }
}
