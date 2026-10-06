//! Node configuration rollouts: staged application, pause/resume/rollback.

use super::error::HubError;
use super::wire::NodeConnectionState;
use super::{now_ms, Hub, HUB_SEQUENCE};
use crate::storage::{DesiredMutation, RolloutRecord, RolloutTargetRecord, RolloutTargetUpdate};
use arkflow_core::control::NodeMaintenanceState;
use std::sync::atomic::Ordering;

impl Hub {
    pub async fn create_rollout(
        &self,
        config_version_id: String,
        node_ids: Vec<String>,
        batch_size: u32,
        actor: Option<String>,
        correlation_id: Option<String>,
    ) -> Result<RolloutRecord, HubError> {
        let batch_size = batch_size.clamp(1, 256);
        if node_ids.is_empty() || node_ids.iter().any(|id| id.trim().is_empty()) {
            return Err(HubError::Invalid("rollout requires nodes".into()));
        }
        let mut unique = std::collections::BTreeSet::new();
        if node_ids.iter().any(|id| !unique.insert(id.clone())) {
            return Err(HubError::Invalid("rollout contains duplicate nodes".into()));
        }
        let now = now_ms();
        let rollout = RolloutRecord {
            rollout_id: format!("rollout-{}", HUB_SEQUENCE.fetch_add(1, Ordering::Relaxed)),
            config_version_id,
            state: "pending".into(),
            batch_size,
            current_batch: 0,
            total_targets: node_ids.len() as u32,
            actor: actor.clone(),
            correlation_id: correlation_id.clone(),
            created_at_ms: now,
            updated_at_ms: now,
        };
        let targets = node_ids
            .into_iter()
            .enumerate()
            .map(|(ordinal, node_id)| RolloutTargetRecord {
                rollout_id: rollout.rollout_id.clone(),
                node_id,
                ordinal: ordinal as u32,
                state: "pending".into(),
                attempt_id: None,
                error: None,
                observed_config_version: None,
                updated_at_ms: now,
            })
            .collect::<Vec<_>>();
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        if storage
            .get_config_version_content(rollout.config_version_id.clone())
            .await
            .map_err(HubError::from)?
            .is_none()
        {
            return Err(HubError::Invalid("configuration version not found".into()));
        }
        storage
            .create_rollout(rollout.clone(), targets)
            .await
            .map_err(HubError::from)?;
        storage
            .record_audit(crate::storage::AuditRecord {
                event_id: 0,
                actor,
                action: "rollout.create".into(),
                resource_type: "rollout".into(),
                resource_id: Some(rollout.rollout_id.clone()),
                node_id: None,
                stream_id: None,
                correlation_id,
                outcome: "accepted".into(),
                failure_code: None,
                message: None,
                occurred_at_ms: now,
            })
            .await
            .map_err(HubError::from)?;
        self.rollouts
            .write()
            .await
            .insert(rollout.rollout_id.clone(), rollout.clone());
        Ok(rollout)
    }

    pub async fn create_rollout_with_content(
        &self,
        config_version_id: String,
        content: String,
        node_ids: Vec<String>,
        batch_size: u32,
        actor: Option<String>,
        correlation_id: Option<String>,
    ) -> Result<RolloutRecord, HubError> {
        let batch_size = batch_size.clamp(1, 256);
        if node_ids.len() != 1 || node_ids.iter().any(|id| id.trim().is_empty()) {
            return Err(HubError::Invalid(
                "single-node rollout requires exactly one node".into(),
            ));
        }
        let now = now_ms();
        let rollout = RolloutRecord {
            rollout_id: format!("rollout-{}", HUB_SEQUENCE.fetch_add(1, Ordering::Relaxed)),
            config_version_id,
            state: "pending".into(),
            batch_size,
            current_batch: 0,
            total_targets: 1,
            actor: actor.clone(),
            correlation_id: correlation_id.clone(),
            created_at_ms: now,
            updated_at_ms: now,
        };
        let targets = vec![RolloutTargetRecord {
            rollout_id: rollout.rollout_id.clone(),
            node_id: node_ids[0].clone(),
            ordinal: 0,
            state: "pending".into(),
            attempt_id: None,
            error: None,
            observed_config_version: None,
            updated_at_ms: now,
        }];
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage
            .create_rollout_with_content(rollout.clone(), targets, content, actor.clone())
            .await
            .map_err(HubError::from)?;
        storage
            .record_audit(crate::storage::AuditRecord {
                event_id: 0,
                actor,
                action: "rollout.create".into(),
                resource_type: "rollout".into(),
                resource_id: Some(rollout.rollout_id.clone()),
                node_id: node_ids.into_iter().next(),
                stream_id: None,
                correlation_id,
                outcome: "accepted".into(),
                failure_code: None,
                message: None,
                occurred_at_ms: now,
            })
            .await
            .map_err(HubError::from)?;
        self.rollouts
            .write()
            .await
            .insert(rollout.rollout_id.clone(), rollout.clone());
        Ok(rollout)
    }

    pub async fn rollout(
        &self,
        rollout_id: &str,
    ) -> Result<Option<(RolloutRecord, Vec<RolloutTargetRecord>)>, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        let Some(rollout) = storage
            .get_rollout(rollout_id.to_owned())
            .await
            .map_err(HubError::from)?
        else {
            return Ok(None);
        };
        let targets = storage
            .list_rollout_targets(rollout_id.to_owned())
            .await
            .map_err(HubError::from)?;
        Ok(Some((rollout, targets)))
    }

    pub async fn rollouts(&self) -> Result<Vec<RolloutRecord>, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage.list_rollouts().await.map_err(HubError::from)
    }

    pub async fn act_rollout(
        &self,
        rollout_id: &str,
        action: &str,
        rollback_config_version: Option<String>,
        actor: Option<String>,
        correlation_id: Option<String>,
    ) -> Result<RolloutRecord, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        let Some(rollout) = storage
            .get_rollout(rollout_id.to_owned())
            .await
            .map_err(HubError::from)?
        else {
            return Err(HubError::Invalid("rollout not found".into()));
        };
        let terminal = matches!(
            rollout.state.as_str(),
            "converged" | "cancelled" | "rolled_back"
        );
        if terminal {
            return Err(HubError::Invalid("rollout is already terminal".into()));
        }
        let now = now_ms();
        match action {
            "pause" => {
                storage
                    .update_rollout(rollout_id, "paused", rollout.current_batch, now)
                    .await
                    .map_err(HubError::from)?;
                let targets = storage
                    .list_rollout_targets(rollout_id.to_owned())
                    .await
                    .map_err(HubError::from)?;
                for target in targets
                    .into_iter()
                    .filter(|target| target.state == "pending")
                {
                    storage
                        .update_rollout_target(RolloutTargetUpdate {
                            rollout_id: rollout_id.to_owned(),
                            node_id: target.node_id,
                            state: "paused".into(),
                            attempt_id: target.attempt_id,
                            error: target.error,
                            observed_config_version: target.observed_config_version,
                            updated_at_ms: now,
                        })
                        .await
                        .map_err(HubError::from)?;
                }
                self.record_rollout_audit(
                    &rollout,
                    "rollout.pause",
                    actor,
                    correlation_id,
                    "accepted",
                    None,
                )
                .await?;
                Ok(RolloutRecord {
                    state: "paused".into(),
                    updated_at_ms: now,
                    ..rollout
                })
            }
            "resume" => {
                if rollout.state != "paused" {
                    return Err(HubError::Invalid("only a paused rollout can resume".into()));
                }
                storage
                    .update_rollout(rollout_id, "applying", rollout.current_batch, now)
                    .await
                    .map_err(HubError::from)?;
                let targets = storage
                    .list_rollout_targets(rollout_id.to_owned())
                    .await
                    .map_err(HubError::from)?;
                for target in targets
                    .into_iter()
                    .filter(|target| target.state == "paused")
                {
                    storage
                        .update_rollout_target(RolloutTargetUpdate {
                            rollout_id: rollout_id.to_owned(),
                            node_id: target.node_id,
                            state: "pending".into(),
                            attempt_id: target.attempt_id,
                            error: target.error,
                            observed_config_version: target.observed_config_version,
                            updated_at_ms: now,
                        })
                        .await
                        .map_err(HubError::from)?;
                }
                self.record_rollout_audit(
                    &rollout,
                    "rollout.resume",
                    actor,
                    correlation_id,
                    "accepted",
                    None,
                )
                .await?;
                Ok(RolloutRecord {
                    state: "applying".into(),
                    updated_at_ms: now,
                    ..rollout
                })
            }
            "cancel" => {
                storage
                    .update_rollout(rollout_id, "cancelled", rollout.current_batch, now)
                    .await
                    .map_err(HubError::from)?;
                let targets = storage
                    .list_rollout_targets(rollout_id.to_owned())
                    .await
                    .map_err(HubError::from)?;
                for target in targets.into_iter().filter(|target| {
                    matches!(target.state.as_str(), "pending" | "paused" | "applying")
                }) {
                    storage
                        .update_rollout_target(RolloutTargetUpdate {
                            rollout_id: rollout_id.to_owned(),
                            node_id: target.node_id,
                            state: "cancelled".into(),
                            attempt_id: target.attempt_id,
                            error: Some("rollout cancelled by operator".into()),
                            observed_config_version: target.observed_config_version,
                            updated_at_ms: now,
                        })
                        .await
                        .map_err(HubError::from)?;
                }
                self.record_rollout_audit(
                    &rollout,
                    "rollout.cancel",
                    actor,
                    correlation_id,
                    "accepted",
                    None,
                )
                .await?;
                Ok(RolloutRecord {
                    state: "cancelled".into(),
                    updated_at_ms: now,
                    ..rollout
                })
            }
            "rollback" => {
                let Some(config_version_id) = rollback_config_version else {
                    return Err(HubError::Invalid("rollback requires config_version".into()));
                };
                let targets = storage
                    .list_rollout_targets(rollout_id.to_owned())
                    .await
                    .map_err(HubError::from)?;
                let rollback = self
                    .create_rollout(
                        config_version_id,
                        targets.into_iter().map(|target| target.node_id).collect(),
                        rollout.batch_size,
                        actor.clone(),
                        correlation_id.clone(),
                    )
                    .await?;
                storage
                    .update_rollout(rollout_id, "rolled_back", rollout.current_batch, now)
                    .await
                    .map_err(HubError::from)?;
                self.record_rollout_audit(
                    &rollout,
                    "rollout.rollback",
                    actor,
                    correlation_id,
                    "accepted",
                    Some(format!("created rollout {}", rollback.rollout_id)),
                )
                .await?;
                Ok(rollback)
            }
            _ => Err(HubError::Invalid(
                "action must be pause, resume, cancel, or rollback".into(),
            )),
        }
    }

    async fn record_rollout_audit(
        &self,
        rollout: &RolloutRecord,
        action: &str,
        actor: Option<String>,
        correlation_id: Option<String>,
        outcome: &str,
        message: Option<String>,
    ) -> Result<(), HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage
            .record_audit(crate::storage::AuditRecord {
                event_id: 0,
                actor,
                action: action.into(),
                resource_type: "rollout".into(),
                resource_id: Some(rollout.rollout_id.clone()),
                node_id: None,
                stream_id: None,
                correlation_id,
                outcome: outcome.into(),
                failure_code: None,
                message: message.map(|value| value.chars().take(256).collect()),
                occurred_at_ms: now_ms(),
            })
            .await
            .map(|_| ())
            .map_err(HubError::from)
    }

    pub async fn reconcile_rollouts(&self) -> Result<usize, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        let active = storage.recover_rollouts().await.map_err(HubError::from)?;
        let mut changes = 0;
        for rollout in active {
            if rollout.state == "paused" {
                continue;
            }
            let targets = storage
                .list_rollout_targets(rollout.rollout_id.clone())
                .await
                .map_err(HubError::from)?;
            let batch_start = rollout.current_batch * rollout.batch_size;
            let batch_end = batch_start + rollout.batch_size;
            let mut batch_failed = false;
            for target in targets
                .iter()
                .filter(|target| target.ordinal >= batch_start && target.ordinal < batch_end)
            {
                match target.state.as_str() {
                    "pending" => {
                        let online =
                            self.nodes
                                .read()
                                .await
                                .get(&target.node_id)
                                .is_some_and(|node| {
                                    node.resource.state == NodeConnectionState::Online
                                        && node.resource.lease_expires_at_ms > now_ms()
                                        && node.resource.maintenance_state
                                            == NodeMaintenanceState::Active
                                });
                        if !online {
                            continue;
                        }
                        let Some(payload_json) = storage
                            .get_config_version_content(rollout.config_version_id.clone())
                            .await
                            .map_err(HubError::from)?
                        else {
                            storage
                                .update_rollout_target(RolloutTargetUpdate {
                                    rollout_id: rollout.rollout_id.clone(),
                                    node_id: target.node_id.clone(),
                                    state: "failed".into(),
                                    attempt_id: None,
                                    error: Some("configuration version content is missing".into()),
                                    observed_config_version: None,
                                    updated_at_ms: now_ms(),
                                })
                                .await
                                .map_err(HubError::from)?;
                            batch_failed = true;
                            changes += 1;
                            continue;
                        };

                        let expected_generation = storage
                            .get_desired(target.node_id.clone(), "__configuration__")
                            .await
                            .map_err(HubError::from)?
                            .map(|desired| desired.generation)
                            .unwrap_or(0);
                        let intent = self
                            .set_desired_state(DesiredMutation {
                                node_id: target.node_id.clone(),
                                stream_id: "__configuration__".into(),
                                desired_state: "configured".into(),
                                config_version_id: Some(rollout.config_version_id.clone()),
                                action_id: None,
                                expected_generation: Some(expected_generation),
                                actor: rollout.actor.clone(),
                                correlation_id: rollout.correlation_id.clone(),
                                idempotency_key: Some(format!(
                                    "{}:{}",
                                    rollout.rollout_id, target.node_id
                                )),
                                intent_type: Some("apply_configuration".into()),
                                payload_json: Some(payload_json),
                            })
                            .await?;
                        storage
                            .update_rollout_target(RolloutTargetUpdate {
                                rollout_id: rollout.rollout_id.clone(),
                                node_id: target.node_id.clone(),
                                state: "applying".into(),
                                attempt_id: Some(intent.intent_id),
                                error: None,
                                observed_config_version: None,
                                updated_at_ms: now_ms(),
                            })
                            .await
                            .map_err(HubError::from)?;
                        changes += 1;
                    }
                    "applying" => {
                        let Some(intent_id) = target.attempt_id.as_deref() else {
                            continue;
                        };
                        let Some(intent) = storage
                            .get_intent(intent_id.to_owned())
                            .await
                            .map_err(HubError::from)?
                        else {
                            continue;
                        };
                        if intent.state == "converged" {
                            storage
                                .update_rollout_target(RolloutTargetUpdate {
                                    rollout_id: rollout.rollout_id.clone(),
                                    node_id: target.node_id.clone(),
                                    state: "succeeded".into(),
                                    attempt_id: target.attempt_id.clone(),
                                    error: None,
                                    observed_config_version: intent.config_version_id.clone(),
                                    updated_at_ms: now_ms(),
                                })
                                .await
                                .map_err(HubError::from)?;
                            changes += 1;
                        } else if matches!(intent.state.as_str(), "blocked" | "superseded") {
                            storage
                                .update_rollout_target(RolloutTargetUpdate {
                                    rollout_id: rollout.rollout_id.clone(),
                                    node_id: target.node_id.clone(),
                                    state: "failed".into(),
                                    attempt_id: target.attempt_id.clone(),
                                    error: intent.failure_class.clone(),
                                    observed_config_version: intent.config_version_id.clone(),
                                    updated_at_ms: now_ms(),
                                })
                                .await
                                .map_err(HubError::from)?;
                            batch_failed = true;
                            changes += 1;
                        }
                    }
                    "failed" => batch_failed = true,
                    _ => {}
                }
            }
            let refreshed = storage
                .list_rollout_targets(rollout.rollout_id.clone())
                .await
                .map_err(HubError::from)?;
            let current_batch = refreshed
                .iter()
                .filter(|target| target.ordinal >= batch_start && target.ordinal < batch_end)
                .collect::<Vec<_>>();
            if batch_failed {
                storage
                    .update_rollout(
                        &rollout.rollout_id,
                        "paused",
                        rollout.current_batch,
                        now_ms(),
                    )
                    .await
                    .map_err(HubError::from)?;
            } else if !current_batch.is_empty()
                && current_batch
                    .iter()
                    .all(|target| target.state == "succeeded")
            {
                let next_batch = rollout.current_batch + 1;
                let complete = next_batch * rollout.batch_size >= rollout.total_targets;
                storage
                    .update_rollout(
                        &rollout.rollout_id,
                        if complete { "converged" } else { "applying" },
                        next_batch,
                        now_ms(),
                    )
                    .await
                    .map_err(HubError::from)?;
                changes += 1;
            } else if rollout.state == "pending" {
                storage
                    .update_rollout(
                        &rollout.rollout_id,
                        "applying",
                        rollout.current_batch,
                        now_ms(),
                    )
                    .await
                    .map_err(HubError::from)?;
                changes += 1;
            }
        }
        Ok(changes)
    }
}
