//! Configuration approval requests: the gate in front of rollout creation
//! (see the `configuration-approval` capability).
//!
//! A request pins an immutable publish snapshot — configuration version,
//! target nodes, batch size — submitted by one principal and decided by
//! another. Approving drives `create_rollout` with the approver as actor;
//! the decision flows through the crash-safe `approving` state and adopts
//! (by correlation id) a rollout a crashed approval already created, so at
//! most one rollout ever exists per request.

use super::error::HubError;
use super::{now_ms, Hub, HUB_SEQUENCE};
use crate::storage::{AuditRecord, ConfigApprovalRequest, ConfigApprovalStatus};
use std::sync::atomic::Ordering;

/// Terminal requests are pruned after 30 days.
pub(crate) const APPROVAL_RETENTION_MS: u64 = 30 * 24 * 60 * 60 * 1000;
/// Terminal requests are pruned above this many retained rows.
pub(crate) const APPROVAL_RETAINED_MAX: usize = 100_000;

impl Hub {
    /// Submit an approval request pinning `(version, targets, batch)`.
    /// Validates the same invariants `create_rollout` would (without
    /// creating anything), so a request is always executable when approved.
    pub async fn submit_config_approval(
        &self,
        config_version_id: String,
        node_ids: Vec<String>,
        batch_size: u32,
        requester_id: String,
    ) -> Result<ConfigApprovalRequest, HubError> {
        if !self.config.approval_required {
            return Err(HubError::Invalid(
                "configuration approval is not enabled on this Hub; set \
                 control_api.approval_required to gate rollout creation"
                    .into(),
            ));
        }
        let batch_size = batch_size.clamp(1, 256);
        if node_ids.is_empty() || node_ids.iter().any(|id| id.trim().is_empty()) {
            return Err(HubError::Invalid("approval request requires nodes".into()));
        }
        let mut unique = std::collections::BTreeSet::new();
        if node_ids.iter().any(|id| !unique.insert(id.clone())) {
            return Err(HubError::Invalid(
                "approval request contains duplicate nodes".into(),
            ));
        }
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        if storage
            .get_config_version_content(config_version_id.clone())
            .await
            .map_err(HubError::from)?
            .is_none()
        {
            return Err(HubError::Invalid("configuration version not found".into()));
        }
        let registered = self.nodes().await;
        let known: std::collections::BTreeSet<String> =
            registered.into_iter().map(|node| node.id).collect();
        let unknown: Vec<String> = node_ids
            .iter()
            .filter(|id| !known.contains(*id))
            .cloned()
            .collect();
        if !unknown.is_empty() {
            return Err(HubError::Invalid(format!(
                "approval request targets unknown node(s): {}",
                unknown.join(", ")
            )));
        }
        let now = now_ms();
        let request = ConfigApprovalRequest {
            request_id: format!("approval-{}", HUB_SEQUENCE.fetch_add(1, Ordering::Relaxed)),
            config_version_id,
            targets: node_ids,
            batch_size,
            requester_id: requester_id.clone(),
            status: ConfigApprovalStatus::Pending,
            approver_id: None,
            decided_at_ms: None,
            rollout_id: None,
            // Unique even under sequence reuse across restarts: the
            // timestamp prefix separates restart epochs.
            correlation_id: format!("approval-{now}-{}", HUB_SEQUENCE.fetch_add(1, Ordering::Relaxed)),
            expires_at_ms: now.saturating_add(self.config.approval_ttl_ms),
            created_at_ms: now,
            updated_at_ms: now,
            failure_note: None,
        };
        storage
            .create_config_approval_request(request.clone())
            .await
            .map_err(HubError::from)?;
        self.record_approval_audit(
            "config_approval.submit",
            &request,
            &requester_id,
            None,
            "accepted",
        )
        .await?;
        Ok(request)
    }

    pub async fn config_approval(
        &self,
        request_id: &str,
    ) -> Result<Option<ConfigApprovalRequest>, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        let mut request = storage
            .get_config_approval_request(request_id)
            .await
            .map_err(HubError::from)?;
        // Lazy expiry: an overdue pending request reads as expired.
        if let Some(request) = request.as_mut() {
            if request.status == ConfigApprovalStatus::Pending
                && request.expires_at_ms <= now_ms()
            {
                request.status = ConfigApprovalStatus::Expired;
            }
        }
        Ok(request)
    }

    pub async fn config_approvals(
        &self,
        status: Option<&str>,
    ) -> Result<Vec<ConfigApprovalRequest>, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        let mut requests = storage
            .list_config_approval_requests(status)
            .await
            .map_err(HubError::from)?;
        let now = now_ms();
        for request in requests.iter_mut() {
            if request.status == ConfigApprovalStatus::Pending && request.expires_at_ms <= now {
                request.status = ConfigApprovalStatus::Expired;
            }
        }
        Ok(requests)
    }

    /// Decide a pending request. `approve` transitions through `approving`
    /// and adopts/creates the rollout (see the module docs); `reject` is
    /// terminal. The approver must differ from the requester.
    pub async fn decide_config_approval(
        &self,
        request_id: &str,
        approve: bool,
        approver_id: String,
        note: Option<String>,
    ) -> Result<ConfigApprovalRequest, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        let request = storage
            .get_config_approval_request(request_id)
            .await
            .map_err(HubError::from)?
            .ok_or_else(|| HubError::Invalid("approval request not found".into()))?;
        if request.requester_id == approver_id {
            return Err(HubError::Invalid(format!(
                "principal '{approver_id}' submitted this request and cannot decide it; \
                 approval requires a second principal"
            )));
        }
        if request.expires_at_ms <= now_ms() {
            // Persist the expiry so the state is durable, then report it.
            let _ = storage
                .begin_decide_config_approval_request(request_id, "expired", &approver_id, now_ms(), now_ms())
                .await;
            return Err(HubError::Invalid(
                "approval request has expired; submit a new request".into(),
            ));
        }
        // A retry of a crashed approval re-enters through the `approving`
        // state: the CAS below is skipped (the reservation already holds)
        // and the flow continues at the adoption step.
        let mut resumed_approving = false;
        match request.status {
            ConfigApprovalStatus::Pending => {}
            ConfigApprovalStatus::Approving if approve => {
                resumed_approving = true;
            }
            status => {
                return Err(HubError::Invalid(format!(
                    "approval request is {} and cannot be decided again",
                    status.as_str()
                )));
            }
        }
        let now = now_ms();
        if !resumed_approving {
            let target_status = if approve { "approving" } else { "rejected" };
            let reserved = storage
                .begin_decide_config_approval_request(request_id, target_status, &approver_id, now, now)
                .await
                .map_err(HubError::from)?;
            if !reserved {
                return Err(HubError::Invalid(
                    "approval request was decided concurrently".into(),
                ));
            }
        }
        if !approve {
            let decided = storage
                .get_config_approval_request(request_id)
                .await
                .map_err(HubError::from)?
                .ok_or_else(|| HubError::Invalid("approval request not found".into()))?;
            self.record_approval_audit(
                "config_approval.reject",
                &decided,
                &approver_id,
                note.as_deref(),
                "accepted",
            )
            .await?;
            return Ok(decided);
        }

        // Crash window: adopt a rollout this request's correlation already
        // produced before creating a new one.
        let adopted = storage
            .find_rollout_by_correlation(&request.correlation_id)
            .await
            .map_err(HubError::from)?;
        let rollout = match adopted {
            Some(existing) => existing,
            None => {
                match self
                    .create_rollout(
                        request.config_version_id.clone(),
                        request.targets.clone(),
                        request.batch_size,
                        Some(approver_id.clone()),
                        Some(request.correlation_id.clone()),
                    )
                    .await
                {
                    Ok(rollout) => rollout,
                    Err(error) => {
                        // Roll the reservation back: the request stays
                        // decidable, the failure reason is recorded.
                        let _ = storage
                            .reset_config_approval_request(
                                request_id,
                                &format!("rollout creation failed: {error}"),
                                now_ms(),
                            )
                            .await;
                        self.record_approval_audit(
                            "config_approval.approve",
                            &request,
                            &approver_id,
                            note.as_deref(),
                            "failed",
                        )
                        .await?;
                        return Err(error);
                    }
                }
            }
        };
        let completed = storage
            .complete_config_approval_request(request_id, &rollout.rollout_id, now_ms())
            .await
            .map_err(HubError::from)?;
        if !completed {
            return Err(HubError::Invalid(
                "approval request state changed during rollout creation".into(),
            ));
        }
        let decided = storage
            .get_config_approval_request(request_id)
            .await
            .map_err(HubError::from)?
            .ok_or_else(|| HubError::Invalid("approval request not found".into()))?;
        self.record_approval_audit(
            "config_approval.approve",
            &decided,
            &approver_id,
            note.as_deref(),
            "accepted",
        )
        .await?;
        Ok(decided)
    }

    /// Periodic maintenance (wired into the retention sweep): expire overdue
    /// pending requests, resolve stuck `approving` rows by the adoption
    /// rule, and prune terminal rows under the age/count bounds.
    pub async fn sweep_config_approvals(&self) -> Result<usize, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage
            .sweep_config_approval_requests(
                now_ms(),
                APPROVAL_RETENTION_MS,
                APPROVAL_RETAINED_MAX,
            )
            .await
            .map_err(HubError::from)
    }

    async fn record_approval_audit(
        &self,
        action: &str,
        request: &ConfigApprovalRequest,
        actor: &str,
        note: Option<&str>,
        outcome: &str,
    ) -> Result<(), HubError> {
        let Some(storage) = self.storage.as_ref() else {
            return Ok(());
        };
        // Scalar metadata only: no configuration bodies, no credentials.
        storage
            .record_audit(AuditRecord {
                event_id: 0,
                actor: Some(actor.to_owned()),
                action: action.to_owned(),
                resource_type: "config_approval".into(),
                resource_id: Some(request.request_id.clone()),
                node_id: None,
                stream_id: None,
                correlation_id: Some(request.correlation_id.clone()),
                outcome: outcome.to_owned(),
                failure_code: None,
                message: Some(format!(
                    "version={} targets={} batch_size={}{}",
                    request.config_version_id,
                    request.targets.join(","),
                    request.batch_size,
                    note.map(|note| format!(" note={note}")).unwrap_or_default()
                )),
                occurred_at_ms: now_ms(),
            })
            .await
            .map_err(HubError::from)?;
        Ok(())
    }
}

impl Hub {
    /// The API-layer gate for forward rollout creation. `Err` carries the
    /// operator-facing message (pointing at the approval flow) when the
    /// approval policy blocks direct creation.
    pub fn ensure_rollout_creation_allowed(&self) -> Result<(), String> {
        if !self.config.approval_required {
            return Ok(());
        }
        Err("configuration approval is required on this Hub; submit              POST /config-approvals pinning this version and targets, then have              a second principal approve it".to_string())
    }

    /// Whether `config_version_id` was ever applied to `node_ids` by some
    /// rollout (its targets carry an observed or terminal application of
    /// that version). Rollback under the approval policy may only target
    /// such versions: a never-applied version is new content riding the
    /// rollback path around the gate. Fail-closed when history has been
    /// pruned beyond the record.
    pub async fn version_was_applied_to(
        &self,
        config_version_id: &str,
        node_ids: &[String],
    ) -> Result<bool, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        let rollouts = storage.list_rollouts().await.map_err(HubError::from)?;
        for rollout in &rollouts {
            if rollout.config_version_id != config_version_id {
                continue;
            }
            let targets = storage
                .list_rollout_targets(&rollout.rollout_id)
                .await
                .map_err(HubError::from)?;
            if targets.iter().any(|target| {
                node_ids.contains(&target.node_id)
                    && (target.observed_config_version.as_deref()
                        == Some(config_version_id)
                        || matches!(
                            target.state.as_str(),
                            "applied" | "converged" | "succeeded"
                        ))
            }) {
                return Ok(true);
            }
        }
        Ok(false)
    }
}


