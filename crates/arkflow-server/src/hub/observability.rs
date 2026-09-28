//! Read-side views: streams, events, audit, metrics, pruning.

use super::*;

impl Hub {
    /// Prometheus accounting for command dispatch. Counters reset on Hub
    /// restart in line with counter semantics.
    pub fn command_metrics(&self) -> &CommandMetrics {
        &self.command_metrics
    }

    /// Reclaim audit history past the retention window and count bound so
    /// the trail stays bounded without losing recent records.
    pub async fn prune_audit_history(&self) -> Result<(), HubError> {
        const RETENTION_MS: i64 = 30 * 24 * 60 * 60 * 1000;
        const RETENTION_MAX: i64 = 100_000;
        if let Some(storage) = &self.storage {
            storage
                .prune_audit_events(now_ms() as i64 - RETENTION_MS, RETENTION_MAX)
                .await
                .map_err(HubError::from)?;
        }
        Ok(())
    }

    /// Bounded retention for the operation history. The reconciler's
    /// terminal-state memory reads Succeeded/terminal rows, so records are
    /// kept for a grace window and a count bound instead of living forever:
    /// without this, every reconcile tick of a stopped Job (or a fenced
    /// placement) appended a fresh persistent row with no reclaim. Pruning a
    /// terminal record past the window can cause one idempotent
    /// re-dispatch, which is bounded and safe.
    pub async fn prune_operation_history(&self) -> Result<(), HubError> {
        const RETENTION_MS: i64 = 24 * 60 * 60 * 1000;
        const RETENTION_MAX: i64 = 4096;
        let cutoff = now_ms() as i64 - RETENTION_MS;
        if let Some(storage) = &self.storage {
            storage
                .prune_operation_history(cutoff, RETENTION_MAX)
                .await
                .map_err(HubError::from)?;
        }
        let mut operations = self.operations.write().await;
        let terminal = |state: &HubOperationState| {
            matches!(
                state,
                HubOperationState::Succeeded
                    | HubOperationState::Failed
                    | HubOperationState::TimedOut
                    | HubOperationState::NodeUnavailable
                    | HubOperationState::Cancelled
                    | HubOperationState::Superseded
            )
        };
        let mut protected_starts = BTreeMap::<String, (u64, u64, String)>::new();
        for (id, record) in operations.iter() {
            if !is_durable_job_start(record) {
                continue;
            }
            let candidate = (record.generation, record.created_at_ms, id.clone());
            let replace = protected_starts
                .get(&record.resource_id)
                .is_none_or(|current| candidate > *current);
            if replace {
                protected_starts.insert(record.resource_id.clone(), candidate);
            }
        }
        let protected_ids = protected_starts
            .into_values()
            .map(|(_, _, id)| id)
            .collect::<BTreeSet<_>>();
        let stale: Vec<String> = operations
            .iter()
            .filter(|(_, record)| {
                terminal(&record.state)
                    && !protected_ids.contains(&record.id)
                    && ((record.finished_at_ms.unwrap_or(record.created_at_ms)) as i64) < cutoff
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in stale {
            operations.remove(&id);
        }
        let mut terminal_ids: Vec<(i64, String)> = operations
            .iter()
            .filter(|(_, record)| terminal(&record.state) && !protected_ids.contains(&record.id))
            .map(|(id, record)| {
                (
                    (record.finished_at_ms.unwrap_or(record.created_at_ms)) as i64,
                    id.clone(),
                )
            })
            .collect();
        terminal_ids.sort();
        let excess = terminal_ids.len().saturating_sub(RETENTION_MAX as usize);
        for (_, id) in terminal_ids.into_iter().take(excess) {
            operations.remove(&id);
        }
        Ok(())
    }

    /// Reclaim processed reconciliation outbox rows so the durable outbox
    /// stays bounded under steady reconcile churn. Unprocessed rows — the
    /// outstanding work queue — are never reclaimed, and the
    /// `outbox_pending`/`outbox_claimed` status counters are unaffected.
    pub async fn prune_outbox_history(&self) -> Result<(), HubError> {
        const RETENTION_MS: i64 = 24 * 60 * 60 * 1000;
        const RETENTION_MAX: i64 = 4096;
        if let Some(storage) = &self.storage {
            storage
                .prune_processed_outbox(now_ms() as i64 - RETENTION_MS, RETENTION_MAX)
                .await
                .map_err(HubError::from)?;
        }
        Ok(())
    }

    /// Reclaim terminal Attempt records so the durable attempt store stays
    /// bounded under steady dispatch churn. Active attempts are never
    /// reclaimed.
    pub async fn prune_attempt_history(&self) -> Result<(), HubError> {
        const RETENTION_MS: i64 = 24 * 60 * 60 * 1000;
        const RETENTION_MAX: i64 = 4096;
        if let Some(storage) = &self.storage {
            storage
                .prune_terminal_attempts(now_ms() as i64 - RETENTION_MS, RETENTION_MAX)
                .await
                .map_err(HubError::from)?;
        }
        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<HubEvent> {
        self.updates.subscribe()
    }

    pub async fn streams(&self, node_id: Option<&str>) -> Vec<(String, StreamStatus)> {
        self.nodes
            .read()
            .await
            .values()
            .filter(|node| node_id.is_none_or(|id| node.resource.id == id))
            .flat_map(|node| {
                node.streams
                    .iter()
                    .cloned()
                    .map(|stream| (node.resource.id.clone(), stream))
            })
            .collect()
    }

    pub async fn stream_resource(
        &self,
        node_id: &str,
        stream_id: &str,
    ) -> Result<Option<serde_json::Value>, HubError> {
        let observed = self
            .nodes
            .read()
            .await
            .get(node_id)
            .and_then(|node| node.streams.iter().find(|stream| stream.id == stream_id))
            .cloned();
        let desired = if let Some(storage) = self.storage.as_ref() {
            storage
                .get_desired(node_id, stream_id)
                .await
                .map_err(HubError::from)?
        } else {
            None
        };
        if observed.is_none() && desired.is_none() {
            return Ok(None);
        }
        let mut resource = observed
            .map(|stream| serde_json::to_value(stream).unwrap_or_default())
            .unwrap_or_else(|| {
                serde_json::json!({
                    "id": stream_id,
                    "state": "unknown",
                    "convergence": "unknown"
                })
            });
        if let Some(object) = resource.as_object_mut() {
            object.insert("node_id".into(), serde_json::Value::String(node_id.into()));
            if let Some(desired) = desired {
                object.insert(
                    "desired".into(),
                    serde_json::json!({
                        "state": desired.desired_state,
                        "generation": desired.generation,
                        "config_version": desired.config_version_id,
                        "action_id": desired.action_id
                    }),
                );
                object.insert(
                    "generation".into(),
                    serde_json::Value::Number(desired.generation.into()),
                );
            }
        }
        Ok(Some(resource))
    }
    pub async fn events(&self, node_id: Option<&str>) -> Vec<HubEvent> {
        let mut events = self
            .events
            .read()
            .await
            .iter()
            .filter(|event| node_id.is_none_or(|id| event.node_id == id))
            .cloned()
            .collect::<Vec<_>>();
        if let Some(storage) = self.storage.as_ref() {
            if let Ok(stored) = storage.list_events(node_id.map(str::to_owned)).await {
                events.extend(stored.into_iter().filter_map(|event| {
                    let node_id = event.node_id?;
                    Some(HubEvent {
                        event_id: Some(event.event_id),
                        node_id,
                        event: ControlEvent {
                            occurred_at_ms: event.occurred_at_ms,
                            event_type: event.event_type,
                            stream_id: event.stream_id,
                            outcome: event.outcome,
                            message: event.message.map(|message| bounded_text(&message, 512)),
                            operation_id: event.intent_id.or(event.attempt_id),
                            correlation_id: event.correlation_id,
                            actor: event.actor,
                        },
                    })
                }));
            }
        }
        events.sort_by_key(|event| std::cmp::Reverse(event.event.occurred_at_ms));
        events.truncate(MAX_EVENTS);
        events
    }

    pub async fn audit(
        &self,
        resource_id: Option<&str>,
    ) -> Result<Vec<crate::storage::AuditRecord>, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage
            .list_audit(resource_id.map(str::to_owned))
            .await
            .map_err(HubError::from)
    }

    pub async fn record_audit_event(
        &self,
        record: crate::storage::AuditRecord,
    ) -> Result<i64, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage.record_audit(record).await.map_err(HubError::from)
    }

    /// Audit action name for a dispatched operation: `job_start` becomes
    /// `job.start`, mirroring the dotted style of the existing audit
    /// vocabulary. Non-Job operations have no Job audit action.
    pub(crate) fn job_audit_action(operation: &str) -> Option<String> {
        operation
            .strip_prefix("job_")
            .map(|verb| format!("job.{verb}"))
    }

    /// Record the acceptance or rejection of a Job lifecycle operation.
    /// The message carries scalar operation metadata only — never the Job
    /// spec or configuration body (`control-plane-identity` MUST NOT).
    /// Best effort: audit failures never fail the mutation itself.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn record_job_operation_audit(
        &self,
        operation: &str,
        resource_id: &str,
        node_id: Option<&str>,
        correlation_id: Option<&str>,
        outcome: &str,
        failure_code: Option<&str>,
        message: String,
    ) {
        let Some(action) = Self::job_audit_action(operation) else {
            return;
        };
        let record = crate::storage::AuditRecord {
            event_id: 0,
            actor: Some("operator".into()),
            action,
            resource_type: "job".into(),
            resource_id: Some(resource_id.to_owned()),
            node_id: node_id.map(str::to_owned),
            stream_id: None,
            correlation_id: correlation_id.map(str::to_owned),
            outcome: outcome.to_owned(),
            failure_code: failure_code.map(str::to_owned),
            message: Some(message),
            occurred_at_ms: now_ms(),
        };
        let _ = self.record_audit_event(record).await;
    }

    pub async fn prune_events(&self, retain: usize) -> Result<usize, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage.prune_events(retain).await.map_err(HubError::from)
    }

    pub async fn metrics(&self, node_id: Option<&str>) -> BTreeMap<String, f64> {
        let nodes = self.nodes.read().await;
        let mut aggregate = BTreeMap::new();
        for node in nodes
            .values()
            .filter(|node| node_id.is_none_or(|id| node.resource.id == id))
        {
            for (key, value) in &node.metrics {
                *aggregate.entry(key.clone()).or_insert(0.0) += value;
            }
        }
        aggregate
    }

    /// Per-node, per-Job kernel metric snapshots for the data-plane Prometheus
    /// export. Only Agents with an unexpired lease are included, so an Agent
    /// that stops reporting (expired lease or deregistration) stops being
    /// exported.
    pub async fn job_metrics(
        &self,
    ) -> Vec<(
        String,
        BTreeMap<String, arkflow_core::executor::metrics::KernelMetricsSnapshot>,
    )> {
        let now = now_ms();
        self.nodes
            .read()
            .await
            .values()
            .filter(|node| node.resource.lease_expires_at_ms > now)
            .filter(|node| !node.jobs.is_empty())
            .map(|node| (node.resource.id.clone(), node.jobs.clone()))
            .collect()
    }

    pub async fn metrics_by_node(&self, node_id: Option<&str>) -> Vec<HubNodeMetrics> {
        self.nodes
            .read()
            .await
            .values()
            .filter(|node| node_id.is_none_or(|id| node.resource.id == id))
            .map(|node| HubNodeMetrics {
                node_id: node.resource.id.clone(),
                metrics: node.metrics.clone(),
            })
            .collect()
    }
}
