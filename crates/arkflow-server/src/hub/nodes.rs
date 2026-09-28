//! Node registry and the Agent pull protocol: register, heartbeat, report, commands.

use super::*;

const ALLOWED_NODE_METRICS: &[&str] = &[
    "input_batches",
    "input_messages",
    "processing_errors",
    "output_batches",
    "output_messages",
    "input_errors",
    "input_reconnects",
    "output_errors",
    "restarts",
    "streams_total",
    "streams_running",
    "kernel_batches_in",
    "kernel_batches_out",
    "kernel_rows",
    "kernel_errors",
    "in_flight",
    "mean_latency_us",
    "checkpoint_duration_ms",
    "checkpoint_failures",
    "watermark_lag_ms",
    "late_events",
    "jobs_total",
    "jobs_running",
    "jobs_ephemeral_state",
    "jobs_recovery_required",
    // Host resource gauges sampled by the Agent (see agent.rs
    // ResourceSampler): ephemeral registry state, exported as-is.
    "node_cpu_usage_percent",
    "node_memory_used_bytes",
    "node_memory_total_bytes",
    "node_memory_available_bytes",
];

fn authenticated_node<'a>(
    nodes: &'a mut BTreeMap<String, NodeRecord>,
    auth: &AgentAuth,
) -> Result<&'a mut NodeRecord, HubError> {
    let node = nodes.get_mut(&auth.node_id).ok_or(HubError::Unauthorized)?;
    if !bool::from(
        auth.session_token
            .as_bytes()
            .ct_eq(node.session_token.as_bytes()),
    ) || now_ms() > node.session_expires_at_ms
    {
        return Err(HubError::Unauthorized);
    }
    Ok(node)
}

pub(crate) fn sanitize_metrics(metrics: BTreeMap<String, f64>) -> BTreeMap<String, f64> {
    metrics
        .into_iter()
        .filter(|(key, value)| {
            ALLOWED_NODE_METRICS.contains(&key.as_str()) && value.is_finite() && *value >= 0.0
        })
        .collect()
}

/// Upper bound on per-Job snapshots kept for one node. Jobs are
/// operator-configured so this is generous; a misbehaving Agent cannot grow
/// Hub memory without bound.
const MAX_REPORTED_JOBS_PER_NODE: usize = 256;

/// Keep the bounded set of reported Job snapshots, discarding finite values
/// only: series cardinality stays at O(jobs x chains) per node.
fn bounded_job_snapshots(
    jobs: BTreeMap<String, arkflow_core::executor::metrics::KernelMetricsSnapshot>,
) -> BTreeMap<String, arkflow_core::executor::metrics::KernelMetricsSnapshot> {
    jobs.into_iter().take(MAX_REPORTED_JOBS_PER_NODE).collect()
}

pub(crate) fn sanitize_capabilities(capabilities: Vec<String>) -> Vec<String> {
    capabilities
        .into_iter()
        .filter(|capability| {
            !capability.is_empty()
                && capability.len() <= 64
                && capability
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || "._-".contains(character))
        })
        .take(32)
        .collect()
}

pub(crate) fn bounded_text(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

pub(crate) fn parse_operator_credential(
    configured: &str,
) -> (&str, OperatorRole, &str, Vec<ResourceScope>) {
    let mut fields = configured.splitn(4, '|');
    let Some(id) = fields.next() else {
        return ("operator", OperatorRole::Admin, configured, Vec::new());
    };
    let Some(role) = fields.next() else {
        return ("operator", OperatorRole::Admin, configured, Vec::new());
    };
    let Some(secret) = fields.next() else {
        return ("operator", OperatorRole::Admin, configured, Vec::new());
    };
    let role = match role {
        "admin" => OperatorRole::Admin,
        "operator" => OperatorRole::Operator,
        "viewer" => OperatorRole::Viewer,
        _ => return ("operator", OperatorRole::Admin, configured, Vec::new()),
    };
    if id.trim().is_empty() || secret.is_empty() {
        ("operator", OperatorRole::Admin, configured, Vec::new())
    } else {
        let scopes = fields
            .next()
            .into_iter()
            .flat_map(|value| value.split(','))
            .filter_map(parse_resource_scope)
            .collect();
        (id, role, secret, scopes)
    }
}

fn parse_resource_scope(value: &str) -> Option<ResourceScope> {
    let (resource_type, resource_id) = value.split_once('=')?;
    if resource_type.trim().is_empty() {
        return None;
    }
    Some(ResourceScope {
        resource_type: resource_type.to_owned(),
        resource_id: (!resource_id.is_empty()).then(|| resource_id.to_owned()),
    })
}

pub(crate) fn required_capabilities(operation: &str) -> Vec<String> {
    match operation {
        "start" | "stop" | "restart" => vec!["stream_lifecycle".into()],
        "job_start"
        | "job_stop"
        | "job_restart"
        | "job_checkpoint"
        | "job_savepoint"
        | "job_checkpoint_commit"
        | "job_savepoint_commit" => {
            vec!["job_runtime".into(), "state_backend".into()]
        }
        "apply_configuration" | "rollback_configuration" => vec!["configuration".into()],
        _ => Vec::new(),
    }
}

impl Hub {
    pub async fn register(&self, request: RegisterRequest) -> Result<RegisterResponse, HubError> {
        let Some(expected) = self.config.node_token.as_deref() else {
            if !self.config.insecure_local {
                return Err(HubError::Unauthorized);
            }
            // Explicit loopback development mode may omit the node token.
            // The standalone server refuses to expose this mode externally.
            let _ = &request.node_token;
            return self.register_after_auth(request).await;
        };
        if expected.trim().is_empty() {
            return Err(HubError::Unauthorized);
        }
        if !bool::from(request.node_token.as_bytes().ct_eq(expected.as_bytes())) {
            return Err(HubError::Unauthorized);
        }
        self.register_after_auth(request).await
    }

    async fn register_after_auth(
        &self,
        request: RegisterRequest,
    ) -> Result<RegisterResponse, HubError> {
        if request.node_id.trim().is_empty() {
            return Err(HubError::Invalid("node_id must not be empty".into()));
        }
        if request.protocol_version != SUPPORTED_PROTOCOL_VERSION {
            let message = format!("unsupported protocol version: {}", request.protocol_version);
            let _ = self
                .record_audit_event(crate::storage::AuditRecord {
                    event_id: 0,
                    actor: None,
                    action: "agent.register".into(),
                    resource_type: "node".into(),
                    resource_id: Some(request.node_id.clone()),
                    node_id: Some(request.node_id.clone()),
                    stream_id: None,
                    correlation_id: None,
                    outcome: "rejected".into(),
                    failure_code: Some("incompatible_protocol".into()),
                    message: Some(message.clone()),
                    occurred_at_ms: now_ms(),
                })
                .await;
            return Err(HubError::Invalid(message));
        }
        let now = now_ms();
        // Session tokens authenticate every agent request after registration,
        // so they MUST come from a CSPRNG: a sequential counter would be
        // enumerable by anyone who can reach the Hub and defeat the
        // constant-time comparisons downstream.
        let session_token: String = {
            use rand::TryRngCore;
            let mut bytes = [0u8; 32];
            rand::rngs::OsRng
                .try_fill_bytes(&mut bytes)
                .expect("OS RNG cannot fail");
            bytes.iter().map(|byte| format!("{byte:02x}")).collect()
        };
        // Older clients do not send a process identity. Keep them compatible
        // by treating the fresh session token as their boot identity; the
        // built-in Agent sends its stable `NodeAgentConfig::boot_id`.
        let registered_boot_id = request
            .boot_id
            .clone()
            .filter(|boot_id| !boot_id.trim().is_empty())
            .unwrap_or_else(|| session_token.clone());
        let resource = HubNode {
            id: request.node_id.clone(),
            protocol_version: request.protocol_version.clone(),
            version: "unknown".into(),
            state: NodeConnectionState::Online,
            capabilities: sanitize_capabilities(request.capabilities.clone()),
            last_seen_at_ms: now,
            lease_expires_at_ms: now + self.config.lease_ttl_ms,
            streams_total: 0,
            streams_running: 0,
            streams_failed: 0,
            maintenance_state: NodeMaintenanceState::Active,
            data_address: request
                .data_address
                .clone()
                .filter(|address| !address.trim().is_empty()),
        };
        let mut nodes = self.nodes.write().await;
        if nodes.len() >= MAX_NODES && !nodes.contains_key(&request.node_id) {
            return Err(HubError::Capacity);
        }
        let old = nodes.remove(&request.node_id);
        let boot_changed = old
            .as_ref()
            .is_some_and(|record| record.boot_id.as_deref() != Some(registered_boot_id.as_str()));
        nodes.insert(
            request.node_id.clone(),
            NodeRecord {
                resource,
                session_token: session_token.clone(),
                session_expires_at_ms: now.saturating_add(self.config.session_ttl_ms),
                boot_id: Some(registered_boot_id.clone()),
                report_seq: 0,
                last_report_at_ms: 0,
                // Pressure history is deliberately not carried across
                // registrations: a reconnecting node re-earns its streak
                // within a few report intervals (bounded rebalance delay).
                pressure_streak: 0,
                // Commands queued for an old process belong to a runtime that
                // no longer exists. Reconciliation below will enqueue the
                // desired state for the new boot.
                commands: if boot_changed {
                    VecDeque::new()
                } else {
                    old.as_ref()
                        .map(|record| record.commands.clone())
                        .unwrap_or_default()
                },
                leased_commands: if boot_changed {
                    BTreeMap::new()
                } else {
                    old.as_ref()
                        .map(|record| record.leased_commands.clone())
                        .unwrap_or_default()
                },
                streams: old
                    .as_ref()
                    .map(|record| record.streams.clone())
                    .unwrap_or_default(),
                operations: old
                    .as_ref()
                    .map(|record| record.operations.clone())
                    .unwrap_or_default(),
                events: old
                    .as_ref()
                    .map(|record| record.events.clone())
                    .unwrap_or_default(),
                configuration: old.as_ref().and_then(|record| record.configuration.clone()),
                // A boot change invalidates the previous process's local Jobs
                // (their start operations are marked unavailable below), so
                // their metric snapshots must not survive the re-registration.
                jobs: if boot_changed {
                    BTreeMap::new()
                } else {
                    old.as_ref()
                        .map(|record| record.jobs.clone())
                        .unwrap_or_default()
                },
                metrics: old.map(|record| record.metrics).unwrap_or_default(),
            },
        );
        drop(nodes);
        let invalidated_job_starts = self
            .invalidate_job_starts_on_boot_change(&request.node_id, boot_changed, now)
            .await;
        if let Some(storage) = self.storage.as_ref() {
            storage
                .upsert_node(NodeMutation {
                    node_id: request.node_id.clone(),
                    version: "unknown".into(),
                    state: "online".into(),
                    capabilities_json: serde_json::to_string(&sanitize_capabilities(
                        request.capabilities,
                    ))
                    .unwrap_or_else(|_| "[]".into()),
                    boot_id: Some(registered_boot_id.clone()),
                    report_seq: Some(0),
                    last_seen_at_ms: now,
                    lease_expires_at_ms: now + self.config.lease_ttl_ms,
                    maintenance_state: None,
                    maintenance_updated_at_ms: None,
                })
                .await
                .map_err(HubError::from)?;
            // The Agent restarts report_seq from 1 on every session rebuild,
            // so the previous session's stored per-stream cursors would
            // silently drop every new observation. Reset them together with
            // the in-memory cursor above.
            storage
                .reset_observed_cursors(request.node_id.clone())
                .await
                .map_err(HubError::from)?;
            for operation in &invalidated_job_starts {
                persist_operation(storage, operation)
                    .await
                    .map_err(HubError::from)?;
            }
            storage
                .wake_node(&request.node_id, now)
                .await
                .map_err(HubError::from)?;
            if let Some(state) = storage
                .get_node_maintenance(&request.node_id)
                .await
                .map_err(HubError::from)?
            {
                let maintenance_state = match state.as_str() {
                    "draining" => NodeMaintenanceState::Draining,
                    "maintenance" => NodeMaintenanceState::Maintenance,
                    _ => NodeMaintenanceState::Active,
                };
                if let Some(node) = self.nodes.write().await.get_mut(&request.node_id) {
                    node.resource.maintenance_state = maintenance_state;
                }
            }
        }
        for job in self.jobs().await? {
            if job.desired_state != "stopped"
                && (job.node_ids.is_empty() || job.node_ids.iter().any(|id| id == &request.node_id))
            {
                self.reconcile_job(&job).await?;
            }
        }
        Ok(RegisterResponse {
            node_id: request.node_id,
            session_token,
            session_ttl_ms: self.config.session_ttl_ms,
            lease_ttl_ms: self.config.lease_ttl_ms,
            poll_interval_ms: self.config.poll_interval_ms,
            protocol_version: default_protocol_version(),
        })
    }

    pub async fn heartbeat(&self, request: HeartbeatRequest) -> Result<(), HubError> {
        if let Some(protocol_version) = request.protocol_version.as_deref() {
            if protocol_version != SUPPORTED_PROTOCOL_VERSION {
                return Err(HubError::Invalid(format!(
                    "unsupported protocol version: {protocol_version}"
                )));
            }
        }
        let mut nodes = self.nodes.write().await;
        let node = authenticated_node(&mut nodes, &request.auth)?;
        let now = now_ms();
        node.resource.last_seen_at_ms = now;
        node.resource.lease_expires_at_ms = now + self.config.lease_ttl_ms;
        node.resource.state = match request.state.as_str() {
            "draining" => NodeConnectionState::Draining,
            _ => NodeConnectionState::Online,
        };
        if let Some(version) = request.software_version {
            node.resource.version = version;
        }
        if !request.capabilities.is_empty() {
            node.resource.capabilities = sanitize_capabilities(request.capabilities);
        }
        Ok(())
    }

    /// A fresh Agent process starts with an empty local JobRuntime. Mark every
    /// previous start attempt for this node unavailable before reconciliation,
    /// otherwise a persisted successful operation would suppress the new start
    /// command even though no local Job exists. Shared by registration and
    /// report so the invalidation semantics cannot drift between them.
    async fn invalidate_job_starts_on_boot_change(
        &self,
        node_id: &str,
        boot_changed: bool,
        now: u64,
    ) -> Vec<HubOperation> {
        if !boot_changed {
            return Vec::new();
        }
        let mut operations = self.operations.write().await;
        operations
            .values_mut()
            .filter(|operation| {
                operation.node_id == node_id
                    && operation.operation == "job_start"
                    && !matches!(
                        operation.state,
                        HubOperationState::Failed
                            | HubOperationState::TimedOut
                            | HubOperationState::NodeUnavailable
                            | HubOperationState::Cancelled
                            | HubOperationState::Superseded
                    )
            })
            .map(|operation| {
                let was_succeeded = operation.state == HubOperationState::Succeeded;
                operation.state = HubOperationState::NodeUnavailable;
                operation.finished_at_ms = Some(now);
                if was_succeeded {
                    operation.failure_class = Some("recovery_required".into());
                    operation.error = Some(
                        "previous successful Job start invalidated by a new Agent process boot"
                            .into(),
                    );
                } else {
                    operation.error =
                        Some("in-flight Job start invalidated by a new Agent process boot".into());
                }
                operation.clone()
            })
            .collect()
    }

    pub async fn report(&self, report: NodeReport) -> Result<(), HubError> {
        let reported_streams = report.streams.clone();
        let reported_configuration = report.configuration.clone();
        let mut nodes = self.nodes.write().await;
        let node = authenticated_node(&mut nodes, &report.auth)?;
        let boot_changed = report
            .boot_id
            .as_deref()
            .is_some_and(|boot_id| node.boot_id.as_deref() != Some(boot_id));
        if let Some(boot_id) = report.boot_id.as_deref() {
            match node.boot_id.as_deref() {
                Some(current) if current == boot_id => {
                    // Same session: the sequence cursor rejects replays.
                    if report.report_seq <= node.report_seq {
                        return Ok(());
                    }
                    node.report_seq = report.report_seq;
                }
                Some(_) => {
                    // A delayed report from an older session (the node has
                    // re-registered since): acknowledge without regressing
                    // the new session's observed state.
                    return Ok(());
                }
                None => {
                    node.boot_id = Some(boot_id.to_owned());
                    node.report_seq = report.report_seq;
                }
            }
        }
        let now = now_ms();
        node.resource.last_seen_at_ms = now;
        node.resource.lease_expires_at_ms = now + self.config.lease_ttl_ms;
        node.resource.state = if report.state == "draining" {
            NodeConnectionState::Draining
        } else {
            NodeConnectionState::Online
        };
        node.resource.version = report.version;
        node.resource.capabilities = sanitize_capabilities(report.capabilities);
        node.resource.streams_total = report.streams.len();
        node.resource.streams_running = report
            .streams
            .iter()
            .filter(|stream| stream.state == arkflow_core::control::StreamState::Running)
            .count();
        node.resource.streams_failed = report
            .streams
            .iter()
            .filter(|stream| stream.state == arkflow_core::control::StreamState::Failed)
            .count();
        node.streams = report.streams;
        node.operations = report.operations;
        node.events = report.events.clone();
        node.metrics = sanitize_metrics(report.metrics);
        node.last_report_at_ms = now;
        node.pressure_streak = if node_under_pressure(&node.metrics) {
            node.pressure_streak.saturating_add(1)
        } else {
            0
        };
        node.jobs = bounded_job_snapshots(report.jobs);
        node.configuration = report.configuration;
        let persisted_version = node.resource.version.clone();
        let persisted_state = format!("{:?}", node.resource.state).to_lowercase();
        let persisted_capabilities =
            serde_json::to_string(&node.resource.capabilities).unwrap_or_else(|_| "[]".into());
        let persisted_boot_id = node.boot_id.clone();
        let persisted_report_seq = Some(node.report_seq);
        let persisted_lease = node.resource.lease_expires_at_ms;
        drop(nodes);
        let invalidated_job_starts = self
            .invalidate_job_starts_on_boot_change(&report.auth.node_id, boot_changed, now)
            .await;
        if let Some(storage) = self.storage.as_ref() {
            for operation in &invalidated_job_starts {
                persist_operation(storage, operation)
                    .await
                    .map_err(HubError::from)?;
            }
            storage
                .upsert_node(NodeMutation {
                    node_id: report.auth.node_id.clone(),
                    version: persisted_version,
                    state: persisted_state,
                    capabilities_json: persisted_capabilities,
                    boot_id: persisted_boot_id,
                    report_seq: persisted_report_seq,
                    last_seen_at_ms: now,
                    lease_expires_at_ms: persisted_lease,
                    maintenance_state: None,
                    maintenance_updated_at_ms: None,
                })
                .await
                .map_err(HubError::from)?;
        }
        let mut events = self.events.write().await;
        for mut event in report.events {
            if events.len() >= MAX_EVENTS {
                events.pop_front();
            }
            event.message = event.message.map(|message| bounded_text(&message, 512));
            events.push_back(HubEvent {
                event_id: None,
                node_id: report.auth.node_id.clone(),
                event,
            });
            if let Some(event) = events.back().cloned() {
                let _ = self.updates.send(event);
            }
        }
        if let Some(storage) = self.storage.as_ref() {
            for stream in &reported_streams {
                let observed_state = serde_json::to_value(stream.state)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .unwrap_or_else(|| "unknown".into());
                let last_error_code = stream.last_error.as_ref().map(|error| error.stage.clone());
                let last_error_message = stream
                    .last_error
                    .as_ref()
                    .map(|error| error.message.clone());
                storage
                    .record_observed(ObservedMutation {
                        node_id: report.auth.node_id.clone(),
                        stream_id: stream.id.clone(),
                        boot_id: report.boot_id.clone(),
                        report_seq: report.report_seq,
                        observed_generation: stream.observed_generation,
                        observed_state,
                        config_version_id: stream.observed_config_version.clone(),
                        action_id: stream.last_completed_action_id.clone(),
                        snapshot_json: serde_json::to_string(&stream)
                            .unwrap_or_else(|_| "{}".into()),
                        last_error_code,
                        last_error_message,
                    })
                    .await
                    .map_err(HubError::from)?;
            }
            if let Some(config_target) = storage
                .get_desired(&report.auth.node_id, "__configuration__")
                .await
                .map_err(HubError::from)?
            {
                let observed_version = report.configuration_version.clone().or_else(|| {
                    reported_streams
                        .iter()
                        .find_map(|stream| stream.observed_config_version.clone())
                });
                if observed_version.is_some() {
                    storage
                        .record_observed(ObservedMutation {
                            node_id: report.auth.node_id.clone(),
                            stream_id: "__configuration__".into(),
                            boot_id: report.boot_id.clone(),
                            report_seq: report.report_seq,
                            observed_generation: Some(config_target.generation),
                            observed_state: "configured".into(),
                            config_version_id: observed_version,
                            action_id: None,
                            snapshot_json: serde_json::to_string(&reported_configuration)
                                .unwrap_or_else(|_| "null".into()),
                            last_error_code: None,
                            last_error_message: None,
                        })
                        .await
                        .map_err(HubError::from)?;
                }
            }
        }
        if boot_changed {
            for job in self.jobs().await? {
                if job.desired_state != "stopped"
                    && (job.node_ids.is_empty()
                        || job.node_ids.iter().any(|id| id == &report.auth.node_id))
                {
                    self.reconcile_job(&job).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn commands(&self, auth: AgentAuth) -> Result<Vec<AgentCommand>, HubError> {
        let mut nodes = self.nodes.write().await;
        let node = authenticated_node(&mut nodes, &auth)?;
        let now = now_ms();
        let mut commands = Vec::new();
        let mut expired = Vec::new();
        while let Some(command) = node.commands.pop_front() {
            if command.expires_at_ms > now {
                node.leased_commands
                    .insert(command.id.clone(), command.clone());
                commands.push(command);
            } else {
                expired.push(command);
            }
        }
        let expired_leases = node
            .leased_commands
            .iter()
            .filter(|(_, command)| command.expires_at_ms <= now)
            .map(|(command_id, _)| command_id.clone())
            .collect::<Vec<_>>();
        for command_id in expired_leases {
            if let Some(command) = node.leased_commands.remove(&command_id) {
                expired.push(command);
            }
        }
        drop(nodes);
        // A queued command is leased even before an Agent receives it. Do not
        // silently discard an expired lease while leaving its operation in an
        // active deduplication state: mark it retryable/terminal so the normal
        // Job reconciler can enqueue a fresh command.  Non-Job commands do
        // not have a desired-state reconciler, so retain enough of the
        // expired command to enqueue a replacement below.
        let mut expired_operations = Vec::new();
        let mut expired_job_ids = BTreeSet::new();
        let mut expired_retries = Vec::new();
        if !expired.is_empty() {
            let mut operations = self.operations.write().await;
            for command in &expired {
                if let Some(operation) = operations.get_mut(&command.operation_id) {
                    if matches!(
                        operation.state,
                        HubOperationState::Queued
                            | HubOperationState::Dispatched
                            | HubOperationState::Acknowledged
                            | HubOperationState::Running
                    ) {
                        operation.state = HubOperationState::TimedOut;
                        operation.finished_at_ms = Some(now);
                        operation.next_retry_at_ms = Some(now);
                        operation.error = Some("command lease expired before execution".into());
                        if matches!(operation.operation.as_str(), "job_start" | "job_stop") {
                            expired_job_ids.insert(operation.resource_id.clone());
                        } else {
                            expired_retries.push(command.clone());
                        }
                        expired_operations.push(operation.clone());
                    }
                }
            }
        }
        if !commands.is_empty() {
            let mut operations = self.operations.write().await;
            for command in &commands {
                if let Some(operation) = operations.get_mut(&command.operation_id) {
                    operation.state = HubOperationState::Dispatched;
                    operation.dispatched_at_ms = Some(now);
                }
            }
        }
        if let Some(storage) = self.storage.as_ref() {
            for operation in &expired_operations {
                persist_operation(storage, operation)
                    .await
                    .map_err(HubError::from)?;
            }
            for command in &commands {
                if let Some(attempt_id) = command.attempt_id.as_deref() {
                    storage
                        .mark_attempt_dispatched(attempt_id, command.expires_at_ms)
                        .await
                        .map_err(HubError::from)?;
                }
            }
        }
        // A command can expire after being queued, or after an Agent polled it
        // and lost the response before execution.  Marking its old operation
        // terminal is not enough for operations without a desired-state
        // reconciler: the old active record would otherwise be the next
        // deduplication hit forever.  Re-enqueue a fresh command with the same
        // payload and generation.  Job start/stop uses the canonical
        // reconciliation path below so a changed Job spec/placement is
        // rebuilt instead of replaying stale command data.
        for command in expired_retries {
            if command.operation.starts_with("job_") {
                let Some(job) = self.job(&command.resource_id).await? else {
                    continue;
                };
                if job.generation != command.generation {
                    continue;
                }
            }
            if let Err(error) = self
                .enqueue_with_metadata(
                    command.node_id.clone(),
                    command.operation.clone(),
                    command.resource_id.clone(),
                    command.correlation_id.clone(),
                    command.payload.clone(),
                    command.generation,
                    command.action_id.clone(),
                    command.config_version_id.clone(),
                    None,
                    None,
                    command.attempt_id.clone(),
                )
                .await
            {
                // The node may have gone offline while the expired command
                // was being requeued.  The next heartbeat/reconciliation can
                // retry it; polling the current command queue should still
                // succeed and return the non-expired commands.
                tracing::warn!(
                    node_id = %command.node_id,
                    operation = %command.operation,
                    resource_id = %command.resource_id,
                    %error,
                    "failed to requeue expired Hub command"
                );
            }
        }
        for job_id in expired_job_ids {
            if let Some(job) = self.job(&job_id).await? {
                self.reconcile_job(&job).await?;
            }
        }
        Ok(commands)
    }

    pub async fn nodes(&self) -> Vec<HubNode> {
        self.nodes
            .read()
            .await
            .values()
            .map(|node| node.resource.clone())
            .collect()
    }

    pub async fn set_node_maintenance(
        &self,
        node_id: &str,
        state: NodeMaintenanceState,
        actor: Option<String>,
        correlation_id: Option<String>,
    ) -> Result<HubNode, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        let state_name = match state {
            NodeMaintenanceState::Active => "active",
            NodeMaintenanceState::Draining => "draining",
            NodeMaintenanceState::Maintenance => "maintenance",
        };
        if !storage
            .set_node_maintenance(
                crate::storage::NodeMaintenanceMutation {
                    node_id: node_id.into(),
                    state: state_name.into(),
                    actor,
                    correlation_id,
                },
                now_ms(),
            )
            .await
            .map_err(HubError::from)?
        {
            return Err(HubError::NotFound);
        }
        let mut nodes = self.nodes.write().await;
        let node = nodes.get_mut(node_id).ok_or(HubError::NotFound)?;
        node.resource.maintenance_state = state;
        Ok(node.resource.clone())
    }
    pub async fn configuration(&self, node_id: &str) -> Option<serde_json::Value> {
        self.nodes
            .read()
            .await
            .get(node_id)
            .and_then(|node| node.configuration.clone())
    }

    pub async fn mark_stale(&self) {
        let now = now_ms();
        let mut nodes = self.nodes.write().await;
        let stale_ids: Vec<String> = nodes
            .values_mut()
            .filter_map(|node| {
                if node.resource.state == NodeConnectionState::Online
                    && node.resource.lease_expires_at_ms <= now
                {
                    node.resource.state = NodeConnectionState::Stale;
                    Some(node.resource.id.clone())
                } else {
                    None
                }
            })
            .collect();
        if stale_ids.is_empty() {
            return;
        }
        let mut operations = self.operations.write().await;
        for operation in operations.values_mut() {
            if stale_ids.iter().any(|id| id == &operation.node_id)
                && matches!(
                    operation.state,
                    HubOperationState::Queued
                        | HubOperationState::Dispatched
                        | HubOperationState::Acknowledged
                        | HubOperationState::Running
                )
            {
                operation.state = HubOperationState::NodeUnavailable;
                operation.finished_at_ms = Some(now);
                operation.error = Some("Node lease expired".into());
            }
        }
    }
}
