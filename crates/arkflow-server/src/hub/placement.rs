//! Job reconciliation and node placement: retention, fencing, ranking.

use super::*;

const MAX_JOB_RECONCILIATIONS_PER_TICK: usize = 256;
/// Fleet-level "node pressuring" judgment: memory used ratio or CPU above
/// the threshold. Evaluated against the LATEST report's gauges; a node
/// without usable gauges is never pressuring (fail-safe: no data, no move).
const PRESSURE_MEMORY_USED_RATIO: f64 = 0.9;
const PRESSURE_CPU_PERCENT: f64 = 90.0;

pub(crate) fn node_under_pressure(metrics: &BTreeMap<String, f64>) -> bool {
    let memory_pressuring = match (
        metrics.get("node_memory_used_bytes"),
        metrics.get("node_memory_total_bytes"),
    ) {
        (Some(used), Some(total)) if total.is_finite() && *total > 0.0 && used.is_finite() => {
            used / total >= PRESSURE_MEMORY_USED_RATIO
        }
        _ => false,
    };
    let cpu_pressuring = metrics
        .get("node_cpu_usage_percent")
        .is_some_and(|cpu| cpu.is_finite() && *cpu >= PRESSURE_CPU_PERCENT);
    memory_pressuring || cpu_pressuring
}

/// How long a node's gauges stay eligible for Hub resource decisions —
/// headroom ranking and rebalance pressure alike — after its last report.
/// Agents report every couple of seconds, so past this window (about five
/// missed report ticks) a silent node ranks as gauge-less and its frozen
/// pressure streak no longer drives relocation. The Agent-side sampler has
/// its own independent staleness window; the two are not coupled.
pub(crate) const RESOURCE_GAUGE_FRESH_MS: u64 = 10_000;

/// Headroom ordering key for placement ranking: fresh-gauged nodes rank
/// (1, memory-available ratio, CPU headroom), gauge-less nodes rank
/// (0, 0, 0) and land after every gauged node, in id order. Larger is
/// better in every component.
fn headroom_key(record: Option<&NodeRecord>, now: u64) -> (u8, f64, f64) {
    let Some(record) = record else {
        return (0, 0.0, 0.0);
    };
    if record.last_report_at_ms == 0
        || now.saturating_sub(record.last_report_at_ms) > RESOURCE_GAUGE_FRESH_MS
    {
        return (0, 0.0, 0.0);
    }
    let (Some(used), Some(total)) = (
        record.metrics.get("node_memory_used_bytes"),
        record.metrics.get("node_memory_total_bytes"),
    ) else {
        return (0, 0.0, 0.0);
    };
    if !total.is_finite() || *total <= 0.0 || !used.is_finite() {
        return (0, 0.0, 0.0);
    }
    let memory_available_ratio = (1.0 - used / total).clamp(0.0, 1.0);
    let cpu_headroom = record
        .metrics
        .get("node_cpu_usage_percent")
        .filter(|cpu| cpu.is_finite())
        .map(|cpu| (100.0 - cpu).clamp(0.0, 100.0))
        .unwrap_or(0.0);
    (1, memory_available_ratio, cpu_headroom)
}

/// Rank eligible placement candidates by resource headroom. Pure and
/// deterministic: the ordered output feeds the unchanged assignment logic,
/// so `split-placement`'s "same input, same mapping" contract holds by
/// construction. Nodes with equal headroom (and all gauge-less nodes) tie
/// on node id.
pub(crate) fn rank_candidates(
    candidates: Vec<String>,
    nodes: &BTreeMap<String, NodeRecord>,
    now: u64,
) -> Vec<String> {
    let mut ranked = candidates;
    ranked.sort_by(|left, right| {
        let left_key = headroom_key(nodes.get(left), now);
        let right_key = headroom_key(nodes.get(right), now);
        right_key
            .0
            .cmp(&left_key.0)
            .then_with(|| right_key.1.total_cmp(&left_key.1))
            .then_with(|| right_key.2.total_cmp(&left_key.2))
            .then_with(|| left.cmp(right))
    });
    ranked
}

impl Hub {
    /// Reconcile a bounded set of durable Jobs so Agent failures and Hub
    /// recovery converge without waiting for a new lifecycle request.
    /// The retained placement set, in the order its placement was actually
    /// dispatched in (remembered at ranked-dispatch time), so a retained
    /// re-dispatch reproduces the identical task→node mapping. Nodes the
    /// memory does not know (e.g. after a Hub restart) are appended in id
    /// order.
    pub(crate) async fn retained_targets_in_dispatch_order(
        &self,
        job_id: &str,
        set: &BTreeSet<String>,
    ) -> Vec<String> {
        let remembered = self
            .placement_order
            .read()
            .await
            .get(job_id)
            .cloned()
            .unwrap_or_default();
        let mut ordered: Vec<String> = remembered
            .into_iter()
            .filter(|node_id| set.contains(node_id))
            .collect();
        for node_id in set {
            if !ordered.contains(node_id) {
                ordered.push(node_id.clone());
            }
        }
        ordered
    }

    /// Nodes in `targets` whose sustained-pressure streak trips the Job's
    /// opt-in rebalance policy, subject to its cooldown hysteresis. A streak
    /// only counts while the node's gauges are still fresh: a node that
    /// stopped reporting freezes its last streak, and stale data must not
    /// drive relocation. Empty unless rebalancing may proceed this tick;
    /// the caller only evicts while at least one target remains (never into
    /// nothing).
    async fn rebalance_evictions(
        &self,
        job: &JobRecord,
        spec: &arkflow_core::job::JobSpec,
        targets: &[String],
    ) -> BTreeSet<String> {
        let Some(policy) = &spec.rebalance else {
            return BTreeSet::new();
        };
        if policy.mode != arkflow_core::job::RebalanceMode::Auto || !job.node_ids.is_empty() {
            return BTreeSet::new();
        }
        if targets.is_empty() {
            return BTreeSet::new();
        }
        let now = now_ms();
        {
            let operations = self.operations.read().await;
            let mut latest_start_ms: Option<u64> = None;
            for operation_record in operations.values() {
                if operation_record.resource_id != job.job_id
                    || operation_record.operation != "job_start"
                    || operation_record.generation != job.generation
                {
                    continue;
                }
                latest_start_ms = Some(
                    latest_start_ms
                        .unwrap_or(0)
                        .max(operation_record.created_at_ms),
                );
            }
            // Hysteresis: a placement dispatched inside the cooldown window
            // — the initial placement included — is not moved again yet.
            if latest_start_ms.is_some_and(|latest| now.saturating_sub(latest) < policy.cooldown_ms)
            {
                return BTreeSet::new();
            }
        }
        let mut evicted = BTreeSet::new();
        {
            let nodes = self.nodes.read().await;
            for node_id in targets {
                let Some(node) = nodes.get(node_id) else {
                    continue;
                };
                // A streak frozen by a node that stopped reporting is not
                // sustained pressure: ranking and relocation must agree
                // that only fresh gauges drive resource decisions.
                if node.last_report_at_ms == 0
                    || now.saturating_sub(node.last_report_at_ms) > RESOURCE_GAUGE_FRESH_MS
                {
                    continue;
                }
                if node.pressure_streak >= policy.pressure_streak.max(1) {
                    evicted.insert(node_id.clone());
                }
            }
        }
        if evicted.len() >= targets.len() {
            // Nowhere to relocate to (single-node fleet, or every target is
            // pressuring): keep running where it is and retry next tick.
            return BTreeSet::new();
        }
        evicted
    }

    pub async fn reconcile_jobs(&self) -> Result<usize, HubError> {
        let jobs = self.jobs().await?;
        let mut dispatched = 0;
        for job in jobs.into_iter().take(MAX_JOB_RECONCILIATIONS_PER_TICK) {
            match self.reconcile_job(&job).await {
                Ok(count) => dispatched += count,
                // One Job whose target is at capacity, whose node expired
                // between the online pre-check and the enqueue, or whose
                // persisted spec no longer compiles must not stall the tick
                // for every other Job; the same ordering failure would repeat
                // each tick.
                Err(
                    error @ (HubError::Capacity | HubError::Invalid(_) | HubError::NodeUnavailable),
                ) => {
                    tracing::warn!(
                        job_id = %job.job_id,
                        error = %error,
                        "skipping Job reconciliation this tick"
                    );
                }
                Err(error) => return Err(error),
            }
        }
        Ok(dispatched)
    }

    pub async fn reconcile_job(&self, job: &JobRecord) -> Result<usize, HubError> {
        let spec: arkflow_core::job::JobSpec = serde_json::from_str(&job.spec_json)
            .map_err(|error| HubError::Invalid(format!("invalid persisted Job spec: {error}")))?;
        let plan = arkflow_core::job::JobPlan::compile(spec.clone())
            .map_err(|error| HubError::Invalid(error.to_string()))?;
        let operation = match job.desired_state.as_str() {
            "running" => "job_start",
            "stopped" => "job_stop",
            _ => return Ok(0),
        };
        // A durable state directory is not a disposable cache.  Once a Job
        // has successfully run, a restart or a version/generation move must
        // restore a compatible completed checkpoint before any source is
        // started again.  The first start of a brand-new Job is exempt: no
        // prior committed state exists yet.
        let persisted_job_starts = if operation == "job_start"
            && spec.state.as_ref().is_some_and(|state| {
                state.durability == arkflow_core::job::StateDurability::Durable
            })
            && spec.requires_state()
            && spec.checkpoint.is_some()
        {
            let records = match &self.storage {
                Some(storage) => storage
                    .list_job_start_operations(job.job_id.clone())
                    .await
                    .map_err(HubError::from)?,
                None => Vec::new(),
            };
            records
                .into_iter()
                .filter_map(|record| {
                    serde_json::from_str::<HubOperation>(&record.operation_json).ok()
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let recovery_required = if operation == "job_start"
            && spec.state.as_ref().is_some_and(|state| {
                state.durability == arkflow_core::job::StateDurability::Durable
            })
            && spec.requires_state()
            && spec.checkpoint.is_some()
        {
            let operations = self.operations.read().await;
            // A successful start is a durable fact even after the Hub fences
            // that operation because the Agent process was replaced.  The
            // `recovery_required` failure class preserves that fact across
            // the in-memory and SQLite operation histories; a plain
            // NodeUnavailable/TimedOut record is not sufficient because it
            // may represent a command that never reached an Agent.
            let previous_generation_started =
                operations.values().any(|operation_record| {
                    operation_record.resource_id == job.job_id
                        && is_durable_job_start(operation_record)
                        && operation_record.generation < job.generation
                }) || persisted_job_starts.iter().any(|operation_record| {
                    is_durable_job_start(operation_record)
                        && operation_record.generation < job.generation
                });
            let current_generation_requires_recovery =
                operations.values().any(|operation_record| {
                    operation_record.resource_id == job.job_id
                        && operation_record.operation == "job_start"
                        && operation_record.generation == job.generation
                        && (operation_record.failure_class.as_deref() == Some("recovery_required")
                            || (operation_record.state == HubOperationState::Succeeded
                                && matches!(job.observed_state.as_str(), "failed" | "stopped")))
                }) || persisted_job_starts.iter().any(|operation_record| {
                    operation_record.resource_id == job.job_id
                        && operation_record.generation == job.generation
                        && operation_record.failure_class.as_deref() == Some("recovery_required")
                });
            // A checkpoint/savepoint pointer is an explicit recovery request.
            // It must never be silently ignored just because the lifecycle
            // operation at the current generation is still marked Succeeded.
            job.checkpoint_id.is_some()
                || previous_generation_started
                || current_generation_requires_recovery
        } else {
            false
        };
        let candidates = if job.node_ids.is_empty() {
            self.nodes
                .read()
                .await
                .iter()
                .filter(|(_, node)| {
                    node.resource.state == NodeConnectionState::Online
                        && node.resource.lease_expires_at_ms > now_ms()
                })
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>()
        } else {
            job.node_ids.clone()
        };
        let targets = {
            let nodes = self.nodes.read().await;
            candidates
                .into_iter()
                .filter(|node_id| {
                    nodes.get(node_id).is_some_and(|node| {
                        node.resource.state == NodeConnectionState::Online
                            && node.resource.lease_expires_at_ms > now_ms()
                            && node.resource.maintenance_state == NodeMaintenanceState::Active
                    })
                })
                .collect::<Vec<_>>()
        };
        // Resource-aware ordering for unpinned placements: highest headroom
        // first, so a colocated Job lands on the freshest node and split
        // round-robin spreads from the best-ranked set. Pinned node_ids pass
        // through verbatim. The dispatched order is remembered further below
        // (only when this ranked order actually drives the dispatch).
        let mut targets = targets;
        if job.node_ids.is_empty() {
            let nodes = self.nodes.read().await;
            targets = rank_candidates(targets, &nodes, now_ms());
        }
        // Opt-in pressure rebalance: exclude nodes whose sustained-pressure
        // streak trips the Job's policy. The abandoned-placement fencing
        // below then supersedes their starts and dispatches their stops.
        let evictions = if operation == "job_start" {
            self.rebalance_evictions(job, &spec, &targets).await
        } else {
            BTreeSet::new()
        };
        if !evictions.is_empty() {
            targets.retain(|node_id| !evictions.contains(node_id));
        }
        let historical_nodes = self
            .operations
            .read()
            .await
            .values()
            .filter(|operation_record| {
                operation_record.resource_id == job.job_id
                    && operation_record.operation == "job_start"
                    // Keep starts from older generations in the placement
                    // history: a generation change may move a Job to another
                    // node, and the old node must receive a stop command.
                    && operation_record.generation <= job.generation
            })
            .map(|operation_record| operation_record.node_id.clone())
            .collect::<BTreeSet<_>>();
        // A successful/current operation is used to retain an automatic
        // placement. Historical failed or expired starts are intentionally
        // excluded here: they may never have reached an Agent and must not
        // pin a newly reconciled Job to a dead node. They remain in
        // `historical_nodes` so a partially executed command can still be
        // fenced with a best-effort stop below.
        let previous_nodes = self
            .operations
            .read()
            .await
            .values()
            .filter(|operation_record| {
                operation_record.resource_id == job.job_id
                    && operation_record.operation == "job_start"
                    && operation_record.generation <= job.generation
                    && !matches!(
                        operation_record.state,
                        HubOperationState::Failed
                            | HubOperationState::TimedOut
                            | HubOperationState::NodeUnavailable
                            | HubOperationState::Cancelled
                            | HubOperationState::Superseded
                    )
            })
            .map(|operation_record| operation_record.node_id.clone())
            .collect::<BTreeSet<_>>();
        let mut previous_nodes_all_online = !previous_nodes.is_empty();
        for node_id in &previous_nodes {
            let online = self.nodes.read().await.get(node_id).is_some_and(|node| {
                node.resource.state == NodeConnectionState::Online
                    && node.resource.lease_expires_at_ms > now_ms()
                    && node.resource.maintenance_state == NodeMaintenanceState::Active
            });
            if !online {
                previous_nodes_all_online = false;
                break;
            }
        }
        let retention_won = operation == "job_start"
            && job.node_ids.is_empty()
            && previous_nodes_all_online
            && evictions.is_empty();
        let targets = if retention_won {
            // Re-dispatch in the placement's original node order: split
            // round-robin and multi-component co-location are order
            // sensitive, and the mapping for a retained placement must not
            // drift between dispatches.
            self.retained_targets_in_dispatch_order(&job.job_id, &previous_nodes)
                .await
        } else if operation == "job_stop" {
            // A stopped Job must reach every node that may still host an
            // older generation. Such a node is not necessarily part of
            // the current explicit placement (for example after a move
            // from A to B), so filtering the already-derived current
            // targets would silently omit A. Include both the durable
            // start history and the current placement, then retain only
            // nodes that can accept a command now.
            let mut target_ids = historical_nodes.clone();
            target_ids.extend(targets.iter().cloned());
            let nodes = self.nodes.read().await;
            target_ids
                .into_iter()
                .filter(|node_id| {
                    nodes.get(node_id).is_some_and(|node| {
                        node.resource.state == NodeConnectionState::Online
                            && node.resource.lease_expires_at_ms > now_ms()
                            && node.resource.maintenance_state == NodeMaintenanceState::Active
                    })
                })
                .collect::<Vec<_>>()
        } else {
            targets
        };
        let target_ids = targets.iter().cloned().collect::<BTreeSet<_>>();
        // Build and validate the assignment BEFORE any fencing: a target set
        // that cannot host the placement (a split side edge across nodes, or
        // a target without the shuffle data plane) must fail the reconcile as
        // a no-op retry — superseding the old placement first would stop the
        // only live runner and leave the Job down while the invalid set
        // persists (for example a pressured node's eviction that leaves a
        // non-shuffle node in the candidate set).
        let assignments = plan
            .assignments_for_nodes(&targets, job.generation)
            .map_err(|error| HubError::Invalid(error.to_string()))?;
        // Re-check the complete task→node map at the dispatch boundary.  The
        // planner already validates it, but keeping this guard here prevents a
        // future assignment source or persistence replay from bypassing the
        // side-edge co-location contract.
        plan.validate_side_edge_assignments(&assignments)
            .map_err(|error| HubError::Invalid(error.to_string()))?;
        // Split placement: validate that every target node runs the data
        // plane, then attach the full task→node map and peer data addresses
        // so each node's graph build can wire its remote edges without any
        // further lookup.
        let mut split_payload = None;
        if spec.placement == arkflow_core::job::PlacementStrategy::Split {
            let nodes = self.nodes.read().await;
            let mut node_data_ports = BTreeMap::new();
            for node_id in &targets {
                let Some(record) = nodes.get(node_id) else {
                    return Err(HubError::Invalid(format!(
                        "split placement target node '{node_id}' is not registered"
                    )));
                };
                if !record
                    .resource
                    .capabilities
                    .iter()
                    .any(|capability| capability == "network_shuffle")
                {
                    return Err(HubError::Invalid(format!(
                        "split placement requires node '{node_id}' with the network_shuffle capability"
                    )));
                }
                let Some(address) = &record.resource.data_address else {
                    return Err(HubError::Invalid(format!(
                        "split placement requires node '{node_id}' to advertise a data address"
                    )));
                };
                node_data_ports.insert(node_id.clone(), address.clone());
            }
            drop(nodes);
            let mut task_nodes = BTreeMap::new();
            for assignment in &assignments {
                task_nodes.insert(assignment.task_id.clone(), assignment.node_id.clone());
            }
            split_payload = Some(serde_json::json!({
                "task_nodes": task_nodes,
                "node_data_ports": node_data_ports,
            }));
        }
        // The dispatch order is only real once the target set validated: a
        // reconcile that fails validation above dispatches nothing, and
        // recording its (never-dispatched) order here would corrupt the
        // retention memory for the still-live placement.
        if operation == "job_start" && job.node_ids.is_empty() && !retention_won {
            self.placement_order
                .write()
                .await
                .insert(job.job_id.clone(), targets.clone());
        }
        if operation == "job_start" {
            // Auto-placement fencing: when the reconciler re-places a Job
            // (its previous placement lost its lease or was partitioned), the
            // abandoned node's Succeeded start at THIS generation still
            // claims the assignment. Without invalidating it, the node is
            // deduped back into the sticky target set on its return and both
            // nodes run the same Job forever. Mark those starts Superseded so
            // the placement history stops claiming them and the nodes receive
            // a stop command when they reappear.
            let abandoned: Vec<HubOperation> = {
                let operations = self.operations.read().await;
                operations
                    .values()
                    .filter(|operation_record| {
                        operation_record.resource_id == job.job_id
                            && operation_record.operation == "job_start"
                            && operation_record.generation == job.generation
                            && operation_record.state == HubOperationState::Succeeded
                            && !target_ids.contains(&operation_record.node_id)
                    })
                    .cloned()
                    .collect()
            };
            if !abandoned.is_empty() {
                // Apply the in-memory transition under the write lock, then
                // persist OUTSIDE it: the storage round-trips are async and
                // holding the operations write lock across them would block
                // every agent poll, report, and command result for the
                // duration of the I/O.
                let mut superseded = Vec::with_capacity(abandoned.len());
                {
                    let mut operations = self.operations.write().await;
                    for mut record in abandoned {
                        record.state = HubOperationState::Superseded;
                        record.superseded_generation = Some(job.generation);
                        operations.insert(record.id.clone(), record.clone());
                        superseded.push(record);
                    }
                }
                if let Some(storage) = self.storage.as_ref() {
                    for record in &superseded {
                        persist_operation(storage, record)
                            .await
                            .map_err(HubError::from)?;
                    }
                }
            }
            // A target that is still valid for the new generation does not
            // need a stop/start bounce. Every historical placement outside
            // the desired set is stale and must be fenced, including starts
            // recorded under an older generation.
            let nodes_to_stop = historical_nodes.difference(&target_ids).collect::<Vec<_>>();
            for node_id in nodes_to_stop {
                // The same terminal-state memory as the dispatch loop: a
                // Succeeded stop for this (node, job, generation) already
                // fenced the abandoned placement; re-enqueuing it every tick
                // would churn persistent operation rows.
                let stop_settled = self
                    .operations
                    .read()
                    .await
                    .values()
                    .any(|operation_record| {
                        operation_record.node_id == *node_id
                            && operation_record.resource_id == job.job_id
                            && operation_record.operation == "job_stop"
                            && operation_record.generation == job.generation
                            && operation_record.state == HubOperationState::Succeeded
                    });
                if stop_settled {
                    continue;
                }
                let is_online = self.nodes.read().await.get(node_id).is_some_and(|node| {
                    node.resource.state == NodeConnectionState::Online
                        && node.resource.lease_expires_at_ms > now_ms()
                        && node.resource.maintenance_state == NodeMaintenanceState::Active
                });
                if is_online {
                    self.enqueue_with_metadata(
                        node_id.clone(),
                        "job_stop".into(),
                        job.job_id.clone(),
                        None,
                        Some(serde_json::json!({"job_id": job.job_id})),
                        job.generation,
                        None,
                        None,
                        None,
                        None,
                        None,
                    )
                    .await?;
                }
            }
        }
        let explicit_recovery_id = job.checkpoint_id.clone();
        let mut recovery_candidates = self
            .job_checkpoints(&job.job_id)
            .await?
            .into_iter()
            .filter(|record| record.status == "completed")
            .filter(|record| match explicit_recovery_id.as_deref() {
                Some(requested) => {
                    record.checkpoint_id == requested
                        && recovery_record_is_compatible(&spec, record)
                }
                None => recovery_record_is_compatible(&spec, record),
            })
            .filter(|record| match spec.recovery {
                arkflow_core::job::RecoveryPolicy::LatestCheckpoint => record.kind == "checkpoint",
                arkflow_core::job::RecoveryPolicy::LatestSavepoint => record.kind == "savepoint",
                arkflow_core::job::RecoveryPolicy::Fail => false,
            })
            .collect::<Vec<_>>();
        recovery_candidates.sort_by(|left, right| {
            right
                .created_at_ms
                .cmp(&left.created_at_ms)
                .then_with(|| right.checkpoint_id.cmp(&left.checkpoint_id))
        });
        let recovery = recovery_candidates
            .into_iter()
            .find(|record| recovery_record_is_valid(&spec, record))
            .map(|record| {
                serde_json::json!({
                    "checkpoint_id": record.checkpoint_id,
                    "savepoint": record.kind == "savepoint",
                })
            });
        if recovery_required && recovery.is_none() {
            return Err(HubError::Invalid(format!(
                "durable Job '{}' requires recovery, but no compatible completed checkpoint is available",
                job.job_id
            )));
        }
        let mut dispatched = 0;
        for node_id in targets {
            // Terminal-state memory: a Succeeded lifecycle operation for THIS
            // (node, job, generation) already satisfies the desired state.
            // Without this skip, every reconcile tick re-enqueued the command
            // and a fresh persistent operation row — an unbounded churn loop
            // for stopped Jobs (and for fenced placements) with no retention
            // able to keep up. A generation bump or desired-state change
            // re-dispatches naturally.
            let already_terminal = self
                .operations
                .read()
                .await
                .values()
                .any(|operation_record| {
                    operation_record.node_id == node_id
                        && operation_record.resource_id == job.job_id
                        && operation_record.operation == operation
                        && operation_record.generation == job.generation
                        && operation_record.state == HubOperationState::Succeeded
                });
            if already_terminal {
                continue;
            }
            let node_assignments = assignments
                .iter()
                .filter(|assignment| assignment.node_id == node_id)
                .cloned()
                .collect::<Vec<_>>();
            if operation == "job_start" && node_assignments.is_empty() {
                continue;
            }
            let mut payload_value = serde_json::json!({
                "job_id": job.job_id,
                "spec": spec,
                "plan": plan,
                "assignments": node_assignments,
                "generation": job.generation,
                "recovery": recovery,
                "recovery_required": recovery_required,
            });
            if let (Some(base), Some(extra)) = (
                payload_value.as_object_mut(),
                split_payload.as_ref().and_then(|extra| extra.as_object()),
            ) {
                for (key, value) in extra {
                    base.insert(key.clone(), value.clone());
                }
            }
            let payload = Some(payload_value);
            self.enqueue_with_metadata(
                node_id,
                operation.into(),
                job.job_id.clone(),
                None,
                payload.clone(),
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

    /// Expire queued/dispatched `job_start`/`job_stop` operations whose
    /// delivery window passed. Each expiry increments the retry count; once
    /// the count reaches `MAX_JOB_OPERATION_RETRIES` the operation settles
    /// as terminal `failed`/`expired` and reconciliation stops re-enqueueing
    /// it. The undeliverable command is dropped so a late Agent poll cannot
    /// execute it, and the transition is persisted so a Hub restart sees the
    /// same state. Expiry marks the operation terminal-but-retriable: the
    /// next `reconcile_jobs` tick re-enqueues it with the inherited retry
    /// count. Checkpoint/savepoint triggers are deliberately out of scope:
    /// their retry path is the poll handler's expired-command re-enqueue,
    /// which replays the original payload — expiring them here would drop
    /// the trigger instead.
    pub async fn expire_stale_job_operations(&self) -> Result<usize, HubError> {
        let now = now_ms();
        let expired: Vec<String> = {
            let operations = self.operations.read().await;
            operations
                .values()
                .filter(|record| {
                    matches!(record.operation.as_str(), "job_start" | "job_stop")
                        && matches!(
                            record.state,
                            HubOperationState::Queued | HubOperationState::Dispatched
                        )
                        && record.expires_at_ms.is_some_and(|expires| expires <= now)
                })
                .map(|record| record.id.clone())
                .collect()
        };
        if expired.is_empty() {
            return Ok(0);
        }
        let mut mutated: Vec<HubOperation> = Vec::with_capacity(expired.len());
        {
            let mut operations = self.operations.write().await;
            for id in expired {
                let Some(record) = operations.get_mut(&id) else {
                    continue;
                };
                // Re-check under the write lock: a command result may have
                // settled the operation between the scan and this transition.
                if !matches!(
                    record.state,
                    HubOperationState::Queued | HubOperationState::Dispatched
                ) || !record.expires_at_ms.is_some_and(|expires| expires <= now)
                {
                    continue;
                }
                record.retry_count += 1;
                record.failure_class = Some("expired".into());
                record.next_retry_at_ms = Some(now);
                if record.retry_count >= MAX_JOB_OPERATION_RETRIES {
                    record.state = HubOperationState::Failed;
                } else {
                    record.state = HubOperationState::TimedOut;
                }
                mutated.push(record.clone());
            }
        }
        if mutated.is_empty() {
            return Ok(0);
        }
        // Drop the undeliverable commands. The operations lock must be
        // released first: enqueue takes the nodes lock before the operations
        // lock, so the reverse order here could deadlock against it.
        {
            let mut nodes = self.nodes.write().await;
            for record in &mutated {
                if let Some(node) = nodes.get_mut(&record.node_id) {
                    node.commands
                        .retain(|command| command.operation_id != record.id);
                    node.leased_commands.remove(&record.command_id);
                }
            }
        }
        if let Some(storage) = self.storage.as_ref() {
            for record in &mutated {
                persist_operation(storage, record)
                    .await
                    .map_err(HubError::from)?;
            }
        }
        for record in &mutated {
            self.command_metrics.record_outcome(
                &record.operation,
                CommandMetrics::outcome_label(record.state).unwrap_or("failed"),
            );
        }
        Ok(mutated.len())
    }

    pub async fn expire_attempts(&self) -> Result<usize, HubError> {
        let Some(storage) = self.storage.as_ref() else {
            return Ok(0);
        };
        storage
            .expire_attempts(now_ms())
            .await
            .map_err(HubError::from)
    }

    /// Consume one durable reconciliation wake-up. If the target node is
    /// offline the outbox row remains unprocessed and its claim lease expires
    /// for a later retry.
    pub async fn reconcile_once(&self, worker_id: &str) -> Result<Option<HubOperation>, HubError> {
        let Some(storage) = self.storage.as_ref() else {
            return Ok(None);
        };
        let Some(outbox) = storage
            .claim_outbox(worker_id, now_ms())
            .await
            .map_err(HubError::from)?
        else {
            return Ok(None);
        };
        let Some(stream_id) = outbox.stream_id.clone() else {
            storage
                .mark_outbox_processed(outbox.outbox_id, now_ms())
                .await
                .map_err(HubError::from)?;
            return Ok(None);
        };
        let Some(desired) = storage
            .get_desired(&outbox.node_id, &stream_id)
            .await
            .map_err(HubError::from)?
        else {
            storage
                .mark_outbox_processed(outbox.outbox_id, now_ms())
                .await
                .map_err(HubError::from)?;
            return Ok(None);
        };
        let online = {
            let nodes = self.nodes.read().await;
            nodes.get(&desired.node_id).is_some_and(|node| {
                node.resource.state == NodeConnectionState::Online
                    && node.resource.lease_expires_at_ms > now_ms()
            })
        };
        if !online {
            return Ok(None);
        }
        let Some(mut attempt) = storage
            .claim_attempt(&outbox.intent_id.clone().unwrap_or_default())
            .await
            .map_err(HubError::from)?
        else {
            return Ok(None);
        };
        // Transient secret pre-resolution: `${secret:...}` references in
        // configuration payloads resolve against the HUB environment at
        // dispatch time only. Storage keeps the verbatim reference (no
        // plaintext at rest), and retries re-resolve, so secret rotation
        // applies to later attempts. `env:`/`file:` references stay
        // node-local and are left for the agent's own materialization.
        if attempt.operation == "apply_configuration" {
            if let Some(payload) = &attempt.payload_json {
                if payload.contains("secret:") {
                    match arkflow_core::secret::resolve_candidate_payload(payload.clone()) {
                        Ok(Some(resolved)) => attempt.payload_json = Some(resolved),
                        Ok(None) => {}
                        Err(error) => {
                            // A missing secret is a permanent misconfiguration,
                            // not a transient dispatch failure: routing it
                            // through the attempt/intent failure machinery
                            // blocks the intent (rollout target -> failed)
                            // instead of leaving the outbox lease to expire
                            // and re-claim the identical payload forever.
                            tracing::warn!(
                                attempt_id = %attempt.attempt_id,
                                %error,
                                "configuration dispatch pre-resolution failed"
                            );
                            storage
                                .complete_attempt(
                                    &attempt.attempt_id,
                                    "failed",
                                    Some("invalid_config".into()),
                                )
                                .await
                                .map_err(HubError::from)?;
                            storage
                                .mark_outbox_processed(outbox.outbox_id, now_ms())
                                .await
                                .map_err(HubError::from)?;
                            return Ok(None);
                        }
                    }
                }
            }
        }
        let operation = self.enqueue_attempt(attempt).await?;
        storage
            .mark_outbox_processed(outbox.outbox_id, now_ms())
            .await
            .map_err(HubError::from)?;
        Ok(Some(operation))
    }
}
