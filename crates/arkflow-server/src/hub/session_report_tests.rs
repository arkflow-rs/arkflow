use super::*;
use std::time::Duration;

fn config() -> HubConfig {
    HubConfig {
        operator_token: Some("operator".into()),
        node_token: Some("node-secret".into()),
        insecure_local: false,
        lease_ttl_ms: 1000,
        poll_interval_ms: 1000,
        session_ttl_ms: default_session_ttl_ms(),
    }
}

fn stream(state: &str) -> arkflow_core::control::StreamStatus {
    serde_json::from_value(serde_json::json!({
        "id": "orders",
        "state": state,
        "metrics": {
            "input_batches": 0,
            "input_messages": 0,
            "processing_errors": 0,
            "output_batches": 0,
            "output_messages": 0,
            "input_errors": 0,
            "input_reconnects": 0,
            "output_errors": 0,
            "restarts": 0,
            "kernel_chains": {},
            "in_flight": 0,
            "mean_latency_us": 0,
            "checkpoint_duration_ms": 0,
            "checkpoint_failures": 0,
            "watermark_lag_ms": 0,
            "late_events": 0
        }
    }))
    .unwrap()
}

async fn registered_hub() -> (Hub, crate::hub::RegisterResponse) {
    let hub = Hub::with_storage(
        config(),
        crate::storage::StorageActor::start(
            crate::storage::ControlPlaneStore::contract("registered_hub").await,
            4,
        ),
    );
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "n1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec![],
            boot_id: None,
        })
        .await
        .unwrap();
    (hub, session)
}

/// Task 7.3: a newly registered session receives a fresh report cursor —
/// sequence 1 of the new session identity is accepted even though the
/// previous session had already reported higher sequences.
#[tokio::test]
async fn new_session_resets_the_report_cursor() {
    let (hub, first) = registered_hub().await;
    let report = |session: &crate::hub::RegisterResponse, seq: u64, state: &str| NodeReport {
        auth: AgentAuth {
            node_id: "n1".into(),
            session_token: session.session_token.clone(),
        },
        version: "test".into(),
        state: "online".into(),
        capabilities: vec![],
        streams: vec![stream(state)],
        operations: vec![],
        events: vec![],
        metrics: BTreeMap::new(),
        jobs: BTreeMap::new(),
        configuration: None,
        configuration_version: None,
        boot_id: Some(session.session_token.clone()),
        report_seq: seq,
        config_versions: Vec::new(),
        job_tasks: BTreeMap::new(),
    };
    hub.report(report(&first, 7, "running")).await.unwrap();
    // Re-register: a fresh session identity with a fresh cursor.
    let second = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "n1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec![],
            boot_id: None,
        })
        .await
        .unwrap();
    // Sequence 1 of the new session is accepted (not stale under the
    // previous session's cursor of 7).
    hub.report(report(&second, 1, "failed")).await.unwrap();
    let streams = hub.streams(Some("n1")).await;
    assert_eq!(streams.len(), 1);
    assert_eq!(
        streams[0].1.state,
        arkflow_core::control::StreamState::Failed
    );
}

// --- Session credential lifetime (harden-agent-session-credentials) ---

fn short_session_config() -> HubConfig {
    HubConfig {
        session_ttl_ms: 80,
        ..config()
    }
}

fn heartbeat_request(session_token: &str) -> HeartbeatRequest {
    HeartbeatRequest {
        auth: AgentAuth {
            node_id: "n1".into(),
            session_token: session_token.to_owned(),
        },
        state: "online".into(),
        protocol_version: Some("v1".into()),
        software_version: None,
        capabilities: vec![],
        rollout_id: None,
    }
}

async fn register_with_boot(hub: &Hub, boot_id: &str) -> RegisterResponse {
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "n1".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec![],
        boot_id: Some(boot_id.to_owned()),
    })
    .await
    .unwrap()
}

/// An expired session credential stops authenticating agent requests, and
/// the rejection must not mutate the node registry.
#[tokio::test]
async fn expired_session_is_rejected_without_registry_mutation() {
    let hub = Hub::new(short_session_config());
    let session = register_with_boot(&hub, "boot-1").await;
    // The registration response advertises the configured session TTL.
    assert_eq!(session.session_ttl_ms, 80);

    hub.heartbeat(heartbeat_request(&session.session_token))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(140)).await;
    assert!(matches!(
        hub.heartbeat(heartbeat_request(&session.session_token))
            .await,
        Err(HubError::Unauthorized)
    ));

    // Registry unchanged: the node is still present, online, untouched.
    let nodes = hub.nodes().await;
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id, "n1");
    assert_eq!(nodes[0].state, NodeConnectionState::Online);
}

/// Re-registration mints a fresh credential and kills the previous one;
/// resources reported by the old session survive when the boot identity
/// is stable.
#[tokio::test]
async fn re_registration_rotates_the_credential_and_preserves_state() {
    let hub = Hub::new(config());
    let first = register_with_boot(&hub, "boot-1").await;
    let report = |session: &RegisterResponse, seq: u64| NodeReport {
        auth: AgentAuth {
            node_id: "n1".into(),
            session_token: session.session_token.clone(),
        },
        version: "test".into(),
        state: "online".into(),
        capabilities: vec![],
        streams: vec![stream("running")],
        operations: vec![],
        events: vec![],
        metrics: BTreeMap::new(),
        jobs: BTreeMap::new(),
        configuration: None,
        configuration_version: None,
        boot_id: Some("boot-1".into()),
        report_seq: seq,
        config_versions: Vec::new(),
        job_tasks: BTreeMap::new(),
    };
    hub.report(report(&first, 1)).await.unwrap();

    let second = register_with_boot(&hub, "boot-1").await;
    assert_ne!(first.session_token, second.session_token);
    assert!(matches!(
        hub.heartbeat(heartbeat_request(&first.session_token)).await,
        Err(HubError::Unauthorized)
    ));
    hub.heartbeat(heartbeat_request(&second.session_token))
        .await
        .unwrap();
    let streams = hub.streams(Some("n1")).await;
    assert_eq!(streams.len(), 1);
}

/// Agents built before `session_ttl_ms` existed must keep parsing
/// registration responses from a Hub that does not send it yet.
#[test]
fn register_response_without_session_ttl_is_accepted() {
    let legacy: RegisterResponse = serde_json::from_value(serde_json::json!({
        "node_id": "n1",
        "session_token": "credential",
        "lease_ttl_ms": 1,
        "poll_interval_ms": 1,
        "protocol_version": "v1"
    }))
    .unwrap();
    assert_eq!(legacy.session_ttl_ms, 0);
}

/// The session TTL elapsing while a command executes must not lose the
/// terminal result: the Agent re-registers, the command-lease replay path
/// re-enqueues the command, and the Hub settles exactly one terminal
/// outcome through it.
#[tokio::test]
async fn expired_session_mid_command_still_settles_one_terminal_result() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES ('cfg-e2e', 'digest', '{}', 'json', 1)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
    let mut hub_config = config();
    hub_config.lease_ttl_ms = 1_000; // command lease duration
    hub_config.session_ttl_ms = 80; // expires long before the command lease
    let hub = Hub::with_storage(hub_config, StorageActor::start(store, 8));
    let registration = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "agent-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["configuration".into()],
            boot_id: Some("boot-1".into()),
        })
        .await
        .unwrap();
    let auth_a = AgentAuth {
        node_id: "agent-a".into(),
        session_token: registration.session_token.clone(),
    };

    hub.create_rollout(
        "cfg-e2e".into(),
        vec!["agent-a".into()],
        1,
        Some("operator".into()),
        Some("e2e-session-expiry".into()),
    )
    .await
    .unwrap();
    hub.reconcile_rollouts().await.unwrap();
    let operation = hub
        .reconcile_once("e2e-session-expiry")
        .await
        .unwrap()
        .unwrap();
    let commands = hub.commands(auth_a.clone()).await.unwrap();
    assert_eq!(commands.len(), 1);
    let command = commands[0].clone();

    // The session expires while the Agent executes the command.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let result = hub
        .command_result(
            auth_a.clone(),
            CommandResult {
                command_id: command.id.clone(),
                operation_id: command.operation_id.clone(),
                state: HubOperationState::Succeeded,
                progress: 100,
                error: None,
                correlation_id: command.correlation_id.clone(),
                generation: command.generation,
                observed_generation: Some(command.generation),
                action_id: command.action_id.clone(),
                failure_class: None,
                config_version_id: Some("cfg-e2e".into()),
                rollout_id: command.rollout_id.clone(),
                observed_checkpoint_id: None,
                checkpoint_manifest_uri: None,
                result: None,
            },
        )
        .await;
    assert!(matches!(result, Err(HubError::Unauthorized)));

    // The Agent re-registers with its stable boot identity: leased command
    // state survives because the boot did not change.
    let reauth = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "agent-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["configuration".into()],
            boot_id: Some("boot-1".into()),
        })
        .await
        .unwrap();
    let auth_b = AgentAuth {
        node_id: "agent-a".into(),
        session_token: reauth.session_token.clone(),
    };
    // The command lease is still valid, so nothing is redelivered yet.
    assert!(hub.commands(auth_b.clone()).await.unwrap().is_empty());

    // Let the command lease expire. Session B's own TTL also elapses
    // during the wait — with a hard TTL every long gap ends in another
    // re-registration, exactly like the real Agent reconnect loop.
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    let reauth2 = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "agent-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["configuration".into()],
            boot_id: Some("boot-1".into()),
        })
        .await
        .unwrap();
    let auth_c = AgentAuth {
        node_id: "agent-a".into(),
        session_token: reauth2.session_token.clone(),
    };
    // Refresh the node lease before polling so the replay re-enqueue
    // finds the node online.
    hub.heartbeat(HeartbeatRequest {
        auth: auth_c.clone(),
        state: "online".into(),
        protocol_version: Some("v1".into()),
        software_version: None,
        capabilities: vec!["configuration".into()],
        rollout_id: None,
    })
    .await
    .unwrap();
    // The first poll triggers the lease-expiry sweep that re-enqueues the
    // command; the replacement lands in the queue after the pop loop, so
    // the next poll hands it back.
    let _ = hub.commands(auth_c.clone()).await.unwrap();
    let redelivered = hub.commands(auth_c.clone()).await.unwrap();
    assert_eq!(redelivered.len(), 1);

    hub.command_result(
        auth_c.clone(),
        CommandResult {
            command_id: redelivered[0].id.clone(),
            operation_id: redelivered[0].operation_id.clone(),
            state: HubOperationState::Succeeded,
            progress: 100,
            error: None,
            correlation_id: redelivered[0].correlation_id.clone(),
            generation: redelivered[0].generation,
            observed_generation: Some(redelivered[0].generation),
            action_id: redelivered[0].action_id.clone(),
            failure_class: None,
            config_version_id: Some("cfg-e2e".into()),
            rollout_id: redelivered[0].rollout_id.clone(),
            observed_checkpoint_id: None,
            checkpoint_manifest_uri: None,
            result: None,
        },
    )
    .await
    .unwrap();
    let settled = hub.operation(&redelivered[0].operation_id).await.unwrap();
    assert_eq!(settled.state, HubOperationState::Succeeded);
    // The pre-expiry operation was timed out by the lease expiry — the
    // result rejected with 401 never settled it. Exactly one terminal
    // outcome exists per operation record.
    let original = hub.operation(&operation.id).await.unwrap();
    assert_eq!(original.state, HubOperationState::TimedOut);
}

/// The outbox and attempt retention wrappers converge their tables
/// through the storage actor while unprocessed outbox rows and active
/// attempts survive, and the status counters stay meaningful.
#[tokio::test]
async fn outbox_and_attempt_history_prunes_converge_through_the_hub() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let hub = Hub::with_storage(config(), StorageActor::start(store.clone(), 8));
    let now = now_ms() as i64;
    store
            .immediate_transaction(|transaction| -> Result<(), crate::storage::StorageError> {
                transaction.execute(
                    "INSERT INTO cp_intents (intent_id, node_id, stream_id, generation, intent_type, state, convergence_state, created_at_ms, updated_at_ms) VALUES ('intent-1', 'n1', 'orders', 1, 'stream_lifecycle', 'converged', 'converged', 1, 1)",
                    [],
                )?;
                transaction.execute(
                    "INSERT INTO cp_outbox (event_key, event_type, node_id, available_at_ms, created_at_ms, processed_at_ms) VALUES ('stale-processed', 'reconcile_intent', 'n1', 1, 1, 100)",
                    [],
                )?;
                transaction.execute(
                    "INSERT INTO cp_outbox (event_key, event_type, node_id, available_at_ms, created_at_ms, processed_at_ms) VALUES ('fresh-processed', 'reconcile_intent', 'n1', 1, 1, ?1)",
                    [now],
                )?;
                transaction.execute(
                    "INSERT INTO cp_attempts (attempt_id, intent_id, command_id, node_id, stream_id, generation, operation, state, finished_at_ms, created_at_ms) VALUES ('old-terminal', 'intent-1', 'cmd-old', 'n1', 'orders', 1, 'apply_configuration', 'succeeded', 100, 1)",
                    [],
                )?;
                transaction.execute(
                    "INSERT INTO cp_attempts (attempt_id, intent_id, command_id, node_id, stream_id, generation, operation, state, finished_at_ms, created_at_ms) VALUES ('live-active', 'intent-1', 'cmd-live', 'n1', 'orders', 1, 'apply_configuration', 'running', NULL, 1)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
    hub.prune_outbox_history().await.unwrap();
    hub.prune_attempt_history().await.unwrap();
    let counts = store
            .immediate_transaction(|transaction| {
                let outbox = transaction.query_row(
                    "SELECT COUNT(*) FROM cp_outbox WHERE event_key IN ('stale-processed', 'fresh-processed')",
                    [],
                    |row| row.get::<_, i64>(0),
                )?;
                let attempts = transaction.query_row(
                    "SELECT COUNT(*) FROM cp_attempts WHERE attempt_id IN ('old-terminal', 'live-active')",
                    [],
                    |row| row.get::<_, i64>(0),
                )?;
                Ok((outbox, attempts))
            })
            .unwrap();
    assert_eq!(
        counts,
        (1, 1),
        "stale processed outbox and terminal attempt rows are reclaimed; fresh and active rows are kept"
    );
}

/// Task 7.3: a delayed report from an older session arrives after the new
/// session registered — the Hub acknowledges it without changing the new
/// session's observed state.
#[tokio::test]
async fn delayed_report_from_an_old_session_is_ignored() {
    let (hub, first) = registered_hub().await;
    let second = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "n1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec![],
            boot_id: None,
        })
        .await
        .unwrap();
    // The new session reports a healthy stream.
    hub.report(NodeReport {
        auth: AgentAuth {
            node_id: "n1".into(),
            session_token: second.session_token.clone(),
        },
        version: "test".into(),
        state: "online".into(),
        capabilities: vec![],
        streams: vec![stream("running")],
        operations: vec![],
        events: vec![],
        metrics: BTreeMap::new(),
        jobs: BTreeMap::new(),
        configuration: None,
        configuration_version: None,
        boot_id: Some(second.session_token.clone()),
        report_seq: 1,
        config_versions: Vec::new(),
        job_tasks: BTreeMap::new(),
    })
    .await
    .unwrap();
    // A delayed report from the OLD session (stale boot identity) claims
    // a failure: the Hub must not regress the observed snapshot.
    // The old session token is no longer authenticated after
    // re-registration, so this surfaces as Unauthorized.
    let stale = hub
        .report(NodeReport {
            auth: AgentAuth {
                node_id: "n1".into(),
                session_token: first.session_token.clone(),
            },
            version: "test".into(),
            state: "online".into(),
            capabilities: vec![],
            streams: vec![stream("failed")],
            operations: vec![],
            events: vec![],
            metrics: BTreeMap::new(),
            jobs: BTreeMap::new(),
            configuration: None,
            configuration_version: None,
            boot_id: Some(first.session_token.clone()),
            report_seq: 99,
            config_versions: Vec::new(),
            job_tasks: BTreeMap::new(),
        })
        .await;
    assert!(stale.is_err(), "the old session token is revoked");
    let streams = hub.streams(Some("n1")).await;
    assert_eq!(
        streams[0].1.state,
        arkflow_core::control::StreamState::Running
    );
}

// ----- resource-aware placement and opt-in rebalancing -----

fn job_spec_json(id: &str) -> String {
    serde_json::json!({
            "id": id,
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        })
        .to_string()
}

fn resource_metrics(used_ratio: f64, cpu: f64) -> BTreeMap<String, f64> {
    let mut metrics = BTreeMap::from([
        ("node_memory_total_bytes".to_string(), 16_000.0),
        ("node_memory_used_bytes".to_string(), 16_000.0 * used_ratio),
        ("node_cpu_usage_percent".to_string(), cpu),
        // Two logical cores = 2000 millicores of declared capacity.
        ("node_cpu_cores".to_string(), 2.0),
    ]);
    metrics.retain(|_, value| value.is_finite() && *value >= 0.0);
    metrics
}

fn shuffle_capabilities() -> Vec<String> {
    vec![
        "job_runtime".to_string(),
        "state_backend".to_string(),
        "network_shuffle".to_string(),
    ]
}

/// Reports refresh the node capability list, so shuffle nodes' reports
/// must keep advertising it (the shared helper sends none).
async fn report_shuffle_node(
    hub: &Hub,
    auth: &AgentAuth,
    used_ratio: f64,
    cpu: f64,
    report_seq: u64,
) {
    hub.report(NodeReport {
        auth: auth.clone(),
        version: "test".into(),
        state: "online".into(),
        capabilities: shuffle_capabilities(),
        streams: vec![],
        operations: vec![],
        events: vec![],
        metrics: resource_metrics(used_ratio, cpu),
        jobs: BTreeMap::new(),
        configuration: None,
        configuration_version: None,
        boot_id: Some("boot".into()),
        report_seq,
        config_versions: Vec::new(),
        job_tasks: BTreeMap::new(),
    })
    .await
    .unwrap();
}

/// Sorted task ids from the node's pending (or most recent) job_start
/// command payload.
async fn start_command_tasks(hub: &Hub, auth: &AgentAuth) -> Vec<String> {
    hub.commands(auth.clone())
        .await
        .unwrap()
        .into_iter()
        .find(|command| command.operation == "job_start")
        .expect("job_start command")
        .payload
        .expect("job_start payload")["assignments"]
        .as_array()
        .map(|assignments| {
            let mut tasks: Vec<String> = assignments
                .iter()
                .filter_map(|assignment| assignment["task_id"].as_str().map(String::from))
                .collect();
            tasks.sort();
            tasks
        })
        .unwrap_or_default()
}

fn rebalance_job_spec_json(id: &str) -> String {
    let mut value: serde_json::Value = serde_json::from_str(&job_spec_json(id)).unwrap();
    value["rebalance"] =
        serde_json::json!({"mode": "auto", "pressure_streak": 2, "cooldown_ms": 0});
    value.to_string()
}

async fn report_resources(hub: &Hub, auth: &AgentAuth, used_ratio: f64, cpu: f64, report_seq: u64) {
    hub.report(NodeReport {
        auth: auth.clone(),
        version: "test".into(),
        state: "online".into(),
        capabilities: vec![],
        streams: vec![],
        operations: vec![],
        events: vec![],
        metrics: resource_metrics(used_ratio, cpu),
        jobs: BTreeMap::new(),
        configuration: None,
        configuration_version: None,
        boot_id: None,
        report_seq,
        config_versions: Vec::new(),
        job_tasks: BTreeMap::new(),
    })
    .await
    .unwrap();
}

async fn start_operations(hub: &Hub, job_id: &str) -> Vec<HubOperation> {
    hub.operations(None)
        .await
        .into_iter()
        .filter(|operation_record| {
            operation_record.resource_id == job_id && operation_record.operation == "job_start"
        })
        .collect()
}

async fn complete_start_commands(hub: &Hub, node_id: &str, session_token: &str) {
    let auth = AgentAuth {
        node_id: node_id.into(),
        session_token: session_token.into(),
    };
    for command in hub.commands(auth.clone()).await.unwrap() {
        hub.command_result(
            auth.clone(),
            CommandResult {
                command_id: command.id,
                operation_id: command.operation_id,
                state: HubOperationState::Succeeded,
                progress: 100,
                error: None,
                correlation_id: command.correlation_id,
                generation: command.generation,
                observed_generation: None,
                action_id: None,
                failure_class: None,
                config_version_id: command.config_version_id,
                rollout_id: command.rollout_id,
                observed_checkpoint_id: None,
                checkpoint_manifest_uri: None,
                result: None,
            },
        )
        .await
        .unwrap();
    }
}

/// One poll that both extracts the node's start task ids AND completes the
/// command: `commands()` leases on first delivery, so a test must not poll
/// twice (the second poll sees nothing and the start stays Dispatched).
async fn poll_start_tasks_and_complete(
    hub: &Hub,
    node_id: &str,
    session_token: &str,
) -> Vec<String> {
    let auth = AgentAuth {
        node_id: node_id.into(),
        session_token: session_token.into(),
    };
    let mut tasks = Vec::new();
    for command in hub.commands(auth.clone()).await.unwrap() {
        if command.operation == "job_start" {
            let mut command_tasks: Vec<String> = command
                .payload
                .as_ref()
                .expect("job_start payload")["assignments"]
                .as_array()
                .map(|assignments| {
                    assignments
                        .iter()
                        .filter_map(|assignment| assignment["task_id"].as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default();
            command_tasks.sort();
            tasks = command_tasks;
        }
        hub.command_result(
            auth.clone(),
            CommandResult {
                command_id: command.id,
                operation_id: command.operation_id,
                state: HubOperationState::Succeeded,
                progress: 100,
                error: None,
                correlation_id: command.correlation_id,
                generation: command.generation,
                observed_generation: None,
                action_id: None,
                failure_class: None,
                config_version_id: command.config_version_id,
                rollout_id: command.rollout_id,
                observed_checkpoint_id: None,
                checkpoint_manifest_uri: None,
                result: None,
            },
        )
        .await
        .unwrap();
    }
    tasks
}

#[test]
fn rank_candidates_is_deterministic_and_prefers_headroom() {
    let now = now_ms();
    let mut nodes = BTreeMap::new();
    for (id, used_ratio, cpu, reported) in [
        ("n-busy", 0.95, 90.0, true),
        ("n-free", 0.10, 5.0, true),
        ("n-mid", 0.50, 50.0, true),
        ("n-blind", 0.0, 0.0, false),
    ] {
        let mut record = NodeRecord {
            resource: HubNode {
                id: id.into(),
                protocol_version: "v1".into(),
                version: "test".into(),
                state: NodeConnectionState::Online,
                capabilities: vec![],
                last_seen_at_ms: now,
                lease_expires_at_ms: now + 1_000,
                streams_total: 0,
                streams_running: 0,
                streams_failed: 0,
                maintenance_state: NodeMaintenanceState::Active,
                data_address: None,
            },
            session_token: String::new(),
            session_expires_at_ms: now + 1_000,
            boot_id: Some("boot".into()),
            report_seq: 0,
            config_versions: Vec::new(),
            job_tasks: BTreeMap::new(),
            commands: VecDeque::new(),
            leased_commands: BTreeMap::new(),
            streams: vec![],
            operations: vec![],
            events: vec![],
            metrics: BTreeMap::new(),
            last_report_at_ms: if reported { now } else { 0 },
            pressure_streak: 0,
            jobs: BTreeMap::new(),
            configuration: None,
        };
        if reported {
            record.metrics = resource_metrics(used_ratio, cpu);
        }
        nodes.insert(id.to_string(), record);
    }
    let rank = |candidates: Vec<String>| {
        rank_candidates(candidates, &nodes, now, &NodeAllocations::new())
            .into_iter()
            .collect::<Vec<_>>()
    };
    let expected = vec![
        "n-free".to_string(),
        "n-mid".to_string(),
        "n-busy".to_string(),
        "n-blind".to_string(),
    ];
    assert_eq!(
        rank(vec![
            "n-blind".into(),
            "n-busy".into(),
            "n-free".into(),
            "n-mid".into()
        ]),
        expected
    );
    // Deterministic: the same input produces the same order.
    assert_eq!(
        rank(vec![
            "n-mid".into(),
            "n-blind".into(),
            "n-free".into(),
            "n-busy".into()
        ]),
        expected,
        "ranking must be a pure function of (candidates, gauges, ids)"
    );
}

#[tokio::test]
async fn first_placement_lands_on_the_higher_headroom_node() {
    let hub = Hub::new(config());
    let session_a = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot".into()),
        })
        .await
        .unwrap();
    let session_b = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-b".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot".into()),
        })
        .await
        .unwrap();
    let _ = (&session_a, &session_b);
    // node-a reports a fuller node; node-b has headroom and must win the
    // first placement even though node-a sorts first by id.
    report_resources(
        &hub,
        &AgentAuth {
            node_id: "node-a".into(),
            session_token: session_a.session_token.clone(),
        },
        0.9,
        10.0,
        1,
    )
    .await;
    report_resources(
        &hub,
        &AgentAuth {
            node_id: "node-b".into(),
            session_token: session_b.session_token.clone(),
        },
        0.1,
        10.0,
        1,
    )
    .await;
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: job_spec_json("orders"),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    let starts = start_operations(&hub, "orders").await;
    assert_eq!(starts.len(), 1);
    assert_eq!(starts[0].node_id, "node-b");
}

#[tokio::test]
async fn gauge_less_fleet_keeps_id_order_placement() {
    let hub = Hub::new(config());
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "node-a".into(),
        node_token: "node-secret".into(),
        protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
        capabilities: vec![],
        boot_id: Some("boot".into()),
    })
    .await
    .unwrap();
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "node-b".into(),
        node_token: "node-secret".into(),
        protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
        capabilities: vec![],
        boot_id: Some("boot".into()),
    })
    .await
    .unwrap();
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: job_spec_json("orders"),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    let starts = start_operations(&hub, "orders").await;
    assert_eq!(starts.len(), 1);
    assert_eq!(starts[0].node_id, "node-a", "no gauges: today's id order");
}

#[tokio::test]
async fn pressure_streak_counts_consecutive_pressuring_reports() {
    let hub = Hub::new(config());
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "n1".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot".into()),
        })
        .await
        .unwrap();
    let auth = AgentAuth {
        node_id: "n1".into(),
        session_token: session.session_token.clone(),
    };
    let streak = || async {
        hub.nodes
            .read()
            .await
            .get("n1")
            .expect("registered node")
            .pressure_streak
    };
    assert_eq!(streak().await, 0);
    report_resources(&hub, &auth, 0.95, 5.0, 1).await;
    assert_eq!(streak().await, 1, "one pressuring report");
    report_resources(&hub, &auth, 0.95, 50.0, 2).await;
    assert_eq!(streak().await, 2, "consecutive pressuring reports accrue");
    report_resources(&hub, &auth, 0.10, 5.0, 3).await;
    assert_eq!(streak().await, 0, "an under-threshold report resets");
    // A report without usable gauges also resets: no data, no pressure.
    hub.report(NodeReport {
        auth: auth.clone(),
        version: "test".into(),
        state: "online".into(),
        capabilities: vec![],
        streams: vec![],
        operations: vec![],
        events: vec![],
        metrics: resource_metrics(0.95, 5.0)
            .into_iter()
            .filter(|(key, _)| key != "node_memory_total_bytes")
            .collect(),
        jobs: BTreeMap::new(),
        configuration: None,
        configuration_version: None,
        boot_id: Some("boot".into()),
        report_seq: 4,
        config_versions: Vec::new(),
        job_tasks: BTreeMap::new(),
    })
    .await
    .unwrap();
    assert_eq!(streak().await, 0, "gauge-less report is not pressuring");
}

#[tokio::test]
async fn opt_in_pressure_rebalance_relocates_with_fencing() {
    let hub = Hub::new(config());
    let session_a = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot".into()),
        })
        .await
        .unwrap();
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "node-b".into(),
        node_token: "node-secret".into(),
        protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
        capabilities: vec![],
        boot_id: Some("boot".into()),
    })
    .await
    .unwrap();
    let auth_a = AgentAuth {
        node_id: "node-a".into(),
        session_token: session_a.session_token.clone(),
    };
    // node-a starts healthier and wins the first placement.
    report_resources(&hub, &auth_a, 0.1, 10.0, 1).await;
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: rebalance_job_spec_json("orders"),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    assert_eq!(start_operations(&hub, "orders").await[0].node_id, "node-a");
    complete_start_commands(&hub, "node-a", &session_a.session_token).await;
    assert_eq!(
        start_operations(&hub, "orders")
            .await
            .into_iter()
            .filter(|operation_record| operation_record.state == HubOperationState::Succeeded)
            .count(),
        1
    );
    // Sustained pressure: `pressure_streak` consecutive pressuring
    // reports from node-a.
    report_resources(&hub, &auth_a, 0.99, 5.0, 2).await;
    report_resources(&hub, &auth_a, 0.99, 5.0, 3).await;
    hub.reconcile_jobs().await.unwrap();
    let starts = start_operations(&hub, "orders").await;
    assert!(
        starts
            .iter()
            .any(|operation_record| operation_record.node_id == "node-a"
                && operation_record.state == HubOperationState::Superseded),
        "the abandoned node's start must be superseded: {starts:?}"
    );
    assert!(
        starts
            .iter()
            .any(|operation_record| operation_record.node_id == "node-b"
                && matches!(
                    operation_record.state,
                    HubOperationState::Queued
                        | HubOperationState::Dispatched
                        | HubOperationState::Acknowledged
                        | HubOperationState::Running
                        | HubOperationState::Succeeded
                )),
        "the Job must be re-placed onto the remaining target: {starts:?}"
    );
    let stops = hub
        .operations(None)
        .await
        .into_iter()
        .filter(|operation_record| {
            operation_record.resource_id == "orders"
                && operation_record.operation == "job_stop"
                && operation_record.node_id == "node-a"
        })
        .count();
    assert_eq!(stops, 1, "the abandoned node must receive a stop command");
}

#[tokio::test]
async fn default_off_policy_never_disturbs_a_pressured_placement() {
    let hub = Hub::new(config());
    let session_a = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot".into()),
        })
        .await
        .unwrap();
    let auth_a = AgentAuth {
        node_id: "node-a".into(),
        session_token: session_a.session_token.clone(),
    };
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: job_spec_json("orders"),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    complete_start_commands(&hub, "node-a", &session_a.session_token).await;
    // Sustained pressure without the opt-in: placement stays untouched.
    for seq in 1..=5 {
        report_resources(&hub, &auth_a, 0.99, 5.0, seq).await;
    }
    hub.reconcile_jobs().await.unwrap();
    let starts = start_operations(&hub, "orders").await;
    assert_eq!(starts.len(), 1);
    assert_eq!(starts[0].node_id, "node-a");
    assert_eq!(starts[0].state, HubOperationState::Succeeded);
    assert!(hub
        .operations(None)
        .await
        .iter()
        .all(|operation_record| operation_record.operation != "job_stop"));
}

#[tokio::test]
async fn single_node_fleet_skips_relocation_under_pressure() {
    let hub = Hub::new(config());
    let session_a = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot".into()),
        })
        .await
        .unwrap();
    let auth_a = AgentAuth {
        node_id: "node-a".into(),
        session_token: session_a.session_token.clone(),
    };
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: rebalance_job_spec_json("orders"),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    complete_start_commands(&hub, "node-a", &session_a.session_token).await;
    for seq in 1..=4 {
        report_resources(&hub, &auth_a, 0.99, 99.0, seq).await;
    }
    hub.reconcile_jobs().await.unwrap();
    let starts = start_operations(&hub, "orders").await;
    assert_eq!(starts.len(), 1);
    assert_eq!(starts[0].node_id, "node-a");
    assert_eq!(starts[0].state, HubOperationState::Succeeded);
    assert!(hub
        .operations(None)
        .await
        .iter()
        .all(|operation_record| operation_record.operation != "job_stop"));
}

#[tokio::test]
async fn stale_gauges_freeze_pressure_out_of_eviction() {
    let hub = Hub::new(config());
    let session_a = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot".into()),
        })
        .await
        .unwrap();
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "node-b".into(),
        node_token: "node-secret".into(),
        protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
        capabilities: vec![],
        boot_id: Some("boot".into()),
    })
    .await
    .unwrap();
    let auth_a = AgentAuth {
        node_id: "node-a".into(),
        session_token: session_a.session_token.clone(),
    };
    // node-a reports fresh gauges and wins the first placement.
    report_resources(&hub, &auth_a, 0.1, 10.0, 1).await;
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: rebalance_job_spec_json("orders"),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    assert_eq!(start_operations(&hub, "orders").await[0].node_id, "node-a");
    complete_start_commands(&hub, "node-a", &session_a.session_token).await;
    // Sustained pressure: the streak trips the policy threshold...
    report_resources(&hub, &auth_a, 0.99, 99.0, 2).await;
    report_resources(&hub, &auth_a, 0.99, 99.0, 3).await;
    // ...but then the node goes silent: the streak freezes high while
    // the gauges age past the freshness window. Frozen, stale data must
    // not drive relocation — otherwise a dead node would keep winning
    // evictions long after its pressure was last observed.
    {
        let mut nodes = hub.nodes.write().await;
        if let Some(node) = nodes.get_mut("node-a") {
            node.last_report_at_ms = now_ms().saturating_sub(RESOURCE_GAUGE_FRESH_MS + 1);
        }
    }
    hub.reconcile_jobs().await.unwrap();
    assert!(
        hub.operations(None)
            .await
            .iter()
            .all(|operation_record| operation_record.operation != "job_stop"),
        "a frozen streak on stale gauges must not evict the placement"
    );
    // One fresh pressuring report restores the streak's freshness: only
    // the gate was holding the eviction back.
    report_resources(&hub, &auth_a, 0.99, 99.0, 4).await;
    hub.reconcile_jobs().await.unwrap();
    assert_eq!(
        hub.operations(None)
            .await
            .into_iter()
            .filter(|operation_record| {
                operation_record.resource_id == "orders"
                    && operation_record.operation == "job_stop"
                    && operation_record.node_id == "node-a"
            })
            .count(),
        1,
        "the same streak on fresh gauges must evict the placement"
    );
}

/// Multi-component co-location is order sensitive: the component the
/// ranked order put on the head node must still be there when the
/// retained placement re-dispatches (e.g. after a version bump), instead
/// of drifting with BTreeSet iteration order.
#[tokio::test]
async fn retained_replacement_reproduces_the_dispatch_order() {
    let hub = Hub::new(config());
    let session_a = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot".into()),
        })
        .await
        .unwrap();
    let session_b = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-b".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot".into()),
        })
        .await
        .unwrap();
    let auth_a = AgentAuth {
        node_id: "node-a".into(),
        session_token: session_a.session_token.clone(),
    };
    let auth_b = AgentAuth {
        node_id: "node-b".into(),
        session_token: session_b.session_token.clone(),
    };
    // node-b reports more headroom and must win the head of the ranked
    // order despite sorting after node-a by id.
    report_resources(&hub, &auth_a, 0.9, 10.0, 1).await;
    report_resources(&hub, &auth_b, 0.1, 10.0, 1).await;

    let two_component_spec = |version: u64| {
        serde_json::json!({
                "id": "orders",
                "version": version,
                "placement": "colocated",
                "operators": [
                    {"id": "source-a", "kind": "source"},
                    {"id": "sink-a", "kind": "sink"},
                    {"id": "source-b", "kind": "source"},
                    {"id": "sink-b", "kind": "sink"}
                ],
                "edges": [
                    {"id": "e-a", "from": "source-a", "to": "sink-a"},
                    {"id": "e-b", "from": "source-b", "to": "sink-b"}
                ],
                "sources": [
                    {"operator_id": "source-a", "input_type": "memory", "time": {"mode": "processing_time"}},
                    {"operator_id": "source-b", "input_type": "memory", "time": {"mode": "processing_time"}}
                ],
                "sinks": [
                    {"operator_id": "sink-a", "output_type": "drop"},
                    {"operator_id": "sink-b", "output_type": "drop"}
                ]
            })
            .to_string()
    };

    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: two_component_spec(1),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    let first_on_b = start_command_tasks(&hub, &auth_b).await;
    let first_on_a = start_command_tasks(&hub, &auth_a).await;
    assert!(!first_on_b.is_empty() && !first_on_a.is_empty());
    assert!(
        first_on_b.iter().all(|task| !first_on_a.contains(task)),
        "components must be split across the two nodes"
    );
    complete_start_commands(&hub, "node-a", &session_a.session_token).await;
    complete_start_commands(&hub, "node-b", &session_b.session_token).await;

    // Version bump: same node set, same healthy state — the retained
    // placement must re-dispatch with the identical component mapping.
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 2,
        spec_json: two_component_spec(2),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 2,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    assert_eq!(
        start_command_tasks(&hub, &auth_b).await,
        first_on_b,
        "the head node must keep its components across re-dispatches"
    );
    assert_eq!(start_command_tasks(&hub, &auth_a).await, first_on_a);
}

/// Eviction must not stop the live placement when the remaining target
/// set cannot host it: split validation runs BEFORE any fencing, so an
/// incapable survivor node turns the eviction into a no-op retry instead
/// of an outage.
#[tokio::test]
async fn eviction_keeps_the_placement_when_survivors_cannot_host_split() {
    let hub = Hub::new(config());
    let mut sessions = BTreeMap::new();
    // Two shuffle-capable nodes place the Job first.
    for node_id in ["node-a", "node-b"] {
        let session = hub
            .register(RegisterRequest {
                data_address: Some(format!("{node_id}:9100")),
                node_id: node_id.into(),
                node_token: "node-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: shuffle_capabilities(),
                boot_id: Some("boot".into()),
            })
            .await
            .unwrap();
        sessions.insert(node_id.to_string(), session.session_token.clone());
    }
    let auth_a = AgentAuth {
        node_id: "node-a".into(),
        session_token: sessions["node-a"].clone(),
    };
    let auth_b = AgentAuth {
        node_id: "node-b".into(),
        session_token: sessions["node-b"].clone(),
    };
    report_shuffle_node(&hub, &auth_a, 0.1, 10.0, 1).await;
    report_shuffle_node(&hub, &auth_b, 0.2, 10.0, 1).await;

    let mut spec: serde_json::Value = serde_json::from_str(&job_spec_json("orders")).unwrap();
    spec["placement"] = serde_json::json!("split");
    spec["rebalance"] = serde_json::json!({"mode": "auto", "pressure_streak": 2, "cooldown_ms": 0});
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: spec.to_string(),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    for (node_id, token) in &sessions {
        complete_start_commands(&hub, node_id, token).await;
    }
    assert_eq!(
        start_operations(&hub, "orders")
            .await
            .into_iter()
            .filter(|operation_record| operation_record.state == HubOperationState::Succeeded)
            .count(),
        2,
        "both shuffle nodes hold a succeeded start"
    );

    // A plain node without the data plane joins the fleet.
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "node-plain".into(),
        node_token: "node-secret".into(),
        protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
        capabilities: vec![],
        boot_id: Some("boot".into()),
    })
    .await
    .unwrap();

    // Sustained pressure on node-a trips the eviction. The incremental
    // re-placement never lets the incapable node into the target set: the
    // failed slot concentrates onto the capable survivor instead, the
    // placement survives, and the evicted node is fenced.
    report_shuffle_node(&hub, &auth_a, 0.99, 5.0, 2).await;
    report_shuffle_node(&hub, &auth_a, 0.99, 5.0, 3).await;
    let job_record = hub
        .jobs()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.job_id == "orders")
        .expect("job record");
    hub.reconcile_job(&job_record).await.unwrap();
    // node-a's successful start is superseded and it receives a stop.
    let starts = start_operations(&hub, "orders").await;
    assert!(
        starts.iter().any(|operation_record| {
            operation_record.node_id == "node-a"
                && operation_record.state == HubOperationState::Superseded
        }),
        "the evicted node's start must be superseded: {starts:?}"
    );
    assert!(
        hub.operations(None)
            .await
            .iter()
            .any(|operation_record| operation_record.node_id == "node-a"
                && operation_record.operation == "job_stop"),
        "the evicted node must receive a stop command"
    );
    // node-plain never entered the target set: it holds no start of the
    // placement.
    assert!(
        !starts
            .iter()
            .any(|operation_record| operation_record.node_id == "node-plain"),
        "the incapable node must never host the split placement: {starts:?}"
    );
}

/// A reconcile whose evicted target set fails validation must not
/// overwrite the remembered dispatch order: the never-dispatched order
/// would corrupt retention for the still-live placement (mapping flip on
/// the next real re-dispatch).
#[tokio::test]
async fn failed_validation_does_not_corrupt_the_remembered_dispatch_order() {
    let hub = Hub::new(config());
    let mut sessions = BTreeMap::new();
    for node_id in ["node-a", "node-b"] {
        let session = hub
            .register(RegisterRequest {
                data_address: Some(format!("{node_id}:9100")),
                node_id: node_id.into(),
                node_token: "node-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: shuffle_capabilities(),
                boot_id: Some("boot".into()),
            })
            .await
            .unwrap();
        sessions.insert(node_id.to_string(), session.session_token.clone());
    }
    let auth_a = AgentAuth {
        node_id: "node-a".into(),
        session_token: sessions["node-a"].clone(),
    };
    let auth_b = AgentAuth {
        node_id: "node-b".into(),
        session_token: sessions["node-b"].clone(),
    };
    // node-a has the most headroom and must take the head of the ranked
    // order: the split round-robin maps task 0 (source) onto node-a.
    report_shuffle_node(&hub, &auth_a, 0.1, 10.0, 1).await;
    report_shuffle_node(&hub, &auth_b, 0.3, 10.0, 1).await;

    let mut spec: serde_json::Value = serde_json::from_str(&job_spec_json("orders")).unwrap();
    spec["placement"] = serde_json::json!("split");
    spec["rebalance"] = serde_json::json!({"mode": "auto", "pressure_streak": 2, "cooldown_ms": 0});
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: spec.to_string(),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    let first_on_a = start_command_tasks(&hub, &auth_a).await;
    let first_on_b = start_command_tasks(&hub, &auth_b).await;
    assert_eq!(first_on_a.len(), 1, "one task per node: {first_on_a:?}");
    assert_eq!(first_on_b.len(), 1, "one task per node: {first_on_b:?}");
    assert_ne!(first_on_a, first_on_b);
    for (node_id, token) in &sessions {
        complete_start_commands(&hub, node_id, token).await;
    }

    // A plain node joins; both capable nodes of the placement go into
    // maintenance: the reconcile fails split validation (no fencing, no
    // dispatch) — and must not record that never-dispatched order either.
    // Maintenance (not lease expiry) keeps the successful starts intact,
    // unlike a stale sweep which settles their operations.
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "node-plain".into(),
        node_token: "node-secret".into(),
        protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
        capabilities: vec![],
        boot_id: Some("boot".into()),
    })
    .await
    .unwrap();
    {
        let mut nodes = hub.nodes.write().await;
        for node_id in ["node-a", "node-b"] {
            if let Some(node) = nodes.get_mut(node_id) {
                node.resource.maintenance_state = NodeMaintenanceState::Maintenance;
            }
        }
    }
    let job_record = hub
        .jobs()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.job_id == "orders")
        .expect("job record");
    assert!(hub.reconcile_job(&job_record).await.is_err());

    // The maintained nodes return; the placement is retained in its
    // original order, so the version bump re-dispatches the same mapping.
    {
        let mut nodes = hub.nodes.write().await;
        for node_id in ["node-a", "node-b"] {
            if let Some(node) = nodes.get_mut(node_id) {
                node.resource.maintenance_state = NodeMaintenanceState::Active;
            }
        }
    }
    let mut bumped: serde_json::Value = serde_json::from_str(&job_spec_json("orders")).unwrap();
    bumped["placement"] = serde_json::json!("split");
    bumped["rebalance"] =
        serde_json::json!({"mode": "auto", "pressure_streak": 2, "cooldown_ms": 0});
    bumped["version"] = serde_json::json!(2);
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 2,
        spec_json: bumped.to_string(),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 2,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    assert_eq!(
        start_command_tasks(&hub, &auth_a).await,
        first_on_a,
        "the retained order must be the original dispatch order, not the failed tick's"
    );
    assert_eq!(start_command_tasks(&hub, &auth_b).await, first_on_b);
}

/// The checkpoint command payload re-derives split assignments; they
/// must match the live placement's dispatch order (Agents ignore the
/// payload assignments today, but they must not lie).
#[tokio::test]
async fn checkpoint_payload_assignments_match_the_live_mapping() {
    let hub = Hub::new(config());
    let mut sessions = BTreeMap::new();
    for node_id in ["node-a", "node-b"] {
        let session = hub
            .register(RegisterRequest {
                data_address: Some(format!("{node_id}:9100")),
                node_id: node_id.into(),
                node_token: "node-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: shuffle_capabilities(),
                boot_id: Some("boot".into()),
            })
            .await
            .unwrap();
        sessions.insert(node_id.to_string(), session.session_token.clone());
    }
    let auth_a = AgentAuth {
        node_id: "node-a".into(),
        session_token: sessions["node-a"].clone(),
    };
    let auth_b = AgentAuth {
        node_id: "node-b".into(),
        session_token: sessions["node-b"].clone(),
    };
    // node-b has the headroom lead: the ranked dispatch order is
    // [node-b, node-a], the opposite of id order.
    report_shuffle_node(&hub, &auth_a, 0.3, 10.0, 1).await;
    report_shuffle_node(&hub, &auth_b, 0.1, 10.0, 1).await;

    let mut spec: serde_json::Value = serde_json::from_str(&job_spec_json("orders")).unwrap();
    spec["placement"] = serde_json::json!("split");
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: spec.to_string(),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    let start_on_a = start_command_tasks(&hub, &auth_a).await;
    let start_on_b = start_command_tasks(&hub, &auth_b).await;
    assert_eq!(start_on_a.len(), 1);
    assert_eq!(start_on_b.len(), 1);
    assert_ne!(
        start_on_a, start_on_b,
        "ranked order [b, a] must swap the mapping"
    );
    for (node_id, token) in &sessions {
        complete_start_commands(&hub, node_id, token).await;
    }

    hub.record_job_checkpoint(JobCheckpointRecord {
        job_id: "orders".into(),
        job_version: 1,
        checkpoint_id: "checkpoint-1".into(),
        kind: "checkpoint".into(),
        status: "completed".into(),
        manifest_uri: None,
        format_version: 1,
        created_at_ms: now_ms(),
        updated_at_ms: now_ms(),
    })
    .await
    .unwrap();
    for (auth, start_tasks) in [(&auth_a, &start_on_a), (&auth_b, &start_on_b)] {
        let checkpoint_tasks = hub
            .commands(auth.clone())
            .await
            .unwrap()
            .into_iter()
            .find(|command| command.operation == "job_checkpoint")
            .expect("checkpoint command")
            .payload
            .expect("checkpoint payload")["assignments"]
            .as_array()
            .map(|assignments| {
                let mut tasks: Vec<String> = assignments
                    .iter()
                    .filter_map(|assignment| assignment["task_id"].as_str().map(String::from))
                    .collect();
                tasks.sort();
                tasks
            })
            .unwrap_or_default();
        assert_eq!(
            &checkpoint_tasks, start_tasks,
            "checkpoint assignments must match the live dispatch mapping"
        );
    }
}

#[tokio::test]
async fn pinned_placement_rejects_auto_rebalance() {
    let hub = Hub::new(config());
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "node-a".into(),
        node_token: "node-secret".into(),
        protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
        capabilities: vec![],
        boot_id: Some("boot".into()),
    })
    .await
    .unwrap();
    let error = hub
        .upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json: rebalance_job_spec_json("orders"),
            desired_state: "running".into(),
            observed_state: "validated".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: vec!["node-a".into()],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap_err();
    assert!(matches!(error, HubError::Invalid(_)));
}

#[tokio::test]
async fn rebalance_cooldown_blocks_a_move_inside_the_window() {
    let hub = Hub::new(config());
    let session_a = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot".into()),
        })
        .await
        .unwrap();
    let auth_a = AgentAuth {
        node_id: "node-a".into(),
        session_token: session_a.session_token.clone(),
    };
    // Same auto policy but with a cooldown far in the future relative to
    // the fresh placement.
    let mut spec: serde_json::Value = serde_json::from_str(&job_spec_json("orders")).unwrap();
    spec["rebalance"] =
        serde_json::json!({"mode": "auto", "pressure_streak": 1, "cooldown_ms": 3_600_000});
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: spec.to_string(),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    complete_start_commands(&hub, "node-a", &session_a.session_token).await;
    // Pressure trip inside the cooldown window: the placement holds.
    for seq in 1..=3 {
        report_resources(&hub, &auth_a, 0.99, 99.0, seq).await;
    }
    hub.reconcile_jobs().await.unwrap();
    let starts = start_operations(&hub, "orders").await;
    assert_eq!(starts.len(), 1);
    assert_eq!(starts[0].node_id, "node-a");
    assert_eq!(starts[0].state, HubOperationState::Succeeded);
    assert!(hub
        .operations(None)
        .await
        .iter()
        .all(|operation_record| operation_record.operation != "job_stop"));
}

/// Partial node failure: only the failed node's tasks move. A replacement
/// candidate takes the failed slot IN the remembered dispatch order, so
/// every surviving node's task set stays byte-identical (no restart, no
/// re-dispatch for survivors), and the failed node is fenced.
#[tokio::test]
async fn partial_node_failure_moves_only_the_failed_tasks() {
    let hub = Hub::new(config());
    let mut sessions = BTreeMap::new();
    for (index, node_id) in ["node-a", "node-b", "node-c", "node-d"]
        .into_iter()
        .enumerate()
    {
        let session = hub
            .register(RegisterRequest {
                data_address: Some(format!("{node_id}:9100")),
                node_id: node_id.into(),
                node_token: "node-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: shuffle_capabilities(),
                boot_id: Some("boot".into()),
            })
            .await
            .unwrap();
        sessions.insert(node_id.to_string(), session.session_token.clone());
        // Distinct memory usage produces a deterministic ranked order:
        // a > b > c > d.
        let auth = AgentAuth {
            node_id: node_id.into(),
            session_token: sessions[node_id].clone(),
        };
        report_shuffle_node(&hub, &auth, 0.1 + 0.1 * index as f64, 10.0, 1).await;
    }

    let mut spec: serde_json::Value = serde_json::from_str(&job_spec_json("orders")).unwrap();
    spec["placement"] = serde_json::json!("split");
    spec["parallelism"] = serde_json::json!(2);
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: spec.to_string(),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();

    let mut initial = BTreeMap::new();
    for node_id in ["node-a", "node-b", "node-c", "node-d"] {
        let tasks = poll_start_tasks_and_complete(&hub, node_id, &sessions[node_id]).await;
        assert_eq!(tasks.len(), 1, "one task per node: {tasks:?}");
        initial.insert(node_id.to_string(), tasks);
    }

    // A fresh shuffle node joins the fleet after the dispatch: it is the
    // replacement candidate for any failed slot.
    let session_e = hub
        .register(RegisterRequest {
            data_address: Some("node-e:9100".into()),
            node_id: "node-e".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: shuffle_capabilities(),
            boot_id: Some("boot".into()),
        })
        .await
        .unwrap();
    sessions.insert("node-e".to_string(), session_e.session_token.clone());
    report_shuffle_node(
        &hub,
        &AgentAuth {
            node_id: "node-e".into(),
            session_token: sessions["node-e"].clone(),
        },
        0.9,
        10.0,
        1,
    )
    .await;

    // node-c fails; node-e is the replacement candidate.
    hub.nodes
        .write()
        .await
        .get_mut("node-c")
        .unwrap()
        .resource
        .maintenance_state = NodeMaintenanceState::Maintenance;
    let job_record = hub
        .jobs()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.job_id == "orders")
        .expect("job record");
    hub.reconcile_job(&job_record).await.unwrap();

    // Survivors: no new start command (their assignments are unchanged) and
    // their successful starts stay satisfied.
    for node_id in ["node-a", "node-b", "node-d"] {
        let auth = AgentAuth {
            node_id: node_id.into(),
            session_token: sessions[node_id].clone(),
        };
        let commands = hub.commands(auth).await.unwrap();
        assert!(
            !commands
                .iter()
                .any(|command| command.operation == "job_start"),
            "survivor {node_id} must not be re-dispatched"
        );
    }
    // The replacement inherits exactly the failed node's task.
    let inherited = poll_start_tasks_and_complete(&hub, "node-e", &sessions["node-e"]).await;
    assert_eq!(
        inherited, initial["node-c"],
        "the replacement node takes over exactly the failed node's task"
    );
    // The failed node is fenced: its start is superseded (the stop command
    // itself is only deliverable once the node is reachable again — an
    // unreachable node accepts no commands by design).
    let starts = start_operations(&hub, "orders").await;
    assert!(starts.iter().any(|operation| {
        operation.node_id == "node-c" && operation.state == HubOperationState::Superseded
    }));
}

/// With no replacement candidate the failed slot concentrates onto a
/// surviving node (keeping the list length — and every other mapping —
/// stable) instead of reshuffling all tasks across the reduced set.
#[tokio::test]
async fn no_replacement_candidate_concentrates_the_failed_slot() {
    let hub = Hub::new(config());
    let mut sessions = BTreeMap::new();
    for (index, node_id) in ["node-a", "node-b"].into_iter().enumerate() {
        let session = hub
            .register(RegisterRequest {
                data_address: Some(format!("{node_id}:9100")),
                node_id: node_id.into(),
                node_token: "node-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: shuffle_capabilities(),
                boot_id: Some("boot".into()),
            })
            .await
            .unwrap();
        sessions.insert(node_id.to_string(), session.session_token.clone());
        let auth = AgentAuth {
            node_id: node_id.into(),
            session_token: sessions[node_id].clone(),
        };
        report_shuffle_node(&hub, &auth, 0.1 + 0.1 * index as f64, 10.0, 1).await;
    }

    let mut spec: serde_json::Value = serde_json::from_str(&job_spec_json("orders")).unwrap();
    spec["placement"] = serde_json::json!("split");
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: spec.to_string(),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();

    let on_a = poll_start_tasks_and_complete(&hub, "node-a", &sessions["node-a"]).await;
    let on_b = poll_start_tasks_and_complete(&hub, "node-b", &sessions["node-b"]).await;
    assert_eq!(on_a.len(), 1);
    assert_eq!(on_b.len(), 1);

    // node-b fails with no candidate available: node-a duplicates into the
    // slot, which also drifts its own assignment — the stale start is
    // superseded and a fresh start with the combined task set dispatches.
    hub.nodes
        .write()
        .await
        .get_mut("node-b")
        .unwrap()
        .resource
        .maintenance_state = NodeMaintenanceState::Maintenance;
    let job_record = hub
        .jobs()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.job_id == "orders")
        .expect("job record");
    hub.reconcile_job(&job_record).await.unwrap();

    let mut combined = on_a.clone();
    combined.extend(on_b.iter().cloned());
    combined.sort();
    let mut replacement = poll_start_tasks_and_complete(&hub, "node-a", &sessions["node-a"]).await;
    replacement.sort();
    assert_eq!(
        replacement, combined,
        "the survivor must receive both slots' tasks"
    );
    let starts = start_operations(&hub, "orders").await;
    assert!(starts.iter().any(|operation| {
        operation.node_id == "node-b" && operation.state == HubOperationState::Superseded
    }));
}

/// A Succeeded start whose recorded assignment fingerprint is gone (a Hub
/// restart wiped the memory) is superseded and re-dispatched once with the
/// same assignment; after the confirmation completes, the skip holds again.
#[tokio::test]
async fn lost_fingerprint_memory_supersedes_and_redispatches_once() {
    let hub = Hub::new(config());
    let mut sessions = BTreeMap::new();
    for node_id in ["node-a", "node-b"] {
        let session = hub
            .register(RegisterRequest {
                data_address: Some(format!("{node_id}:9100")),
                node_id: node_id.into(),
                node_token: "node-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: shuffle_capabilities(),
                boot_id: Some("boot".into()),
            })
            .await
            .unwrap();
        sessions.insert(node_id.to_string(), session.session_token.clone());
    }
    let mut spec: serde_json::Value = serde_json::from_str(&job_spec_json("orders")).unwrap();
    spec["placement"] = serde_json::json!("split");
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: spec.to_string(),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    let auth_a = AgentAuth {
        node_id: "node-a".into(),
        session_token: sessions["node-a"].clone(),
    };
    let original = poll_start_tasks_and_complete(&hub, "node-a", &sessions["node-a"]).await;
    poll_start_tasks_and_complete(&hub, "node-b", &sessions["node-b"]).await;

    // Simulate the Hub restart: the fingerprint memory (and the order
    // memory) are gone.
    hub.start_dispatch_fingerprints.write().await.clear();
    hub.placement_order.write().await.remove("orders");
    let job_record = hub
        .jobs()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.job_id == "orders")
        .expect("job record");
    hub.reconcile_job(&job_record).await.unwrap();

    let starts = start_operations(&hub, "orders").await;
    assert!(
        starts.iter().any(|operation| {
            operation.node_id == "node-a" && operation.state == HubOperationState::Superseded
        }),
        "the unprovable start must be superseded: {starts:?}"
    );
    // The re-dispatch carries the same task (sorted previous-node order is
    // deterministic), and completing it restores the steady-state skip.
    assert_eq!(
        poll_start_tasks_and_complete(&hub, "node-a", &sessions["node-a"]).await,
        original,
        "the confirmation re-dispatch keeps the assignment stable"
    );
    poll_start_tasks_and_complete(&hub, "node-b", &sessions["node-b"]).await;
    hub.reconcile_job(&job_record).await.unwrap();
    let commands = hub.commands(auth_a).await.unwrap();
    assert!(
        !commands
            .iter()
            .any(|command| command.operation == "job_start"),
        "after the confirmation the dispatch skip holds again"
    );
}

/// A failed runtime observation invalidates the node's Succeeded start so
/// the reconciler re-dispatches it (a crashed kernel no longer waits for an
/// operator-driven generation bump).
#[tokio::test]
async fn failed_observation_redispatches_the_nodes_start() {
    let hub = Hub::new(config());
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot".into()),
        })
        .await
        .unwrap();
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: job_spec_json("orders"),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    let auth = AgentAuth {
        node_id: "node-a".into(),
        session_token: session.session_token.clone(),
    };
    poll_start_tasks_and_complete(&hub, "node-a", &session.session_token).await;
    // Steady state: the succeeded start satisfies the skip.
    let job_record = hub
        .jobs()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.job_id == "orders")
        .expect("job record");
    hub.reconcile_job(&job_record).await.unwrap();
    assert!(!hub
        .commands(auth.clone())
        .await
        .unwrap()
        .iter()
        .any(|command| command.operation == "job_start"));

    // The kernel dies (for example a remote edge exhausted its reconnect
    // budget) and the agent reports the failure.
    hub.report_job_observation(JobObservationRequest {
        auth: auth.clone(),
        job_id: "orders".into(),
        generation: 1,
        state: "failed".into(),
        error: Some("remote edge failed".into()),
    })
    .await
    .unwrap();
    let starts = start_operations(&hub, "orders").await;
    assert!(
        starts.iter().any(|operation| {
            operation.state == HubOperationState::TimedOut
                && operation.failure_class.as_deref() == Some("recovery_required")
        }),
        "the succeeded start must be settled for re-dispatch: {starts:?}"
    );
    hub.reconcile_job(&job_record).await.unwrap();
    assert!(
        hub.commands(auth)
            .await
            .unwrap()
            .iter()
            .any(|command| command.operation == "job_start"),
        "the reconcile must re-dispatch the start after the runtime failure"
    );
}

/// A Job declaring more CPU than any node's declared capacity surfaces an
/// explicit insufficient-capacity error instead of stacking onto the fleet.
#[tokio::test]
async fn declared_cpu_beyond_capacity_fails_explicitly() {
    let hub = Hub::new(config());
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot".into()),
        })
        .await
        .unwrap();
    let auth = AgentAuth {
        node_id: "node-a".into(),
        session_token: session.session_token.clone(),
    };
    report_shuffle_node(&hub, &auth, 0.1, 10.0, 1).await;
    // 2 cores = 2000 millicores; the colocated Job's two tasks request
    // 1500 each = 3000 total.
    let mut spec: serde_json::Value = serde_json::from_str(&job_spec_json("orders")).unwrap();
    spec["resources"] = serde_json::json!({"cpu_millicores": 1500});
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: spec.to_string(),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap_err();
}

/// A declared Job fits a node whose remaining declared capacity covers its
/// share, and ranking prefers the node with more EFFECTIVE headroom when
/// raw gauges are equal.
#[tokio::test]
async fn declared_job_lands_on_effective_headroom() {
    let hub = Hub::new(config());
    let mut sessions = BTreeMap::new();
    for node_id in ["node-a", "node-b"] {
        let session = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: node_id.into(),
                node_token: "node-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: vec![],
                boot_id: Some("boot".into()),
            })
            .await
            .unwrap();
        sessions.insert(node_id.to_string(), session.session_token.clone());
    }
    // Identical gauges: 2 cores, low usage, plenty of memory.
    report_shuffle_node(
        &hub,
        &AgentAuth {
            node_id: "node-a".into(),
            session_token: sessions["node-a"].clone(),
        },
        0.1,
        10.0,
        1,
    )
    .await;
    report_shuffle_node(
        &hub,
        &AgentAuth {
            node_id: "node-b".into(),
            session_token: sessions["node-b"].clone(),
        },
        0.1,
        10.0,
        1,
    )
    .await;

    // A first declared Job lands on node-a (node-id tie-break) and holds
    // 1600 of node-a's 2000 millicores.
    let mut heavy: serde_json::Value = serde_json::from_str(&job_spec_json("heavy")).unwrap();
    heavy["resources"] = serde_json::json!({"cpu_millicores": 800});
    hub.upsert_job(JobRecord {
        job_id: "heavy".into(),
        version: 1,
        spec_json: heavy.to_string(),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    let heavy_tasks = poll_start_tasks_and_complete(&hub, "node-a", &sessions["node-a"]).await;
    assert_eq!(
        heavy_tasks.len(),
        2,
        "the colocated job lands whole: {heavy_tasks:?}"
    );

    // The second declared Job needs 400 millicores: node-a's effective
    // headroom is 400 millicores and its effective memory ratio dropped,
    // so node-b ranks first and the job lands there.
    let mut light: serde_json::Value = serde_json::from_str(&job_spec_json("light")).unwrap();
    light["resources"] = serde_json::json!({"cpu_millicores": 200});
    hub.upsert_job(JobRecord {
        job_id: "light".into(),
        version: 1,
        spec_json: light.to_string(),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    let light_tasks = poll_start_tasks_and_complete(&hub, "node-b", &sessions["node-b"]).await;
    assert_eq!(
        light_tasks.len(),
        2,
        "the light job lands whole on node-b: {light_tasks:?}"
    );
}

/// Undeclared Jobs are never gated: the same loaded node still receives
/// them exactly as before.
#[tokio::test]
async fn undeclared_jobs_bypass_the_resource_gate() {
    let hub = Hub::new(config());
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot".into()),
        })
        .await
        .unwrap();
    let auth = AgentAuth {
        node_id: "node-a".into(),
        session_token: session.session_token.clone(),
    };
    report_shuffle_node(&hub, &auth, 0.99, 99.0, 1).await;
    // No resources declared: places onto the (only, fully used) node as
    // before — the gate applies only to declared Jobs.
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: job_spec_json("orders"),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    let tasks = poll_start_tasks_and_complete(&hub, "node-a", &session.session_token).await;
    assert!(!tasks.is_empty());
}
