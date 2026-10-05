//! Agent session: registration with Hub failover, the main run loop, the
//! session loop (heartbeats, reports, command polling), and the completed
//! command cache.
use super::commands::{agent_auth_query, bearer_auth, execute_command, post_json, send_result};
use super::config::{agent_capabilities, NodeAgentConfig};
use super::kernel::JobRuntime;
use super::now_ms;
use super::resources::{
    data_plane_tls_from_env, merge_resource_gauges, spawn_resource_sampler, ResourceSampler,
};
use crate::hub::{
    AgentAuth, AgentCommand, CommandResult, HeartbeatRequest, NodeReport, RegisterRequest,
    RegisterResponse,
};
use arkflow_core::configuration::redacted_config;
use arkflow_core::control_plane::ControlPlane;
use reqwest::Client;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub async fn run(
    cp: ControlPlane,
    config: NodeAgentConfig,
    cancellation: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut backoff = Duration::from_millis(250);
    // Consecutive registration failures since the last success; a full cycle
    // across every candidate is what escalates to the exponential backoff.
    let mut failed_attempts: usize = 0;
    let mut completed_commands = CompletedCommandCache::new(1024);
    let mut job_runtime = JobRuntime::default();
    // Host resource gauges: sampled on an interval derived from the report
    // cadence for the whole process lifetime, so re-registration churn
    // never resets the view.
    let resource_sampler = spawn_resource_sampler(config.report_interval, cancellation.clone());
    // Cross-node shuffle data plane: one listener per Agent process. A bind
    // failure degrades to the co-location contract (warn, no listener) rather
    // than blocking node startup — observability and placement still work.
    let mut data_address: Option<String> = None;
    if let Some(port) = config.data_port {
        let data_secret = std::env::var("ARKFLOW_DATA_PLANE_SECRET")
            .ok()
            .filter(|secret| !secret.is_empty())
            .or_else(|| (!config.node_token.is_empty()).then(|| config.node_token.clone()));
        let data_plane_tls = data_plane_tls_from_env()?;
        let manager = data_secret
            .and_then(|data_secret| {
                match arkflow_core::executor::remote::DataPlaneCredentials::new(
                    config.node_id.clone(),
                    data_secret,
                ) {
                    Ok(credentials) => Some(credentials),
                    Err(error) => {
                        warn!(node_id = %config.node_id, %error, "invalid data-plane credentials; running colocated-only");
                        None
                    }
                }
            })
            .and_then(|credentials| {
                let manager_config = arkflow_core::executor::remote::NetworkManagerConfig {
                    credentials: Some(credentials),
                    channel_capacity: 1024,
                    tls: data_plane_tls.clone(),
                    .. arkflow_core::executor::remote::NetworkManagerConfig::default()
                };
                match arkflow_core::executor::remote::NetworkManager::with_config(
                    manager_config,
                ) {
                    Ok(manager) => Some(manager),
                    Err(error) => {
                        warn!(node_id = %config.node_id, %error, "invalid data-plane resource configuration; running colocated-only");
                        None
                    }
                }
            });
        if let Some(manager) = manager {
            manager.spawn();
            match config
                .data_host
                .as_deref()
                .unwrap_or("127.0.0.1")
                .parse::<std::net::IpAddr>()
            {
                Ok(bind_host) => match manager
                    .bind_tcp(std::net::SocketAddr::from((bind_host, port)))
                    .await
                {
                    Ok(bound) => {
                        data_address = config
                            .data_host
                            .as_ref()
                            .map(|host| format!("{host}:{bound}"));
                        match &data_address {
                            Some(address) => info!(
                                node_id = %config.node_id,
                                %address,
                                "Network shuffle data plane listening"
                            ),
                            None => warn!(
                                node_id = %config.node_id,
                                port = bound,
                                "data plane bound without data_host; the node stays colocated-only"
                            ),
                        }
                        job_runtime.data_plane = Some(manager);
                    }
                    Err(error) => {
                        manager.shutdown();
                        warn!(node_id = %config.node_id, %error, "data plane bind failed; running without network shuffle");
                    }
                },
                Err(error) => {
                    manager.shutdown();
                    warn!(node_id = %config.node_id, %error, "data_host must be a bindable IP address; running colocated-only");
                }
            }
        }
    }
    let network_shuffle = job_runtime.data_plane.is_some();
    // hub-ha stage 3 failover: the queue's front is the next candidate. A
    // successful registration pins its address at the front; a standby 503
    // rotates immediately; a standby's `leader_url` hint jumps the queue.
    let mut candidates: std::collections::VecDeque<String> = if config.hub_urls.is_empty() {
        std::iter::once(config.hub_url.clone()).collect()
    } else {
        config.hub_urls.iter().cloned().collect()
    };
    let failover_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut active_config = config.clone();
    let mut client = build_agent_client(&active_config.hub_url)?;
    // Reason token for the next switch's audit log, carried over from the
    // failure that triggered the rotation.
    let mut pending_reason: &'static str = "transport_error";
    loop {
        if cancellation.is_cancelled() {
            job_runtime.stop_all().await;
            if let Some(manager) = &job_runtime.data_plane {
                manager.shutdown();
            }
            return Ok(());
        }
        let Some(next) = candidates.front().cloned() else {
            return Err("no hub candidate addresses configured".into());
        };
        if next != active_config.hub_url {
            info!(
                node_id = %config.node_id,
                from = %active_config.hub_url,
                to = %next,
                reason = pending_reason,
                "Switching control-plane Hub candidate"
            );
            client = build_agent_client(&next)?;
            active_config.hub_url = next;
            failover_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        match register(&client, &active_config, data_address.clone()).await {
            Ok(session) => {
                info!(node_id = %config.node_id, hub = %active_config.hub_url, "Compute node registered with control-plane Hub");
                backoff = Duration::from_millis(250);
                failed_attempts = 0;
                if let Err(error) = run_session(
                    &client,
                    &cp,
                    &active_config,
                    session,
                    cancellation.clone(),
                    &mut completed_commands,
                    job_runtime.clone(),
                    network_shuffle,
                    &resource_sampler,
                    &failover_counter,
                )
                .await
                {
                    warn!(node_id = %config.node_id, error = %error, "Hub Agent session ended; reconnecting");
                }
                // The winner stays at the front: reconnects prefer the Hub
                // that last accepted a registration. Demotion or death of
                // that Hub surfaces as a Standby/Transport failure below and
                // rotates from there.
            }
            Err(RegisterFailure::Standby { leader_url }) => {
                warn!(
                    node_id = %config.node_id,
                    hub = %active_config.hub_url,
                    "Hub is a standby; advancing to the next candidate"
                );
                match leader_url {
                    // A trusted standby hint points straight at the elected
                    // leader — one probe instead of a full scan.
                    Some(leader) => {
                        pending_reason = "leader_hint";
                        jump_to_candidate(&mut candidates, leader);
                    }
                    None => {
                        pending_reason = "standby_advance";
                        rotate_candidates(&mut candidates);
                    }
                }
                failed_attempts += 1;
            }
            Err(RegisterFailure::Transport(message)) => {
                warn!(
                    node_id = %config.node_id,
                    hub = %active_config.hub_url,
                    error = %message,
                    "Hub Agent registration failed"
                );
                pending_reason = "transport_error";
                rotate_candidates(&mut candidates);
                failed_attempts += 1;
            }
        }
        // A full failed cycle across every candidate triggers the jittered
        // exponential backoff; within a cycle candidates rotate after only a
        // short fixed pause (a standby 503 must not burn the backoff).
        let cycle_len = candidates.len().max(1);
        let sleep_duration = if failed_attempts > 0 && failed_attempts.is_multiple_of(cycle_len) {
            let current = backoff;
            backoff = (backoff * 2).min(Duration::from_secs(10));
            jittered_backoff(current)
        } else {
            Duration::from_millis(200)
        };
        tokio::select! {
            _ = cancellation.cancelled() => {
                job_runtime.stop_all().await;
                if let Some(manager) = &job_runtime.data_plane {
                    manager.shutdown();
                }
                return Ok(())
            },
            _ = tokio::time::sleep(sleep_duration) => {}
        }
    }
}

/// Rotate the failover queue: the failed front candidate moves to the back.
pub(super) fn rotate_candidates(candidates: &mut std::collections::VecDeque<String>) {
    if let Some(front) = candidates.pop_front() {
        candidates.push_back(front);
    }
}

/// Move `target` to the front of the failover queue (deduplicated), used for
/// standby `leader_url` hints.
pub(super) fn jump_to_candidate(
    candidates: &mut std::collections::VecDeque<String>,
    target: String,
) {
    candidates.retain(|candidate| candidate != &target);
    candidates.push_front(target);
}

/// Whether an HTTP host is this machine: loopback addresses (the whole
/// 127/8, `::1`, bracketed IPv6 forms) plus `0.0.0.0`/`::` (unspecified
/// addresses that connect to the local host) and the `localhost` literal.
pub(super) fn is_loopback_host(host: &str) -> bool {
    if host == "localhost" {
        return true;
    }
    let candidate = host.trim_start_matches('[').trim_end_matches(']');
    candidate
        .parse::<std::net::IpAddr>()
        .map(|ip| ip.is_loopback() || ip.is_unspecified())
        .unwrap_or(false)
}

/// Build the Agent's HTTP client. Loopback hubs are never proxied: system
/// proxy settings (macOS/Windows proxy configuration or stray env vars) that
/// intercept 127.0.0.1 traffic silently break registration and command
/// polling, and proxying a same-host control connection is always a
/// misconfiguration. Every request carries a hard timeout: a Hub that dies
/// mid-request (or a connection accepted into a dead listener's backlog that
/// never responds) must fail the session after a bound so the reconnect loop
/// — not a hung socket — owns recovery.
pub(super) fn build_agent_client(hub_url: &str) -> Result<Client, reqwest::Error> {
    let builder = Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10));
    let loopback = url::Url::parse(hub_url)
        .ok()
        .and_then(|url| url.host_str().map(is_loopback_host))
        .unwrap_or(false);
    if loopback {
        return builder.no_proxy().build();
    }
    builder.build()
}

/// Classified registration failure (hub-ha stage 3): the failover loop
/// rotates candidates immediately on `Standby` but only escalates to the
/// exponential backoff after a full failed cycle.
pub(super) enum RegisterFailure {
    /// 503 `hub_standby`: the Hub is reachable but not the leader. Carries
    /// the standby's `leader_url` hint when the shared lease row advertises
    /// the elected leader's address.
    Standby { leader_url: Option<String> },
    /// Connection-level failure or a non-standby HTTP error.
    Transport(String),
}

impl std::fmt::Display for RegisterFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Standby { leader_url } => match leader_url {
                Some(leader) => write!(formatter, "hub is a standby (leader hint: {leader})"),
                None => write!(formatter, "hub is a standby"),
            },
            Self::Transport(message) => write!(formatter, "{message}"),
        }
    }
}

pub(super) async fn register(
    client: &Client,
    config: &NodeAgentConfig,
    data_address: Option<String>,
) -> Result<RegisterResponse, RegisterFailure> {
    let response = client
        .post(format!(
            "{}{}{}",
            config.hub_url, config.api_prefix, "/agent/register"
        ))
        .json(&RegisterRequest {
            node_id: config.node_id.clone(),
            node_token: config.node_token.clone(),
            protocol_version: "v1".into(),
            // data_address is Some only when the data plane actually bound —
            // a node whose port was taken must not advertise shuffle.
            capabilities: agent_capabilities(data_address.is_some()),
            boot_id: Some(config.boot_id.clone()),
            data_address,
        })
        .send()
        .await
        .map_err(|error| RegisterFailure::Transport(error.to_string()))?;
    let status = response.status();
    if !status.is_success() {
        if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            let problem: serde_json::Value = response
                .json()
                .await
                .map_err(|error| RegisterFailure::Transport(error.to_string()))?;
            if problem["code"] == "hub_standby" {
                let leader_url = problem["details"]["leader_url"].as_str().map(str::to_owned);
                return Err(RegisterFailure::Standby { leader_url });
            }
        }
        return Err(RegisterFailure::Transport(format!(
            "HTTP {status}: registration rejected"
        )));
    }
    response
        .json()
        .await
        .map_err(|error| RegisterFailure::Transport(error.to_string()))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_session(
    client: &Client,
    cp: &ControlPlane,
    config: &NodeAgentConfig,
    session: RegisterResponse,
    cancellation: CancellationToken,
    completed_commands: &mut CompletedCommandCache,
    job_runtime: JobRuntime,
    network_shuffle: bool,
    resource_sampler: &ResourceSampler,
    failover_counter: &std::sync::atomic::AtomicU64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let auth = AgentAuth {
        node_id: config.node_id.clone(),
        session_token: session.session_token,
    };
    let mut heartbeat = tokio::time::interval(config.heartbeat_interval);
    let mut report_tick = tokio::time::interval(config.report_interval);
    let mut poll = tokio::time::interval(config.poll_interval);
    let mut command_tasks = JoinSet::new();
    let mut in_flight_commands = HashSet::new();
    let mut report_seq = 0_u64;
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                command_tasks.abort_all();
                while command_tasks.join_next().await.is_some() {}
                job_runtime.stop_all().await;
                let _ = post_json(client, format!("{}{}{}", config.hub_url, config.api_prefix, "/agent/heartbeat"), &HeartbeatRequest { auth: auth.clone(), state: "draining".into(), protocol_version: Some("v1".into()), software_version: Some(env!("CARGO_PKG_VERSION").into()), capabilities: agent_capabilities(network_shuffle), rollout_id: None }).await;
                return Ok(())
            },
            joined = command_tasks.join_next(), if !command_tasks.is_empty() => {
                match joined {
                    Some(Ok((command_id, Ok(result)))) => {
                        in_flight_commands.remove(&command_id);
                        remember_completed_command(completed_commands, command_id, result);
                    }
                    Some(Ok((command_id, Err(error)))) => {
                        in_flight_commands.remove(&command_id);
                        command_tasks.abort_all();
                        while command_tasks.join_next().await.is_some() {}
                        return Err(error);
                    }
                    Some(Err(error)) => {
                        command_tasks.abort_all();
                        while command_tasks.join_next().await.is_some() {}
                        return Err(error.into());
                    }
                    None => {}
                }
            },
            _ = heartbeat.tick() => { post_json(client, format!("{}{}{}", config.hub_url, config.api_prefix, "/agent/heartbeat"), &HeartbeatRequest { auth: auth.clone(), state: if cp.health().is_running() { "online".into() } else { "starting".into() }, protocol_version: Some("v1".into()), software_version: Some(env!("CARGO_PKG_VERSION").into()), capabilities: agent_capabilities(network_shuffle), rollout_id: None }).await?; }
            _ = report_tick.tick() => { report_seq = report_seq.saturating_add(1); post_json(client, format!("{}{}{}", config.hub_url, config.api_prefix, "/agent/report"), &report(cp, &auth, &config.boot_id, report_seq, &job_runtime, network_shuffle, resource_sampler, &config.hub_url, failover_counter.load(std::sync::atomic::Ordering::Relaxed)).await).await?; }
            _ = poll.tick() => {
                let finished = job_runtime.take_finished().await;
                for (index, (job_id, generation, outcome)) in finished.iter().enumerate() {
                    let (state, error) = match outcome {
                        Ok(()) => ("stopped".into(), None),
                        Err(error) => ("failed".into(), Some(error.clone())),
                    };
                    if let Err(delivery) = post_json(
                        client,
                        format!("{}{}{}", config.hub_url, config.api_prefix, "/agent/job-observations"),
                        &crate::hub::JobObservationRequest {
                            auth: auth.clone(),
                            job_id: job_id.clone(),
                            generation: *generation,
                            state,
                            error,
                        },
                    ).await {
                        // Park this observation and everything behind it: the
                        // task is already removed from the runtime map, so
                        // dropping the observation would leave the Hub
                        // reporting the job as running forever.
                        let mut undelivered = finished[index..].to_vec();
                        undelivered[0] = (job_id.clone(), *generation, outcome.clone());
                        job_runtime.park_observations(undelivered).await;
                        return Err(delivery);
                    }
                }
                let query = agent_auth_query(&auth.node_id);
                let commands: Vec<AgentCommand> = bearer_auth(client.get(format!("{}{}{}?{}", config.hub_url, config.api_prefix, "/agent/commands", query)), &auth.session_token).send().await?.error_for_status()?.json().await?;
                for command in commands {
                    if let Some(result) = replay_cached_command(completed_commands, &command.id) {
                        send_result(client, config, &auth, result).await?;
                        continue;
                    }
                    if !in_flight_commands.insert(command.id.clone()) {
                        continue;
                    }
                    let command_id = command.id.clone();
                    let command_client = client.clone();
                    let command_cp = cp.clone();
                    let command_config = config.clone();
                    let command_auth = auth.clone();
                    let command_runtime = job_runtime.clone();
                    command_tasks.spawn(async move {
                        let result = execute_command(
                            &command_client,
                            &command_cp,
                            &command_config,
                            &command_auth,
                            &command,
                            &command_runtime,
                        )
                        .await;
                        (command_id, result)
                    });
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn report(
    cp: &ControlPlane,
    auth: &AgentAuth,
    // The report boot identity belongs to the Agent process, not the
    // per-registration session credential. The Hub uses the session token to
    // fence delayed transport messages and this stable identity to decide
    // whether a new local JobRuntime must be reconstructed.
    boot_id: &str,
    report_seq: u64,
    job_runtime: &JobRuntime,
    network_shuffle: bool,
    resource_sampler: &ResourceSampler,
    connected_hub: &str,
    hub_failovers: u64,
) -> NodeReport {
    let streams = cp.runtime_manager().snapshots().await;
    let configuration_version = cp
        .runtime_manager()
        .observed_config_version()
        .await
        .or_else(|| {
            streams
                .iter()
                .find_map(|stream| stream.observed_config_version.clone())
        });
    let mut metrics = std::collections::BTreeMap::new();
    for stream in &streams {
        let values = [
            ("input_batches", stream.metrics.input_batches),
            ("input_messages", stream.metrics.input_messages),
            ("processing_errors", stream.metrics.processing_errors),
            ("output_batches", stream.metrics.output_batches),
            ("output_messages", stream.metrics.output_messages),
            ("input_errors", stream.metrics.input_errors),
            ("input_reconnects", stream.metrics.input_reconnects),
            ("output_errors", stream.metrics.output_errors),
            ("restarts", stream.metrics.restarts),
        ];
        for (name, value) in values {
            *metrics.entry(name.into()).or_insert(0.0) += value as f64;
        }
    }
    metrics.insert("streams_total".into(), streams.len() as f64);
    metrics.insert(
        "streams_running".into(),
        streams
            .iter()
            .filter(|stream| stream.state == arkflow_core::control::StreamState::Running)
            .count() as f64,
    );
    metrics.extend(job_runtime.metrics().await);
    // hub-ha stage 3 observability: which Hub this report targets and how
    // many candidate switches the process has performed.
    metrics.insert("hub_failovers".into(), hub_failovers as f64);
    // Host resource gauges ride the same map; a missing or stale sample is
    // simply omitted (observability must never block reporting).
    if let Some(snapshot) = resource_sampler.fresh(now_ms()) {
        merge_resource_gauges(&mut metrics, snapshot);
    }
    NodeReport {
        auth: auth.clone(),
        version: env!("CARGO_PKG_VERSION").into(),
        state: if cp.health().is_running() {
            "online".into()
        } else {
            "starting".into()
        },
        capabilities: agent_capabilities(network_shuffle),
        streams,
        operations: cp.operations().await,
        events: cp.events().await,
        metrics,
        jobs: job_runtime.job_snapshots().await,
        job_tasks: job_runtime.job_tasks().await,
        configuration: redacted_config(&cp.configuration().await).ok(),
        configuration_version,
        // The report rides every poll tick; truncate client-side to the
        // Hub's bound so a long-lived version store cannot bloat reports.
        config_versions: cp
            .versions()
            .unwrap_or_default()
            .into_iter()
            .take(128)
            .collect(),
        boot_id: Some(boot_id.into()),
        report_seq,
        connected_hub: Some(connected_hub.into()),
    }
}

/// Bounded, insertion-ordered cache of completed command results. When the
/// bound is reached the OLDEST entry is evicted one at a time: clearing the
/// cache wholesale made the Hub's redeliveries of still-active lifecycle
/// commands re-execute (a redelivered job_start would cancel and restart a
/// running Job), breaking the at-most-once lifecycle guarantee.
#[derive(Default)]
pub(super) struct CompletedCommandCache {
    entries: HashMap<String, CommandResult>,
    order: std::collections::VecDeque<String>,
    capacity: usize,
}

impl CompletedCommandCache {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: std::collections::VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    pub(super) fn replay(&self, command_id: &str) -> Option<CommandResult> {
        self.entries.get(command_id).cloned()
    }

    pub(super) fn remember(&mut self, command_id: String, result: CommandResult) {
        if !self.entries.contains_key(&command_id) {
            while self.entries.len() >= self.capacity {
                if let Some(oldest) = self.order.pop_front() {
                    self.entries.remove(&oldest);
                }
            }
            self.order.push_back(command_id.clone());
        }
        self.entries.insert(command_id, result);
    }
}

pub(super) fn replay_cached_command(
    cache: &CompletedCommandCache,
    command_id: &str,
) -> Option<CommandResult> {
    cache.replay(command_id)
}

pub(super) fn remember_completed_command(
    cache: &mut CompletedCommandCache,
    command_id: String,
    result: CommandResult,
) {
    cache.remember(command_id, result);
}

/// Equal-jitter backoff: sleep uniformly in `[backoff/2, backoff]`.
///
/// Many Agents losing their session at the same moment (a Hub restart, a fleet
/// of expired credentials) would otherwise retry in lockstep — the exponential
/// growth is identical for every node. Randomizing within the window keeps the
/// exponential bound while desynchronizing the re-registration burst.
pub(super) fn jittered_backoff(backoff: Duration) -> Duration {
    use rand::Rng;
    if backoff.is_zero() {
        return backoff;
    }
    let low = (backoff / 2).as_millis() as u64;
    let high = backoff.as_millis() as u64;
    Duration::from_millis(rand::rng().random_range(low..=high))
}

/// The jitter must stay inside the equal-jitter window `[backoff/2, backoff]`
/// so the exponential bound survives while simultaneous retries desynchronize.
#[test]
fn jittered_backoff_stays_within_the_equal_jitter_window() {
    for backoff in [
        Duration::from_millis(250),
        Duration::from_secs(1),
        Duration::from_secs(10),
    ] {
        for _ in 0..200 {
            let sleep = jittered_backoff(backoff);
            assert!(
                sleep >= backoff / 2 && sleep <= backoff,
                "sleep {sleep:?} outside [{:?}, {backoff:?}]",
                backoff / 2
            );
        }
    }
    assert_eq!(jittered_backoff(Duration::ZERO), Duration::ZERO);
}
