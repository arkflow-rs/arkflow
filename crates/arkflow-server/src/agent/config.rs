//! Agent configuration: the NodeAgentConfig derived from the engine config
//! and the capability vocabulary advertised to the Hub.
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct NodeAgentConfig {
    /// Hub base URL of the active candidate. The failover loop clones the
    /// config with a different `hub_url` per candidate (see `run`), so every
    /// session-scoped call site reads the active address from here.
    pub hub_url: String,
    /// Failover candidates in scan order; `hub_url` is always one of them.
    pub hub_urls: Vec<String>,
    pub api_prefix: String,
    pub node_id: String,
    pub node_token: String,
    pub boot_id: String,
    pub heartbeat_interval: Duration,
    pub report_interval: Duration,
    pub poll_interval: Duration,
    /// Data-plane listen port. `Some` enables the cross-node shuffle data
    /// plane and advertises the `network_shuffle` capability to the Hub;
    /// placement continues to keep every edge co-located until the Hub
    /// learns to split them, so the default (`None`) stays byte-compatible.
    pub data_port: Option<u16>,
    /// Routable host advertised to peers for the data plane. Required for
    /// split placement; without it the node stays colocated-only.
    pub data_host: Option<String>,
}

/// Trim trailing `/` and drop duplicates, preserving first-seen order.
fn normalize_hub_urls(urls: &[String]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    urls.iter()
        .map(|url| url.trim_end_matches('/'))
        .filter(|url| !url.is_empty())
        .filter(|url| seen.insert((*url).to_owned()))
        .map(str::to_owned)
        .collect()
}

impl NodeAgentConfig {
    pub fn from_engine(config: &arkflow_core::config::EngineConfig) -> Option<Self> {
        let hub_urls = normalize_hub_urls(&config.node.agent.hub_urls);
        let hub_url = hub_urls.first()?.clone();
        let node_id = config
            .node
            .agent
            .node_id
            .clone()
            .or_else(|| std::env::var("ARKFLOW_NODE_ID").ok())?;
        let node_token = config
            .node
            .agent
            .node_token
            .clone()
            .or_else(|| std::env::var("ARKFLOW_NODE_TOKEN").ok())
            .unwrap_or_default();
        let ttl = config.node.agent.agent_lease_ttl_ms.max(3_000);
        let boot_nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        Some(Self {
            hub_url,
            hub_urls,
            api_prefix: config
                .node
                .control_api
                .api_prefix
                .trim_end_matches('/')
                .into(),
            node_id,
            node_token,
            // PID alone can be reused after a real process restart. Include
            // a startup nonce so the Hub invalidates successful starts from
            // the previous local JobRuntime even when the OS reuses the PID.
            boot_id: format!("boot-{}-{boot_nonce}", std::process::id()),
            heartbeat_interval: Duration::from_millis(ttl / 3),
            report_interval: Duration::from_secs(2),
            poll_interval: Duration::from_secs(1),
            data_port: config.node.data_plane.data_port,
            data_host: config.node.data_plane.data_host.clone(),
        })
    }
}

/// Node capabilities advertised at registration and refreshed by heartbeats.
pub(super) fn agent_capabilities(network_shuffle: bool) -> Vec<String> {
    let mut capabilities = vec![
        "stream_lifecycle".to_string(),
        "configuration".to_string(),
        "metrics".to_string(),
        "job_runtime".to_string(),
        "state_backend".to_string(),
        "checkpoint_recovery".to_string(),
    ];
    if network_shuffle {
        capabilities.push("network_shuffle".to_string());
    }
    capabilities
}
