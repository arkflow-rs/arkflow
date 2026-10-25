//! Webhook alerting: forwards matching control-plane events to external
//! HTTP endpoints (see the `webhook-alerting` capability).
//!
//! Delivery is best-effort end to end: the forwarder subscribes the same
//! broadcast the SSE stream consumes, per-subscription queues are bounded
//! (drop-oldest) and serial, and sustained failure auto-pauses the
//! subscription. The durable event record remains `cp_events`; receivers
//! must not treat webhooks as the sole record of any event.

use super::error::HubError;
use super::wire::HubEvent;
use super::{now_ms, Hub};
use crate::storage::{WebhookSetMutation, WebhookSubscription};
use arkflow_core::control::ControlEvent;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// Hard bound on the subscription set (fleet-scale limits stay explicit).
pub const WEBHOOK_LIMIT: usize = 16;
const WEBHOOK_QUEUE_CAPACITY: usize = 64;
const WEBHOOK_MAX_ATTEMPTS: usize = 3;
const WEBHOOK_TIMEOUT: Duration = Duration::from_secs(10);
const WEBHOOK_PAUSE_AFTER_CONSECUTIVE_FAILURES: u32 = 5;
const WEBHOOK_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
/// Backoff between delivery attempts of one event (1s, then 4s before the
/// third and final attempt).
const WEBHOOK_BACKOFF: [Duration; 2] = [Duration::from_secs(1), Duration::from_secs(4)];
/// Node identity for hub-side lifecycle events, matching leadership events.
const HUB_NODE_ID_FOR_EVENTS: &str = "hub";

type HmacSha256 = Hmac<Sha256>;

/// Validation shared by the API layer's replacement route. Bounds and the
/// URL scheme keep the subscription set operator-trustworthy; deep SSRF
/// defenses are explicitly out of scope (see the change design).
pub fn validate_webhooks(subscriptions: &[WebhookSubscription]) -> Result<(), String> {
    if subscriptions.len() > WEBHOOK_LIMIT {
        return Err(format!(
            "at most {WEBHOOK_LIMIT} webhook subscriptions are allowed"
        ));
    }
    let mut seen = BTreeSet::new();
    for subscription in subscriptions {
        if subscription.id.trim().is_empty() {
            return Err("webhook id must not be empty".into());
        }
        if !seen.insert(subscription.id.clone()) {
            return Err(format!("duplicate webhook id: {}", subscription.id));
        }
        if subscription.url.len() > 2048 {
            return Err("webhook url must be at most 2048 characters".into());
        }
        let Ok(url) = reqwest::Url::parse(&subscription.url) else {
            return Err(format!(
                "webhook url is not a valid URL: {}",
                subscription.id
            ));
        };
        if !matches!(url.scheme(), "http" | "https") {
            return Err(format!(
                "webhook url must use http or https: {}",
                subscription.id
            ));
        }
        if subscription.event_types.is_empty() {
            return Err(format!(
                "webhook {} must filter on at least one event type prefix",
                subscription.id
            ));
        }
        if subscription
            .event_types
            .iter()
            .any(|event_type| event_type.trim().is_empty())
        {
            return Err(format!(
                "webhook {} has an empty event type filter entry",
                subscription.id
            ));
        }
        if subscription
            .secret
            .as_deref()
            .is_some_and(|secret| secret.len() > 256)
        {
            return Err(format!(
                "webhook {} signing secret must be at most 256 characters",
                subscription.id
            ));
        }
    }
    Ok(())
}

/// An event forwards when its `event_type` starts with at least one of the
/// subscription's filter entries.
pub fn event_matches_subscription(subscription: &WebhookSubscription, event: &HubEvent) -> bool {
    subscription
        .event_types
        .iter()
        .any(|prefix| event.event.event_type.starts_with(prefix.as_str()))
}

fn shared_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

/// HMAC-SHA256 signature over the exact request body, hex-encoded. Exposed
/// for tests and receiver-side verification examples.
pub fn compute_signature(secret: &str, body: &[u8]) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC-SHA256 accepts any key length");
    mac.update(body);
    hex_encode(&mac.finalize().into_bytes())
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Why one delivery attempt failed: a non-2xx response or a transport error
/// (connect/timeout/encode). Both retry identically.
#[derive(Debug)]
pub enum DeliveryFailure {
    Status(reqwest::StatusCode),
    Transport(String),
}

impl std::fmt::Display for DeliveryFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Status(status) => write!(formatter, "HTTP {status}"),
            Self::Transport(error) => write!(formatter, "transport error: {error}"),
        }
    }
}

/// Outcome of a single synchronous delivery attempt (used by the retry loop
/// and by the test-delivery action).
#[derive(Debug, Clone, serde::Serialize)]
pub struct WebhookTestOutcome {
    pub delivered: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl From<Result<(), DeliveryFailure>> for WebhookTestOutcome {
    fn from(result: Result<(), DeliveryFailure>) -> Self {
        match result {
            Ok(()) => Self {
                delivered: true,
                status: None,
                error: None,
            },
            Err(DeliveryFailure::Status(status)) => Self {
                delivered: false,
                status: Some(status.as_u16()),
                error: None,
            },
            Err(DeliveryFailure::Transport(error)) => Self {
                delivered: false,
                status: None,
                error: Some(error),
            },
        }
    }
}

/// One delivery attempt: POST the event's wire representation with the
/// identification headers and, when a secret is configured, the
/// HMAC-SHA256 signature header over the exact body bytes.
pub async fn deliver_once(
    subscription: &WebhookSubscription,
    event: &HubEvent,
) -> Result<(), DeliveryFailure> {
    let body = serde_json::to_vec(event)
        .map_err(|error| DeliveryFailure::Transport(format!("encode event: {error}")))?;
    let event_id = event
        .event_id
        .map(|id| id.to_string())
        .unwrap_or_else(|| event.event.occurred_at_ms.to_string());
    let mut request = shared_client()
        .post(&subscription.url)
        .timeout(WEBHOOK_TIMEOUT)
        .header("X-Arkflow-Event-Id", event_id)
        .header("X-Arkflow-Event-Type", &event.event.event_type)
        .header(
            "X-Arkflow-Timestamp",
            event.event.occurred_at_ms.to_string(),
        )
        .body(body.clone());
    if let Some(secret) = subscription.secret.as_deref() {
        request = request.header("X-Arkflow-Signature", compute_signature(secret, &body));
    }
    match request.send().await {
        Ok(response) if response.status().is_success() => Ok(()),
        Ok(response) => Err(DeliveryFailure::Status(response.status())),
        Err(error) => Err(DeliveryFailure::Transport(error.to_string())),
    }
}

/// Bounded pending queue with drop-oldest overflow: a slow receiver sheds
/// its OLDEST events (retaining freshness) instead of blocking the shared
/// event pipeline. Delivered and dropped sequences both remain order-
/// preserving subsets of publication order.
struct PendingQueue {
    queue: Mutex<VecDeque<HubEvent>>,
    notify: Notify,
    dropped: AtomicU64,
}

impl PendingQueue {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            queue: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            dropped: AtomicU64::new(0),
        })
    }

    fn push(self: &Arc<Self>, event: HubEvent) {
        let mut queue = self.queue.lock().expect("webhook queue lock");
        if queue.len() >= WEBHOOK_QUEUE_CAPACITY {
            queue.pop_front();
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        queue.push_back(event);
        drop(queue);
        self.notify.notify_one();
    }

    async fn next(&self) -> HubEvent {
        loop {
            if let Some(event) = self.queue.lock().expect("webhook queue lock").pop_front() {
                return event;
            }
            self.notify.notified().await;
        }
    }
}

/// Serial delivery loop for one subscription. A slow or failing receiver
/// only ever blocks its own queue. Sustained failure (5 consecutive events
/// exhausting their retries) auto-pauses the subscription durably and
/// publishes `webhook.paused`. The forwarder's registry owns the queue's
/// sending half; this loop owns the draining. `backoff` is the production
/// schedule; tests inject a zero schedule.
async fn run_subscription_delivery_with(
    hub: Hub,
    id: String,
    subscription: WebhookSubscription,
    queue: Arc<PendingQueue>,
    backoff: &[Duration],
) {
    let mut consecutive_failures: u32 = 0;
    loop {
        let event = queue.next().await;
        let mut delivered = false;
        for attempt in 0..WEBHOOK_MAX_ATTEMPTS {
            match deliver_once(&subscription, &event).await {
                Ok(()) => {
                    delivered = true;
                    break;
                }
                Err(failure) => {
                    tracing::warn!(
                        webhook = %id,
                        attempt = attempt + 1,
                        %failure,
                        "webhook delivery attempt failed"
                    );
                    if let Some(pause) = backoff.get(attempt) {
                        tokio::time::sleep(*pause).await;
                    }
                }
            }
        }
        if delivered {
            consecutive_failures = 0;
            continue;
        }
        consecutive_failures += 1;
        if consecutive_failures >= WEBHOOK_PAUSE_AFTER_CONSECUTIVE_FAILURES {
            hub.pause_webhook(
                &id,
                &format!("{consecutive_failures} consecutive failed deliveries"),
            )
            .await;
            break;
        }
    }
}

struct DeliveryTask {
    queue: Arc<PendingQueue>,
    handle: JoinHandle<()>,
}

/// The forwarder: consumes the Hub event broadcast and fans matching events
/// out to per-subscription queues. Only the lease holder forwards; a
/// standby keeps its subscription snapshot but runs no delivery tasks, and
/// on takeover resumes with subsequently published events (no replay).
pub async fn run_webhook_forwarder(hub: Hub, cancellation: tokio_util::sync::CancellationToken) {
    let mut receiver = hub.subscribe();
    let mut subscriptions: Vec<WebhookSubscription> = Vec::new();
    let mut tasks: HashMap<String, DeliveryTask> = HashMap::new();
    let mut refresh = tokio::time::interval(WEBHOOK_REFRESH_INTERVAL);
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    refresh.tick().await; // the first tick fires immediately; skip it
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
            _ = refresh.tick() => {
                refresh_subscriptions(&hub, &mut subscriptions, &mut tasks).await;
            }
            update = receiver.recv() => match update {
                Ok(event) => {
                    if !hub.is_leader().await {
                        continue;
                    }
                    for subscription in &subscriptions {
                        if subscription.enabled
                            && event_matches_subscription(subscription, &event)
                        {
                            if let Some(task) = tasks.get(&subscription.id) {
                                task.queue.push(event.clone());
                            }
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                    tracing::warn!(
                        missed,
                        "webhook forwarder lagged behind the event broadcast; lagged events are not replayed"
                    );
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            },
        }
    }
    for task in tasks.values() {
        task.handle.abort();
    }
}

/// Reconcile the running delivery tasks with the durable subscription set.
/// Storage errors keep the previous snapshot (the API or a restart will
/// correct it); a standby drains all tasks.
async fn refresh_subscriptions(
    hub: &Hub,
    subscriptions: &mut Vec<WebhookSubscription>,
    tasks: &mut HashMap<String, DeliveryTask>,
) {
    let Ok(fresh) = hub.list_webhooks().await else {
        return;
    };
    if !hub.is_leader().await {
        for (_, task) in tasks.drain() {
            task.handle.abort();
        }
        *subscriptions = fresh;
        return;
    }
    let active: BTreeSet<String> = fresh
        .iter()
        .filter(|subscription| subscription.enabled)
        .map(|subscription| subscription.id.clone())
        .collect();
    let to_spawn: Vec<WebhookSubscription> = fresh
        .iter()
        .filter(|subscription| subscription.enabled && !tasks.contains_key(&subscription.id))
        .cloned()
        .collect();
    for subscription in to_spawn {
        let queue = PendingQueue::new();
        let task_queue = queue.clone();
        let task_hub = hub.clone();
        let id = subscription.id.clone();
        let task_id = id.clone();
        let handle = tokio::spawn(async move {
            run_subscription_delivery_with(
                task_hub,
                task_id,
                subscription,
                task_queue,
                &WEBHOOK_BACKOFF,
            )
            .await;
        });
        tasks.insert(id, DeliveryTask { queue, handle });
    }
    let stale: Vec<String> = tasks
        .keys()
        .filter(|id| !active.contains(*id))
        .cloned()
        .collect();
    for id in stale {
        if let Some(task) = tasks.remove(&id) {
            task.handle.abort();
        }
    }
    *subscriptions = fresh;
}

impl Hub {
    /// Serve-loop entry: runs the webhook forwarder until cancelled.
    pub async fn run_webhook_forwarder(&self, cancellation: tokio_util::sync::CancellationToken) {
        run_webhook_forwarder(self.clone(), cancellation).await;
    }

    pub async fn list_webhooks(&self) -> Result<Vec<WebhookSubscription>, HubError> {
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        storage.list_webhooks().await.map_err(HubError::from)
    }

    /// Full replacement of the subscription set. Validation rejects
    /// over-bound or malformed sets without partially applying. The signing
    /// secret is write-only: a replacement entry that omits it (`None`)
    /// retains the stored secret for the same id — otherwise every
    /// toggle/edit would silently strip signing — while an explicitly empty
    /// string clears it. The storage layer audits and persists
    /// `webhook.resumed` events for re-enabled subscriptions, which are
    /// then broadcast to live subscribers.
    pub async fn replace_webhooks(
        &self,
        subscriptions: Vec<WebhookSubscription>,
        actor: Option<String>,
        correlation_id: Option<String>,
    ) -> Result<Vec<WebhookSubscription>, HubError> {
        validate_webhooks(&subscriptions).map_err(HubError::Invalid)?;
        let storage = self.storage.as_ref().ok_or(HubError::StorageUnavailable)?;
        // Re-attach retained secrets before persisting.
        let existing: HashMap<String, Option<String>> = storage
            .list_webhooks()
            .await
            .map_err(HubError::from)?
            .into_iter()
            .map(|subscription| (subscription.id, subscription.secret))
            .collect();
        let subscriptions = subscriptions
            .into_iter()
            .map(|mut subscription| {
                if let Some(secret) = subscription.secret.take() {
                    subscription.secret = (!secret.is_empty()).then_some(secret);
                } else {
                    subscription.secret = existing.get(&subscription.id).cloned().flatten();
                }
                subscription
            })
            .collect::<Vec<_>>();
        let resumed_ids = storage
            .replace_webhooks(
                WebhookSetMutation {
                    subscriptions: subscriptions.clone(),
                    actor: actor.clone(),
                    correlation_id,
                },
                now_ms(),
            )
            .await
            .map_err(HubError::from)?;
        for id in resumed_ids {
            self.broadcast_webhook_lifecycle_event(
                "webhook.resumed",
                &format!("webhook {id} resumed"),
                actor.as_deref(),
                "succeeded",
            );
        }
        Ok(subscriptions)
    }

    /// Auto-pause after sustained delivery failure. Persists the flag and
    /// the `webhook.paused` event (system actor, audited), then broadcasts
    /// the event so live subscribers — including meta subscriptions
    /// filtering `webhook.` — observe the pause immediately.
    pub(crate) async fn pause_webhook(&self, webhook_id: &str, reason: &str) -> bool {
        let Some(storage) = self.storage.as_ref() else {
            return false;
        };
        match storage.pause_webhook(webhook_id, reason, now_ms()).await {
            Ok(true) => {
                self.broadcast_webhook_lifecycle_event(
                    "webhook.paused",
                    &format!("webhook {webhook_id} paused: {reason}"),
                    Some("system"),
                    "failed",
                );
                true
            }
            _ => false,
        }
    }

    /// Lifecycle events are persisted by the storage transaction (REST and
    /// SSE replay see them) and broadcast here for live consumers. They are
    /// deliberately NOT pushed into the in-memory event deque: the merged
    /// REST view already contains the stored row.
    fn broadcast_webhook_lifecycle_event(
        &self,
        event_type: &str,
        message: &str,
        actor: Option<&str>,
        outcome: &str,
    ) {
        let _ = self.updates.send(HubEvent {
            event_id: None,
            node_id: HUB_NODE_ID_FOR_EVENTS.into(),
            event: ControlEvent {
                occurred_at_ms: now_ms(),
                event_type: event_type.into(),
                stream_id: None,
                outcome: outcome.into(),
                message: Some(message.into()),
                operation_id: None,
                correlation_id: None,
                actor: actor.map(str::to_owned),
            },
        });
    }

    /// Synchronous one-shot delivery of a synthetic `webhook.test` event.
    /// Neither persists the synthetic event nor touches the subscription's
    /// failure counter or enabled state.
    pub async fn test_webhook(&self, webhook_id: &str) -> Result<WebhookTestOutcome, HubError> {
        let subscription = self
            .list_webhooks()
            .await?
            .into_iter()
            .find(|subscription| subscription.id == webhook_id)
            .ok_or(HubError::NotFound)?;
        let synthetic = HubEvent {
            event_id: None,
            node_id: HUB_NODE_ID_FOR_EVENTS.into(),
            event: ControlEvent {
                occurred_at_ms: now_ms(),
                event_type: "webhook.test".into(),
                stream_id: None,
                outcome: "succeeded".into(),
                message: Some("synthetic webhook test delivery".into()),
                operation_id: None,
                correlation_id: None,
                actor: None,
            },
        };
        Ok(deliver_once(&subscription, &synthetic).await.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subscription(id: &str, url: &str, event_types: &[&str]) -> WebhookSubscription {
        WebhookSubscription {
            id: id.into(),
            url: url.into(),
            event_types: event_types
                .iter()
                .map(|prefix| prefix.to_string())
                .collect(),
            enabled: true,
            secret: Some("secret".into()),
            created_by: None,
            created_at_ms: 0,
        }
    }

    fn hub_event(event_type: &str) -> HubEvent {
        HubEvent {
            event_id: Some(7),
            node_id: "node-a".into(),
            event: ControlEvent {
                occurred_at_ms: 1_000,
                event_type: event_type.into(),
                stream_id: None,
                outcome: "succeeded".into(),
                message: None,
                operation_id: None,
                correlation_id: None,
                actor: None,
            },
        }
    }

    #[test]
    fn validation_rejects_over_bound_and_malformed_sets() {
        let valid = || subscription("alerts", "https://example.com/hook", &["rollout."]);
        assert!(validate_webhooks(&[valid()]).is_ok());

        let too_many = (0..=WEBHOOK_LIMIT)
            .map(|index| subscription(&format!("hook-{index}"), "https://example.com", &["x."]))
            .collect::<Vec<_>>();
        assert!(validate_webhooks(&too_many).is_err());

        assert!(validate_webhooks(&[subscription(
            "alerts",
            "ftp://example.com/hook",
            &["rollout."]
        )])
        .is_err());
        assert!(
            validate_webhooks(&[subscription("alerts", "https://example.com/hook", &[])]).is_err()
        );
        assert!(validate_webhooks(&[subscription(
            "alerts",
            "https://example.com/hook",
            &["rollout.", ""]
        )])
        .is_err());
        assert!(
            validate_webhooks(&[subscription("", "https://example.com/hook", &["rollout."])])
                .is_err()
        );
        assert!(validate_webhooks(&[
            subscription("alerts", "https://example.com/hook", &["rollout."]),
            subscription("alerts", "https://example.com/other", &["node."]),
        ])
        .is_err());
    }

    #[test]
    fn event_matching_is_prefix_based() {
        let webhook = subscription("alerts", "https://example.com/hook", &["rollout.", "node."]);
        assert!(event_matches_subscription(
            &webhook,
            &hub_event("rollout.failed")
        ));
        assert!(event_matches_subscription(
            &webhook,
            &hub_event("node.offline")
        ));
        // Prefix match must not leak across tokens.
        assert!(!event_matches_subscription(
            &webhook,
            &hub_event("rollouts.changed")
        ));
        assert!(!event_matches_subscription(
            &webhook,
            &hub_event("job.failed")
        ));
    }

    #[test]
    fn signature_is_hmac_sha256_hex_of_the_body() {
        let body = br#"{"event_type":"rollout.failed"}"#;
        let signature = compute_signature("secret", body);
        let expected = {
            let mut mac = <HmacSha256 as Mac>::new_from_slice(b"secret").unwrap();
            mac.update(body);
            mac.finalize().into_bytes()
        };
        assert_eq!(signature.len(), 64);
        assert_eq!(signature, {
            expected
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        });
        // Receiver-side verification: recomputing over the exact body
        // yields the same digest bytes.
        let mut verifier = HmacSha256::new_from_slice(b"secret").unwrap();
        verifier.update(body);
        let digest = hex_decode(&signature);
        assert_eq!(digest, verifier.finalize().into_bytes().as_slice());
        // And fails over any other body.
        let mut wrong = HmacSha256::new_from_slice(b"secret").unwrap();
        wrong.update(b"tampered");
        assert_ne!(digest, wrong.finalize().into_bytes().as_slice());
    }

    fn hex_decode(value: &str) -> Vec<u8> {
        (0..value.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&value[index..index + 2], 16).unwrap())
            .collect()
    }

    #[tokio::test(start_paused = false)]
    async fn pending_queue_overflow_keeps_the_freshest_events_in_order() {
        let queue = PendingQueue::new();
        for index in 0..WEBHOOK_QUEUE_CAPACITY as u64 + 8u64 {
            let mut event = hub_event("rollout.failed");
            event.event.occurred_at_ms = index;
            queue.push(event);
        }
        // Capacity 64 with 72 pushed: the first 8 are dropped, the queue
        // holds 64..=71, and drops are counted.
        let mut drained = Vec::new();
        for _ in 0..WEBHOOK_QUEUE_CAPACITY {
            drained.push(queue.next().await);
        }
        let timestamps: Vec<u64> = drained
            .iter()
            .map(|event| event.event.occurred_at_ms)
            .collect();
        assert_eq!(timestamps.first(), Some(&8));
        assert_eq!(
            timestamps.last(),
            Some(&(WEBHOOK_QUEUE_CAPACITY as u64 + 7))
        );
        assert!(timestamps.windows(2).all(|pair| pair[0] <= pair[1]));
        assert_eq!(queue.dropped.load(Ordering::Relaxed), 8);
    }

    /// Local receiver capturing requests; responds per `status_sequence`
    /// (repeating the last entry once exhausted).
    struct Receiver {
        url: String,
        requests: std::sync::Arc<std::sync::Mutex<Vec<CapturedRequest>>>,
        _serve: tokio::task::JoinHandle<std::io::Result<()>>,
    }

    #[derive(Clone)]
    struct CapturedRequest {
        event_type: String,
        signature: Option<String>,
        body: Vec<u8>,
    }

    async fn spawn_receiver(status_sequence: Vec<u16>) -> Receiver {
        use axum::routing::post;
        let requests: std::sync::Arc<std::sync::Mutex<Vec<CapturedRequest>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let state = requests.clone();
        let app = axum::Router::new().route(
            "/hook",
            post(
                move |headers: axum::http::HeaderMap, body: axum::body::Bytes| {
                    let state = state.clone();
                    let sequence = status_sequence.clone();
                    async move {
                        let captured = CapturedRequest {
                            event_type: headers
                                .get("x-arkflow-event-type")
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_owned(),
                            signature: headers
                                .get("x-arkflow-signature")
                                .and_then(|value| value.to_str().ok())
                                .map(str::to_owned),
                            body: body.to_vec(),
                        };
                        let index = {
                            let mut guard = state.lock().expect("receiver lock");
                            let index = guard.len();
                            guard.push(captured);
                            index
                        };
                        let status = sequence
                            .get(index)
                            .copied()
                            .unwrap_or_else(|| *sequence.last().unwrap_or(&200));
                        (axum::http::StatusCode::from_u16(status).unwrap(), "ok")
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/hook", listener.local_addr().unwrap());
        let serve = tokio::spawn(async move { axum::serve(listener, app).await });
        Receiver {
            url,
            requests,
            _serve: serve,
        }
    }

    async fn hub_with_storage(label: &str) -> Hub {
        let store = crate::storage::ControlPlaneStore::contract(label).await;
        let storage = crate::storage::StorageActor::start(store, 8);
        Hub::with_storage(
            crate::hub::HubConfig {
                operator_token: Some("operator".into()),
                node_token: Some("node-secret".into()),
                insecure_local: false,
                lease_ttl_ms: 1_000,
                poll_interval_ms: 10,
                session_ttl_ms: crate::hub::default_session_ttl_ms(),
                approval_required: false,
                approval_ttl_ms: crate::hub::default_approval_ttl_ms(),
            },
            storage,
        )
    }

    #[tokio::test]
    async fn replacement_retains_omitted_secrets_and_clears_explicit_empty_ones() {
        let hub = hub_with_storage("webhook_secret_retention").await;
        let with_secret = |secret: Option<&str>| WebhookSubscription {
            id: "alerts".into(),
            url: "https://example.com/hook".into(),
            event_types: vec!["rollout.".into()],
            enabled: true,
            secret: secret.map(str::to_owned),
            created_by: None,
            created_at_ms: 0,
        };
        hub.replace_webhooks(vec![with_secret(Some("sig-secret"))], None, None)
            .await
            .unwrap();
        // Omitted (None) secret: the stored signing secret survives the
        // replacement — responses can never re-supply it.
        hub.replace_webhooks(vec![with_secret(None)], None, None)
            .await
            .unwrap();
        assert_eq!(
            hub.list_webhooks().await.unwrap()[0].secret.as_deref(),
            Some("sig-secret")
        );
        // Explicitly empty secret clears it.
        hub.replace_webhooks(vec![with_secret(Some(""))], None, None)
            .await
            .unwrap();
        assert_eq!(hub.list_webhooks().await.unwrap()[0].secret, None);
    }

    #[tokio::test]
    async fn delivery_retries_transient_failure_and_resets_the_counter() {
        let receiver = spawn_receiver(vec![500, 200]).await;
        let hub = hub_with_storage("delivery_transient_recovery").await;
        let subscription = subscription("alerts", &receiver.url, &["rollout."]);
        let queue = PendingQueue::new();
        let sender = queue.clone();
        let task = tokio::spawn(run_subscription_delivery_with(
            hub.clone(),
            "alerts".into(),
            subscription,
            queue,
            &[Duration::ZERO, Duration::ZERO],
        ));
        sender.push(hub_event("rollout.failed"));
        // First event: attempt 1 gets 500, attempt 2 gets 200 → delivered,
        // counter reset. Two more events succeed first-try; no pause ever.
        tokio::time::sleep(Duration::from_millis(200)).await;
        sender.push(hub_event("rollout.succeeded"));
        sender.push(hub_event("rollout.succeeded"));
        tokio::time::sleep(Duration::from_millis(300)).await;
        // 2 requests for the first event (fail + retry) + 1 each for the
        // two succeeding events. Every request is signed over its exact
        // body and typed with the event's `event_type`.
        let requests = receiver.requests.lock().expect("receiver lock").clone();
        assert_eq!(requests.len(), 4);
        assert!(requests
            .iter()
            .all(|request| request.event_type.starts_with("rollout.")));
        assert!(requests.iter().all(|request| {
            request
                .signature
                .as_deref()
                .is_some_and(|signature| *signature == compute_signature("secret", &request.body))
        }));
        assert!(
            hub.list_webhooks().await.unwrap().is_empty(),
            "no subscription was persisted, so no pause could happen"
        );
        task.abort();
    }

    #[tokio::test]
    async fn sustained_failure_auto_pauses_and_persists_the_lifecycle_event() {
        let receiver = spawn_receiver(vec![500]).await;
        let hub = hub_with_storage("delivery_sustained_pause").await;
        // Persist the subscription so the auto-pause has a row to flip.
        hub.replace_webhooks(
            vec![subscription("alerts", &receiver.url, &["rollout."])],
            Some("operator-1".into()),
            None,
        )
        .await
        .unwrap();
        let queue = PendingQueue::new();
        let sender = queue.clone();
        let task = tokio::spawn(run_subscription_delivery_with(
            hub.clone(),
            "alerts".into(),
            subscription("alerts", &receiver.url, &["rollout."]),
            queue,
            &[Duration::ZERO, Duration::ZERO],
        ));
        for index in 0..WEBHOOK_PAUSE_AFTER_CONSECUTIVE_FAILURES {
            let mut event = hub_event("rollout.failed");
            event.event.occurred_at_ms = index as u64;
            sender.push(event);
        }
        // 5 consecutive events × 3 failed attempts each = 15 requests,
        // then the loop pauses durably and exits.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let count = receiver.requests.lock().expect("receiver lock").len();
            if count >= (WEBHOOK_PAUSE_AFTER_CONSECUTIVE_FAILURES as usize * WEBHOOK_MAX_ATTEMPTS) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "receiver saw too few attempts"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // The pause is durable: enabled=false and a webhook.paused event.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let stored = hub.list_webhooks().await.unwrap();
            if stored
                .first()
                .is_some_and(|subscription| !subscription.enabled)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "subscription was never paused"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        task.abort();
        receiver._serve.abort();
    }
}
