//! Lease-based leadership for HA Hub deployments (hub-ha stage 2).
//!
//! Multiple Hub processes may share one durable store; a singleton lease row
//! (`cp_hub_lease`) elects exactly one leader. Standbys serve nothing but
//! health/readiness/metrics and never run periodic work; promotion reloads
//! the durable control-plane view before the instance starts serving. With
//! `HubHaConfig::default()` (disabled) every gate here is a no-op and
//! single-instance behavior is unchanged.

use super::*;

/// HA election configuration. Default is disabled.
#[derive(Debug, Clone)]
pub struct HubHaConfig {
    pub enabled: bool,
    /// Lease lifetime. Renewal cadence is ttl/3 (min 1s); the failover window
    /// is bounded by this TTL plus one probe. Assumes NTP-aligned clocks.
    pub lease_ttl_ms: u64,
    /// Explicit holder identity. Defaults to a per-process unique id.
    pub holder_id: Option<String>,
}

impl Default for HubHaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            lease_ttl_ms: 15_000,
            holder_id: None,
        }
    }
}

impl HubHaConfig {
    /// The holder id used in the lease row. Explicit ids win; the generated
    /// default mixes hostname, pid, and boot time so two processes on one
    /// host also stay distinct.
    pub fn holder_id(&self) -> String {
        if let Some(holder) = self
            .holder_id
            .as_deref()
            .map(str::trim)
            .filter(|holder| !holder.is_empty())
        {
            return holder.to_owned();
        }
        let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".into());
        format!("{}:{}:{}", host, std::process::id(), now_ms())
    }
}

/// Current leadership view of this Hub process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Leadership {
    /// HA disabled: this instance is the only Hub and every gate passes.
    Disabled,
    /// HA enabled, lease not held. Periodic work and operator/agent routes
    /// are gated off; readiness reports not-ready with the standby role.
    Standby { since_ms: u64 },
    /// HA enabled and this process holds the lease with `epoch` fencing.
    Leader { epoch: u64, since_ms: u64 },
}

impl Leadership {
    pub fn role(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Standby { .. } => "standby",
            Self::Leader { .. } => "leader",
        }
    }

    /// Whether periodic work and operator/agent routes may run. `Disabled`
    /// always passes so the default single-instance Hub is unaffected.
    pub fn is_leader(&self) -> bool {
        matches!(self, Self::Leader { .. } | Self::Disabled)
    }

    pub fn epoch(&self) -> Option<u64> {
        match self {
            Self::Leader { epoch, .. } => Some(*epoch),
            _ => None,
        }
    }
}

const HUB_NODE_ID_FOR_EVENTS: &str = "hub";

impl Hub {
    /// Opt into lease election. Call before `serve_hub`. Resolves the holder
    /// identity once so every later lease call uses the same id.
    pub fn with_ha(mut self, mut ha: HubHaConfig) -> Self {
        let resolved = ha.holder_id();
        ha.holder_id = Some(resolved);
        self.ha = ha;
        self
    }

    pub fn ha_config(&self) -> &HubHaConfig {
        &self.ha
    }

    pub async fn leadership(&self) -> Leadership {
        self.leadership.read().await.clone()
    }

    /// Fast gate used by the periodic loops and the HTTP standby middleware.
    pub async fn is_leader(&self) -> bool {
        self.leadership.read().await.is_leader()
    }

    pub fn leadership_transitions(&self) -> u64 {
        self.leadership_transitions.load(Ordering::Relaxed)
    }

    /// HA startup entry: an enabled Hub always starts as standby and earns
    /// leadership through the lease, never by assumption.
    pub async fn enter_election(&self) {
        if !self.ha.enabled {
            return;
        }
        let mut leadership = self.leadership.write().await;
        if matches!(*leadership, Leadership::Disabled) {
            *leadership = Leadership::Standby { since_ms: now_ms() };
            drop(leadership);
            tracing::info!(
                holder = %self.ha.holder_id(),
                ttl_ms = self.ha.lease_ttl_ms,
                "HA enabled: starting as standby pending lease acquisition"
            );
        }
    }

    async fn transition_to(&self, next: Leadership, reason: &str) {
        let previous = {
            let mut leadership = self.leadership.write().await;
            if *leadership == next {
                return;
            }
            std::mem::replace(&mut *leadership, next.clone())
        };
        self.leadership_transitions.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            from = previous.role(),
            to = next.role(),
            epoch = next.epoch(),
            reason,
            "hub leadership transition"
        );
        let event = ControlEvent {
            occurred_at_ms: now_ms(),
            event_type: "hub.leadership".into(),
            stream_id: None,
            outcome: next.role().into(),
            message: Some(format!(
                "hub leadership: {} ({} -> {}, epoch {:?})",
                reason,
                previous.role(),
                next.role(),
                next.epoch()
            )),
            operation_id: None,
            correlation_id: None,
            actor: Some("system".into()),
        };
        let hub_event = HubEvent {
            event_id: None,
            node_id: HUB_NODE_ID_FOR_EVENTS.into(),
            event,
        };
        let mut events = self.events.write().await;
        if events.len() >= MAX_EVENTS {
            events.pop_front();
        }
        events.push_back(hub_event.clone());
        drop(events);
        let _ = self.updates.send(hub_event);
    }

    /// Demote to standby. Periodic loops observe this on their next tick and
    /// go idle; the HTTP gate starts rejecting non-health routes.
    pub async fn step_down(&self, reason: &str) {
        self.transition_to(Leadership::Standby { since_ms: now_ms() }, reason)
            .await;
    }

    /// Clear the in-memory control-plane view and rebuild it from the durable
    /// store. Promotion must not serve state remembered by a previous term:
    /// a demoted ex-leader can hold entries newer than the durable cut.
    pub async fn reload_durable_state_for_promotion(&self) -> Result<(), HubError> {
        if self.storage.as_ref().is_none() {
            return Err(HubError::StorageUnavailable);
        }
        self.nodes.write().await.clear();
        self.placement_order.write().await.clear();
        self.operations.write().await.clear();
        self.rollouts.write().await.clear();
        self.jobs.write().await.clear();
        self.job_versions.write().await.clear();
        self.job_checkpoints.write().await.clear();
        self.lifecycle.write().await.recovered = false;
        // Non-terminal operations died with the previous leader's command
        // queues; `recover_persisted_state` settles them as timed out so the
        // reconcile retry path re-drives durable work.
        self.recover_persisted_state().await?;
        self.restore_persisted_operations().await?;
        Ok(())
    }

    /// One election cycle: leaders renew, standbys try to acquire. Promotion
    /// reloads durable state first and releases the lease again when that
    /// fails (fail-closed: no half-recovered leader).
    pub async fn run_election_tick(&self) -> Leadership {
        if !self.ha.enabled {
            return Leadership::Disabled;
        }
        let Some(storage) = self.storage.clone() else {
            return Leadership::Disabled;
        };
        let holder = self.ha.holder_id();
        let ttl = self.ha.lease_ttl_ms;
        let now = now_ms();
        // Bind before matching: a guard held through the match arms would
        // deadlock against the write taken by `transition_to`.
        let current = self.leadership.read().await.clone();
        match current {
            Leadership::Leader { epoch, .. } => {
                match storage.renew_hub_lease(&holder, ttl, now).await {
                    Ok(crate::storage::HubLeaseRenew::Renewed { epoch: renewed }) => {
                        if renewed != epoch {
                            // Impossible unless the row was tampered with;
                            // track the authoritative epoch anyway.
                            self.transition_to(
                                Leadership::Leader {
                                    epoch: renewed,
                                    since_ms: now,
                                },
                                "epoch_corrected",
                            )
                            .await;
                        }
                    }
                    Ok(crate::storage::HubLeaseRenew::Lost) => {
                        self.step_down("lease_lost").await;
                    }
                    Err(error) => {
                        // Storage unreachable: assume the lease may be lost.
                        // Fencing epochs keep a recovered store consistent.
                        tracing::warn!(
                            %error,
                            "hub lease renewal failed; stepping down until storage recovers"
                        );
                        self.step_down("storage_unreachable").await;
                    }
                }
            }
            Leadership::Standby { .. } => {
                match storage.try_acquire_hub_lease(&holder, ttl, now).await {
                    Ok(crate::storage::HubLeaseAcquire::Acquired { epoch }) => {
                        match self.reload_durable_state_for_promotion().await {
                            Ok(()) => {
                                self.transition_to(
                                    Leadership::Leader { epoch, since_ms: now },
                                    "lease_acquired",
                                )
                                .await;
                            }
                            Err(error) => {
                                tracing::error!(
                                    %error,
                                    epoch,
                                    "promotion recovery failed; releasing the lease and staying standby"
                                );
                                let _ = storage
                                    .release_hub_lease(&holder, now_ms())
                                    .await;
                            }
                        }
                    }
                    Ok(crate::storage::HubLeaseAcquire::HeldByOther(snapshot)) => {
                        tracing::debug!(
                            holder = %snapshot.holder,
                            epoch = snapshot.epoch,
                            "lease held by another hub; remaining standby"
                        );
                    }
                    Err(error) => {
                        tracing::warn!(%error, "hub lease acquire failed; remaining standby");
                    }
                }
            }
            Leadership::Disabled => {}
        }
        self.leadership.read().await.clone()
    }

    /// Graceful shutdown: release the lease immediately so a standby can take
    /// over without waiting for the TTL to expire.
    pub async fn release_leadership(&self) {
        if !self.ha.enabled {
            return;
        }
        // Bind before the `if let`: holding the read guard through the body
        // would deadlock against step_down's write.
        let current = self.leadership.read().await.clone();
        if let Leadership::Leader { .. } = current {
            if let Some(storage) = self.storage.clone() {
                let _ = storage
                    .release_hub_lease(&self.ha.holder_id(), now_ms())
                    .await;
            }
            self.step_down("shutdown").await;
        }
    }
}
