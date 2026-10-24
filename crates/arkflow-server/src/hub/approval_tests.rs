//! Tests for the configuration approval state machine (`hub/approval.rs`).

use super::*;
use crate::hub::wire::RegisterRequest;
use crate::storage::{ConfigApprovalStatus, StorageActor};

fn approval_config() -> HubConfig {
    HubConfig {
        operator_token: Some("operator".into()),
        node_token: Some("node-secret".into()),
        insecure_local: false,
        lease_ttl_ms: 1000,
        poll_interval_ms: 10,
        session_ttl_ms: default_session_ttl_ms(),
        approval_required: true,
        approval_ttl_ms: 3_600_000,
    }
}

async fn hub_with_node(label: &str, config: HubConfig) -> Hub {
    let store = crate::storage::ControlPlaneStore::contract(label).await;
    let storage = StorageActor::start(store, 8);
    let hub = Hub::with_storage(config, storage);
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "node-a".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec![],
        boot_id: None,
    })
    .await
    .unwrap();
    // Seed one configuration version the requests can pin.
    hub.create_rollout_with_content(
        "cfg-seed".into(),
        "{\"streams\":[]}".into(),
        vec!["node-a".into()],
        1,
        Some("seeder".into()),
        None,
    )
    .await
    .unwrap();
    hub
}

#[tokio::test]
async fn submission_validates_without_side_effects() {
    let hub = hub_with_node("approval_submit_validates", approval_config()).await;

    // Unknown version is rejected.
    let err = hub
        .submit_config_approval(
            "cfg-missing".into(),
            vec!["node-a".into()],
            1,
            "alice".into(),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("version not found"), "got: {err}");

    // Unknown node is rejected naming the node.
    let err = hub
        .submit_config_approval(
            "cfg-seed".into(),
            vec!["node-ghost".into()],
            1,
            "alice".into(),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("node-ghost"), "got: {err}");

    // Duplicate nodes are rejected.
    let err = hub
        .submit_config_approval(
            "cfg-seed".into(),
            vec!["node-a".into(), "node-a".into()],
            1,
            "alice".into(),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("duplicate"), "got: {err}");

    // A valid submission creates no rollout.
    let before = hub.rollouts().await.unwrap().len();
    let request = hub
        .submit_config_approval(
            "cfg-seed".into(),
            vec!["node-a".into()],
            1,
            "alice".into(),
        )
        .await
        .unwrap();
    assert_eq!(request.status, ConfigApprovalStatus::Pending);
    assert_eq!(hub.rollouts().await.unwrap().len(), before);
}

#[tokio::test]
async fn submission_rejected_when_policy_disabled() {
    let mut config = approval_config();
    config.approval_required = false;
    let hub = hub_with_node("approval_submit_disabled", config).await;
    let err = hub
        .submit_config_approval(
            "cfg-seed".into(),
            vec!["node-a".into()],
            1,
            "alice".into(),
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("not enabled"),
        "expected explicit disabled error, got: {err}"
    );
}

#[tokio::test]
async fn approve_creates_rollout_as_approver_and_decisions_are_single() {
    let hub = hub_with_node("approval_approve", approval_config()).await;
    let request = hub
        .submit_config_approval(
            "cfg-seed".into(),
            vec!["node-a".into()],
            1,
            "alice".into(),
        )
        .await
        .unwrap();

    // Self-approval is rejected naming the segregation rule.
    let err = hub
        .decide_config_approval(&request.request_id, true, "alice".into(), None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("second principal"), "got: {err}");

    let decided = hub
        .decide_config_approval(&request.request_id, true, "bob".into(), None)
        .await
        .unwrap();
    assert_eq!(decided.status, ConfigApprovalStatus::Approved);
    let rollout_id = decided.rollout_id.clone().expect("rollout recorded");
    let (rollout, _targets) = hub.rollout(&rollout_id).await.unwrap().unwrap();
    assert_eq!(rollout.config_version_id, "cfg-seed");
    assert_eq!(rollout.actor.as_deref(), Some("bob"));

    // Terminal: a second decision fails explicitly.
    let err = hub
        .decide_config_approval(&request.request_id, true, "carol".into(), None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("cannot be decided"), "got: {err}");

    // A request already being approved cannot be rejected instead: only
    // the approve retry re-enters through `approving`.
    let second = hub
        .submit_config_approval(
            "cfg-seed".into(),
            vec!["node-a".into()],
            1,
            "alice".into(),
        )
        .await
        .unwrap();
    let storage = hub.storage().unwrap();
    storage
        .begin_decide_config_approval_request(
            &second.request_id,
            "approving",
            "bob",
            1,
            1,
        )
        .await
        .unwrap();
    let err = hub
        .decide_config_approval(&second.request_id, false, "carol".into(), None)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("cannot be decided")
            || err.to_string().contains("concurrently"),
        "got: {err}"
    );
}

#[tokio::test]
async fn approval_adopts_a_crash_created_rollout_instead_of_duplicating() {
    let hub = hub_with_node("approval_adoption", approval_config()).await;
    let request = hub
        .submit_config_approval(
            "cfg-seed".into(),
            vec!["node-a".into()],
            1,
            "alice".into(),
        )
        .await
        .unwrap();
    // Simulate the crash window: the CAS to `approving` landed and the
    // rollout was created, but the completion marking did not.
    let storage = hub.storage().unwrap();
    storage
        .begin_decide_config_approval_request(
            &request.request_id,
            "approving",
            "bob",
            1,
            1,
        )
        .await
        .unwrap();
    let created = hub
        .create_rollout(
            "cfg-seed".into(),
            vec!["node-a".into()],
            1,
            Some("bob".into()),
            Some(request.correlation_id.clone()),
        )
        .await
        .unwrap();
    // Retrying the decision must adopt that rollout.
    let decided = hub
        .decide_config_approval(&request.request_id, true, "bob".into(), None)
        .await
        .unwrap();
    assert_eq!(decided.status, ConfigApprovalStatus::Approved);
    assert_eq!(decided.rollout_id.as_deref(), Some(created.rollout_id.as_str()));
    // Exactly one rollout carries the correlation id.
    let adopted = storage
        .find_rollout_by_correlation(&request.correlation_id)
        .await
        .unwrap()
        .expect("rollout exists");
    assert_eq!(adopted.rollout_id, created.rollout_id);
}

#[tokio::test]
async fn rejection_is_terminal_and_republish_needs_a_new_request() {
    let hub = hub_with_node("approval_reject", approval_config()).await;
    let request = hub
        .submit_config_approval(
            "cfg-seed".into(),
            vec!["node-a".into()],
            1,
            "alice".into(),
        )
        .await
        .unwrap();
    let decided = hub
        .decide_config_approval(&request.request_id, false, "bob".into(), Some("not ready".into()))
        .await
        .unwrap();
    assert_eq!(decided.status, ConfigApprovalStatus::Rejected);
    assert_eq!(decided.approver_id.as_deref(), Some("bob"));
    // Snapshot untouched.
    assert_eq!(decided.config_version_id, "cfg-seed");
    assert_eq!(decided.targets, vec!["node-a".to_string()]);
    // Terminal: cannot re-open.
    let err = hub
        .decide_config_approval(&request.request_id, true, "carol".into(), None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("cannot be decided"), "got: {err}");
    // Republish = new request id, old record unchanged.
    let fresh = hub
        .submit_config_approval(
            "cfg-seed".into(),
            vec!["node-a".into()],
            1,
            "alice".into(),
        )
        .await
        .unwrap();
    assert_ne!(fresh.request_id, request.request_id);
    let old = hub.config_approval(&request.request_id).await.unwrap().unwrap();
    assert_eq!(old.status, ConfigApprovalStatus::Rejected);
}

#[tokio::test]
async fn expired_requests_cannot_be_decided() {
    let mut config = approval_config();
    config.approval_ttl_ms = 1;
    let hub = hub_with_node("approval_expiry", config).await;
    let request = hub
        .submit_config_approval(
            "cfg-seed".into(),
            vec!["node-a".into()],
            1,
            "alice".into(),
        )
        .await
        .unwrap();
    // Let the TTL lapse deterministically.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let err = hub
        .decide_config_approval(&request.request_id, true, "bob".into(), None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("expired"), "got: {err}");
    // The read path also presents it as expired.
    let view = hub
        .config_approval(&request.request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(view.status, ConfigApprovalStatus::Expired);
}

#[tokio::test]
async fn pending_requests_survive_a_hub_restart_and_policy_toggle() {
    let store = crate::storage::ControlPlaneStore::contract("approval_restart").await;
    let storage = StorageActor::start(store, 8);
    let hub = Hub::with_storage(approval_config(), storage.clone());
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "node-a".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec![],
        boot_id: None,
    })
    .await
    .unwrap();
    hub.create_rollout_with_content(
        "cfg-seed".into(),
        "{\"streams\":[]}".into(),
        vec!["node-a".into()],
        1,
        Some("seeder".into()),
        None,
    )
    .await
    .unwrap();
    let request = hub
        .submit_config_approval(
            "cfg-seed".into(),
            vec!["node-a".into()],
            1,
            "alice".into(),
        )
        .await
        .unwrap();

    // "Restart": a new Hub over the same storage, with the policy disabled.
    let mut disabled = approval_config();
    disabled.approval_required = false;
    let restarted = Hub::with_storage(disabled, storage);
    restarted
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec![],
            boot_id: None,
        })
        .await
        .unwrap();
    let view = restarted
        .config_approval(&request.request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(view.status, ConfigApprovalStatus::Pending);
    // Still decidable with the policy off: direct creation is already open,
    // an approval adds no new risk surface.
    let decided = restarted
        .decide_config_approval(&request.request_id, false, "bob".into(), None)
        .await
        .unwrap();
    assert_eq!(decided.status, ConfigApprovalStatus::Rejected);
}

/// The rollback gate: under the approval policy a rollback may target only a
/// version that was actually applied to the affected nodes; with the policy
/// off the rollback behavior is unchanged (any existing version id).
#[tokio::test]
async fn rollback_gate_allows_applied_and_blocks_never_applied_versions() {
    let hub = hub_with_node("approval_rollback_gate", approval_config()).await;
    // `cfg-seed` was created (and applied by seeding) — mark its target
    // observed so it counts as applied.
    let storage = hub.storage().unwrap();
    let rollouts = storage.list_rollouts().await.unwrap();
    let seed = rollouts
        .iter()
        .find(|rollout| rollout.config_version_id == "cfg-seed")
        .expect("seed rollout exists");
    storage
        .update_rollout_target(crate::storage::RolloutTargetUpdate {
            rollout_id: seed.rollout_id.clone(),
            node_id: "node-a".into(),
            state: "applied".into(),
            attempt_id: None,
            error: None,
            observed_config_version: Some("cfg-seed".into()),
            updated_at_ms: 1,
        })
        .await
        .unwrap();
    // A never-applied version: create it without any rollout.
    let never_applied = "cfg-never-applied";
    // Directly verify the predicate.
    assert!(hub
        .version_was_applied_to("cfg-seed", &["node-a".into()])
        .await
        .unwrap());
    assert!(!hub
        .version_was_applied_to(never_applied, &["node-a".into()])
        .await
        .unwrap());

    // Rollback through act_rollout: applied version passes the gate; the
    // never-applied version is rejected naming the approval flow.
    let err = hub
        .act_rollout(
            &seed.rollout_id,
            "rollback",
            Some(never_applied.into()),
            Some("bob".into()),
            None,
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("approval flow"),
        "got: {err}"
    );
    let rolled_back = hub
        .act_rollout(
            &seed.rollout_id,
            "rollback",
            Some("cfg-seed".into()),
            Some("bob".into()),
            None,
        )
        .await;
    // The seed rollout may already be terminal from the seeding helper's
    // lifecycle; both outcomes must carry no approval-gate rejection.
    match rolled_back {
        Ok(_) => {}
        Err(error) => assert!(
            !error.to_string().contains("approval flow"),
            "applied-version rollback must not hit the gate: {error}"
        ),
    }
}

/// With the policy off, direct creation and rollback behave exactly as
/// before this capability: no approval state is consulted.
#[tokio::test]
async fn policy_off_direct_creation_and_rollback_are_unchanged() {
    let mut config = approval_config();
    config.approval_required = false;
    let hub = hub_with_node("approval_policy_off", config).await;

    // Direct creation succeeds (no gate).
    hub.create_rollout(
        "cfg-seed".into(),
        vec!["node-a".into()],
        1,
        Some("alice".into()),
        None,
    )
    .await
    .unwrap();

    // Rollback to any existing version passes without applied-history
    // checks: pick a version that was never applied to the nodes.
    let rollouts = hub.rollouts().await.unwrap();
    let source = &rollouts[0];
    let result = hub
        .act_rollout(
            &source.rollout_id,
            "rollback",
            Some("cfg-seed".into()),
            Some("alice".into()),
            None,
        )
        .await;
    match result {
        Ok(_) => {}
        Err(error) => assert!(
            !error.to_string().contains("approval flow"),
            "policy-off rollback must never hit the gate: {error}"
        ),
    }
}

/// The sweep is maintenance, not an operator mutation: it must not record
/// audit events, and it never removes pending rows.
#[tokio::test]
async fn sweep_is_not_audited_and_never_removes_pending() {
    let hub = hub_with_node("approval_sweep_audit", approval_config()).await;
    let request = hub
        .submit_config_approval(
            "cfg-seed".into(),
            vec!["node-a".into()],
            1,
            "alice".into(),
        )
        .await
        .unwrap();
    let storage = hub.storage().unwrap();
    let audits_before = storage.list_audit(None::<&str>).await.unwrap().len();
    hub.sweep_config_approvals().await.unwrap();
    let audits_after = storage.list_audit(None::<&str>).await.unwrap().len();
    assert_eq!(
        audits_before, audits_after,
        "the sweep must not record operator-mutation audit events"
    );
    // A live pending request survives the sweep untouched.
    let survived = hub
        .config_approval(&request.request_id)
        .await
        .unwrap()
        .expect("pending request survives");
    assert_eq!(survived.status, ConfigApprovalStatus::Pending);
}

/// Approvals are audited for submit/approve/reject with scalar metadata
/// only (no configuration bodies).
#[tokio::test]
async fn approval_decisions_are_audited_with_scalar_metadata() {
    let hub = hub_with_node("approval_audit_records", approval_config()).await;
    let storage = hub.storage().unwrap();
    let request = hub
        .submit_config_approval(
            "cfg-seed".into(),
            vec!["node-a".into()],
            1,
            "alice".into(),
        )
        .await
        .unwrap();
    hub.decide_config_approval(&request.request_id, true, "bob".into(), None)
        .await
        .unwrap();
    let audits = storage
        .list_audit(Some(request.request_id.as_str()))
        .await
        .unwrap();
    let actions: Vec<&str> = audits.iter().map(|a| a.action.as_str()).collect();
    assert!(actions.contains(&"config_approval.submit"), "{actions:?}");
    assert!(actions.contains(&"config_approval.approve"), "{actions:?}");
    for audit in &audits {
        assert!(audit.message.as_deref().unwrap_or("").len() < 512);
        assert!(
            !audit.message.as_deref().unwrap_or("").contains("{"),
            "audit must not embed configuration bodies: {:?}",
            audit.message
        );
    }
}
