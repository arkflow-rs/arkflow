//! Hub mTLS listener, Agent client TLS/mTLS, SAN identity binding, and startup
//! configuration override tests (PA-P1-07 Phase 3).

use super::{build_hub_tls_acceptor, serve_hub, ServerConfig};
use crate::agent::{self, NodeAgentConfig};
use crate::hub::{default_approval_ttl_ms, default_session_ttl_ms, Hub, HubConfig};
use crate::startup_config::{apply_env_overrides, HubStartupConfig};
use crate::storage::{ControlPlaneStore, StorageActor};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

struct TlsFixtures {
    _dir: tempfile::TempDir,
    ca_path: String,
    hub_cert_path: String,
    hub_key_path: String,
    node_a_san_cert_path: String,
    node_a_san_key_path: String,
    node_b_san_cert_path: String,
    node_b_san_key_path: String,
    generic_cert_path: String,
    generic_key_path: String,
}

fn write_file(dir: &Path, name: &str, contents: &str) -> String {
    let path = dir.join(name);
    std::fs::write(&path, contents).unwrap();
    path.to_string_lossy().into_owned()
}

fn generate_tls_fixtures() -> TlsFixtures {
    use rcgen::{
        string::Ia5String, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
        SanType,
    };
    let dir = tempfile::tempdir().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca_params, &ca_key);

    let mut hub_params =
        CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()]).unwrap();
    hub_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ServerAuth);
    let hub_key = KeyPair::generate().unwrap();
    let hub_cert = hub_params.signed_by(&hub_key, &issuer).unwrap();

    let make_client = |name: &str, san_uri: Option<&str>| {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .extended_key_usages
            .push(ExtendedKeyUsagePurpose::ClientAuth);
        if let Some(uri) = san_uri {
            params
                .subject_alt_names
                .push(SanType::URI(Ia5String::try_from(uri.to_owned()).unwrap()));
        }
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &issuer).unwrap();
        (
            write_file(dir.path(), &format!("{name}.crt"), &cert.pem()),
            write_file(dir.path(), &format!("{name}.key"), &key.serialize_pem()),
        )
    };

    let (node_a_san_cert_path, node_a_san_key_path) =
        make_client("node-a", Some("urn:arkflow:node:node-a"));
    let (node_b_san_cert_path, node_b_san_key_path) =
        make_client("node-b", Some("urn:arkflow:node:node-b"));
    let (generic_cert_path, generic_key_path) = make_client("generic-client", None);

    TlsFixtures {
        ca_path: write_file(dir.path(), "ca.crt", &ca.pem()),
        hub_cert_path: write_file(dir.path(), "hub.crt", &hub_cert.pem()),
        hub_key_path: write_file(dir.path(), "hub.key", &hub_key.serialize_pem()),
        node_a_san_cert_path,
        node_a_san_key_path,
        node_b_san_cert_path,
        node_b_san_key_path,
        generic_cert_path,
        generic_key_path,
        _dir: dir,
    }
}

fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr.to_string()
}

fn client_with_tls(ca_path: &str, client_identity: Option<(&str, &str)>) -> reqwest::Client {
    let mut builder = reqwest::Client::builder().no_proxy();
    let ca_bytes = std::fs::read(ca_path).unwrap();
    let mut cursor = std::io::Cursor::new(&ca_bytes);
    for cert in rustls_pemfile::certs(&mut cursor) {
        builder = builder
            .add_root_certificate(reqwest::Certificate::from_der(cert.unwrap().as_ref()).unwrap());
    }
    if let Some((cert_path, key_path)) = client_identity {
        let cert = std::fs::read_to_string(cert_path).unwrap();
        let key = std::fs::read_to_string(key_path).unwrap();
        builder = builder
            .identity(reqwest::Identity::from_pem(format!("{cert}\n{key}").as_bytes()).unwrap());
    }
    builder.build().unwrap()
}

#[test]
fn hub_tls_config_validation_and_env_overrides() {
    let fixtures = generate_tls_fixtures();

    // Missing CA for agent_required / required is rejected.
    let err = build_hub_tls_acceptor(&ServerConfig {
        tls_cert: Some(fixtures.hub_cert_path.clone()),
        tls_key: Some(fixtures.hub_key_path.clone()),
        tls_client_auth: Some("agent_required".into()),
        ..ServerConfig::default()
    })
    .err()
    .unwrap();
    assert!(err.to_string().contains("ARKFLOW_HUB_TLS_CA"), "{err}");

    // Invalid tls_client_auth value is rejected.
    let err = build_hub_tls_acceptor(&ServerConfig {
        tls_cert: Some(fixtures.hub_cert_path.clone()),
        tls_key: Some(fixtures.hub_key_path.clone()),
        tls_ca: Some(fixtures.ca_path.clone()),
        tls_client_auth: Some("bogus".into()),
        ..ServerConfig::default()
    })
    .err()
    .unwrap();
    assert!(
        err.to_string()
            .contains("invalid ARKFLOW_HUB_TLS_CLIENT_AUTH"),
        "{err}"
    );

    // CA without server cert/key is rejected.
    let err = build_hub_tls_acceptor(&ServerConfig {
        tls_ca: Some(fixtures.ca_path.clone()),
        ..ServerConfig::default()
    })
    .err()
    .unwrap();
    assert!(err.to_string().contains("ARKFLOW_HUB_TLS_CERT"), "{err}");

    // CA set + unset tls_client_auth defaults to agent_required (`mtls_enforced = true`).
    let (acceptor, enforced) = build_hub_tls_acceptor(&ServerConfig {
        tls_cert: Some(fixtures.hub_cert_path.clone()),
        tls_key: Some(fixtures.hub_key_path.clone()),
        tls_ca: Some(fixtures.ca_path.clone()),
        tls_client_auth: None,
        ..ServerConfig::default()
    })
    .unwrap();
    assert!(acceptor.is_some());
    assert!(enforced);

    // CA set + tls_client_auth=none disables client auth (`mtls_enforced = false`).
    let (acceptor, enforced) = build_hub_tls_acceptor(&ServerConfig {
        tls_cert: Some(fixtures.hub_cert_path),
        tls_key: Some(fixtures.hub_key_path),
        tls_ca: Some(fixtures.ca_path),
        tls_client_auth: Some("none".into()),
        ..ServerConfig::default()
    })
    .unwrap();
    assert!(acceptor.is_some());
    assert!(!enforced);

    // Startup config env overrides wire new mTLS and bootstrap flags.
    let mut startup = HubStartupConfig::default();
    let env: HashMap<&str, &str> = [
        ("ARKFLOW_HUB_TLS_CA", "/tmp/ca.pem"),
        ("ARKFLOW_HUB_TLS_CLIENT_AUTH", "required"),
        ("ARKFLOW_HUB_NODE_BOOTSTRAP_DISABLED", "true"),
        ("ARKFLOW_ALLOW_SHARED_NODE_TOKEN_BREAK_GLASS", "1"),
    ]
    .into_iter()
    .collect();
    apply_env_overrides(&mut startup, &|name| {
        env.get(name).map(|v| (*v).to_string())
    });
    assert_eq!(startup.server.tls_ca.as_deref(), Some("/tmp/ca.pem"));
    assert_eq!(startup.server.tls_client_auth.as_deref(), Some("required"));
    assert!(startup.server.node_bootstrap_disabled);
    assert!(startup.server.allow_shared_node_token_break_glass);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_mtls_agent_required_allows_operators_enforces_agent_cert_and_san_binding() {
    let fixtures = generate_tls_fixtures();
    let addr = free_addr();
    let hub = Hub::with_storage(
        HubConfig {
            operator_token: Some("ops|admin|op-secret".into()),
            node_token: Some("bootstrap-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 50,
            session_ttl_ms: default_session_ttl_ms(),
            approval_required: false,
            approval_ttl_ms: default_approval_ttl_ms(),
        },
        StorageActor::start(
            ControlPlaneStore::contract("hub_mtls_agent_required").await,
            8,
        ),
    );
    let cancel = CancellationToken::new();
    let server_cfg = ServerConfig {
        address: addr.clone(),
        tls_cert: Some(fixtures.hub_cert_path.clone()),
        tls_key: Some(fixtures.hub_key_path.clone()),
        tls_ca: Some(fixtures.ca_path.clone()),
        tls_client_auth: Some("agent_required".into()),
        ..ServerConfig::default()
    };
    let task = tokio::spawn(serve_hub(hub.clone(), server_cfg, cancel.clone()));
    let base_url = format!("https://{addr}");

    // Wait until HTTPS listener accepts connections.
    let no_cert_client = client_with_tls(&fixtures.ca_path, None);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if no_cert_client
            .get(format!("{base_url}/api/v1/nodes"))
            .bearer_auth("op-secret")
            .send()
            .await
            .is_ok_and(|resp| resp.status() == reqwest::StatusCode::OK)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "HTTPS Hub listener did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // (1) Agent without client certificate is rejected (401 + missing_client_certificate audit).
    let resp = no_cert_client
        .post(format!("{base_url}/api/v1/agent/register"))
        .json(&serde_json::json!({
            "node_id": "node-a",
            "node_token": "bootstrap-secret",
            "protocol_version": "v1",
            "capabilities": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    // (2) Agent with node-b SAN registering as node-a is rejected (cert_node_identity_mismatch).
    let node_b_client = client_with_tls(
        &fixtures.ca_path,
        Some((
            &fixtures.node_b_san_cert_path,
            &fixtures.node_b_san_key_path,
        )),
    );
    let resp = node_b_client
        .post(format!("{base_url}/api/v1/agent/register"))
        .json(&serde_json::json!({
            "node_id": "node-a",
            "node_token": "bootstrap-secret",
            "protocol_version": "v1",
            "capabilities": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    // (3) Real agent::run with matching node-a SAN cert registers over HTTPS mTLS.
    let agent_cancel = CancellationToken::new();
    let agent_cfg = NodeAgentConfig {
        hub_url: base_url.clone(),
        hub_urls: vec![base_url.clone()],
        api_prefix: "/api/v1".into(),
        node_id: "node-a".into(),
        node_token: "bootstrap-secret".into(),
        boot_id: "boot-mtls-a".into(),
        heartbeat_interval: Duration::from_millis(50),
        report_interval: Duration::from_millis(50),
        poll_interval: Duration::from_millis(50),
        data_port: None,
        data_host: None,
        node_credential_file: None,
        tls_ca: Some(fixtures.ca_path.clone()),
        tls_cert: Some(fixtures.node_a_san_cert_path.clone()),
        tls_key: Some(fixtures.node_a_san_key_path.clone()),
    };
    let empty_cp = arkflow_core::control_plane::ControlPlane::new(
        arkflow_core::config::EngineConfig {
            streams: Vec::new(),
            jobs: Vec::new(),
            logging: arkflow_core::config::LoggingConfig::default(),
            node: arkflow_core::config::NodeConfig::default(),
        },
        arkflow_core::runtime::RuntimeManager::new(),
    );
    // Paired Agent TLS cert/key validation: half-configured client mTLS fails startup.
    let err = agent::run(
        empty_cp.clone(),
        NodeAgentConfig {
            tls_cert: Some(fixtures.node_a_san_cert_path.clone()),
            tls_key: None,
            ..agent_cfg.clone()
        },
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("tls_key"), "{err}");

    let mut agent_task = tokio::spawn(agent::run(empty_cp, agent_cfg, agent_cancel.clone()));
    let reg_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while hub.nodes().await.is_empty() {
        assert!(tokio::time::Instant::now() < reg_deadline);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // (4) Verify audits captured missing_client_certificate & cert_node_identity_mismatch + accepted.
    let audits = hub.audit(Some("node-a")).await.unwrap();
    assert!(audits.iter().any(|r| r.action == "agent.auth_rejected"
        && r.failure_code.as_deref() == Some("missing_client_certificate")));
    assert!(audits.iter().any(|r| r.action == "agent.auth_rejected"
        && r.failure_code.as_deref() == Some("cert_node_identity_mismatch")));
    assert!(audits
        .iter()
        .any(|r| r.action == "agent.register" && r.outcome == "accepted"));

    agent_cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), &mut agent_task).await;
    agent_task.abort();
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), task).await;

    // Also test generic CA-signed client cert under `required` mode rejects no-cert handshakes.
    let addr_req = free_addr();
    let cancel_req = CancellationToken::new();
    let hub_req = Hub::with_storage(
        HubConfig {
            operator_token: Some("ops|admin|op-secret".into()),
            node_token: Some("bootstrap-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 50,
            session_ttl_ms: default_session_ttl_ms(),
            approval_required: false,
            approval_ttl_ms: default_approval_ttl_ms(),
        },
        StorageActor::start(ControlPlaneStore::contract("hub_mtls_required").await, 8),
    );
    let task_req = tokio::spawn(serve_hub(
        hub_req,
        ServerConfig {
            address: addr_req.clone(),
            tls_cert: Some(fixtures.hub_cert_path),
            tls_key: Some(fixtures.hub_key_path),
            tls_ca: Some(fixtures.ca_path.clone()),
            tls_client_auth: Some("required".into()),
            ..ServerConfig::default()
        },
        cancel_req.clone(),
    ));
    let generic_client = client_with_tls(
        &fixtures.ca_path,
        Some((&fixtures.generic_cert_path, &fixtures.generic_key_path)),
    );
    let url_req = format!("https://{addr_req}/api/v1/nodes");
    let deadline_req = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let last_probe = match generic_client
            .get(&url_req)
            .bearer_auth("op-secret")
            .send()
            .await
        {
            Ok(resp) if resp.status() == reqwest::StatusCode::OK => break,
            Ok(resp) => format!("status={}", resp.status()),
            Err(error) => format!("err={error:?}"),
        };
        assert!(
            tokio::time::Instant::now() < deadline_req,
            "required mTLS hub did not become ready ({last_probe})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        no_cert_client
            .get(&url_req)
            .bearer_auth("op-secret")
            .send()
            .await
            .is_err(),
        "tls_client_auth=required must reject TLS connections without a client certificate"
    );
    cancel_req.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), task_req).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_required_mtls_session_bound_to_san_rejects_uncertified_or_empty_san_requests() {
    let fixtures = generate_tls_fixtures();
    let addr = free_addr();
    let cancel = CancellationToken::new();
    let hub = Hub::with_storage(
        HubConfig {
            operator_token: Some("ops|admin|op-secret".into()),
            node_token: Some("bootstrap-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 50,
            session_ttl_ms: default_session_ttl_ms(),
            approval_required: false,
            approval_ttl_ms: default_approval_ttl_ms(),
        },
        StorageActor::start(
            ControlPlaneStore::contract("hub_mtls_bound_session").await,
            8,
        ),
    );
    let task = tokio::spawn(serve_hub(
        hub.clone(),
        ServerConfig {
            address: addr.clone(),
            tls_cert: Some(fixtures.hub_cert_path.clone()),
            tls_key: Some(fixtures.hub_key_path.clone()),
            tls_ca: Some(fixtures.ca_path.clone()),
            tls_client_auth: Some("agent_required".into()),
            ..ServerConfig::default()
        },
        cancel.clone(),
    ));
    let base_url = format!("https://{addr}");

    let node_a_client = client_with_tls(
        &fixtures.ca_path,
        Some((
            &fixtures.node_a_san_cert_path,
            &fixtures.node_a_san_key_path,
        )),
    );
    let no_cert_client = client_with_tls(&fixtures.ca_path, None);
    let empty_san_client = client_with_tls(
        &fixtures.ca_path,
        Some((&fixtures.generic_cert_path, &fixtures.generic_key_path)),
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let reg_json: serde_json::Value = loop {
        match node_a_client
            .post(format!("{base_url}/api/v1/agent/register"))
            .json(&serde_json::json!({
                "node_id": "node-a",
                "node_token": "bootstrap-secret",
                "protocol_version": "v1",
                "capabilities": []
            }))
            .send()
            .await
        {
            Ok(resp) if resp.status() == reqwest::StatusCode::OK => {
                break resp.json().await.unwrap();
            }
            _ => {
                assert!(tokio::time::Instant::now() < deadline);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    };
    let session_token = reg_json["session_token"].as_str().unwrap();

    // Matching node-a SAN cert succeeds on post-register heartbeat.
    let ok_resp = node_a_client
        .post(format!("{base_url}/api/v1/agent/heartbeat"))
        .json(&serde_json::json!({
            "node_id": "node-a",
            "session_token": session_token,
            "state": "online",
            "capabilities": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(ok_resp.status(), reqwest::StatusCode::NO_CONTENT);

    // Even under tls_client_auth=optional, a session-token holder without a cert MUST be rejected.
    let no_cert_resp = no_cert_client
        .post(format!("{base_url}/api/v1/agent/heartbeat"))
        .json(&serde_json::json!({
            "node_id": "node-a",
            "session_token": session_token,
            "state": "online",
            "capabilities": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(no_cert_resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    // A generic CA-signed cert with empty node SANs MUST also be rejected for the bound session.
    let empty_san_resp = empty_san_client
        .get(format!("{base_url}/api/v1/agent/commands?node_id=node-a"))
        .bearer_auth(session_token)
        .send()
        .await
        .unwrap();
    assert_eq!(empty_san_resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
}
