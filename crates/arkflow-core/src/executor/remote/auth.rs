//! Authenticated data-plane sessions: shared credentials, per-edge session
//! identity, and the HMAC handshake payloads.

use super::wire::Quad;
use crate::Error;
use hmac::{Hmac, Mac};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::sync::Arc;
use subtle::ConstantTimeEq;

/// Version of the authenticated data-plane handshake.  A version mismatch
/// is a protocol error rather than a best-effort downgrade.
pub(crate) const DATA_PLANE_PROTOCOL_VERSION: u16 = 1;
pub(crate) const HANDSHAKE_NONCE_LEN: usize = 32;

/// Credentials shared by the two Agents participating in a data-plane
/// session.  The secret is never serialized into a Job command or logged.
#[derive(Clone)]
pub struct DataPlaneCredentials {
    pub local_node: String,
    pub(crate) shared_secret: Arc<[u8]>,
}

impl std::fmt::Debug for DataPlaneCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DataPlaneCredentials")
            .field("local_node", &self.local_node)
            .field("shared_secret", &"<redacted>")
            .finish()
    }
}

impl DataPlaneCredentials {
    pub fn new(
        local_node: impl Into<String>,
        shared_secret: impl AsRef<[u8]>,
    ) -> Result<Self, Error> {
        let shared_secret = shared_secret.as_ref();
        if shared_secret.is_empty() {
            return Err(Error::Config(
                "network shuffle data-plane secret must not be empty".into(),
            ));
        }
        Ok(Self {
            local_node: local_node.into(),
            shared_secret: Arc::from(shared_secret.to_vec().into_boxed_slice()),
        })
    }

    pub(crate) fn session(
        &self,
        peer_node: impl Into<String>,
        job_id: impl Into<String>,
        generation: u64,
        quad: Quad,
    ) -> SessionAuth {
        SessionAuth {
            source_node: self.local_node.clone(),
            destination_node: peer_node.into(),
            job_id: job_id.into(),
            generation,
            quad,
            shared_secret: self.shared_secret.clone(),
        }
    }
}

/// Expected identity for an inbound quad registration.  The manager's local
/// node and secret are process-wide; the peer, Job, and generation are bound
/// when the Agent builds the particular Job graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerExpectation {
    pub source_node: String,
    pub job_id: String,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct JobSessionKey {
    pub(crate) job_id: String,
    pub(crate) generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct EdgeSessionKey {
    job: Option<JobSessionKey>,
    pub(crate) quad: Quad,
}

impl EdgeSessionKey {
    pub(crate) fn legacy(quad: Quad) -> Self {
        Self { job: None, quad }
    }

    pub(crate) fn for_job(job_id: impl Into<String>, generation: u64, quad: Quad) -> Self {
        Self {
            job: Some(JobSessionKey {
                job_id: job_id.into(),
                generation,
            }),
            quad,
        }
    }

    pub(crate) fn job(&self) -> Option<&JobSessionKey> {
        self.job.as_ref()
    }
}

impl PeerExpectation {
    pub(crate) fn edge_key(&self, quad: Quad) -> EdgeSessionKey {
        EdgeSessionKey::for_job(self.job_id.clone(), self.generation, quad)
    }
}

#[derive(Clone)]
pub(crate) struct SessionAuth {
    pub(crate) source_node: String,
    pub(crate) destination_node: String,
    pub(crate) job_id: String,
    generation: u64,
    pub(crate) quad: Quad,
    pub(crate) shared_secret: Arc<[u8]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HandshakePayload {
    pub(crate) protocol_version: u16,
    pub(crate) source_node: String,
    pub(crate) destination_node: String,
    pub(crate) job_id: String,
    pub(crate) generation: u64,
    pub(crate) quad: Quad,
    pub(crate) nonce: Vec<u8>,
    pub(crate) acknowledgement: bool,
    mac: Vec<u8>,
}

type HmacSha256 = Hmac<Sha256>;

impl SessionAuth {
    pub(crate) fn edge_key(&self) -> EdgeSessionKey {
        EdgeSessionKey::for_job(self.job_id.clone(), self.generation, self.quad)
    }

    pub(crate) fn client_handshake(&self) -> Result<HandshakePayload, Error> {
        let mut nonce = vec![0; HANDSHAKE_NONCE_LEN];
        rand::rng().fill(nonce.as_mut_slice());
        let mut payload = HandshakePayload {
            protocol_version: DATA_PLANE_PROTOCOL_VERSION,
            source_node: self.source_node.clone(),
            destination_node: self.destination_node.clone(),
            job_id: self.job_id.clone(),
            generation: self.generation,
            quad: self.quad,
            nonce,
            acknowledgement: false,
            mac: Vec::new(),
        };
        payload.mac = handshake_mac(&self.shared_secret, &payload, b"client");
        Ok(payload)
    }

    pub(crate) fn server_ack(
        &self,
        request: &HandshakePayload,
        local_node: &str,
    ) -> HandshakePayload {
        let mut response = HandshakePayload {
            protocol_version: DATA_PLANE_PROTOCOL_VERSION,
            source_node: local_node.to_owned(),
            destination_node: request.source_node.clone(),
            job_id: request.job_id.clone(),
            generation: request.generation,
            quad: request.quad,
            nonce: request.nonce.clone(),
            acknowledgement: true,
            mac: Vec::new(),
        };
        response.mac = handshake_mac(&self.shared_secret, &response, b"server");
        response
    }

    pub(crate) fn verify_server_ack(&self, payload: &HandshakePayload) -> Result<(), Error> {
        if payload.protocol_version != DATA_PLANE_PROTOCOL_VERSION
            || payload.source_node != self.destination_node
            || payload.destination_node != self.source_node
            || payload.job_id != self.job_id
            || payload.generation != self.generation
            || payload.quad != self.quad
            || !payload.acknowledgement
            || payload.nonce.len() != HANDSHAKE_NONCE_LEN
        {
            return Err(Error::Process(
                "remote edge handshake acknowledgement identity mismatch".into(),
            ));
        }
        verify_handshake_mac(&self.shared_secret, payload, b"server")
    }
}

fn handshake_mac(secret: &[u8], payload: &HandshakePayload, direction: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts every key length");
    mac.update(&canonical_handshake_bytes(payload, direction));
    mac.finalize().into_bytes().to_vec()
}

pub(crate) fn verify_handshake_mac(
    secret: &[u8],
    payload: &HandshakePayload,
    direction: &[u8],
) -> Result<(), Error> {
    let expected = handshake_mac(secret, payload, direction);
    if expected.as_slice().ct_eq(payload.mac.as_slice()).into() {
        Ok(())
    } else {
        Err(Error::Process(
            "remote edge handshake credential mismatch".into(),
        ))
    }
}

fn canonical_handshake_bytes(payload: &HandshakePayload, direction: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&payload.protocol_version.to_le_bytes());
    for value in [
        payload.source_node.as_bytes(),
        payload.destination_node.as_bytes(),
        payload.job_id.as_bytes(),
    ] {
        bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
        bytes.extend_from_slice(value);
    }
    bytes.extend_from_slice(&payload.generation.to_le_bytes());
    bytes.extend_from_slice(&payload.quad.src_op.to_le_bytes());
    bytes.extend_from_slice(&payload.quad.src_subtask.to_le_bytes());
    bytes.extend_from_slice(&payload.quad.dst_op.to_le_bytes());
    bytes.extend_from_slice(&payload.quad.dst_subtask.to_le_bytes());
    bytes.extend_from_slice(&(payload.nonce.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&payload.nonce);
    bytes.push(u8::from(payload.acknowledgement));
    bytes.extend_from_slice(&(direction.len() as u64).to_le_bytes());
    bytes.extend_from_slice(direction);
    bytes
}
