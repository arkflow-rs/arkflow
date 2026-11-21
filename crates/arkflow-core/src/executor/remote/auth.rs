//! Authenticated data-plane sessions: shared credentials, per-edge session
//! identity, the HMAC handshake payloads, and the v2 per-frame authentication
//! derived from the session key agreement.

use super::wire::{FrameHeader, Quad, FRAME_TAG_LEN};
use crate::Error;
use hmac::{Hmac, Mac};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::sync::Arc;
use subtle::ConstantTimeEq;

/// Version of the authenticated data-plane handshake.  A version mismatch
/// is a protocol error rather than a best-effort downgrade.
///
/// v2: the handshake exchanges BOTH nonces and derives per-connection,
/// per-direction session keys; every post-handshake frame carries a
/// truncated HMAC tag over header, payload, and a connection-level
/// monotonic sequence number. v1 frames carry no authentication.
pub(crate) const DATA_PLANE_PROTOCOL_VERSION: u16 = 2;
pub(crate) const HANDSHAKE_NONCE_LEN: usize = 32;
/// Session keys are 32-byte HMAC-SHA256 keys, one per direction.
pub(crate) const SESSION_KEY_LEN: usize = 32;

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
            sent_nonce: Arc::new(std::sync::Mutex::new(None)),
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
    /// The nonce of the handshake this connection last SENT — the server ack
    /// must echo it. Comparing against the recorded nonce (not merely the
    /// length) makes a replayed, validly-MACed ack from an EARLIER handshake
    /// for the same quad fail: without it a plaintext connection could accept
    /// a captured old receipt as the fresh handshake acknowledgement. Shared
    /// across clones: every clone represents the same session, and the most
    /// recent handshake (reconnects included) defines the expected nonce.
    sent_nonce: Arc<std::sync::Mutex<Option<Vec<u8>>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HandshakePayload {
    pub(crate) protocol_version: u16,
    pub(crate) source_node: String,
    pub(crate) destination_node: String,
    pub(crate) job_id: String,
    pub(crate) generation: u64,
    pub(crate) quad: Quad,
    /// The sender's fresh nonce. The acknowledgement echoes the request's
    /// nonce here (anti-replay, see `SessionAuth::sent_nonce`) and carries
    /// its own fresh nonce in `server_nonce` — both sides contribute
    /// entropy to the session key derivation.
    pub(crate) nonce: Vec<u8>,
    /// The acknowledging side's fresh nonce; empty on the client request.
    /// Together with the client nonce it salts the per-connection session
    /// keys, so every handshake produces independent keys even with the
    /// same long-lived shared secret.
    #[serde(default)]
    pub(crate) server_nonce: Vec<u8>,
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
            server_nonce: Vec::new(),
            acknowledgement: false,
            mac: Vec::new(),
        };
        payload.mac = handshake_mac(&self.shared_secret, &payload, b"client");
        // Record the sent nonce: the server ack for THIS connection must echo
        // it (see the field docs).
        if let Ok(mut sent) = self.sent_nonce.lock() {
            *sent = Some(payload.nonce.clone());
        }
        Ok(payload)
    }

    pub(crate) fn server_ack(
        &self,
        request: &HandshakePayload,
        local_node: &str,
    ) -> HandshakePayload {
        let mut server_nonce = vec![0; HANDSHAKE_NONCE_LEN];
        rand::rng().fill(server_nonce.as_mut_slice());
        let mut response = HandshakePayload {
            protocol_version: DATA_PLANE_PROTOCOL_VERSION,
            source_node: local_node.to_owned(),
            destination_node: request.source_node.clone(),
            job_id: request.job_id.clone(),
            generation: request.generation,
            quad: request.quad,
            nonce: request.nonce.clone(),
            server_nonce,
            acknowledgement: true,
            mac: Vec::new(),
        };
        response.mac = handshake_mac(&self.shared_secret, &response, b"server");
        response
    }

    pub(crate) fn verify_server_ack(&self, payload: &HandshakePayload) -> Result<(), Error> {
        // The ack must echo the nonce of the handshake THIS connection sent:
        // a length-only check accepted a replayed, validly-MACed ack from an
        // earlier handshake for the same quad.
        let expected_nonce = self
            .sent_nonce
            .lock()
            .map(|sent| sent.clone())
            .unwrap_or_default();
        if payload.protocol_version != DATA_PLANE_PROTOCOL_VERSION
            || payload.source_node != self.destination_node
            || payload.destination_node != self.source_node
            || payload.job_id != self.job_id
            || payload.generation != self.generation
            || payload.quad != self.quad
            || !payload.acknowledgement
            || payload.server_nonce.len() != HANDSHAKE_NONCE_LEN
            || expected_nonce
                .as_deref()
                .is_none_or(|sent| payload.nonce != sent)
        {
            return Err(Error::Process(
                "remote edge handshake acknowledgement identity mismatch".into(),
            ));
        }
        verify_handshake_mac(&self.shared_secret, payload, b"server")
    }

    /// Derives this connection's frame cryptos from the verified server
    /// acknowledgement: the send key protects frames this side writes (c2s
    /// for the client), the receive key protects frames it reads (s2c).
    /// Must only be called after `verify_server_ack` accepted the payload.
    pub(crate) fn frame_crypto_client(
        &self,
        ack: &HandshakePayload,
    ) -> Result<(FrameCrypto, FrameCrypto), Error> {
        let client_nonce = self
            .sent_nonce
            .lock()
            .map(|sent| sent.clone().unwrap_or_default())
            .unwrap_or_default();
        derive_session_keys(
            &self.shared_secret,
            &client_nonce,
            &ack.server_nonce,
            &self.job_id,
            self.generation,
            &self.quad,
        )
    }

    /// Server-side counterpart: derives both direction cryptos from the
    /// verified request and the ack about to be sent.
    pub(crate) fn frame_crypto_server(
        &self,
        request: &HandshakePayload,
        response: &HandshakePayload,
    ) -> Result<(FrameCrypto, FrameCrypto), Error> {
        derive_session_keys(
            &self.shared_secret,
            &request.nonce,
            &response.server_nonce,
            &request.job_id,
            request.generation,
            &request.quad,
        )
    }
}

/// Per-connection, per-direction frame authenticator (protocol v2). The send
/// side stamps every frame with a truncated HMAC over the encoded header,
/// payload, and its monotonic sequence counter; the receive side verifies
/// the tag and enforces strictly-increasing sequence numbers, so captured
/// frames cannot be replayed and any modified byte fails closed.
#[derive(Debug)]
pub(crate) struct FrameCrypto {
    key: [u8; SESSION_KEY_LEN],
    /// Send side: sequence number of the NEXT frame to seal. Receive side:
    /// sequence number expected next (first post-handshake frame is 0).
    next_seq: u64,
}

impl FrameCrypto {
    pub(crate) fn new(key: [u8; SESSION_KEY_LEN]) -> Self {
        Self { key, next_seq: 0 }
    }

    /// Computes the authentication tag for one outgoing frame and advances
    /// the sequence counter.
    pub(crate) fn seal(&mut self, header: &FrameHeader, payload: &[u8], tag_out: &mut [u8]) {
        let seq = self.next_seq;
        self.next_seq = seq.wrapping_add(1);
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC accepts every key length");
        mac.update(&header.encode_fixed());
        mac.update(payload);
        mac.update(&seq.to_le_bytes());
        let tag = mac.finalize().into_bytes();
        tag_out.copy_from_slice(&tag[..FRAME_TAG_LEN]);
    }

    /// Verifies one incoming frame's tag and sequence number before any
    /// payload parsing; a mismatch is a deterministic protocol error.
    pub(crate) fn open(
        &mut self,
        header: &FrameHeader,
        payload: &[u8],
        tag: &[u8],
    ) -> Result<(), Error> {
        if tag.len() != FRAME_TAG_LEN {
            return Err(Error::Process(
                "remote edge frame authentication tag has the wrong length".into(),
            ));
        }
        let expected_seq = self.next_seq;
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC accepts every key length");
        mac.update(&header.encode_fixed());
        mac.update(payload);
        mac.update(&expected_seq.to_le_bytes());
        let expected = mac.finalize().into_bytes();
        if !bool::from(expected[..FRAME_TAG_LEN].ct_eq(tag)) {
            return Err(Error::Process(
                "remote edge frame authentication failed".into(),
            ));
        }
        self.next_seq = expected_seq.wrapping_add(1);
        Ok(())
    }
}

/// Derives the two per-direction session keys for one connection from the
/// long-lived shared secret and BOTH handshake nonces. The nonce pair salts
/// HKDF so every handshake yields independent keys; the identity info binds
/// the keys to this exact job/generation/quad so a key cannot be replayed
/// onto a different edge.
fn derive_session_keys(
    shared_secret: &[u8],
    client_nonce: &[u8],
    server_nonce: &[u8],
    job_id: &str,
    generation: u64,
    quad: &Quad,
) -> Result<(FrameCrypto, FrameCrypto), Error> {
    if client_nonce.len() != HANDSHAKE_NONCE_LEN || server_nonce.len() != HANDSHAKE_NONCE_LEN {
        return Err(Error::Process(
            "remote edge handshake nonces are missing or malformed".into(),
        ));
    }
    let mut info = Vec::with_capacity(128);
    info.extend_from_slice(b"arkflow-data-plane/v2");
    info.extend_from_slice(&(job_id.len() as u64).to_le_bytes());
    info.extend_from_slice(job_id.as_bytes());
    info.extend_from_slice(&generation.to_le_bytes());
    info.extend_from_slice(&quad.src_op.to_le_bytes());
    info.extend_from_slice(&quad.src_subtask.to_le_bytes());
    info.extend_from_slice(&quad.dst_op.to_le_bytes());
    info.extend_from_slice(&quad.dst_subtask.to_le_bytes());
    // The nonce pair salts the extract step so both sides' fresh entropy
    // shapes every derived byte; the identity info above pins the keys to
    // this exact job/generation/quad edge, and each direction expands its
    // own key so the two keys are cryptographically independent.
    let mut salt = Vec::with_capacity(2 * HANDSHAKE_NONCE_LEN);
    salt.extend_from_slice(client_nonce);
    salt.extend_from_slice(server_nonce);
    let hk = hkdf::Hkdf::<Sha256>::new(Some(&salt), shared_secret);
    let derive = |direction: &[u8]| -> Result<[u8; SESSION_KEY_LEN], Error> {
        let mut keyed_info = info.clone();
        keyed_info.extend_from_slice(&(direction.len() as u64).to_le_bytes());
        keyed_info.extend_from_slice(direction);
        let mut key = [0u8; SESSION_KEY_LEN];
        hk.expand(&keyed_info, &mut key)
            .map_err(|_| Error::Process("remote edge session key derivation failed".into()))?;
        Ok(key)
    };
    let c2s = derive(b"c2s")?;
    let s2c = derive(b"s2c")?;
    Ok((FrameCrypto::new(c2s), FrameCrypto::new(s2c)))
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
    bytes.extend_from_slice(&(payload.server_nonce.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&payload.server_nonce);
    bytes.push(u8::from(payload.acknowledgement));
    bytes.extend_from_slice(&(direction.len() as u64).to_le_bytes());
    bytes.extend_from_slice(direction);
    bytes
}
