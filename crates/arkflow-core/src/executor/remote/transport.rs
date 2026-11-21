//! Frame framing over async sockets: fixed-header frame reads and writes with
//! payload and idle limits, authenticated (protocol v2) frame variants, and
//! the TCP/TLS edge transport.

use super::auth::{FrameCrypto, JobSessionKey};
use super::manager::PendingReceipts;
#[cfg(test)]
use super::wire::MAX_FRAME_LEN;
use super::wire::{FrameHeader, FrameKind, Quad, TieredFrame, FRAME_HEADER_LEN, FRAME_TAG_LEN};
use crate::Error;
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::timeout;

// ---------------------------------------------------------------------------
// Frame framing over async sockets
// ---------------------------------------------------------------------------

/// Writes one frame: header then payload, ideally buffered behind the caller's
/// `BufWriter` so small frames coalesce until the flush tick.
#[cfg(test)]
pub(crate) async fn write_frame(
    writer: &mut (impl AsyncWrite + Unpin),
    quad: Quad,
    kind: FrameKind,
    payload: &[u8],
) -> Result<(), Error> {
    write_frame_with_limit(writer, quad, kind, payload, MAX_FRAME_LEN).await
}

/// Reads one frame: a 24-byte header followed by its payload.
#[cfg(test)]
pub(crate) async fn read_frame(
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<(FrameHeader, Bytes), Error> {
    read_frame_with_limits(reader, MAX_FRAME_LEN, None).await
}

/// Reads a frame while applying the configured payload and per-read idle
/// limits.  The header is decoded before allocating the payload buffer; the
/// payload is handed back as shared-ownership [`Bytes`] so downstream
/// zero-copy slicing stays possible.
pub(crate) async fn read_frame_with_limits(
    reader: &mut (impl AsyncRead + Unpin),
    max_frame_len: u32,
    idle_timeout: Option<std::time::Duration>,
) -> Result<(FrameHeader, Bytes), Error> {
    let mut header_bytes = vec![0u8; FRAME_HEADER_LEN];
    read_exact_with_idle(reader, &mut header_bytes, idle_timeout)
        .await
        .map_err(|error| {
            Error::Process(format!("remote edge closed while reading header: {error}"))
        })?;
    let header = FrameHeader::decode_with_max(&header_bytes, max_frame_len)?;
    let payload = read_payload_with_idle(reader, header.len as usize, idle_timeout).await?;
    Ok((header, payload))
}

/// Allocates and reads exactly `len` payload bytes as [`Bytes`]: one
/// allocation, shared ownership, no copy on hand-off.
async fn read_payload_with_idle(
    reader: &mut (impl AsyncRead + Unpin),
    len: usize,
    idle_timeout: Option<std::time::Duration>,
) -> Result<Bytes, Error> {
    let mut payload = BytesMut::with_capacity(len);
    payload.resize(len, 0);
    read_exact_with_idle(reader, &mut payload, idle_timeout)
        .await
        .map_err(|error| {
            Error::Process(format!("remote edge closed while reading payload: {error}"))
        })?;
    Ok(payload.freeze())
}

/// Writes one authenticated (protocol v2) frame: header, payload, then the
/// 16-byte truncated HMAC tag computed by `crypto` over header, payload, and
/// the connection-level monotonic sequence number.
pub(crate) async fn write_frame_sealed(
    writer: &mut (impl AsyncWrite + Unpin),
    quad: Quad,
    kind: FrameKind,
    payload: &[u8],
    max_frame_len: u32,
    crypto: &mut FrameCrypto,
) -> Result<(), Error> {
    if (payload.len() as u64) > u64::from(max_frame_len) {
        return Err(Error::Process(format!(
            "remote edge frame payload of {} bytes exceeds the {} limit",
            payload.len(),
            max_frame_len
        )));
    }
    let header = FrameHeader {
        quad,
        len: payload.len() as u32,
        kind,
    };
    let mut bytes = Vec::with_capacity(FRAME_HEADER_LEN + payload.len() + FRAME_TAG_LEN);
    header.encode_into(&mut bytes);
    bytes.extend_from_slice(payload);
    let tag_offset = bytes.len();
    bytes.resize(tag_offset + FRAME_TAG_LEN, 0);
    crypto.seal(&header, payload, &mut bytes[tag_offset..]);
    writer
        .write_all(&bytes)
        .await
        .map_err(|error| Error::Process(format!("remote edge write failed: {error}")))?;
    Ok(())
}

/// Reads one authenticated frame and verifies its tag and sequence number
/// BEFORE the payload is handed back for parsing: a forged, modified, or
/// replayed frame never reaches the payload consumers.
pub(crate) async fn read_frame_sealed(
    reader: &mut (impl AsyncRead + Unpin),
    max_frame_len: u32,
    idle_timeout: Option<std::time::Duration>,
    crypto: &mut FrameCrypto,
) -> Result<(FrameHeader, Bytes), Error> {
    let mut header_bytes = vec![0u8; FRAME_HEADER_LEN];
    read_exact_with_idle(reader, &mut header_bytes, idle_timeout)
        .await
        .map_err(|error| {
            Error::Process(format!("remote edge closed while reading header: {error}"))
        })?;
    let header = FrameHeader::decode_with_max(&header_bytes, max_frame_len)?;
    let payload = read_payload_with_idle(reader, header.len as usize, idle_timeout).await?;
    let mut tag = [0u8; FRAME_TAG_LEN];
    read_exact_with_idle(reader, &mut tag, idle_timeout)
        .await
        .map_err(|error| {
            Error::Process(format!(
                "remote edge closed while reading frame tag: {error}"
            ))
        })?;
    crypto.open(&header, &payload, &tag)?;
    Ok((header, payload))
}

/// Dispatches one frame write to the authenticated or plaintext variant
/// depending on whether a session authenticator is present.
pub(crate) async fn write_sealed_or_plain(
    writer: &mut (impl AsyncWrite + Unpin),
    quad: Quad,
    kind: FrameKind,
    payload: &[u8],
    max_frame_len: u32,
    crypto: Option<&mut FrameCrypto>,
) -> Result<(), Error> {
    match crypto {
        Some(crypto) => {
            write_frame_sealed(writer, quad, kind, payload, max_frame_len, crypto).await
        }
        None => write_frame_with_limit(writer, quad, kind, payload, max_frame_len).await,
    }
}

/// The boxed future returned by [`read_dispatch`] (type-complexity alias).
pub(crate) type ReadDispatchFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<(FrameHeader, Bytes), Error>> + Send + 'a>,
>;

/// Dispatches one frame read to the authenticated or plaintext variant
/// depending on whether a session authenticator is present (select-arm
/// friendly: exactly one future, no in-arm awaits).
pub(crate) fn read_dispatch<'a>(
    reader: &'a mut (impl AsyncRead + Unpin + Send),
    max_frame_len: u32,
    idle_timeout: Option<std::time::Duration>,
    crypto: Option<&'a mut FrameCrypto>,
) -> ReadDispatchFuture<'a> {
    Box::pin(async move {
        match crypto {
            Some(crypto) => read_frame_sealed(reader, max_frame_len, idle_timeout, crypto).await,
            None => read_frame_with_limits(reader, max_frame_len, idle_timeout).await,
        }
    })
}

/// Receipt-channel frame read with a two-level idle budget: a connection
/// that still awaits receipts belongs to a (possibly slow) live downstream
/// and gets the long budget; an idle one keeps fast dead-peer discovery.
/// Zero bytes are consumed on the restart path, so retrying is framing-safe;
/// after the first byte the frame always completes under the long budget.
/// `crypto` authenticates the frame before the payload is handed back; the
/// handshake-acknowledgement frame on an authenticated connection arrives
/// with `None` (it precedes the session keys).
pub(crate) async fn read_receipt_frame(
    reader: &mut (impl AsyncRead + Unpin),
    max_frame_len: u32,
    short_idle: std::time::Duration,
    receipt_wait: std::time::Duration,
    pending: &PendingReceipts,
    crypto: Option<&mut FrameCrypto>,
) -> Result<TieredFrame, Error> {
    let had_pending = !pending.is_empty();
    let budget = if had_pending {
        receipt_wait
    } else {
        short_idle
    };
    let mut header_bytes = vec![0u8; FRAME_HEADER_LEN];
    match timeout(budget, reader.read(&mut header_bytes[..1])).await {
        Err(_) => {
            if !had_pending && !pending.is_empty() {
                return Ok(TieredFrame::RacedIdleRestart);
            }
            return Err(Error::Process(
                "remote edge closed while reading header: read idle timeout".into(),
            ));
        }
        Ok(Err(error)) => {
            return Err(Error::Process(format!(
                "remote edge closed while reading header: {error}"
            )));
        }
        Ok(Ok(0)) => {
            return Err(Error::Process(
                "remote edge closed while reading header: early eof".into(),
            ));
        }
        Ok(Ok(_)) => {}
    }
    read_exact_with_idle(reader, &mut header_bytes[1..], Some(receipt_wait))
        .await
        .map_err(|error| {
            Error::Process(format!("remote edge closed while reading header: {error}"))
        })?;
    let header = FrameHeader::decode_with_max(&header_bytes, max_frame_len)?;
    let payload = read_payload_with_idle(reader, header.len as usize, Some(receipt_wait)).await?;
    if let Some(crypto) = crypto {
        let mut tag = [0u8; FRAME_TAG_LEN];
        read_exact_with_idle(reader, &mut tag, Some(receipt_wait))
            .await
            .map_err(|error| {
                Error::Process(format!(
                    "remote edge closed while reading frame tag: {error}"
                ))
            })?;
        crypto.open(&header, &payload, &tag)?;
    }
    Ok(TieredFrame::Frame((header, payload)))
}

pub(crate) async fn read_exact_with_idle(
    reader: &mut (impl AsyncRead + Unpin),
    buffer: &mut [u8],
    idle_timeout: Option<std::time::Duration>,
) -> std::io::Result<()> {
    let mut offset = 0;
    while offset < buffer.len() {
        let read = match idle_timeout {
            Some(idle_timeout) => timeout(idle_timeout, reader.read(&mut buffer[offset..]))
                .await
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "read idle timeout")
                })??,
            None => reader.read(&mut buffer[offset..]).await?,
        };
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "early eof",
            ));
        }
        // A successful partial read is progress.  The next iteration starts a
        // fresh idle timer instead of charging the peer for the whole frame.
        offset += read;
    }
    Ok(())
}

pub(crate) async fn write_frame_with_limit(
    writer: &mut (impl AsyncWrite + Unpin),
    quad: Quad,
    kind: FrameKind,
    payload: &[u8],
    max_frame_len: u32,
) -> Result<(), Error> {
    if (payload.len() as u64) > u64::from(max_frame_len) {
        return Err(Error::Process(format!(
            "remote edge frame payload of {} bytes exceeds the {} limit",
            payload.len(),
            max_frame_len
        )));
    }
    let mut bytes = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    FrameHeader {
        quad,
        len: payload.len() as u32,
        kind,
    }
    .encode_into(&mut bytes);
    bytes.extend_from_slice(payload);
    writer
        .write_all(&bytes)
        .await
        .map_err(|error| Error::Process(format!("remote edge write failed: {error}")))?;
    Ok(())
}

/// A byte stream carrying remote-edge frames. `TcpStream` in production; the
/// loopback halves of `tokio::io::duplex` in kernel semantic tests.
pub trait RemoteStream: AsyncRead + AsyncWrite + Unpin + Send + 'static {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> RemoteStream for T {}

/// Establishes the outbound connection for one quad. TCP in production; a
/// duplex factory in tests.
#[async_trait::async_trait]
pub trait EdgeTransport: Send + Sync {
    /// Connect to the node serving `quad`. Implementations should retry
    /// transient failures; returning `Err` fails the edge (and with it the
    /// attempt) closed.
    async fn connect(&self, quad: Quad) -> Result<Box<dyn RemoteStream>, Error>;
}

/// TCP transport with bounded exponential backoff, mirroring Arroyo's
/// `OutNetworkLink::connect`.
#[derive(Clone)]
pub(crate) struct TcpEdgeTransport {
    pub addr: std::net::SocketAddr,
    pub max_attempts: usize,
    /// When set, the outbound connection completes the fleet-CA mTLS
    /// handshake before the edge protocol runs. Handshake failures share
    /// the same retry/backoff budget as TCP failures.
    pub tls: Option<DataPlaneTlsConfig>,
}

#[async_trait::async_trait]
impl EdgeTransport for TcpEdgeTransport {
    async fn connect(&self, _quad: Quad) -> Result<Box<dyn RemoteStream>, Error> {
        let max_attempts = self.max_attempts.max(1);
        let mut last_error = None;
        for attempt in 0..max_attempts {
            // Bound one TCP establishment attempt: a peer that accepts the
            // SYN but never completes the handshake must not hold an edge
            // unready for the whole retry budget.
            let stream = match tokio::time::timeout(
                EDGE_CONNECT_TIMEOUT,
                tokio::net::TcpStream::connect(self.addr),
            )
            .await
            {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => {
                    let backoff = connect_backoff(attempt);
                    tracing::warn!(
                        "remote edge connect to {} failed (attempt {}/{}): {error}; retrying in {backoff:?}",
                        self.addr,
                        attempt + 1,
                        max_attempts
                    );
                    last_error = Some(error);
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                Err(_) => {
                    let backoff = connect_backoff(attempt);
                    tracing::warn!(
                        "remote edge connect to {} timed out after {:?} (attempt {}/{}); retrying in {backoff:?}",
                        self.addr,
                        EDGE_CONNECT_TIMEOUT,
                        attempt + 1,
                        max_attempts
                    );
                    last_error = Some(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "remote edge TCP connect timed out",
                    ));
                    tokio::time::sleep(backoff).await;
                    continue;
                }
            };
            let _ = stream.set_nodelay(true);
            let stream = match &self.tls {
                Some(tls) => {
                    match tokio::time::timeout(EDGE_CONNECT_TIMEOUT, tls.connect(stream)).await {
                        Ok(Ok(tls_stream)) => Box::new(tls_stream) as Box<dyn RemoteStream>,
                        Ok(Err(error)) => {
                            let backoff = connect_backoff(attempt);
                            tracing::warn!(
                            "remote edge TLS connect to {} failed (attempt {}/{}): {error}; retrying in {backoff:?}",
                            self.addr,
                            attempt + 1,
                            max_attempts
                        );
                            last_error = Some(std::io::Error::other(error.to_string()));
                            tokio::time::sleep(backoff).await;
                            continue;
                        }
                        Err(_) => {
                            let backoff = connect_backoff(attempt);
                            tracing::warn!(
                            "remote edge TLS handshake to {} timed out after {:?} (attempt {}/{}); retrying in {backoff:?}",
                            self.addr,
                            EDGE_CONNECT_TIMEOUT,
                            attempt + 1,
                            max_attempts
                        );
                            last_error = Some(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "remote edge TLS handshake timed out",
                            ));
                            tokio::time::sleep(backoff).await;
                            continue;
                        }
                    }
                }
                None => Box::new(stream) as Box<dyn RemoteStream>,
            };
            return Ok(stream);
        }
        Err(Error::Process(format!(
            "remote edge connect to {} failed after {max_attempts} attempts: {}",
            self.addr,
            last_error
                .map(|error| error.to_string())
                .unwrap_or_else(|| "unknown error".into())
        )))
    }
}

/// Plaintext transport constructor for tests and existing call sites; the
/// TLS-carrying form is built by the graph from the job's edge context.
impl TcpEdgeTransport {
    #[cfg(test)]
    pub(crate) fn plaintext(addr: std::net::SocketAddr, max_attempts: usize) -> Self {
        Self {
            addr,
            max_attempts,
            tls: None,
        }
    }
}

/// Bound on one TCP connect / TLS handshake attempt. `max_attempts` counts
/// retries, so without this a peer that accepts the socket but stalls the
/// handshake could hold an edge unready indefinitely.
const EDGE_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

pub(crate) fn connect_backoff(attempt: usize) -> std::time::Duration {
    // Grows with the attempt count, capped well below a checkpoint timeout so
    // a partitioned peer fails the attempt closed rather than hanging it.
    std::time::Duration::from_millis((50u64.saturating_mul(attempt as u64 + 1)).min(1_000))
}

/// Accepted-connection queue shared between the transport listener and the
/// serve loop.
pub(crate) type AcceptedQueue = (
    flume::Sender<Box<dyn RemoteStream>>,
    flume::Receiver<Box<dyn RemoteStream>>,
);

pub(crate) struct ConnectionFailure {
    pub(crate) job: Option<JobSessionKey>,
    pub(crate) error: Error,
}

impl From<Error> for ConnectionFailure {
    fn from(error: Error) -> Self {
        Self { job: None, error }
    }
}

/// TLS material for the cross-node data plane: the local node's
/// certificate and key plus the fleet CA that anchors peer verification.
/// Present = mTLS in both directions (the server requires a client
/// certificate chained to the same CA; the client verifies the server the
/// same way under the fixed name below). The HMAC session handshake still
/// runs on top: TLS is transport encryption plus a "certificate issued by
/// the fleet CA" gate, while node identity and job/generation binding stay
/// with the application-layer handshake.
#[derive(Clone)]
pub struct DataPlaneTlsConfig {
    connector: std::sync::Arc<tokio_rustls::TlsConnector>,
    acceptor: std::sync::Arc<tokio_rustls::TlsAcceptor>,
}

/// The fixed SNI/ServerName the outbound side verifies and node
/// certificates must carry as a SAN. Node identity itself is proven by the
/// HMAC handshake, so the name is only the chain-verification carrier.
pub(crate) const DATA_PLANE_TLS_SERVER_NAME: &str = "arkflow-data-plane";

impl DataPlaneTlsConfig {
    /// Build both directions from PEM material. Any parse or key-mismatch
    /// error is explicit: a half-loaded TLS config must fail startup, not
    /// silently degrade to plaintext.
    pub fn from_pem(cert_pem: &str, key_pem: &str, ca_pem: &str) -> Result<Self, Error> {
        // Feature unification across the workspace can enable more than one
        // rustls crypto provider; pin ring explicitly (idempotent).
        let _ = rustls::crypto::ring::default_provider().install_default();
        let fail = |context: &str, error: &str| {
            Error::Config(format!("data-plane TLS {context}: {error}"))
        };
        let read_certs =
            |pem: &str,
             context: &str|
             -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, Error> {
                let mut reader = std::io::BufReader::new(pem.as_bytes());
                let mut certs = Vec::new();
                for result in rustls_pemfile::certs(&mut reader) {
                    certs.push(result.map_err(|error| fail(context, &error.to_string()))?);
                }
                if certs.is_empty() {
                    return Err(fail(context, "no PEM certificates found"));
                }
                Ok(certs)
            };
        let cert_chain = read_certs(cert_pem, "certificate")?;
        let ca_certs = read_certs(ca_pem, "fleet CA")?;
        let private_key = {
            let mut reader = std::io::BufReader::new(key_pem.as_bytes());
            rustls_pemfile::private_key(&mut reader)
                .map_err(|error| fail("private key", &error.to_string()))?
                .ok_or_else(|| fail("private key", "no PEM private key found"))?
        };
        let mut ca_store = rustls::RootCertStore::empty();
        ca_store.add_parsable_certificates(ca_certs.clone());
        if ca_store.is_empty() {
            return Err(fail("fleet CA", "no parsable certificates"));
        }
        let ca_store_for_client = ca_store.clone();
        let server_config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(
                rustls::server::WebPkiClientVerifier::builder(std::sync::Arc::new(ca_store))
                    .build()
                    .map_err(|error| fail("client verifier", &error.to_string()))?,
            )
            .with_single_cert(cert_chain.clone(), private_key.clone_key())
            .map_err(|error| fail("server config", &error.to_string()))?;
        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(ca_store_for_client)
            .with_client_auth_cert(cert_chain, private_key)
            .map_err(|error| fail("client config", &error.to_string()))?;
        Ok(Self {
            connector: std::sync::Arc::new(tokio_rustls::TlsConnector::from(std::sync::Arc::new(
                client_config,
            ))),
            acceptor: std::sync::Arc::new(tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(
                server_config,
            ))),
        })
    }

    /// Outbound TLS wrap of an established TCP stream.
    pub async fn connect(
        &self,
        stream: tokio::net::TcpStream,
    ) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>, Error> {
        let name = rustls::pki_types::ServerName::try_from(DATA_PLANE_TLS_SERVER_NAME.to_owned())
            .map_err(|error| fail_tls_name(&error.to_string()))?;
        self.connector
            .connect(name, stream)
            .await
            .map_err(|error| Error::Process(format!("data-plane TLS connect failed: {error}")))
    }

    /// Inbound TLS handshake of an accepted TCP stream.
    pub async fn accept(
        &self,
        stream: tokio::net::TcpStream,
    ) -> Result<tokio_rustls::server::TlsStream<tokio::net::TcpStream>, Error> {
        self.acceptor
            .accept(stream)
            .await
            .map_err(|error| Error::Process(format!("data-plane TLS accept failed: {error}")))
    }
}

fn fail_tls_name(error: &str) -> Error {
    Error::Config(format!("data-plane TLS server name invalid: {error}"))
}
