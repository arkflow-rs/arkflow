/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! Helper functions for codec integration in output components

use arkflow_core::codec::Codec;
use arkflow_core::{Bytes, Error, MessageBatchRef, DEFAULT_BINARY_VALUE_FIELD};
use std::sync::Arc;

/// Apply codec encoding to message batch
///
/// # Arguments
/// * `msg` - The message batch to encode
/// * `codec` - Optional codec to apply
///
/// # Returns
/// * `Ok(Vec<Bytes>)` - Encoded bytes or binary representation
/// * `Err(Error)` - If codec application fails
pub async fn apply_codec_encode(
    msg: &MessageBatchRef,
    codec: &Option<Arc<dyn Codec>>,
) -> Result<Vec<Bytes>, Error> {
    if let Some(c) = codec {
        // Clone the MessageBatch for encoding
        let msg_clone = (**msg).clone();
        c.encode(msg_clone).await
    } else {
        // Default: convert to binary
        let binary_data = msg.to_binary(DEFAULT_BINARY_VALUE_FIELD)?;
        // Convert Vec<&[u8]> to Vec<Vec<u8>>
        Ok(binary_data.into_iter().map(|b| b.to_vec()).collect())
    }
}

/// Reject a payload list that is shorter than the batch. `to_binary` skips
/// NULL cells, so a batch with any NULL `__value__` produces fewer payloads
/// than rows. Outputs that route each payload by its row position (topics,
/// keys, Redis fields) must fail instead of binding later rows to earlier
/// routing values.
pub fn ensure_row_aligned(payloads: &[Bytes], rows: usize, output: &str) -> Result<(), Error> {
    if payloads.len() != rows {
        return Err(Error::Process(format!(
            "{output} output: default binary encoding produced {} payloads for {rows} rows; a \
             NULL `{DEFAULT_BINARY_VALUE_FIELD}` cannot be routed by position — set `value_field` \
             or ensure every row has a payload",
            payloads.len()
        )));
    }
    Ok(())
}

/// Classify a sqlx write-path failure per `connector-recovery-contract`
/// (output errors are documentation-grade for the kernel — sinks are fatal
/// — but the variant must be truthful): transient transport/availability
/// failures map to [`Error::Disconnection`] (a fresh connection could
/// succeed); deterministic failures (SQL syntax, constraint violations,
/// type/decode errors, driver configuration) map to [`Error::Process`].
///
/// Server errors are decided by SQLSTATE class through the driver-agnostic
/// [`sqlx::error::DatabaseError::code`] (uniform for MySQL/Postgres).
/// MySQL additionally reports transport loss through error numbers 2006
/// ("server has gone away") and 2013 ("lost connection") under the generic
/// `HY000` class, so the classifier downcasts for those before falling
/// back to the SQLSTATE.
#[cfg(any(feature = "connectors-sql", feature = "ai-vector"))]
pub(crate) fn classify_sqlx_error(error: &sqlx::Error) -> Error {
    match error {
        sqlx::Error::Io(_) => Error::Disconnection,
        sqlx::Error::Tls(_) => Error::Disconnection,
        sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed | sqlx::Error::WorkerCrashed => {
            Error::Disconnection
        }
        // A failed BEGIN is almost always a broken/idle-closed connection.
        sqlx::Error::BeginFailed => Error::Disconnection,
        sqlx::Error::Database(db) => {
            if db
                .try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
                .is_some_and(|mysql| matches!(mysql.number(), 2006 | 2013))
            {
                return Error::Disconnection;
            }
            match db.code().as_deref().map(str::trim) {
                Some(code) if sqlstate_is_transient(code) => Error::Disconnection,
                _ => Error::Process(format!(
                    "SQL write failed deterministically (retrying cannot help): {error}"
                )),
            }
        }
        // Configuration/type/decode/etc. are deterministic given the batch.
        _ => Error::Process(format!(
            "SQL write failed deterministically (retrying cannot help): {error}"
        )),
    }
}

/// Whether retrying on a fresh connection can succeed for this SQLSTATE:
/// `08` connection exception, `40` transaction rollback (serialization
/// failure / deadlock — a retry of the whole transaction may pass),
/// `57` operator intervention (server shutdown / admin cancel),
/// `53300` too many connections. Everything else (syntax `42xxx`,
/// constraint `23xxx`, data exceptions `22xxx`) is deterministic.
#[cfg(any(feature = "connectors-sql", feature = "ai-vector"))]
fn sqlstate_is_transient(code: &str) -> bool {
    code.starts_with("08") || code.starts_with("40") || code.starts_with("57") || code == "53300"
}

/// [`classify_sqlx_error`] with a deterministic-failure context: the
/// transient `Disconnection` carries no payload (its Display is
/// "Connection lost"), so only the `Process` arm gets the component prefix.
#[cfg(any(feature = "connectors-sql", feature = "ai-vector"))]
pub(crate) fn classify_sqlx_error_ctx(error: &sqlx::Error, context: &str) -> Error {
    match classify_sqlx_error(error) {
        Error::Process(_) => Error::Process(format!("{context}: {error}")),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::MessageBatch;

    #[tokio::test]
    async fn test_apply_codec_encode_no_codec() {
        // Create a simple message batch
        let batch = MessageBatch::new_binary(vec![b"test data".to_vec()]).unwrap();
        let msg_ref = std::sync::Arc::new(batch);
        let codec: Option<Arc<dyn Codec>> = None;

        let result = apply_codec_encode(&msg_ref, &codec).await;
        assert!(result.is_ok());

        let bytes = result.unwrap();
        assert_eq!(bytes.len(), 1);
        assert_eq!(bytes[0], b"test data".to_vec());
    }

    /// A codec that encodes every batch to one fixed payload, mirroring the
    /// shape of the real JSON codec (one Bytes per row).
    struct FixedBytesCodec;

    #[async_trait::async_trait]
    impl arkflow_core::codec::Encoder for FixedBytesCodec {
        async fn encode(&self, _batch: MessageBatch) -> Result<Vec<Bytes>, Error> {
            Ok(vec![b"encoded".to_vec()])
        }
    }

    #[async_trait::async_trait]
    impl arkflow_core::codec::Decoder for FixedBytesCodec {
        async fn decode(&self, _b: Vec<Bytes>) -> Result<MessageBatch, Error> {
            Err(Error::Process("not implemented in test codec".to_string()))
        }
    }

    #[tokio::test]
    async fn test_apply_codec_encode_with_codec_uses_codec_output() {
        let batch = MessageBatch::new_binary(vec![b"raw".to_vec()]).unwrap();
        let msg_ref = std::sync::Arc::new(batch);
        let codec: Option<Arc<dyn Codec>> = Some(std::sync::Arc::new(FixedBytesCodec));

        let bytes = apply_codec_encode(&msg_ref, &codec).await.unwrap();
        assert_eq!(bytes, vec![b"encoded".to_vec()]);
    }

    /// A codec whose encoder always fails, to check error propagation.
    struct FailingCodec;

    #[async_trait::async_trait]
    impl arkflow_core::codec::Encoder for FailingCodec {
        async fn encode(&self, _batch: MessageBatch) -> Result<Vec<Bytes>, Error> {
            Err(Error::Process("encode boom".to_string()))
        }
    }

    #[async_trait::async_trait]
    impl arkflow_core::codec::Decoder for FailingCodec {
        async fn decode(&self, _b: Vec<Bytes>) -> Result<MessageBatch, Error> {
            Err(Error::Process("decode boom".to_string()))
        }
    }

    #[tokio::test]
    async fn test_apply_codec_encode_propagates_codec_error() {
        let batch = MessageBatch::new_binary(vec![b"raw".to_vec()]).unwrap();
        let msg_ref = std::sync::Arc::new(batch);
        let codec: Option<Arc<dyn Codec>> = Some(std::sync::Arc::new(FailingCodec));

        let err = apply_codec_encode(&msg_ref, &codec).await.unwrap_err();
        assert!(format!("{err}").contains("encode boom"), "{err}");
    }

    /// Test double standing in for a driver `DatabaseError`: carries a
    /// SQLSTATE `code()` like both the MySQL and Postgres drivers do
    /// through the trait method.
    #[cfg(any(feature = "connectors-sql", feature = "ai-vector"))]
    #[derive(Debug)]
    struct FakeDbError {
        code: Option<String>,
    }
    #[cfg(any(feature = "connectors-sql", feature = "ai-vector"))]
    impl std::fmt::Display for FakeDbError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "fake database error")
        }
    }
    #[cfg(any(feature = "connectors-sql", feature = "ai-vector"))]
    impl std::error::Error for FakeDbError {}
    #[cfg(any(feature = "connectors-sql", feature = "ai-vector"))]
    impl sqlx::error::DatabaseError for FakeDbError {
        fn message(&self) -> &str {
            "fake database error"
        }
        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            self.code.as_deref().map(std::borrow::Cow::Borrowed)
        }
        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }
    }

    #[cfg(any(feature = "connectors-sql", feature = "ai-vector"))]
    fn db_error(code: Option<&str>) -> sqlx::Error {
        sqlx::Error::Database(Box::new(FakeDbError {
            code: code.map(str::to_string),
        }))
    }

    #[cfg(any(feature = "connectors-sql", feature = "ai-vector"))]
    #[test]
    fn classify_sqlx_error_transients() {
        use crate::output::codec_helper::classify_sqlx_error;
        // Transport family.
        for error in [
            sqlx::Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "reset",
            )),
            sqlx::Error::PoolTimedOut,
            sqlx::Error::PoolClosed,
            sqlx::Error::WorkerCrashed,
            sqlx::Error::BeginFailed,
            // Connection-exception SQLSTATE class (08): PG 08006, MySQL 08S01.
            db_error(Some("08006")),
            db_error(Some("08S01")),
            // Transaction rollback class (40): serialization / deadlock.
            db_error(Some("40001")),
            db_error(Some("40P01")),
            // Operator intervention (57): server shutdown / cancel.
            db_error(Some("57P01")),
            // Too many connections.
            db_error(Some("53300")),
        ] {
            assert!(
                matches!(classify_sqlx_error(&error), Error::Disconnection),
                "should be transient: {error}"
            );
        }
    }

    #[cfg(any(feature = "connectors-sql", feature = "ai-vector"))]
    #[test]
    fn classify_sqlx_error_deterministic() {
        use crate::output::codec_helper::classify_sqlx_error;
        // Syntax (42xxx), constraint (23xxx), data exceptions (22xxx) and
        // the generic class without a transport number.
        for error in [
            db_error(Some("42601")),
            db_error(Some("23505")),
            db_error(Some("22012")),
            db_error(Some("HY000")),
            db_error(None),
            // Non-database errors are deterministic given the batch.
            sqlx::Error::Configuration("bad config".into()),
            sqlx::Error::ColumnNotFound("col".into()),
            sqlx::Error::Protocol("protocol violation".into()),
        ] {
            assert!(
                matches!(classify_sqlx_error(&error), Error::Process(_)),
                "should be deterministic: {error}"
            );
        }
    }
}
