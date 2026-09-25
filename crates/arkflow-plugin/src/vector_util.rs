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

//! Shared helpers for the AI/vector components (Qdrant/Milvus/pgvector
//! processors and outputs). Every helper takes the component name so error
//! messages keep their per-component prefix.

use std::sync::Arc;
use std::time::Duration;

use arkflow_core::{Error, MessageBatch, MessageBatchRef};
use datafusion::arrow::array::{Array, ArrayRef, FixedSizeListArray, Float32Array, ListArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use reqwest::Client;

/// Reads a Float32 vector column (FixedSizeList(Float32) or List(Float32))
/// into per-row vectors. Null rows and null inner elements are rejected with
/// errors that name the column, the row, and (for inner nulls) the slot.
pub(crate) fn extract_vectors(
    component: &str,
    batch: &MessageBatchRef,
    field: &str,
) -> Result<Vec<Vec<f32>>, Error> {
    let column = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == field)
        .map(|index| batch.column(index))
        .ok_or_else(|| {
            Error::Process(format!(
                "{component}: input column '{field}' not found"
            ))
        })?;
    let rows = column.len();
    let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(rows);
    match column.data_type() {
        DataType::FixedSizeList(_, dim) => {
            let list = column
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| not_a_vector_error(component, field))?;
            let values = list
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| not_a_vector_error(component, field))?;
            for row in 0..rows {
                if list.is_null(row) {
                    return Err(null_vector_error(component, field, row));
                }
                let start = row as i64 * *dim as i64;
                vectors.push(
                    (start..start + *dim as i64)
                        .map(|slot| {
                            if values.is_null(slot as usize) {
                                Err(null_vector_element_error(
                                    component,
                                    field,
                                    row,
                                    slot as usize,
                                ))
                            } else {
                                Ok(values.value(slot as usize))
                            }
                        })
                        .collect::<Result<Vec<f32>, Error>>()?,
                );
            }
        }
        DataType::List(_) => {
            let list = column
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| not_a_vector_error(component, field))?;
            let values = list
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| not_a_vector_error(component, field))?;
            for row in 0..rows {
                if list.is_null(row) {
                    return Err(null_vector_error(component, field, row));
                }
                let offsets = list.value_offsets();
                vectors.push(
                    (offsets[row]..offsets[row + 1])
                        .map(|slot| {
                            let slot = slot as usize;
                            if values.is_null(slot) {
                                Err(null_vector_element_error(component, field, row, slot))
                            } else {
                                Ok(values.value(slot))
                            }
                        })
                        .collect::<Result<Vec<f32>, Error>>()?,
                );
            }
        }
        other => {
            return Err(Error::Process(format!(
                "{component}: column '{field}' must be FixedSizeList(Float32) or List(Float32), got {other:?}"
            )));
        }
    }
    if let Some((row, _)) = vectors.iter().enumerate().find(|(_, v)| v.is_empty()) {
        return Err(Error::Process(format!(
            "{component}: column '{field}' has an empty vector at row {row}"
        )));
    }
    Ok(vectors)
}

fn not_a_vector_error(component: &str, field: &str) -> Error {
    Error::Process(format!(
        "{component}: column '{field}' is not a Float32 vector list"
    ))
}

fn null_vector_error(component: &str, field: &str, row: usize) -> Error {
    Error::Process(format!(
        "{component}: column '{field}' has a null vector at row {row}"
    ))
}

fn null_vector_element_error(component: &str, field: &str, row: usize, slot: usize) -> Error {
    Error::Process(format!(
        "{component}: column '{field}' has a null vector element at row {row}, slot {slot}"
    ))
}

/// Appends a UTF-8 column of `values` to a copy of `batch`.
pub(crate) fn append_column(
    component: &str,
    batch: &MessageBatch,
    target_field: &str,
    values: &[String],
) -> Result<MessageBatch, Error> {
    let schema = batch.schema();
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(target_field, DataType::Utf8, true)));
    let mut columns: Vec<ArrayRef> = (0..batch.num_columns())
        .map(|index| batch.column(index).clone())
        .collect();
    columns.push(Arc::new(StringArray::from(values.to_vec())));

    let record_batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(
        |e| Error::Process(format!("{component}: batch rebuild failed: {e}")),
    )?;
    Ok(MessageBatch::new_arrow(record_batch))
}

/// Truncates an HTTP response body for inclusion in error messages.
pub(crate) fn truncate_body(body: &str) -> &str {
    match body.char_indices().nth(512) {
        Some((index, _)) => &body[..index],
        None => body,
    }
}

/// Builds the shared HTTP client: total timeout from the component config,
/// and loopback endpoints (local dev instances, tests) bypass a system
/// proxy — proxying localhost is never what a user means.
pub(crate) fn build_http_client(timeout_ms: u64, base_url: &str) -> Result<Client, Error> {
    let is_loopback = reqwest::Url::parse(&format!("{}/", base_url.trim_end_matches('/')))
        .ok()
        .and_then(|url| url.host_str().map(|host| host.to_ascii_lowercase()))
        .map(|host| host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "[::1]")
        .unwrap_or(false);
    let mut builder = Client::builder().timeout(Duration::from_millis(timeout_ms));
    if is_loopback {
        builder = builder.no_proxy();
    }
    builder
        .build()
        .map_err(|e| Error::Config(format!("Unable to create HTTP client: {e}")))
}

/// Quotes a SQL identifier and escapes embedded double quotes (`"` → `""`)
/// so identifiers cannot break out of the quoting.
pub(crate) fn escape_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Percent-encodes a collection name as a single RFC 3986 path segment:
/// unreserved characters (ALPHA / DIGIT / `-` / `.` / `_` / `~`) pass
/// through byte-for-byte, everything else becomes an uppercase `%XX`
/// sequence. This keeps ordinary names' URLs identical while names
/// containing `/`, `?`, `#`, spaces, etc. still address the same
/// collection instead of corrupting the request path.
pub(crate) fn encode_path_segment(name: &str) -> String {
    let mut encoded = String::with_capacity(name.len());
    for byte in name.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char)
            }
            other => encoded.push_str(&format!("%{other:02X}")),
        }
    }
    encoded
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Arc;

    /// Byte-at-a-time HTTP/1.1 mock server shared by the vector-component
    /// tests. Logs `(head, body)` per request and tracks the maximum
    /// observed in-flight request count.
    pub(crate) struct MockApi {
        addr: std::net::SocketAddr,
        requests: Arc<std::sync::Mutex<Vec<(String, String)>>>,
        max_in_flight: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl MockApi {
        pub(crate) fn spawn<F>(handler: F) -> Self
        where
            F: Fn(&str) -> (u16, String) + Send + Sync + 'static,
        {
            let handler = Arc::new(handler);
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let tracker_in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let max_in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let request_log = requests.clone();
            let tracker_max = max_in_flight.clone();
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let mut stream = match stream {
                        Ok(stream) => stream,
                        Err(_) => break,
                    };
                    let handler = handler.clone();
                    let request_log = request_log.clone();
                    let tracker_in_flight = tracker_in_flight.clone();
                    let tracker_max = tracker_max.clone();
                    std::thread::spawn(move || {
                        let now = tracker_in_flight
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                            + 1;
                        tracker_max.fetch_max(now, std::sync::atomic::Ordering::SeqCst);

                        let mut buffer = Vec::new();
                        let mut byte = [0u8; 1];
                        loop {
                            use std::io::Read;
                            if stream.read_exact(&mut byte).is_err() {
                                break;
                            }
                            buffer.push(byte[0]);
                            if buffer.ends_with(b"\r\n\r\n") {
                                break;
                            }
                        }
                        let head = String::from_utf8_lossy(&buffer).to_string();
                        let content_length = head
                            .to_ascii_lowercase()
                            .split("content-length:")
                            .nth(1)
                            .and_then(|rest| rest.split("\r\n").next())
                            .and_then(|value| value.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        let mut body_bytes = vec![0u8; content_length];
                        if content_length > 0 {
                            use std::io::Read;
                            let _ = stream.read_exact(&mut body_bytes);
                        }
                        let body = String::from_utf8_lossy(&body_bytes).to_string();
                        request_log.lock().unwrap().push((head.clone(), body.clone()));

                        let (status, response_body) = handler(&body);
                        // Decrement before the response is written: with
                        // `Connection: close` the client opens its next
                        // request's connection as soon as it sees the bytes,
                        // and that handler's `fetch_add` could otherwise race
                        // this thread's `fetch_sub`, inflating the observed
                        // in-flight count past the real client-side cap.
                        tracker_in_flight.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        let response = format!(
                            "HTTP/1.1 {status} MOCK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
                            response_body.len()
                        );
                        use std::io::Write;
                        let _ = stream.write_all(response.as_bytes());
                        let _ = stream.flush();
                    });
                }
            });
            Self {
                addr,
                requests,
                max_in_flight,
            }
        }

        pub(crate) fn addr(&self) -> std::net::SocketAddr {
            self.addr
        }

        pub(crate) fn requests(&self) -> Vec<(String, String)> {
            self.requests.lock().unwrap().clone()
        }

        pub(crate) fn last_request(&self) -> (String, String) {
            self.requests
                .lock()
                .unwrap()
                .last()
                .cloned()
                .expect("at least one request recorded")
        }

        pub(crate) fn max_in_flight(&self) -> usize {
            self.max_in_flight.load(std::sync::atomic::Ordering::SeqCst)
        }
    }
}
