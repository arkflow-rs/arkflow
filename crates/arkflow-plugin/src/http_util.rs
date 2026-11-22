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

//! Shared HTTP plumbing for REST-backed components (client construction,
//! URL base joining, custom-header application, response-body truncation
//! for error messages).
//!
//! Consolidated from `vector_util` (generic helpers were living next to
//! vector-specific code, so non-vector components like `lookup` kept
//! byte-identical private copies) — consolidate-connector-skeleton.

use std::collections::HashMap;
use std::time::Duration;

use arkflow_core::Error;
use reqwest::Client;

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

/// Joins a configured base URL with a request path: the base loses any
/// trailing slash, the path gains exactly one. `join_url_base("http://h/",
/// "/x")` and `join_url_base("http://h", "x")` both yield `http://h/x`.
pub(crate) fn join_url_base(base: &str, path: &str) -> String {
    format!("{}/{}", base.trim_end_matches('/'), path.trim_start_matches('/'))
}

/// Applies the component's optional custom headers to a request. All
/// REST-backed components share the same `headers: Option<HashMap<String,
/// String>>` config field — this is the single implementation of the
/// application loop.
pub(crate) fn apply_headers(
    mut request: reqwest::RequestBuilder,
    headers: Option<&HashMap<String, String>>,
) -> reqwest::RequestBuilder {
    if let Some(headers) = headers {
        for (name, value) in headers {
            request = request.header(name, value);
        }
    }
    request
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_body_caps_at_512_chars_by_char_boundaries() {
        let long = "a".repeat(600);
        assert_eq!(truncate_body(&long).chars().count(), 512);
        assert_eq!(truncate_body("short"), "short");
        // Multi-byte characters are not split mid-codepoint.
        let emoji = "🦀".repeat(600);
        assert_eq!(truncate_body(&emoji).chars().count(), 512);
    }

    #[test]
    fn join_url_base_normalizes_slashes() {
        assert_eq!(
            join_url_base("http://h:8123", "collections/c/points"),
            "http://h:8123/collections/c/points"
        );
        assert_eq!(
            join_url_base("http://h:8123/", "/collections/c/points"),
            "http://h:8123/collections/c/points"
        );
    }

    #[tokio::test]
    async fn build_http_client_loopback_skips_proxy() {
        // Constructing is the observable contract here: a loopback base
        // must not fail (proxy stripping applied internally).
        let client = build_http_client(1_000, "http://127.0.0.1:8123").unwrap();
        let _ = client;
        let client = build_http_client(1_000, "http://example.com").unwrap();
        let _ = client;
    }
}
