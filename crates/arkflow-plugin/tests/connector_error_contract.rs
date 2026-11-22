//! Structural pins for the connector error-variant contract.
//!
//! Normative source: `openspec/specs/connector-recovery-contract`. The
//! kernel branches on `Error` variants (EOF = graceful end of a bounded
//! source; Disconnection = reconnect; everything else = fatal), so a
//! connector that picks the wrong variant silently changes Job semantics.
//! Per-connector behavioral tests exist, but they only cover the files
//! their author remembered — the Pulsar `Ok(None) -> EOF` bug escaped the
//! batch-1 fixes exactly because nothing scanned the siblings. These rules
//! turn that drift into a red test at review time.
//!
//! Rules (production code only — everything after the first
//! `#[cfg(test)]` in a file is ignored):
//!
//! * R-EOF-INPUT — `Error::EOF` in `src/input/` is legal only in bounded
//!   sources (`BOUNDED_EOF_INPUT`) or on teardown paths (receiver closed /
//!   cancelled) marked with `// contract-eof: teardown` on the same or the
//!   preceding line.
//! * R-EOF-OUTPUT — `Error::EOF` never appears in `src/output/`.
//! * R-GUARD-OUTPUT — `Error::Disconnection` in `src/output/` is legal only
//!   in the write-path transient classifiers (`OUTPUT_DISCONNECTION_ALLOW`);
//!   "not connected" guards must use `Connection(msg)` instead.
//! * R-CHANNEL — `src/input/multiple_inputs.rs` must surface interior
//!   channel closure as `Error::InputChannelClosed`, not a `Process`
//!   re-wrap.

use std::collections::HashMap;
use std::path::PathBuf;

use arkflow_core::{Error, MessageBatch, MessageBatchRef, Resource};

/// Bounded sources: natural exhaustion (`Error::EOF`) is their normal end.
const BOUNDED_EOF_INPUT: &[&str] = &[
    "file.rs",
    "generate.rs",
    "memory.rs",
    "multiple_inputs.rs",
    "sql.rs",
];

/// Outputs whose *write path* legitimately classifies transient transport
/// failures as `Error::Disconnection` (kernel still treats sink errors as
/// fatal; the variant is documentation-grade honesty here). `codec_helper.rs`
/// hosts the shared sqlx classifier itself.
const OUTPUT_DISCONNECTION_ALLOW: &[&str] = &[
    "codec_helper.rs",
    "nats.rs",
    "pgvector.rs",
    "redis.rs",
    "sql.rs",
];

const TEARDOWN_MARKER: &str = "contract-eof: teardown";

/// Production section of a source file: everything before the first
/// `#[cfg(test)]`.
fn production_section(source: &str) -> &str {
    match source.find("#[cfg(test)]") {
        Some(idx) => &source[..idx],
        None => source,
    }
}

fn plugin_src_dir(subdir: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("src");
    path.push(subdir);
    path
}

fn rust_files(dir: &PathBuf) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let entries =
        std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {:?} failed: {e}", dir));
    for entry in entries {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let source = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("read {:?} failed: {e}", path));
            out.push((name, source));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn violations(
    file: &str,
    section: &str,
    needle: &str,
    allowed: impl Fn(&str, usize) -> bool,
) -> Vec<String> {
    let mut found = Vec::new();
    for (idx, line) in section.lines().enumerate() {
        if line.contains(needle) {
            let line_no = idx + 1;
            if !allowed(line, line_no) {
                found.push(format!("{file}:{line_no}: {}", line.trim()));
            }
        }
    }
    found
}

#[test]
fn r_eof_input_is_bounded_or_marked_teardown() {
    let mut report = Vec::new();
    for (file, source) in rust_files(&plugin_src_dir("input")) {
        let section = production_section(&source);
        let allowlisted = BOUNDED_EOF_INPUT.contains(&file.as_str());
        report.extend(violations(&file, section, "Error::EOF", |line, line_no| {
            if allowlisted {
                return true;
            }
            // Marker on the same line or within the three lines above (the
            // explaining comment may span multiple lines).
            let lines: Vec<&str> = section.lines().collect();
            (0..=3).any(|back| {
                line_no
                    .checked_sub(1 + back)
                    .and_then(|idx| lines.get(idx))
                    .is_some_and(|l| l.contains(TEARDOWN_MARKER))
            }) || line.contains(TEARDOWN_MARKER)
        }));
    }
    assert!(
        report.is_empty(),
        "R-EOF-INPUT violated: `Error::EOF` outside bounded sources without a \
         `{TEARDOWN_MARKER}` marker. An unbounded source ending its stream must \
         return `Error::Disconnection` so the kernel reconnects instead of \
         broadcasting EOS. Violations:\n{}",
        report.join("\n")
    );
}

#[test]
fn r_eof_output_is_forbidden() {
    let mut report = Vec::new();
    for (file, source) in rust_files(&plugin_src_dir("output")) {
        report.extend(violations(
            &file,
            production_section(&source),
            "Error::EOF",
            |_, _| false,
        ));
    }
    assert!(
        report.is_empty(),
        "R-EOF-OUTPUT violated: outputs must never return `Error::EOF` (they do \
         not end streams). Violations:\n{}",
        report.join("\n")
    );
}

#[test]
fn r_output_disconnection_is_classifier_only() {
    let mut report = Vec::new();
    for (file, source) in rust_files(&plugin_src_dir("output")) {
        let allowlisted = OUTPUT_DISCONNECTION_ALLOW.contains(&file.as_str());
        report.extend(violations(
            &file,
            production_section(&source),
            "Error::Disconnection",
            |_, _| allowlisted,
        ));
    }
    assert!(
        report.is_empty(),
        "R-GUARD-OUTPUT violated: `Error::Disconnection` in an output outside \
         {:?}. 'Not connected' guards must use `Connection(msg)`; transient \
         write-path classification belongs in the allowed classifiers. \
         Violations:\n{}",
        OUTPUT_DISCONNECTION_ALLOW,
        report.join("\n")
    );
}

#[test]
fn r_multiple_inputs_uses_input_channel_closed() {
    let (file, source) = rust_files(&plugin_src_dir("input"))
        .into_iter()
        .find(|(name, _)| name == "multiple_inputs.rs")
        .expect("multiple_inputs.rs exists");
    let section = production_section(&source);
    assert!(
        section.contains("Error::InputChannelClosed"),
        "R-CHANNEL violated: {file} must surface interior channel closure as \
         `Error::InputChannelClosed` (the kernel handles it structurally), not \
         re-wrap it into `Error::Process`."
    );
}

// ---------------------------------------------------------------------------
// Behavioral pins for the not-connected guards of allowlisted outputs.
//
// R-GUARD-OUTPUT above is file-granular: a "not connected" guard regressing
// to `Error::Disconnection` inside an allowlisted file would slip through.
// These pins close that gap (spec: guards map to `Connection(msg)`).

fn resource() -> Resource {
    Resource {
        temporary: HashMap::new(),
        input_names: std::cell::RefCell::new(vec![]),
    }
}

async fn guard_error(output_type: &str, config: serde_json::Value) -> Error {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        arkflow_plugin::output::init().expect("plugin output init");
    });
    let out = arkflow_core::output::OutputConfig {
        output_type: output_type.into(),
        name: None,
        codec: None,
        config: Some(config),
    };
    let output = out.build(&resource()).expect("build output");
    let batch: MessageBatchRef =
        std::sync::Arc::new(MessageBatch::new_binary(vec![b"x".to_vec()]).unwrap());
    output
        .write(batch)
        .await
        .expect_err("write must fail before connect")
}

#[tokio::test]
async fn nats_output_write_before_connect_is_connection_error() {
    let err = guard_error(
        "nats",
        serde_json::json!({
            "url": "nats://127.0.0.1:1",
            "mode": {"type": "regular", "subject": {"type": "value", "value": "s"}},
            "value_field": null,
        }),
    )
    .await;
    assert!(matches!(err, Error::Connection(_)), "{err:?}");
}

#[tokio::test]
async fn redis_output_write_before_connect_is_connection_error() {
    let err = guard_error(
        "redis",
        serde_json::json!({
            "mode": {"type": "single", "url": "redis://127.0.0.1:1"},
            "redis_type": {"type": "publish", "channel": {"type": "value", "value": "ch"}},
            "value_field": null,
        }),
    )
    .await;
    assert!(matches!(err, Error::Connection(_)), "{err:?}");
}

// ---------------------------------------------------------------------------
// Connector-skeleton hygiene pins (consolidate-connector-skeleton).

#[test]
fn input_delivery_channels_use_the_shared_capacity() {
    // Reject any `flume::bounded::<T>(<literal>)` — the capacity must come
    // from the shared DELIVERY_CHANNEL_CAPACITY constant, whatever the
    // literal's value.
    fn literal_capacity(line: &str) -> bool {
        let Some(start) = line.find("bounded") else {
            return false;
        };
        let after = &line[start..];
        let Some(open) = after.find('(') else {
            return false;
        };
        let rest = &after[open + 1..];
        let arg = rest.split([')', ',']).next().unwrap_or("");
        let arg = arg.trim();
        // `::<T>(1000)` style puts the turbofish before the paren; the
        // paren argument here is the capacity itself.
        !arg.is_empty() && arg.chars().all(|c| c.is_ascii_digit())
    }

    let mut report = Vec::new();
    for (file, source) in rust_files(&plugin_src_dir("input")) {
        for (idx, line) in production_section(&source).lines().enumerate() {
            if line.contains("flume::bounded") && literal_capacity(line) {
                report.push(format!("{}:{}: {}", file, idx + 1, line.trim()));
            }
        }
    }
    assert!(
        report.is_empty(),
        "input delivery channels must use DELIVERY_CHANNEL_CAPACITY (1024, \
         kernel-aligned) instead of a numeric-literal capacity. \
         Violations:\n{}",
        report.join("\n")
    );
}

#[test]
fn outputs_must_not_hand_roll_power_backoff() {
    let mut report = Vec::new();
    for (file, source) in rust_files(&plugin_src_dir("output")) {
        for (idx, line) in production_section(&source).lines().enumerate() {
            if line.contains("2u64.pow") {
                report.push(format!("{}:{}: {}", file, idx + 1, line.trim()));
            }
        }
    }
    assert!(
        report.is_empty(),
        "outputs must use the shared RetryUtils backoff (capped, \
         overflow-safe) instead of hand-rolled 2u64.pow schedules. \
         Violations:\n{}",
        report.join("\n")
    );
}
