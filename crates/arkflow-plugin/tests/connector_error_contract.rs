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
//! * R-POW — no integer `.pow(`-family backoff math in `src/output/` or
//!   `src/input/` production code (variant-proof: underscore and
//!   saturating/checked/wrapping spellings all flag; a non-backoff pow
//!   needs `// contract-allow: pow`).
//! * R-RETRY-ADOPTER — every shared-retry adopter file (`RETRY_ADOPTERS`)
//!   must reference `crate::retry` in its production code.
//!
//! The capacity rule (delivery channels must use the shared
//! `DELIVERY_CHANNEL_CAPACITY` constant) lives below for historical
//! reasons; normatively it belongs to `connector-resilience`.

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
    "amqp09.rs",
    "codec_helper.rs",
    "iceberg.rs",
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
async fn iceberg_output_write_before_connect_is_connection_error() {
    let err = guard_error(
        "iceberg",
        serde_json::json!({
            "catalog": {"uri": "http://127.0.0.1:1/catalog", "warehouse": "demo"},
            "namespace": "analytics",
            "table": "events",
            "exactly_once": false,
            "writer_id": null,
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

#[cfg(feature = "connectors-amqp09")]
#[tokio::test]
async fn amqp09_output_write_before_connect_is_connection_error() {
    let err = guard_error(
        "amqp09",
        serde_json::json!({
            "url": "amqp://127.0.0.1:1/%2f",
            "exchange": "",
            "routing_key": {"type": "value", "value": "q"},
        }),
    )
    .await;
    assert!(matches!(err, Error::Connection(_)), "{err:?}");
}

// ---------------------------------------------------------------------------
// Connector-skeleton hygiene pins (consolidate-connector-skeleton).

/// Whether a `flume::bounded` call's capacity argument is a numeric
/// literal. Works on whitespace-normalized text (newlines collapsed) so
/// multi-line calls are covered; `1_024` separators are stripped, so any
/// numeric form fails while `DELIVERY_CHANNEL_CAPACITY` passes.
fn numeric_capacity_literal(normalized_call: &str) -> bool {
    let stripped = normalized_call.replace('_', "");
    let arg = stripped
        .split('(')
        .nth(1)
        .unwrap_or("")
        .split(')')
        .next()
        .unwrap_or("")
        .trim()
        .trim_end_matches(',')
        .trim();
    let ty_suffix_ok = |suffix: &str| {
        matches!(
            suffix,
            "" | "usize"
                | "isize"
                | "u8"
                | "u16"
                | "u32"
                | "u64"
                | "u128"
                | "i8"
                | "i16"
                | "i32"
                | "i64"
                | "i128"
        )
    };
    // Decimal, hex (0x), binary (0b) and octal (0o) literals with an
    // optional type suffix (1024usize, 0x200usize, …).
    let (digits, suffix) = if let Some(rest) = arg.strip_prefix("0x") {
        let end = rest
            .find(|c: char| !c.is_ascii_hexdigit())
            .unwrap_or(rest.len());
        (&rest[..end], rest[end..].trim())
    } else if let Some(rest) = arg.strip_prefix("0b") {
        let end = rest
            .find(|c: char| c != '0' && c != '1')
            .unwrap_or(rest.len());
        (&rest[..end], rest[end..].trim())
    } else if let Some(rest) = arg.strip_prefix("0o") {
        let end = rest
            .find(|c: char| !('0'..='7').contains(&c))
            .unwrap_or(rest.len());
        (&rest[..end], rest[end..].trim())
    } else {
        let end = arg.find(|c: char| !c.is_ascii_digit()).unwrap_or(arg.len());
        (&arg[..end], arg[end..].trim())
    };
    !digits.is_empty() && ty_suffix_ok(suffix)
}

#[test]
fn numeric_capacity_literal_shapes() {
    // Negative fixtures (violations must be detected).
    for call in [
        "flume::bounded::<Delivery>(1000)",
        "flume::bounded::<Delivery>(1_024)",
        "flume::bounded(512)",
        "flume::bounded(1024)",
        "flume::bounded::<Vec<u8>>( 1000 )",
        "flume::bounded::<Delivery>(512,)",
        "flume::bounded::<Delivery>(1024usize)",
        "flume::bounded::<Delivery>(\n 1000,\n)",
        "flume::bounded::<Delivery>(0x200)",
        "flume::bounded::<Delivery>(0b100_0000_0000)",
        "flume::bounded(0o2000)",
        "flume::bounded::<Delivery>(0x200usize)",
    ] {
        assert!(numeric_capacity_literal(call), "must flag: {call}");
    }
    // Positive fixtures (shared constant must pass). Even the "right"
    // numeric value fails by design — the constant is the single source.
    for call in [
        "flume::bounded::<Delivery>(DELIVERY_CHANNEL_CAPACITY)",
        "flume::bounded::<Delivery>( crate::DELIVERY_CHANNEL_CAPACITY )",
    ] {
        assert!(!numeric_capacity_literal(call), "must pass: {call}");
    }
}

#[test]
fn input_delivery_channels_use_the_shared_capacity() {
    // Reject any `flume::bounded::<T>(<literal>)` — the capacity must come
    // from the shared DELIVERY_CHANNEL_CAPACITY constant, whatever the
    // literal's value or line layout. The production section is
    // newline-normalized first so multi-line calls are covered.
    let mut report = Vec::new();
    for (file, source) in rust_files(&plugin_src_dir("input")) {
        // Normalize newlines to spaces but remember each original line's
        // offset in the normalized text so violations report real lines.
        let section = production_section(&source);
        let mut normalized = String::with_capacity(section.len());
        let mut line_starts: Vec<usize> = Vec::new();
        for line in section.split_inclusive('\n') {
            line_starts.push(normalized.len());
            normalized.push_str(line.trim_end_matches('\n'));
            normalized.push(' ');
        }
        let mut idx = 0;
        while let Some(found) = normalized[idx..].find("flume::bounded") {
            let start = idx + found;
            let call: String = normalized[start..]
                .chars()
                .take_while(|c| *c != ';')
                .collect();
            if numeric_capacity_literal(&call) {
                let line_no = line_starts
                    .iter()
                    .rposition(|off| *off <= start)
                    .map_or(1, |pos| pos + 1);
                report.push(format!("{file}:{line_no}: {}", call.trim()));
            }
            idx = start + "flume::bounded".len();
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

/// Backoff-shaped `pow` calls: any `.pow(` / `.checked_pow(` /
/// `.saturating_pow(` / `.wrapping_pow(` method call. Matching the METHOD
/// shape (not a `2u64.pow` literal receiver) is variant-proof:
/// `2_u64.pow`, `2u64.saturating_pow`, `base.checked_pow` all flag.
const POW_NEEDLES: &[&str] = &[
    ".pow(",
    ".checked_pow(",
    ".saturating_pow(",
    ".wrapping_pow(",
];

#[test]
fn connectors_must_not_hand_roll_power_backoff() {
    // Match the METHOD-call shape, not a literal receiver: the original
    // `2u64.pow` substring needle was bypassed by `2_u64.pow` (underscore)
    // and `2u64.saturating_pow` — both shipped in real output code while
    // this test stayed green. Any integer `.pow(` family call in
    // connector production code is a backoff schedule; a non-backoff
    // integer pow needs an explicit `// contract-allow: pow` line marker
    // (same precedent as the teardown marker above).
    const ALLOW_MARKER: &str = "contract-allow: pow";

    let mut report = Vec::new();
    for subdir in ["input", "output"] {
        for (file, source) in rust_files(&plugin_src_dir(subdir)) {
            for (idx, line) in production_section(&source).lines().enumerate() {
                if POW_NEEDLES.iter().any(|needle| line.contains(needle))
                    && !line.contains(ALLOW_MARKER)
                {
                    report.push(format!("{subdir}/{file}:{}: {}", idx + 1, line.trim()));
                }
            }
        }
    }
    assert!(
        report.is_empty(),
        "connectors must use the shared RetryUtils backoff (capped, \
         overflow-safe) instead of hand-rolled pow schedules; \
         non-backoff integer pows need a `// {ALLOW_MARKER}` marker. \
         Violations:\n{}",
        report.join("\n")
    );
}

#[test]
fn pow_needles_catch_the_historical_bypass_shapes() {
    // Both bypass shapes shipped in real output code (influxdb's
    // `2_u64.pow`, http's `saturating_pow`) while the old literal needle
    // stayed green — these fixtures pin the regression.
    for line in [
        "100 * 2_u64.pow(attempt)",
        "100u64.saturating_mul(2u64.saturating_pow(n - 1))",
        "let x = base.checked_pow(exp).unwrap();",
        "let y = v.wrapping_pow(2);",
    ] {
        assert!(
            POW_NEEDLES.iter().any(|needle| line.contains(needle)),
            "must flag: {line}"
        );
    }
    for line in [
        "flume::bounded::<Delivery>(DELIVERY_CHANNEL_CAPACITY)",
        "let z = x + y;",
    ] {
        assert!(
            !POW_NEEDLES.iter().any(|needle| line.contains(needle)),
            "must pass: {line}"
        );
    }
}

/// Shared-retry adopters (connector-resilience contract): each file's
/// production code must reference `crate::retry`. A connector that starts
/// retrying without the shared implementation must join this list — and
/// then the pow/loop pins above apply to it too.
const RETRY_ADOPTERS: &[&str] = &[
    "input/kafka.rs",
    "input/pulsar.rs",
    "output/http.rs",
    "output/influxdb.rs",
    "output/milvus.rs",
    "output/mqtt.rs",
    "output/qdrant.rs",
    "processor/embedding.rs",
    "processor/llm.rs",
    "processor/milvus_search.rs",
    "processor/vector_search.rs",
];

#[test]
fn connector_retry_adopters_use_the_shared_implementation() {
    let mut report = Vec::new();
    for entry in RETRY_ADOPTERS {
        let (subdir, file) = entry
            .split_once('/')
            .expect("adopter paths are subdir/file");
        let sources = rust_files(&plugin_src_dir(subdir));
        let (_, source) = sources
            .iter()
            .find(|(name, _)| name == file)
            .unwrap_or_else(|| panic!("adopter file missing: {entry}"));
        if !production_section(source).contains("crate::retry") {
            report.push(entry.to_string());
        }
    }
    assert!(
        report.is_empty(),
        "every shared-retry adopter must reference crate::retry in its \
         production code (connector-resilience contract). Missing:\n{}",
        report.join("\n")
    );
}
