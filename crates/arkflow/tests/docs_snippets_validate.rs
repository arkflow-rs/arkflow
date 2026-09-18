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

//! Deep validation for every ```yaml code block in the maintained
//! documentation (`docs/docs/`). Each block must carry a classification in
//! its fence metastring (`validate=full`, `validate=fragment wrap=<kind>`, or
//! `validate=foreign reason="..."` — the vocabulary is documented in
//! `docs/DOCUMENTATION.md` and mirrored by `docs/scripts/docs-check.mjs`).
//!
//! `full` and `fragment` blocks are completed (fragments via wrap templates)
//! and validated through the real engine path — `EngineConfig::from_file`
//! plus `validate_config`, the same checks as `--validate`. Because
//! `EngineConfig` does not deny unknown fields, wrapping alone could pass
//! vacuously, so the test additionally asserts the parsed configuration
//! contains the snippet at its wrap target. `foreign` blocks only receive a
//! YAML well-formedness check; their reason is required. Silent skipping is
//! not permitted.

use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::io::Write as _;

/// Classifications accepted by this test and by `docs/scripts/docs-check.mjs`.
const CLASSIFICATIONS: &[&str] = &["full", "fragment", "foreign"];
/// Wrap kinds accepted for `validate=fragment` by both gates.
const WRAP_KINDS: &[&str] = &[
    "input",
    "output",
    "processors",
    "durability",
    "buffer",
    "stream",
    "codec",
    "engine",
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root resolves")
}

fn docs_root() -> PathBuf {
    repo_root().join("docs/docs")
}

fn walk_markdown(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("docs/docs is readable") {
        let entry = entry.expect("docs/docs entry is readable");
        let path = entry.path();
        if path.is_dir() {
            walk_markdown(&path, out);
        } else if path.extension().map(|e| e == "md" || e == "mdx").unwrap_or(false) {
            out.push(path);
        }
    }
}

struct Block {
    /// 1-based line number of the fence line (where the marker goes).
    line: usize,
    /// Fence metastring with the leading `yaml` language tag removed.
    meta: String,
    body: String,
}

/// Extract fenced code blocks whose info string starts with `yaml`.
/// Mirrors the fence toggle logic in docs/scripts/docs-check.mjs.
fn yaml_blocks(text: &str) -> Vec<Block> {
    let mut blocks = Vec::new();
    let mut in_fence = false;
    let mut fence_start = 0usize;
    let mut meta = String::new();
    let mut body = String::new();
    for (index, line) in text.lines().enumerate() {
        let trimmed = line.trim_start();
        let opens_fence = trimmed.starts_with("```") || trimmed.starts_with("~~~");
        if opens_fence {
            if !in_fence {
                in_fence = true;
                fence_start = index + 1; // 1-based line number of the fence line
                meta = trimmed.trim_start_matches("```").trim_start_matches("~~~").trim().to_string();
                body.clear();
            } else {
                in_fence = false;
                if let Some(rest) = meta.strip_prefix("yaml") {
                    if rest.is_empty() || rest.starts_with(' ') {
                        blocks.push(Block {
                            line: fence_start,
                            meta: rest.trim().to_string(),
                            body: body.clone(),
                        });
                    }
                }
            }
            continue;
        }
        if in_fence {
            body.push_str(line);
            body.push('\n');
        }
    }
    blocks
}

struct Classification {
    kind: String,
    wrap: Option<String>,
    reason: Option<String>,
}

/// Parse the classification marker; `Err` describes the violation.
fn classify(meta: &str) -> Result<Classification, String> {
    let kind = meta
        .split_whitespace()
        .find_map(|token| token.strip_prefix("validate="))
        .ok_or_else(|| {
            "missing classification marker (add validate=full, validate=fragment wrap=<input|output|processors|durability|engine>, or validate=foreign reason=\"...\")".to_string()
        })?;
    if !CLASSIFICATIONS.contains(&kind) {
        return Err(format!(
            "unknown validate kind 'validate={kind}' (expected full, fragment, or foreign)"
        ));
    }
    let wrap = meta.split_whitespace().find_map(|token| token.strip_prefix("wrap="));
    let reason = meta.split_whitespace().find_map(|token| token.strip_prefix("reason="));
    match kind {
        "full" => Ok(Classification { kind: kind.into(), wrap: None, reason: None }),
        "fragment" => {
            let wrap = wrap.ok_or_else(|| {
                "validate=fragment requires wrap=<input|output|processors|durability|engine>"
            })?;
            if !WRAP_KINDS.contains(&wrap) {
                return Err(format!(
                    "unknown wrap kind 'wrap={wrap}' (expected one of: {})",
                    WRAP_KINDS.join(", ")
                ));
            }
            Ok(Classification { kind: kind.into(), wrap: Some(wrap.into()), reason: None })
        }
        _ => {
            let reason = reason
                .map(|r| r.trim_matches('"').to_string())
                .filter(|r| !r.trim().is_empty())
                .ok_or_else(|| {
                    "validate=foreign requires a reason=\"...\" (why this block is not an ArkFlow config)"
                })?;
            Ok(Classification { kind: kind.into(), wrap: None, reason: Some(reason) })
        }
    }
}

/// Deep-merge `src` into `dst`; `src` wins on conflicts.
fn deep_merge(dst: &mut Value, src: &Value) {
    match (dst, src) {
        (Value::Object(d), Value::Object(s)) => {
            for (key, value) in s {
                match d.get_mut(key) {
                    Some(slot) => deep_merge(slot, value),
                    None => {
                        d.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (dst, src) => *dst = src.clone(),
    }
}

/// Minimal offline-free engine configuration the fragments merge into.
/// Stubs use `input/memory` (all fields optional) and `output/drop`
/// (its builder ignores config entirely).
fn stub_config() -> Value {
    serde_yaml::from_str(
        "streams:\n  - id: docs-snippet\n    input:\n      type: memory\n    pipeline:\n      processors: []\n    output:\n      type: drop\n",
    )
    .expect("stub config parses")
}

/// Complete a parsed snippet into a full engine configuration.
fn wrap_fragment(fragment: &Value, wrap: &str) -> Value {
    let mut config = stub_config();
    let stream = &mut config["streams"][0];
    match wrap {
        // The snippet carries its own stream-level key (`input:`, `output:`,
        // `durability:`, `buffer:`) and merges over the stub.
        "input" | "output" | "durability" | "buffer" | "stream" => deep_merge(stream, fragment),
        // The snippet carries a `codec:` section, which lives under the
        // stream's input (or output).
        "codec" => deep_merge(stream.get_mut("input").expect("stub has input"), fragment),
        "processors" => match fragment {
            Value::Array(items) => {
                stream["pipeline"]["processors"] = Value::Array(items.clone());
            }
            // metadata-style snippet: `pipeline: {processors: [...]}`
            Value::Object(map) if map.contains_key("pipeline") => {
                deep_merge(stream.get_mut("pipeline").expect("stub has pipeline"), &map["pipeline"]);
            }
            _ => panic!("processors snippet must be a list or a pipeline mapping"),
        },
        "engine" => deep_merge(&mut config, fragment),
        other => panic!("unhandled wrap kind {other}"),
    }
    config
}

/// Assert every key path of `child` survives in `parent`. Scalars are not
/// compared (serialization formats vary, e.g. durations); the structure is
/// the guard against silently-dropped sections.
fn contains(parent: &Value, child: &Value) -> bool {
    match (parent, child) {
        (Value::Null, Value::Null) => true,
        // A null slot cannot hold a structured child — e.g. an Optional
        // section that deserialized as absent.
        (Value::Null, _) => false,
        (Value::Object(p), Value::Object(c)) => c
            .iter()
            .all(|(key, value)| p.get(key).map(|target| contains(target, value)).unwrap_or(false)),
        (Value::Array(p), Value::Array(c)) => {
            p.len() >= c.len() && p.iter().zip(c.iter()).all(|(target, value)| contains(target, value))
        }
        _ => true,
    }
}

/// Where the parsed configuration must contain the fragment.
fn containment_target<'a>(wrap: &str, fragment: &Value, parsed: &'a Value) -> &'a Value {
    match wrap {
        "input" | "output" | "durability" | "buffer" | "stream" => &parsed["streams"][0],
        "codec" => &parsed["streams"][0]["input"],
        // A bare list targets the processors list; a `pipeline:` mapping
        // targets the stream.
        "processors" => match fragment {
            Value::Array(_) => &parsed["streams"][0]["pipeline"]["processors"],
            _ => &parsed["streams"][0],
        },
        "engine" => parsed,
        _ => parsed,
    }
}

/// Parse and semantically validate a complete engine configuration, mirroring
/// `--validate` (see examples_validate.rs). Returns the parsed config so
/// callers can run containment checks.
fn validate_engine_config(yaml: &str, label: &str) -> Result<arkflow_core::config::EngineConfig, String> {
    let mut temp = tempfile::Builder::new()
        .prefix("arkflow-docs-snippet-")
        .suffix(".yaml")
        .tempfile()
        .map_err(|e| format!("{label}: cannot create temp file: {e}"))?;
    temp.write_all(yaml.as_bytes())
        .map_err(|e| format!("{label}: cannot write temp file: {e}"))?;
    let path = temp.path().to_path_buf();

    let config = arkflow_core::config::EngineConfig::from_file(path.to_str().expect("temp path is valid UTF-8"))
        .map_err(|e| format!("{label}: failed to load configuration: {e}"))?;
    config
        .stream_ids()
        .map_err(|e| format!("{label}: stream id check failed: {e}"))?;
    config
        .job_specs()
        .map_err(|e| format!("{label}: job spec check failed: {e}"))?;
    let report = arkflow_core::configuration::validate_config(&config);
    if !report.valid {
        let details = report
            .errors
            .iter()
            .map(|issue| format!("{}: {}", issue.path, issue.message))
            .collect::<Vec<_>>()
            .join("; ");
        return Err(format!("{label}: {details}"));
    }
    drop(temp);
    Ok(config)
}

#[tokio::test]
async fn docs_yaml_snippets_validate() {
    arkflow_plugin::initialize().expect("component catalogue registers");

    // Snippets may reference auxiliary files relative to the repository root,
    // matching how users run the binary from there.
    std::env::set_current_dir(repo_root()).expect("chdir to repository root");

    let mut files = Vec::new();
    walk_markdown(&docs_root(), &mut files);
    files.sort();

    let mut failures = Vec::new();
    let mut checked = 0usize;
    for file in &files {
        let text = std::fs::read_to_string(file).expect("markdown file is readable");
        let relative = file
            .strip_prefix(&repo_root())
            .expect("file under repository root")
            .to_string_lossy()
            .to_string();
        for block in yaml_blocks(&text) {
            let label = format!("{relative}:{}", block.line);
            let classification = match classify(&block.meta) {
                Ok(c) => c,
                Err(problem) => {
                    failures.push(format!("- {label}: {problem}"));
                    continue;
                }
            };
            checked += 1;
            let result = match classification.kind.as_str() {
                "full" => validate_engine_config(&block.body, &label).map(|_| ()),
                "fragment" => {
                    let wrap = classification.wrap.as_deref().expect("wrap is set");
                    let fragment: Value = match serde_yaml::from_str(&block.body) {
                        Ok(v) => v,
                        Err(e) => {
                            failures.push(format!("- {label}: snippet is not valid YAML: {e}"));
                            continue;
                        }
                    };
                    let wrapped = wrap_fragment(&fragment, wrap);
                    let wrapped_yaml = serde_yaml::to_string(&wrapped)
                        .map_err(|e| format!("{label}: cannot serialize wrapped config: {e}"));
                    let wrapped_yaml = match wrapped_yaml {
                        Ok(y) => y,
                        Err(problem) => {
                            failures.push(format!("- {problem}"));
                            continue;
                        }
                    };
                    validate_engine_config(&wrapped_yaml, &label).and_then(|config| {
                        let serialized = serde_yaml::to_string(&config)
                            .map_err(|e| format!("{label}: cannot re-serialize config: {e}"))?;
                        let parsed: Value = serde_yaml::from_str(&serialized)
                            .map_err(|e| format!("{label}: cannot re-parse config: {e}"))?;
                        if !contains(containment_target(wrap, &fragment, &parsed), &fragment) {
                            return Err(format!(
                                "{label}: the configuration dropped part of the snippet at wrap target '{wrap}' — the snippet does not match the current schema shape"
                            ));
                        }
                        Ok(())
                    })
                }
                _ => {
                    if let Err(e) = serde_yaml::from_str::<serde_yaml::Value>(&block.body) {
                        Err(format!(
                            "{label}: foreign block (reason: {}) is not well-formed YAML: {e}",
                            classification.reason.as_deref().unwrap_or("?")
                        ))
                    } else {
                        Ok(())
                    }
                }
            };
            if let Err(problem) = result {
                failures.push(format!("- {problem}"));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "documentation snippet validation failed (checked {checked} yaml blocks):\n{}",
        failures.join("\n")
    );
    assert!(checked > 0, "no yaml blocks were discovered under docs/docs — the walk is broken");
}

/// The classification vocabulary in `docs/scripts/docs-check.mjs` must stay
/// identical to this test's; drift would let one gate accept blocks the other
/// rejects. The sets are parsed out of the Node source to fail loudly.
#[test]
fn node_gate_vocabulary_is_in_sync() {
    let source =
        std::fs::read_to_string(repo_root().join("docs/scripts/docs-check.mjs"))
            .expect("docs-check.mjs is readable");

    fn parse_set(source: &str, name: &str) -> BTreeSet<String> {
        let anchor = format!("{name} = new Set([");
        let start = source
            .find(&anchor)
            .unwrap_or_else(|| panic!("{name} declaration not found in docs-check.mjs"));
        let rest = &source[start + anchor.len()..];
        let end = rest.find(']').expect("set literal is closed");
        rest[..end]
            .split(',')
            .map(|item| item.trim().trim_matches('\'').trim_matches('"').to_string())
            .filter(|item| !item.is_empty())
            .collect()
    }

    let node_classifications = parse_set(&source, "YAML_CLASSIFICATIONS");
    let node_wraps = parse_set(&source, "YAML_WRAP_KINDS");

    let rust_classifications: BTreeSet<String> =
        CLASSIFICATIONS.iter().map(|s| s.to_string()).collect();
    let rust_wraps: BTreeSet<String> = WRAP_KINDS.iter().map(|s| s.to_string()).collect();

    assert_eq!(
        node_classifications, rust_classifications,
        "YAML_CLASSIFICATIONS drifted between docs-check.mjs and docs_snippets_validate.rs"
    );
    assert_eq!(
        node_wraps, rust_wraps,
        "YAML_WRAP_KINDS drifted between docs-check.mjs and docs_snippets_validate.rs"
    );
}
