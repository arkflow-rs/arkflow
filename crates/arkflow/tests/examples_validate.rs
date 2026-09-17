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

//! Offline validation for every example registered in
//! `docs/reference/example-manifest.json`.
//!
//! The checks mirror `arkflow --config <file> --validate`: parse the
//! configuration, verify stream ids and declared Job specs, and run the
//! semantic configuration validation. Examples that cannot be validated
//! offline carry an explicit `"validate": false` plus a `"reason"` in the
//! manifest; silent skipping is not permitted.

use serde_json::Value;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root resolves")
}

fn docs_root() -> PathBuf {
    repo_root().join("docs")
}

fn manifest_entries() -> Vec<(String, PathBuf, Option<bool>, Option<String>)> {
    let manifest_path = docs_root().join("reference/example-manifest.json");
    let raw = std::fs::read_to_string(&manifest_path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", manifest_path.display()));
    let manifest: Value = serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("{} is not valid JSON: {e}", manifest_path.display()));
    let examples = manifest["examples"]
        .as_array()
        .unwrap_or_else(|| panic!("{} has no examples array", manifest_path.display()));

    examples
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let name = entry["name"]
                .as_str()
                .unwrap_or_else(|| panic!("example #{index} has no name"))
                .to_string();
            // Manifest paths are relative to docs/ (they start with "../").
            let path = docs_root().join(
                entry["path"]
                    .as_str()
                    .unwrap_or_else(|| panic!("example {name} has no path")),
            );
            let validate = entry["validate"].as_bool();
            let reason = entry["reason"].as_str().map(|s| s.to_string());
            (name, path, validate, reason)
        })
        .collect()
}

fn deep_validate(path: &Path) -> Result<(), String> {
    let config = arkflow_core::config::EngineConfig::from_file(
        path.to_str().expect("example path is valid UTF-8"),
    )
    .map_err(|e| format!("failed to load configuration: {e}"))?;

    config
        .stream_ids()
        .map_err(|e| format!("stream id check failed: {e}"))?;
    config
        .job_specs()
        .map_err(|e| format!("job spec check failed: {e}"))?;

    let report = arkflow_core::configuration::validate_config(&config);
    if report.valid {
        Ok(())
    } else {
        let details = report
            .errors
            .iter()
            .map(|issue| format!("{}: {}", issue.path, issue.message))
            .collect::<Vec<_>>()
            .join("; ");
        Err(details)
    }
}

#[tokio::test]
async fn registered_examples_validate_offline() {
    arkflow_plugin::initialize().expect("component catalogue registers");

    // Examples reference auxiliary files (e.g. .proto descriptors) relative to
    // the repository root, matching how users run the binary from there.
    std::env::set_current_dir(repo_root()).expect("chdir to repository root");

    let mut failures = Vec::new();
    for (name, path, validate, reason) in manifest_entries() {
        if validate == Some(false) {
            let reason = reason.unwrap_or_else(|| {
                format!("example {name} sets \"validate\": false without a reason")
            });
            println!("skipping {name}: {reason}");
            continue;
        }
        if !path.exists() {
            failures.push(format!("- {name}: {} does not exist", path.display()));
            continue;
        }
        if let Err(error) = deep_validate(&path) {
            failures.push(format!("- {name} ({}): {error}", path.display()));
        }
    }

    assert!(
        failures.is_empty(),
        "example validation failed for {} example(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn exclusions_always_carry_a_reason() {
    for (name, _, validate, reason) in manifest_entries() {
        if validate == Some(false) {
            assert!(
                reason.as_deref().map(|r| !r.trim().is_empty()).unwrap_or(false),
                "example {name} sets \"validate\": false but its manifest entry has no reason"
            );
        }
    }
}
