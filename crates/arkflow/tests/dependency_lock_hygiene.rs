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

//! Guards against duplicate major-stack versions in `Cargo.lock`.
//!
//! The workspace deliberately stays on a single `datafusion`/`arrow`
//! generation and a single vendored `zstd`. Transitive updates (e.g.
//! `datafusion-federation` pulling in a newer datafusion line, or
//! `compression-codecs` pulling in zstd 0.14) silently split the stack,
//! compiling the entire arrow/datafusion tree twice. This test fails the
//! workspace run when that happens, so dependency-refresh PRs that break
//! the pins are caught in CI instead of doubling build times.
//!
//! To restore a single version, re-pin the offending transitive crate,
//! e.g.:
//!
//! ```text
//! cargo update -p datafusion-federation --precise 0.5.5
//! cargo update -p async-compression --precise 0.4.42
//! cargo update -p compression-codecs --precise 0.4.38
//! ```

use std::collections::BTreeMap;
use std::path::PathBuf;

/// Crates that must resolve to exactly one version in the lockfile:
/// the arrow family (including `arrow-pyarrow`), the datafusion family
/// (including subcrates like `datafusion-common`), and the vendored
/// zstd C library pair.
fn is_guarded(name: &str) -> bool {
    name.starts_with("arrow")
        || name.starts_with("datafusion")
        || name == "zstd"
        || name == "zstd-safe"
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root resolves")
}

/// Minimal `[[package]]` reader for Cargo.lock: returns `(name, version)`
/// pairs in file order. Std-only on purpose — no extra dependency for a
/// lockfile sanity check.
fn lock_packages() -> Vec<(String, String)> {
    let text = std::fs::read_to_string(repo_root().join("Cargo.lock"))
        .expect("Cargo.lock is readable from the test");

    let mut packages = Vec::new();
    let mut current_name: Option<String> = None;
    for line in text.lines() {
        let line = line.trim();
        if let Some(value) = line.strip_prefix("name = ") {
            current_name = Some(value.trim_matches('"').to_string());
        } else if let Some(value) = line.strip_prefix("version = ") {
            if let Some(name) = current_name.take() {
                packages.push((name, value.trim_matches('"').to_string()));
            }
        }
    }
    packages
}

#[test]
fn lockfile_keeps_arrow_datafusion_and_zstd_single_version() {
    let mut duplicated: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, version) in lock_packages() {
        if is_guarded(&name) {
            duplicated.entry(name).or_default().push(version);
        }
    }
    duplicated.retain(|_, versions| versions.len() > 1);

    assert!(
        duplicated.is_empty(),
        "Cargo.lock resolved multiple versions of the arrow/datafusion/zstd stacks \
         (the whole stack gets compiled twice):\n{duplicated:?}\n\
         Re-pin the transitive crate that introduced the split, e.g.\n\
         `cargo update -p datafusion-federation --precise 0.5.5`\n\
         `cargo update -p async-compression --precise 0.4.42`\n\
         `cargo update -p compression-codecs --precise 0.4.38`"
    );
}
