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
//!
//! `rand` is the known exception: three major versions (0.8/0.9/0.10)
//! coexist because transitive dependencies pin incompatible ranges —
//! `jsonwebtoken`/`nkeys`/`rsa` need ^0.8, `arrow-avro` (via datafusion)
//! needs ^0.9, `apache-avro`/`ballista`/`async-nats` need ^0.10. None can
//! be collapsed from this side without upstream releases, so a
//! single-version assertion would be permanently red. What the workspace
//! CAN guarantee is the centralization convention: no member crate
//! declares `rand` outside `[workspace.dependencies]` (a direct `rand =
//! "0.10"` next to a workspace `rand = "0.9"` is how the split grew a
//! version of our own making). That is what
//! [`workspace_crates_declare_rand_only_through_the_workspace`] pins.

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

/// Every workspace member's `[dependencies]`/`[dev-dependencies]` must
/// reference `rand` through the workspace table (`rand = { workspace =
/// true }`), never with a direct version requirement. A direct
/// declaration is how the workspace grew a third rand generation of its
/// own making (plugin `rand = "0.10"` next to workspace `rand = "0.9"`);
/// version selection stays in `[workspace.dependencies]` where the whole
/// graph can see it.
#[test]
fn workspace_crates_declare_rand_only_through_the_workspace() {
    let members_dir = repo_root().join("crates");
    let mut violations = Vec::new();

    let mut entries: Vec<_> = std::fs::read_dir(&members_dir)
        .expect("crates/ directory is readable")
        .collect::<Result<Vec<_>, _>>()
        .expect("crates/ entries are readable");
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let manifest = entry.path().join("Cargo.toml");
        if !manifest.is_file() {
            // Skip non-crate entries (e.g. macOS .DS_Store) rather than
            // failing on junk the workspace never declares.
            continue;
        }
        let text = std::fs::read_to_string(&manifest).unwrap_or_else(|error| {
            panic!(
                "member manifest {} is readable: {error}",
                manifest.display()
            )
        });

        let mut in_dependencies = false;
        for line in text.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') {
                in_dependencies = trimmed == "[dependencies]" || trimmed == "[dev-dependencies]";
                continue;
            }
            if !in_dependencies {
                continue;
            }
            if trimmed.starts_with("rand") && trimmed[4..].trim_start().starts_with('=') {
                let value = trimmed[trimmed.find('=').unwrap() + 1..].trim();
                let is_workspace_ref =
                    value == "{ workspace = true }" || value.starts_with("{ workspace = true,");
                if !is_workspace_ref {
                    violations.push(format!(
                        "{}: `{trimmed}`",
                        manifest
                            .strip_prefix(repo_root())
                            .unwrap_or(&manifest)
                            .display()
                    ));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "workspace crates must declare `rand` through the workspace table, \
         not with a direct version (that is how the third rand generation \
         entered the lock):\n{violations:#?}"
    );
}
