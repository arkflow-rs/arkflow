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

//! Snapshot enforcement for the generated documentation artifacts.
//!
//! `docs/reference/component-inventory.json` and
//! `docs/static/config-schema.json` must always match the live component
//! registry and engine schema. The committed files are the seam between the
//! Rust CI (code → JSON, enforced here) and the Node docs CI (JSON → pages,
//! enforced by `docs:check`), so the docs pipeline never needs a Rust
//! toolchain.
//!
//! Regenerate the committed artifacts with:
//!
//! ```text
//! ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot
//! ```

use std::path::PathBuf;

const REGEN_ENV: &str = "ARKFLOW_REGENERATE_DOCS";
const REGEN_COMMAND: &str =
    "ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot";

fn docs_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs")
        .canonicalize()
        .expect("docs directory exists next to the workspace")
}

fn serialize(value: &serde_json::Value) -> String {
    let mut pretty = serde_json::to_string_pretty(value).expect("values serialize");
    pretty.push('\n');
    pretty
}

#[test]
fn committed_docs_artifacts_match_the_registry() {
    arkflow_plugin::initialize().expect("the built-in component catalogue registers");

    let artifacts = [
        (
            docs_root().join("reference/component-inventory.json"),
            serialize(&arkflow_core::component::export_registry()),
            "component inventory",
        ),
        (
            docs_root().join("static/config-schema.json"),
            serialize(&arkflow_core::component::build_config_schema()),
            "engine configuration schema",
        ),
    ];

    if std::env::var(REGEN_ENV).ok().as_deref() == Some("1") {
        for (path, content, label) in &artifacts {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("target directory is creatable");
            }
            std::fs::write(path, content)
                .unwrap_or_else(|e| panic!("cannot write {label}: {e}"));
            println!("regenerated {}", path.display());
        }
        return;
    }

    let mut stale = Vec::new();
    for (path, content, label) in &artifacts {
        match std::fs::read_to_string(path) {
            Ok(committed) if committed == *content => {}
            Ok(_) => stale.push(format!(
                "- {} ({}) does not match the live registry",
                path.display(),
                label
            )),
            Err(e) => stale.push(format!(
                "- {} ({}) cannot be read: {}",
                path.display(),
                label,
                e
            )),
        }
    }

    assert!(
        stale.is_empty(),
        "documentation artifacts are stale:\n{}\nRegenerate with: {}",
        stale.join("\n"),
        REGEN_COMMAND
    );
}
