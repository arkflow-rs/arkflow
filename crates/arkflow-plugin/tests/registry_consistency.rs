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

//! Every component must register its functional builder and its docs
//! metadata under the same name. A builder without metadata silently
//! disappears from `components list`, the docs inventory, and the IDE
//! schema; metadata without a builder is dead registry data.

use arkflow_core::component::{list_components_by_kind, ComponentKind};
use std::collections::BTreeSet;

fn builder_names(kind: ComponentKind) -> BTreeSet<String> {
    match kind {
        ComponentKind::Input => arkflow_core::input::registered_names(),
        ComponentKind::Output => arkflow_core::output::registered_names(),
        ComponentKind::Processor => arkflow_core::processor::registered_names(),
        ComponentKind::Buffer => arkflow_core::buffer::registered_names(),
        ComponentKind::Codec => arkflow_core::codec::registered_names(),
        ComponentKind::Temporary => arkflow_core::temporary::registered_names(),
    }
    .into_iter()
    .collect()
}

fn metadata_names(kind: ComponentKind) -> BTreeSet<String> {
    list_components_by_kind(kind)
        .iter()
        .map(|m| m.name.clone())
        .collect()
}

#[test]
fn builder_and_metadata_registrations_are_aligned() {
    arkflow_plugin::initialize().expect("component catalogue registers");

    let mut problems = Vec::new();
    for kind in ComponentKind::all() {
        let builders = builder_names(kind);
        let metadata = metadata_names(kind);
        for name in builders.difference(&metadata) {
            problems.push(format!(
                "{} `{}` registers a builder but no docs metadata",
                kind.as_str(),
                name
            ));
        }
        for name in metadata.difference(&builders) {
            problems.push(format!(
                "{} `{}` has docs metadata but no registered builder",
                kind.as_str(),
                name
            ));
        }
    }

    assert!(
        problems.is_empty(),
        "builder/metadata registry misalignment:\n{}",
        problems.join("\n")
    );
}
