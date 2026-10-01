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

//! Component metadata honesty gate: every registered `config_example` must
//! (a) validate against the component's own declared JSON Schema, and
//! (b) build through the real builder. Ghost fields, wrong field names, or
//! examples missing required fields (e.g. batch without `timeout_ms`) fail
//! here instead of shipping to users' IDE completion.
//!
//! Builders that require live services at construction time are exempted in
//! `BUILD_EXEMPTIONS` with a reason; schema validation still applies to them.

use arkflow_core::component::{list_components_by_kind, ComponentKind};

/// Components whose `build()` needs a live external service; the real-build
/// leg is skipped for them (the schema leg still runs). Keeping this list
/// short is the point — new offline-buildable components get no exemption.
fn build_exemptions() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        // Creates a Pulsar producer eagerly; needs a broker.
        (
            "output",
            "pulsar",
            "builds a Pulsar producer (needs broker)",
        ),
    ]
}

/// Minimal JSON-Schema validation covering the subset our metadata uses:
/// `type`, `properties`, `required`, `additionalProperties: false`, `enum`,
/// `oneOf`, `items`. Returns all violations found.
fn validate(value: &serde_json::Value, schema: &serde_json::Value, path: &str) -> Vec<String> {
    let mut errors = Vec::new();
    let obj = match schema.as_object() {
        Some(o) => o,
        None => return errors,
    };

    if let Some(types) = obj.get("type").and_then(|t| t.as_str()) {
        let ok = match (types, value) {
            ("object", serde_json::Value::Object(_)) => true,
            ("array", serde_json::Value::Array(_)) => true,
            ("string", serde_json::Value::String(_)) => true,
            ("boolean", serde_json::Value::Bool(_)) => true,
            ("number", serde_json::Value::Number(_)) => true,
            ("integer", serde_json::Value::Number(n)) => n.is_i64() || n.is_u64(),
            _ => false,
        };
        if !ok {
            errors.push(format!("{path}: expected type {types}, got {value}"));
            return errors;
        }
    }

    if let Some(allowed) = obj.get("enum").and_then(|e| e.as_array()) {
        if !allowed.contains(value) {
            errors.push(format!("{path}: {value} not in enum {allowed:?}"));
        }
    }

    if let (serde_json::Value::Object(map), Some(props)) =
        (value, obj.get("properties").and_then(|p| p.as_object()))
    {
        for (key, sub) in props {
            if let Some(v) = map.get(key) {
                errors.extend(validate(v, sub, &format!("{path}.{key}")));
            }
        }
        if obj.get("additionalProperties") == Some(&serde_json::json!(false)) {
            for key in map.keys() {
                if !props.contains_key(key) {
                    errors.push(format!(
                        "{path}: field `{key}` is not declared in the schema (additionalProperties: false)"
                    ));
                }
            }
        }
        if let Some(required) = obj.get("required").and_then(|r| r.as_array()) {
            for req in required.iter().filter_map(|r| r.as_str()) {
                if !map.contains_key(req) {
                    errors.push(format!("{path}: missing required field `{req}`"));
                }
            }
        }
    }

    if let (serde_json::Value::Array(items), Some(item_schema)) = (value, obj.get("items")) {
        for (i, item) in items.iter().enumerate() {
            errors.extend(validate(item, item_schema, &format!("{path}[{i}]")));
        }
    }

    if let Some(variants) = obj.get("oneOf").and_then(|o| o.as_array()) {
        let matched = variants
            .iter()
            .map(|v| validate(value, v, path))
            .filter(|errs| errs.is_empty())
            .count();
        // oneOf requires EXACTLY one matching branch (JSON Schema), not
        // at least one.
        if matched != 1 {
            errors.push(format!(
                "{path}: matches {matched} oneOf variants; expected exactly one"
            ));
        }
    }

    errors
}

fn build_example(
    kind: ComponentKind,
    name: &str,
    example: &serde_json::Value,
) -> Result<(), String> {
    let resource = arkflow_core::Resource {
        temporary: std::collections::HashMap::new(),
        input_names: std::cell::RefCell::new(vec![]),
    };
    let config = Some(example.clone());
    let outcome = match kind {
        ComponentKind::Input => arkflow_core::input::InputConfig {
            input_type: name.to_string(),
            name: None,
            codec: None,
            config: config.clone(),
        }
        .build(&resource)
        .map(|_| ()),
        ComponentKind::Output => arkflow_core::output::OutputConfig {
            output_type: name.to_string(),
            name: None,
            codec: None,
            config: config.clone(),
        }
        .build(&resource)
        .map(|_| ()),
        ComponentKind::Processor => arkflow_core::processor::ProcessorConfig {
            processor_type: name.to_string(),
            name: None,
            config: config.clone(),
        }
        .build(&resource)
        .map(|_| ()),
        ComponentKind::Buffer => arkflow_core::buffer::BufferConfig {
            buffer_type: name.to_string(),
            name: None,
            config: config.clone(),
        }
        .build(&resource)
        .map(|_| ()),
        ComponentKind::Codec => arkflow_core::codec::CodecConfig {
            codec_type: name.to_string(),
            name: None,
            config: config.clone(),
        }
        .build(&resource)
        .map(|_| ()),
        ComponentKind::Temporary => {
            return Err("temporary components are not built here".to_string())
        }
    };
    outcome.map_err(|e| format!("{e}"))
}

#[tokio::test]
async fn metadata_examples_validate_against_their_schema_and_build() {
    arkflow_plugin::initialize().expect("component catalogue registers");

    let exemptions = build_exemptions();
    let mut problems: Vec<String> = Vec::new();
    let mut checked = 0usize;

    for kind in ComponentKind::all() {
        for meta in list_components_by_kind(kind) {
            let Some(example) = &meta.config_example else {
                continue;
            };
            checked += 1;

            // (a) the example satisfies the declared schema
            for err in validate(example, &meta.config_schema, &meta.name) {
                problems.push(format!(
                    "{} `{}`: example violates its own schema: {err}",
                    kind.as_str(),
                    meta.name
                ));
            }

            // (b) the example builds through the real builder (unless the
            // builder needs a live service)
            let exempt = exemptions
                .iter()
                .find(|(k, n, _)| *k == kind.as_str() && *n == meta.name);
            match exempt {
                Some((_, _, reason)) => {
                    eprintln!(
                        "{} `{}`: build leg skipped ({reason})",
                        kind.as_str(),
                        meta.name
                    );
                }
                None => {
                    if let Err(e) = build_example(kind, &meta.name, example) {
                        problems.push(format!(
                            "{} `{}`: example config does not build: {e}",
                            kind.as_str(),
                            meta.name
                        ));
                    }
                }
            }
        }
    }

    assert!(
        checked > 30,
        "expected to check a substantial number of examples, checked {checked}"
    );
    assert!(
        problems.is_empty(),
        "component metadata examples are not honest:\n{}",
        problems.join("\n")
    );
}

/// Inputs whose data arrives already typed (file is parsed by its format,
/// sql rows come from the SELECT, modbus from register reads) must reject a
/// configured codec at build time instead of building it and silently
/// dropping the decoded result.
#[tokio::test]
async fn dead_codec_config_is_rejected_by_builders() {
    arkflow_plugin::initialize().expect("component catalogue registers");

    let resource = arkflow_core::Resource {
        temporary: std::collections::HashMap::new(),
        input_names: std::cell::RefCell::new(vec![]),
    };
    for input_type in ["file", "sql", "modbus"] {
        let input = arkflow_core::input::InputConfig {
            input_type: input_type.to_string(),
            name: None,
            codec: Some(arkflow_core::codec::CodecConfig {
                codec_type: "json".to_string(),
                name: None,
                config: None,
            }),
            config: None,
        };
        let err = match input.build(&resource) {
            Err(e) => e,
            Ok(_) => panic!("{input_type} input must reject a codec at build time"),
        };
        assert!(
            format!("{err}").contains("codec"),
            "{input_type} input must explain the codec rejection: {err}"
        );
    }
}
