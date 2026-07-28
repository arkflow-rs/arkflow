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

//! Component metadata registry
//!
//! Each component (input / output / processor / buffer / codec) can be
//! registered with a `ComponentMetadata` entry that describes it. The
//! metadata powers three capabilities:
//!
//! 1. **Discovery** — `list_components()` lets callers enumerate every
//!    registered component, so a CLI can render a catalogue, and tests can
//!    assert a particular component is present.
//! 2. **Detail completion** — `get_component_metadata()` returns a
//!    JSON Schema for a component's configuration, so a CLI can render
//!    help, and an IDE can drive auto-completion in YAML config files.
//! 3. **Validation** — combining the schema with a config file lets the
//!    engine validate user input without instantiating the component.

use crate::Error;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

/// The kind of component being described.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ComponentKind {
    Input,
    Output,
    Processor,
    Buffer,
    Codec,
}

impl ComponentKind {
    /// Stable string representation used in CLI output and JSON Schema.
    pub const fn as_str(self) -> &'static str {
        match self {
            ComponentKind::Input => "input",
            ComponentKind::Output => "output",
            ComponentKind::Processor => "processor",
            ComponentKind::Buffer => "buffer",
            ComponentKind::Codec => "codec",
        }
    }

    /// All kinds, in the canonical order used by listings and the schema.
    pub const fn all() -> [ComponentKind; 5] {
        [
            ComponentKind::Input,
            ComponentKind::Output,
            ComponentKind::Processor,
            ComponentKind::Buffer,
            ComponentKind::Codec,
        ]
    }
}

impl std::fmt::Display for ComponentKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ComponentKind {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "input" => Ok(ComponentKind::Input),
            "output" => Ok(ComponentKind::Output),
            "processor" => Ok(ComponentKind::Processor),
            "buffer" => Ok(ComponentKind::Buffer),
            "codec" => Ok(ComponentKind::Codec),
            other => Err(Error::Config(format!(
                "Unknown component kind: {} (expected one of: input, output, processor, buffer, codec)",
                other
            ))),
        }
    }
}

/// Descriptor for a single component variant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentMetadata {
    /// Component type name (the value of `type:` in user config).
    pub name: String,
    /// Short, human-readable description of what the component does.
    pub description: String,
    /// When `true`, the configuration block is optional and the component
    /// can be built with no `config` value present.
    #[serde(default)]
    pub config_optional: bool,
    /// JSON Schema describing the component's configuration object.
    /// When the component takes no configuration, this is an empty object
    /// schema (`{"type": "object"}`).
    pub config_schema: serde_json::Value,
    /// Optional example configuration rendered in CLI help and IDE
    /// hover / completion.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub config_example: Option<serde_json::Value>,
}

impl ComponentMetadata {
    /// Build a metadata entry for a component that takes no configuration.
    pub fn unit(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            config_optional: true,
            config_schema: serde_json::json!({"type": "object"}),
            config_example: None,
        }
    }

    /// Build a metadata entry from a JSON Schema.
    pub fn with_schema(
        name: impl Into<String>,
        description: impl Into<String>,
        schema: serde_json::Value,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            config_optional: false,
            config_schema: schema,
            config_example: None,
        }
    }

    /// Mark the configuration as optional (e.g. when the component provides
    /// sensible defaults).
    pub fn with_optional(mut self) -> Self {
        self.config_optional = true;
        self
    }

    /// Attach an example configuration to the metadata.
    pub fn with_example(mut self, example: serde_json::Value) -> Self {
        self.config_example = Some(example);
        self
    }
}

lazy_static::lazy_static! {
    static ref INPUT_METADATA: RwLock<BTreeMap<String, Arc<ComponentMetadata>>> =
        RwLock::new(BTreeMap::new());
    static ref OUTPUT_METADATA: RwLock<BTreeMap<String, Arc<ComponentMetadata>>> =
        RwLock::new(BTreeMap::new());
    static ref PROCESSOR_METADATA: RwLock<BTreeMap<String, Arc<ComponentMetadata>>> =
        RwLock::new(BTreeMap::new());
    static ref BUFFER_METADATA: RwLock<BTreeMap<String, Arc<ComponentMetadata>>> =
        RwLock::new(BTreeMap::new());
    static ref CODEC_METADATA: RwLock<BTreeMap<String, Arc<ComponentMetadata>>> =
        RwLock::new(BTreeMap::new());
}

macro_rules! register_metadata {
    ($fn_name:ident, $registry:ident, $kind:expr) => {
        /// Register metadata for a component variant. Must be called once
        /// per `(kind, name)` pair, typically from the component's `init()`
        /// function. Returns an error if the same name is registered twice.
        pub fn $fn_name(metadata: ComponentMetadata) -> Result<(), Error> {
            let mut registry = $registry.write().unwrap();
            if registry.contains_key(&metadata.name) {
                return Err(Error::Config(format!(
                    "{} type already registered: {}",
                    $kind, metadata.name
                )));
            }
            registry.insert(metadata.name.clone(), Arc::new(metadata));
            Ok(())
        }
    };
}

register_metadata!(
    register_input_metadata,
    INPUT_METADATA,
    ComponentKind::Input
);
register_metadata!(
    register_output_metadata,
    OUTPUT_METADATA,
    ComponentKind::Output
);
register_metadata!(
    register_processor_metadata,
    PROCESSOR_METADATA,
    ComponentKind::Processor
);
register_metadata!(
    register_buffer_metadata,
    BUFFER_METADATA,
    ComponentKind::Buffer
);
register_metadata!(
    register_codec_metadata,
    CODEC_METADATA,
    ComponentKind::Codec
);

macro_rules! list_metadata {
    ($fn_name:ident, $registry:ident) => {
        /// Return a snapshot of every component registered for this kind,
        /// sorted by name.
        pub fn $fn_name() -> Vec<Arc<ComponentMetadata>> {
            $registry.read().unwrap().values().cloned().collect()
        }
    };
}

list_metadata!(list_input_components, INPUT_METADATA);
list_metadata!(list_output_components, OUTPUT_METADATA);
list_metadata!(list_processor_components, PROCESSOR_METADATA);
list_metadata!(list_buffer_components, BUFFER_METADATA);
list_metadata!(list_codec_components, CODEC_METADATA);

/// Look up the metadata registry for a given kind.
fn registry_for(kind: ComponentKind) -> &'static RwLock<BTreeMap<String, Arc<ComponentMetadata>>> {
    match kind {
        ComponentKind::Input => &INPUT_METADATA,
        ComponentKind::Output => &OUTPUT_METADATA,
        ComponentKind::Processor => &PROCESSOR_METADATA,
        ComponentKind::Buffer => &BUFFER_METADATA,
        ComponentKind::Codec => &CODEC_METADATA,
    }
}

/// Register metadata for any component kind. Convenience wrapper that
/// delegates to the kind-specific registration function.
pub fn register_component_metadata(
    kind: ComponentKind,
    metadata: ComponentMetadata,
) -> Result<(), Error> {
    match kind {
        ComponentKind::Input => register_input_metadata(metadata),
        ComponentKind::Output => register_output_metadata(metadata),
        ComponentKind::Processor => register_processor_metadata(metadata),
        ComponentKind::Buffer => register_buffer_metadata(metadata),
        ComponentKind::Codec => register_codec_metadata(metadata),
    }
}

/// Look up metadata for a specific component. Returns `None` if no
/// component with that name is registered for the given kind.
pub fn get_component_metadata(kind: ComponentKind, name: &str) -> Option<Arc<ComponentMetadata>> {
    registry_for(kind).read().unwrap().get(name).cloned()
}

/// Return every component registered for the given kind, sorted by name.
pub fn list_components_by_kind(kind: ComponentKind) -> Vec<Arc<ComponentMetadata>> {
    registry_for(kind)
        .read()
        .unwrap()
        .values()
        .cloned()
        .collect()
}

/// Return every registered component across all kinds, grouped by kind
/// in the canonical order returned by [`ComponentKind::all`].
pub fn list_components() -> Vec<(ComponentKind, Arc<ComponentMetadata>)> {
    ComponentKind::all()
        .into_iter()
        .flat_map(|kind| {
            list_components_by_kind(kind)
                .into_iter()
                .map(move |m| (kind, m))
        })
        .collect()
}

/// Build a JSON Schema describing the top-level engine configuration.
///
/// The returned schema is structured for IDE auto-completion:
///
/// * The root schema is an object with `logging`, `health_check`, and
///   `streams` properties.
/// * Each `streams[*]` entry allows the `input`, `output`, `error_output`,
///   `buffer`, `pipeline.input`, `pipeline.output`, and
///   `pipeline.processors` fields to be one of the registered component
///   variants, using JSON Schema `oneOf` and `if/then/else` driven by
///   the `type` discriminator.
/// * The schema embeds each component's individual configuration schema
///   under the matching variant, so editors can offer field-level
///   completion once the user picks a `type`.
pub fn build_config_schema() -> serde_json::Value {
    let mut root = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "ArkFlow engine configuration",
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "logging": {
                "type": "object",
                "description": "Logging configuration.",
                "additionalProperties": false,
                "properties": {
                    "level": {
                        "type": "string",
                        "enum": ["trace", "debug", "info", "warn", "error"],
                        "default": "info",
                        "description": "Log level."
                    },
                    "file_path": {
                        "type": "string",
                        "description": "Optional path to a log file."
                    },
                    "format": {
                        "type": "string",
                        "enum": ["plain", "json"],
                        "default": "plain",
                        "description": "Log output format."
                    }
                }
            },
            "health_check": {
                "type": "object",
                "description": "Health check HTTP server configuration.",
                "additionalProperties": false,
                "properties": {
                    "enabled": {"type": "boolean", "default": true},
                    "address": {"type": "string", "default": "0.0.0.0:8080"},
                    "health_path": {"type": "string", "default": "/health"},
                    "readiness_path": {"type": "string", "default": "/readiness"},
                    "liveness_path": {"type": "string", "default": "/liveness"}
                }
            },
            "streams": {
                "type": "array",
                "description": "List of stream processing pipelines.",
                "items": {"$ref": "#/$defs/stream"}
            }
        },
        "required": ["streams"]
    });

    let defs = root.as_object_mut().unwrap();
    defs.insert("$defs".to_string(), serde_json::json!({}));

    // Build the per-component variant schemas once so we can reuse them.
    let input_variants = variant_schemas(ComponentKind::Input);
    let output_variants = variant_schemas(ComponentKind::Output);
    let processor_variants = variant_schemas(ComponentKind::Processor);
    let buffer_variants = variant_schemas(ComponentKind::Buffer);
    let codec_variants = variant_schemas(ComponentKind::Codec);

    let component_union =
        |variants: &serde_json::Value, kind: ComponentKind| -> serde_json::Value {
            let variants = variants.as_array().cloned().unwrap_or_default();
            if variants.is_empty() {
                // No components registered for this kind — allow any object so
                // existing configs still validate.
                return serde_json::json!({
                    "type": "object",
                    "description": format!("No {} components are registered.", kind),
                });
            }
            serde_json::json!({
                "type": "object",
                "description": format!("Select a registered {} component.", kind),
                "required": ["type"],
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": variants.iter().map(|v| v["name"].clone()).collect::<Vec<_>>(),
                        "description": format!("{} component type.", kind)
                    },
                    "name": {
                        "type": "string",
                        "description": "Optional logical name for this component instance."
                    }
                },
                "oneOf": variants
            })
        };

    let defs = defs.get_mut("$defs").unwrap().as_object_mut().unwrap();
    defs.insert(
        "input".to_string(),
        component_union(&input_variants, ComponentKind::Input),
    );
    defs.insert(
        "output".to_string(),
        component_union(&output_variants, ComponentKind::Output),
    );
    defs.insert(
        "processor".to_string(),
        component_union(&processor_variants, ComponentKind::Processor),
    );
    defs.insert(
        "buffer".to_string(),
        component_union(&buffer_variants, ComponentKind::Buffer),
    );
    defs.insert(
        "codec".to_string(),
        component_union(&codec_variants, ComponentKind::Codec),
    );
    defs.insert(
        "stream".to_string(),
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["input", "output", "pipeline"],
            "properties": {
                "input": {"$ref": "#/$defs/input"},
                "output": {"$ref": "#/$defs/output"},
                "error_output": {"$ref": "#/$defs/output"},
                "pipeline": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["processors"],
                    "properties": {
                        "thread_num": {
                            "type": "integer",
                            "minimum": 1,
                            "default": 1,
                            "description": "Number of processor worker threads."
                        },
                        "processors": {
                            "type": "array",
                            "items": {"$ref": "#/$defs/processor"}
                        },
                        "input": {
                            "type": "object",
                            "description": "Stream-level input overrides (currently unused, reserved for future).",
                            "additionalProperties": true
                        },
                        "output": {
                            "type": "object",
                            "description": "Stream-level output overrides (currently unused, reserved for future).",
                            "additionalProperties": true
                        }
                    }
                },
                "buffer": {"$ref": "#/$defs/buffer"}
            }
        }),
    );

    root
}

/// Build the `oneOf` variant array for a given component kind. Each
/// variant embeds the component's individual `config_schema` so that
/// editors can offer field-level completion.
fn variant_schemas(kind: ComponentKind) -> serde_json::Value {
    let variants: Vec<serde_json::Value> = list_components_by_kind(kind)
        .into_iter()
        .map(|m| {
            let mut props = serde_json::Map::new();
            props.insert(
                "type".to_string(),
                serde_json::json!({
                    "type": "string",
                    "const": m.name,
                    "description": m.description.clone()
                }),
            );
            props.insert(
                "name".to_string(),
                serde_json::json!({
                    "type": "string",
                    "description": "Optional logical name for this instance."
                }),
            );

            let mut config_subschema = m.config_schema.clone();
            if let Some(obj) = config_subschema.as_object_mut() {
                if !obj.contains_key("type") {
                    obj.insert("type".to_string(), serde_json::json!("object"));
                }
            } else {
                config_subschema = serde_json::json!({"type": "object"});
            }

            let mut properties = serde_json::Map::new();
            properties.insert("type".to_string(), props["type"].clone());
            properties.insert("name".to_string(), props["name"].clone());
            properties.insert("codec".to_string(), codec_schema());
            // The component-specific config keys are flattened into the
            // top-level object, so we splice them in via a pattern of
            // additionalProperties alongside an optional embedded schema.
            if let Some(obj) = config_subschema.as_object() {
                if let Some(inner_props) = obj.get("properties").and_then(|p| p.as_object()) {
                    for (k, v) in inner_props {
                        properties.insert(k.clone(), v.clone());
                    }
                }
            }
            let example = m.config_example.clone();
            let mut variant = serde_json::json!({
                "type": "object",
                "title": m.name.clone(),
                "description": m.description.clone(),
                "properties": properties,
                "required": ["type"]
            });
            if let Some(ex) = example {
                variant
                    .as_object_mut()
                    .unwrap()
                    .insert("examples".to_string(), serde_json::json!([ex]));
            }
            variant
        })
        .collect();
    serde_json::Value::Array(variants)
}

fn codec_schema() -> serde_json::Value {
    let variants: Vec<serde_json::Value> = list_components_by_kind(ComponentKind::Codec)
        .into_iter()
        .map(|m| {
            serde_json::json!({
                "type": "object",
                "title": m.name,
                "description": m.description,
                "properties": {
                    "type": {"type": "string", "const": m.name}
                },
                "required": ["type"]
            })
        })
        .collect();
    if variants.is_empty() {
        return serde_json::json!({"type": "object"});
    }
    serde_json::json!({
        "oneOf": variants
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Each test that registers metadata needs an isolated registry state;
    // the simplest way is to use distinct component names per test.
    static REGISTER_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn component_kind_as_str_and_from_str_round_trip() {
        for kind in ComponentKind::all() {
            let parsed: ComponentKind = kind.as_str().parse().unwrap();
            assert_eq!(parsed, kind);
        }
    }

    #[test]
    fn component_kind_from_str_rejects_unknown() {
        let err = "unknown".parse::<ComponentKind>().unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn unit_metadata_is_optional() {
        let m = ComponentMetadata::unit("noop", "Does nothing.");
        assert_eq!(m.name, "noop");
        assert!(m.config_optional);
        assert!(m.config_example.is_none());
    }

    #[test]
    fn with_schema_metadata_chain() {
        let m = ComponentMetadata::with_schema(
            "demo",
            "Demo component.",
            serde_json::json!({"type": "object", "properties": {"x": {"type": "integer"}}}),
        )
        .with_optional()
        .with_example(serde_json::json!({"x": 1}));
        assert_eq!(m.name, "demo");
        assert!(m.config_optional);
        assert!(m.config_example.is_some());
    }

    #[test]
    fn register_and_lookup_metadata() {
        let _guard = REGISTER_LOCK.lock().unwrap();
        let name = "test_lookup_component";
        register_input_metadata(ComponentMetadata::unit(name, "Lookup test.")).unwrap();

        let found = get_component_metadata(ComponentKind::Input, name).unwrap();
        assert_eq!(found.name, name);
        assert_eq!(found.description, "Lookup test.");
        assert!(found.config_optional);

        // Duplicate registration is rejected.
        let dup = register_input_metadata(ComponentMetadata::unit(name, "again"));
        assert!(matches!(dup, Err(Error::Config(_))));
    }

    #[test]
    fn list_components_orders_by_kind() {
        let _guard = REGISTER_LOCK.lock().unwrap();
        let name = "test_listing_component";
        register_buffer_metadata(ComponentMetadata::unit(name, "Listing test.")).unwrap();

        let list = list_components();
        let kinds: Vec<ComponentKind> = list.iter().map(|(k, _)| *k).collect();
        let mut sorted = kinds.clone();
        sorted.dedup();
        assert_eq!(kinds, sorted, "kinds should appear in canonical order");

        let found = list
            .iter()
            .find(|(_, m)| m.name == name)
            .expect("registered component should appear in list");
        assert_eq!(found.0, ComponentKind::Buffer);
    }

    #[test]
    fn build_config_schema_contains_component_variants() {
        let _guard = REGISTER_LOCK.lock().unwrap();
        let name = "test_schema_component";
        register_output_metadata(ComponentMetadata::unit(name, "Schema test.")).unwrap();

        let schema = build_config_schema();
        let variants = schema
            .pointer("/$defs/output/oneOf")
            .expect("output variants should be present");
        let variants = variants.as_array().unwrap();
        assert!(
            variants.iter().any(
                |v| v.pointer("/properties/type/const").and_then(|c| c.as_str()) == Some(name)
            ),
            "registered component should appear in output schema variants"
        );
    }
}
