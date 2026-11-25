//! Missing-feature hints for the compile-time gated component families.
//!
//! Minimal builds (`--no-default-features --features engine-minimal`, plus
//! any family feature combination) simply do not register the gated
//! component types, so a config referencing them would fail with an ordinary
//! "Unknown <kind> type" error. This table — compiled unconditionally, one
//! row per gated type name (design D4 of `add-cargo-feature-gating`) — is
//! registered into the core feature-hint registry at `initialize()` time so
//! the error names the exact cargo feature to enable, e.g.
//! `Unknown input type: kafka (requires the 'connectors-kafka' feature)`.

use arkflow_core::component::feature_hints::register_feature_hint;
use arkflow_core::Error;

/// Every cargo feature on this crate that gates a component family, in the
/// order of `[features].default`. The tests below pin the hint table to
/// these names so a renamed or removed feature cannot strand a stale hint.
#[cfg(test)]
pub(crate) const FAMILY_FEATURES: &[&str] = &[
    "connectors-sql",
    "connectors-kafka",
    "connectors-python",
    "connectors-mongodb",
    "connectors-pulsar",
    "connectors-hdfs",
    "connectors-mqtt",
    "connectors-nats",
    "connectors-redis",
    "connectors-websocket",
    "connectors-modbus",
    "ai-vector",
    "processors-vrl",
    "codecs-protobuf",
];

/// (component type name, hint appended to the unknown-type error). Kinds
/// sharing a type name (`kafka` input and output) share one row — the
/// feature family is the same.
const HINTS: &[(&str, &str)] = &[
    ("sql", "requires the 'connectors-sql' feature"),
    ("kafka", "requires the 'connectors-kafka' feature"),
    ("python", "requires the 'connectors-python' feature"),
    ("mongodb", "requires the 'connectors-mongodb' feature"),
    ("pulsar", "requires the 'connectors-pulsar' feature"),
    ("mqtt", "requires the 'connectors-mqtt' feature"),
    ("nats", "requires the 'connectors-nats' feature"),
    ("redis", "requires the 'connectors-redis' feature"),
    ("websocket", "requires the 'connectors-websocket' feature"),
    ("modbus", "requires the 'connectors-modbus' feature"),
    ("qdrant", "requires the 'ai-vector' feature"),
    ("milvus", "requires the 'ai-vector' feature"),
    ("pgvector", "requires the 'ai-vector' feature"),
    ("embedding", "requires the 'ai-vector' feature"),
    ("llm", "requires the 'ai-vector' feature"),
    ("vector_search", "requires the 'ai-vector' feature"),
    ("milvus_search", "requires the 'ai-vector' feature"),
    ("pgvector_search", "requires the 'ai-vector' feature"),
    ("vrl", "requires the 'processors-vrl' feature"),
    ("protobuf", "requires the 'codecs-protobuf' feature"),
    ("schema_registry", "requires the 'codecs-protobuf' feature"),
    (
        "arrow_to_protobuf",
        "requires the 'codecs-protobuf' feature",
    ),
    (
        "protobuf_to_arrow",
        "requires the 'codecs-protobuf' feature",
    ),
];

/// Register one hint per gated type name. Called from `initialize()` ahead
/// of the kind registrations; re-running is a no-op (same name, same hint).
pub(crate) fn register() -> Result<(), Error> {
    for (type_name, hint) in HINTS {
        register_feature_hint(type_name, hint)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every hint must point at a feature that actually exists in this
    /// crate's manifest, so renaming a feature cannot strand the hint table.
    #[test]
    fn hints_reference_real_family_features() {
        let manifest = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
            .expect("read arkflow-plugin Cargo.toml");
        for (_, hint) in HINTS {
            let feature = hint_feature_name(hint);
            assert!(
                FAMILY_FEATURES.contains(&feature),
                "hint {hint:?} names a feature outside FAMILY_FEATURES: {feature:?}"
            );
            assert!(
                manifest.contains(&format!("{feature} = ")),
                "feature {feature:?} not declared in arkflow-plugin/Cargo.toml [features]"
            );
        }
    }

    /// Ungated component type names must not carry hints — their
    /// unknown-type error stays plain (spec: ungated unknown types).
    #[test]
    fn ungated_names_have_no_hint() {
        for ungated in ["http", "file", "generate", "memory", "lookup", "json"] {
            assert!(
                !HINTS.iter().any(|(name, _)| *name == ungated),
                "{ungated} is ungated and must not have a feature hint"
            );
        }
    }

    fn hint_feature_name(hint: &str) -> &str {
        hint.split('\'')
            .nth(1)
            .expect("hint quotes its feature name")
    }
}
