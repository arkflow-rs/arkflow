//! Missing-feature hints for compile-time gated component types.
//!
//! When a build disables a connector family (the `arkflow-plugin` cargo
//! features, see the `add-cargo-feature-gating` change), the affected
//! component types are simply never registered, so a config that is valid on
//! a full build fails with an ordinary "Unknown <kind> type" error that
//! gives the user no path forward. The plugin crate registers one hint per
//! gated type name at initialization time; the builder lookup paths for
//! every kind route their unknown-type error through
//! [`unknown_type_message`], which appends the hint so the message names the
//! exact cargo feature to enable.
//!
//! The table is deliberately tiny and compiled into every build (a few dozen
//! string pairs): hint data must exist in builds where the gated code does
//! not.

use crate::Error;
use lazy_static::lazy_static;
use std::collections::BTreeMap;
use std::sync::RwLock;

lazy_static! {
    static ref FEATURE_HINTS: RwLock<BTreeMap<String, String>> = RwLock::new(BTreeMap::new());
}

/// Register a missing-feature hint for a component type name.
///
/// Re-registering the identical (name, hint) pair is a no-op so re-running
/// plugin `initialize()` stays retryable; a different hint for an
/// already-registered name is a registration bug and fails loudly.
pub fn register_feature_hint(type_name: &str, hint: &str) -> Result<(), Error> {
    let mut hints = FEATURE_HINTS.write().unwrap();
    match hints.get(type_name) {
        Some(existing) if existing == hint => Ok(()),
        Some(existing) => Err(Error::Config(format!(
            "feature hint for type '{}' already registered with a different message: {:?} vs {:?}",
            type_name, existing, hint
        ))),
        None => {
            hints.insert(type_name.to_string(), hint.to_string());
            Ok(())
        }
    }
}

/// The registered missing-feature hint for a type name, if any.
pub fn feature_hint(type_name: &str) -> Option<String> {
    FEATURE_HINTS.read().unwrap().get(type_name).cloned()
}

/// Build the unknown-type error message for a kind, appending the registered
/// missing-feature hint when the type name has one.
pub(crate) fn unknown_type_message(kind: &str, type_name: &str) -> String {
    match feature_hint(type_name) {
        Some(hint) => format!("Unknown {} type: {} ({})", kind, type_name, hint),
        None => format!("Unknown {} type: {}", kind, type_name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_type_without_hint_is_ordinary_message() {
        // No hint registered for this name in this test process.
        let name = "definitely-not-a-component-type";
        assert_eq!(
            unknown_type_message("input", name),
            format!("Unknown input type: {}", name)
        );
        assert_eq!(feature_hint(name), None);
    }

    #[test]
    fn registered_hint_is_appended_and_idempotent() {
        let name = "hint-test-type";
        register_feature_hint(name, "requires the 'connectors-test' feature").unwrap();
        // Idempotent re-registration.
        register_feature_hint(name, "requires the 'connectors-test' feature").unwrap();
        assert_eq!(
            unknown_type_message("processor", name),
            "Unknown processor type: hint-test-type (requires the 'connectors-test' feature)"
        );
        // A conflicting hint is a bug and must fail.
        assert!(register_feature_hint(name, "different").is_err());
    }
}
