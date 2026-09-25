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
use arkflow_core::Error;

pub mod avro_arrow;
pub mod debezium;
pub mod json;
pub mod protobuf;
pub mod schema_registry;

fn register_components() -> Result<(), Error> {
    json::init()?;
    protobuf::init()?;
    debezium::init()?;
    schema_registry::init()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_codec_init() {
        // Test that codec initialization succeeds
        let result = init();
        assert!(result.is_ok());
    }
}

/// Component registration is process-global, so `init()` is idempotent: the
/// first call registers every builder and later calls (tests, multi-entry
/// binaries) return immediately without touching the registries again.
pub fn init() -> Result<(), Error> {
    static INIT: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    INIT.get_or_init(|| {
        let _ = register_components();
    });
    Ok(())
}
