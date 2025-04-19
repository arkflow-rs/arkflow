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
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::WindowUDF;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::debug;

lazy_static::lazy_static! {
    static ref UDFS: RwLock<HashMap<String,Arc<WindowUDF>>> = RwLock::new(HashMap::new());
}

/// Register a new window UDF (User Defined Function).
///
/// This function wraps the provided WindowUDF instance in an Arc and stores it in the global UDFS list,
/// so it can later be registered with the FunctionRegistry.
///
/// # Arguments
/// * `udf` - The WindowUDF instance to register.
pub fn register(udf: WindowUDF) {
    let mut udfs = UDFS.write().expect("Failed to acquire write lock for UDFS");
    let name = udf.name();
    if udfs.contains_key(name) {
        panic!("Window UDF with name '{}' already registered", name);
    };
    udfs.insert(name.to_string(), Arc::new(udf));
}

pub(crate) fn init<T: FunctionRegistry>(registry: &mut T) -> Result<(), Error> {
    let window_udfs = UDFS
        .read()
        .expect("Failed to acquire read lock for window UDFS");
    window_udfs
        .iter()
        .try_for_each(|(_, udf)| {
            let existing_udf = registry.register_udwf(Arc::clone(udf))?;
            if let Some(existing_udf) = existing_udf {
                debug!("Overwrite existing window UDF: {}", existing_udf.name());
            }
            Ok(()) as datafusion::common::Result<()>
        })
        .map_err(|e| Error::Config(format!("Failed to register window UDFs: {}", e)))
}
