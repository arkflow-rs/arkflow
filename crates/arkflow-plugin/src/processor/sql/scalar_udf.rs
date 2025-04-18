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
use datafusion::logical_expr::ScalarUDF;
use std::sync::{Arc, RwLock};
use tracing::log::debug;

/// Module for managing scalar user-defined functions (UDFs) for SQL processing.
///
/// This module provides functionality to register and initialize UDFs in a thread-safe manner.
/// UDFs are registered globally and then added to the SQL function registry during context initialization.
lazy_static::lazy_static! {
    static ref SCALAR_UDFS: RwLock<Vec<Arc<ScalarUDF>>> = RwLock::new(Vec::new());
}

/// Register a new scalar UDF.
///
/// This function adds a UDF to the global registry. The UDF will be available for use
/// in SQL queries after the next call to `init`.
///
/// # Arguments
///
/// * `udf` - The UDF to register, wrapped in an Arc for shared ownership.
pub fn register(udf: Arc<ScalarUDF>) {
    let mut udfs = SCALAR_UDFS
        .write()
        .expect("Failed to acquire write lock for SCALAR_UDFS");
    udfs.push(udf);
}

pub(crate) fn init(registry: &mut dyn FunctionRegistry) -> Result<(), Error> {
    let udfs = SCALAR_UDFS
        .read()
        .expect("Failed to acquire read lock for SCALAR_UDFS");
    udfs.iter()
        .try_for_each(|udf| {
            let existing_udf = registry.register_udf(Arc::clone(&udf))?;
            if let Some(existing_udf) = existing_udf {
                debug!("Overwrite existing UDF: {}", existing_udf.name());
            }
            Ok(()) as datafusion::common::Result<()>
        })
        .map_err(|e| Error::Config(format!("Failed to register UDFs: {}", e)))
}
