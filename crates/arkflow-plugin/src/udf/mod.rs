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

/// Module for managing scalar user-defined functions (UDFs) for SQL processing.
///
/// This module provides functionality to register and initialize UDFs in a thread-safe manner.
/// UDFs are registered globally and then added to the SQL function registry during context initialization.
use arkflow_core::Error;
use datafusion::execution::FunctionRegistry;

pub mod aggregate_udf;
pub mod scalar_udf;
pub mod window_udf;

/// Initializes and registers all user-defined functions (UDFs).
///
/// This function calls the `init` function of each UDF module (aggregate, scalar, window)
/// to register their respective functions with the provided `FunctionRegistry`.
///
/// # Arguments
///
/// * `registry` - A mutable reference to a type implementing `FunctionRegistry` where the UDFs will be registered.
///
/// # Errors
///
/// Returns an `Error` if any of the underlying `init` calls fail during registration.
pub(crate) fn init<T: FunctionRegistry>(registry: &mut T) -> Result<(), Error> {
    aggregate_udf::init(registry)?;
    scalar_udf::init(registry)?;
    window_udf::init(registry)?;
    Ok(())
}
