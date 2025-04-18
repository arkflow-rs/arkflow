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
use std::sync::Arc;
use tracing::log::debug;

pub mod aggregate_udf;
pub mod scalar_udf;
pub mod window_udf;

pub(crate) fn init(registry: &mut dyn FunctionRegistry) -> Result<(), Error> {
    let scalar_udfs = crate::processor::udf::scalar_udf::UDFS
        .read()
        .expect("Failed to acquire read lock for scalar UDFS");
    scalar_udfs
        .iter()
        .try_for_each(|udf| {
            let existing_udf = registry.register_udf(Arc::clone(udf))?;
            if let Some(existing_udf) = existing_udf {
                debug!("Overwrite existing scalar UDF: {}", existing_udf.name());
            }
            Ok(()) as datafusion::common::Result<()>
        })
        .map_err(|e| Error::Config(format!("Failed to register scalar UDFs: {}", e)))?;

    let aggregate_udfs = crate::processor::udf::aggregate_udf::UDFS
        .read()
        .expect("Failed to acquire read lock for aggregate UDFS");
    aggregate_udfs
        .iter()
        .try_for_each(|udf| {
            let existing_udf = registry.register_udaf(Arc::clone(udf))?;
            if let Some(existing_udf) = existing_udf {
                debug!("Overwrite existing aggregate UDF: {}", existing_udf.name());
            }
            Ok(()) as datafusion::common::Result<()>
        })
        .map_err(|e| Error::Config(format!("Failed to register aggregate UDFs: {}", e)))?;

    let window_udfs = crate::processor::udf::window_udf::UDFS
        .read()
        .expect("Failed to acquire read lock for windows UDFS");
    window_udfs
        .iter()
        .try_for_each(|udf| {
            let existing_udf = registry.register_udwf(Arc::clone(udf))?;
            if let Some(existing_udf) = existing_udf {
                debug!("Overwrite existing windows UDF: {}", existing_udf.name());
            }
            Ok(()) as datafusion::common::Result<()>
        })
        .map_err(|e| Error::Config(format!("Failed to register windows UDFs: {}", e)))?;
    Ok(())
}
