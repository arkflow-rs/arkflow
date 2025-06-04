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
use arkflow_core::processor::{Processor, ProcessorBuilder};
use arkflow_core::{processor, Error, MessageBatch, Resource};
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::pyarrow::{FromPyArrow, ToPyArrow};
use pyo3::prelude::*;
use pyo3::types::PyList;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::ffi::CString;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PythonProcessorConfig {
    /// Python code to execute
    script: Option<String>,
    /// Python module to import
    module: Option<String>,
    /// Function name to call for processing
    function: String,
    /// Additional Python paths
    python_path: Vec<String>,
}

struct PythonProcessor {
    config: PythonProcessorConfig,
}

#[async_trait]
impl Processor for PythonProcessor {
    async fn process(&self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        let config = self.config.clone();
        let result = tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| -> Result<Vec<MessageBatch>, Error> {
                let sys = py
                    .import("sys")
                    .map_err(|_| Error::Process("Failed to import sys".to_string()))?;
                let binding = sys
                    .getattr("path")
                    .map_err(|_| Error::Process("Failed to get sys.path".to_string()))?;
                let path = binding
                    .downcast::<PyList>()
                    .map_err(|_| Error::Process("Failed to downcast sys.path".to_string()))?;
                let _ = &config
                    .python_path
                    .iter()
                    .for_each(|p| path.set_item(0, p).unwrap());
                path.insert(0, ".").unwrap();

                // Get the Python module either from the script or from an imported module
                let py_module = if let Some(module_name) = &config.module {
                    py.import(module_name).map_err(|e| {
                        Error::Process(format!("Failed to import module {}: {}", module_name, e))
                    })?
                } else {
                    // If no module specified, use __main__
                    py.import("__main__").map_err(|_| {
                        Error::Process("Failed to import __main__ module".to_string())
                    })?
                };

                if let Some(script) = &config.script {
                    let string = CString::new(script.clone())
                        .map_err(|e| Error::Process(format!("Failed to create CString: {}", e)))?;
                    py.run(&string, None, None).map_err(|e| {
                        Error::Process(format!("Failed to run Python script: {}", e))
                    })?;
                }

                // Convert MessageBatch to PyArrow
                let py_batch = batch.to_pyarrow(py).map_err(|e| {
                    Error::Process(format!("Failed to convert MessageBatch to PyArrow: {}", e))
                })?;

                // Get the processing function
                let func = py_module.getattr(&config.function).map_err(|e| {
                    Error::Process(format!(
                        "Failed to get function '{}': {}",
                        &config.function, e
                    ))
                })?;

                // Call the Python function with the batch
                let result = func.call1((py_batch,)).map_err(|e| {
                    Error::Process(format!("Failed to call Python function: {}", e))
                })?;
                let py_list = result.downcast::<PyList>().map_err(|_| {
                    Error::Process("Failed to downcast Python result to PyList".to_string())
                })?;
                let vec_rb = py_list
                    .into_iter()
                    .map(|item| {
                        RecordBatch::from_pyarrow_bound(&item).map_err(|e| {
                            Error::Process(format!(
                                "Failed to convert PyArrow to RecordBatch: {}",
                                e
                            ))
                        })
                    })
                    .collect::<Result<Vec<RecordBatch>, Error>>()?;
                let vec_mb = vec_rb
                    .into_iter()
                    .map(|rb| MessageBatch::new_arrow(rb))
                    .collect::<Vec<_>>();
                Ok(vec_mb)
            })
        })
        .await
        .map_err(|_| Error::Process("Failed to spawn blocking task".to_string()))?;
        result
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

struct PythonProcessorBuilder;
impl ProcessorBuilder for PythonProcessorBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Python processor configuration is missing".to_string(),
            ));
        }

        let config: PythonProcessorConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(PythonProcessor { config }))
    }
}

pub fn init() -> Result<(), Error> {
    processor::register_processor_builder("python", Arc::new(PythonProcessorBuilder))
}
