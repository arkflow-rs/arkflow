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
    func: PyObject, // Stores the Python function to be called
}

#[async_trait]
impl Processor for PythonProcessor {
    async fn process(&self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        let func_to_call = Python::with_gil(|py| self.func.clone_ref(py));

        let result = tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| -> Result<Vec<MessageBatch>, Error> {
                // Convert MessageBatch to PyArrow
                let py_batch = batch.to_pyarrow(py).map_err(|e| {
                    Error::Process(format!("Failed to convert MessageBatch to PyArrow: {}", e))
                })?;

                let func_bound = func_to_call.bind(py);
                let result = func_bound
                    .call1((py_batch,))
                    .map_err(|e| Error::Process(format!("Python function call failed: {}", e)))?;
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
        .map_err(|e| Error::Process(format!("Failed to spawn blocking task: {}", e)))?;
        result
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl PythonProcessor {
    fn new(config: PythonProcessorConfig) -> Result<Self, Error> {
        Python::with_gil(|py| -> Result<Self, Error> {
            let sys = py
                .import("sys")
                .map_err(|_| Error::Process("Failed to import sys".to_string()))?;
            let binding = sys
                .getattr("path")
                .map_err(|_| Error::Process("Failed to get sys.path".to_string()))?;
            let path = binding
                .downcast::<PyList>()
                .map_err(|_| Error::Process("Failed to downcast sys.path".to_string()))?;
            path.insert(0, ".").unwrap();
            let _ = &config
                .python_path
                .iter()
                .for_each(|p| path.insert(0, p).unwrap());

            // Get the Python module either from the script or from an imported module
            let py_module = if let Some(module_name) = &config.module {
                py.import(module_name).map_err(|e| {
                    Error::Process(format!("Failed to import module {}: {}", module_name, e))
                })?
            } else {
                // If no module specified, use __main__
                py.import("__main__").map_err(|e| {
                    Error::Process(format!("Failed to import __main__ module: {}", e))
                })?
            };

            if let Some(script) = &config.script {
                let string = CString::new(script.as_str())
                    .map_err(|e| Error::Process(format!("Failed to create CString: {}", e)))?;
                py.run(&string, None, None)
                    .map_err(|e| Error::Process(format!("Failed to run Python script: {}", e)))?;
            }

            // Get the processing function
            let func = py_module.getattr(&config.function).map_err(|e| {
                Error::Process(format!(
                    "Failed to get function '{}': {}",
                    &config.function, e
                ))
            })?;

            // Convert the bound function reference to a PyObject for storage.
            let func_obj: PyObject = func.into_any().unbind();
            Ok(PythonProcessor { func: func_obj })
        })
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
        Ok(Arc::new(PythonProcessor::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    processor::register_processor_builder("python", Arc::new(PythonProcessorBuilder))
}
