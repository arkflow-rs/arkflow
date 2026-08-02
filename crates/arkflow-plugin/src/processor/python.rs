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
use arkflow_core::component::{register_processor_metadata, ComponentMetadata};
use arkflow_core::processor::{Processor, ProcessorBuilder};
use arkflow_core::{Error, MessageBatch, MessageBatchRef, ProcessResult, Resource};
use arrow_array::RecordBatch;
use arrow_pyarrow::{FromPyArrow, ToPyArrow};
use async_trait::async_trait;
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
    #[serde(default = "default_module")]
    module: String,
    /// Function name to call for processing
    function: String,
    /// Additional Python paths
    #[serde(default = "default_python_path")]
    python_path: Vec<String>,
}

struct PythonProcessor {
    func: Py<PyAny>, // Stores the Python function to be called
}

#[async_trait]
impl Processor for PythonProcessor {
    async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        let func_to_call = Python::attach(|py| self.func.clone_ref(py));

        let result = tokio::task::spawn_blocking(move || {
            Python::attach(|py| -> Result<Vec<RecordBatch>, Error> {
                // Convert MessageBatch to PyArrow
                let py_batch = batch.record_batch().to_pyarrow(py).map_err(|e| {
                    Error::Process(format!("Failed to convert MessageBatch to PyArrow: {}", e))
                })?;

                let func_bound = func_to_call.bind(py);
                let result = func_bound
                    .call1((py_batch,))
                    .map_err(|e| Error::Process(format!("Python function call failed: {}", e)))?;

                let py_list = result.cast::<PyList>().map_err(|_| {
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
                Ok(vec_rb)
            })
        })
        .await
        .map_err(|e| Error::Process(format!("Failed to spawn blocking task: {}", e)))??;

        let vec_mb = result
            .into_iter()
            .map(MessageBatch::new_arrow)
            .collect::<Vec<_>>();

        if vec_mb.is_empty() {
            Ok(ProcessResult::None)
        } else if vec_mb.len() == 1 {
            Ok(ProcessResult::Single(Arc::new(
                vec_mb.into_iter().next().unwrap(),
            )))
        } else {
            Ok(ProcessResult::Multiple(
                vec_mb.into_iter().map(Arc::new).collect(),
            ))
        }
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl PythonProcessor {
    fn new(config: PythonProcessorConfig) -> Result<Self, Error> {
        Python::attach(|py| -> Result<Self, Error> {
            let sys = py
                .import("sys")
                .map_err(|_| Error::Process("Failed to import sys".to_string()))?;
            let binding = sys
                .getattr("path")
                .map_err(|_| Error::Process("Failed to get sys.path".to_string()))?;
            let path = binding
                .cast::<PyList>()
                .map_err(|_| Error::Process("Failed to downcast sys.path".to_string()))?;
            path.insert(0, ".").unwrap();
            let _ = &config
                .python_path
                .iter()
                .for_each(|p| path.insert(0, p).unwrap());

            // Get the Python module either from the script or from an imported module
            let py_module = py.import(&config.module).map_err(|e| {
                Error::Process(format!("Failed to import {} module: {}", config.module, e))
            })?;

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
                    config.function, e
                ))
            })?;

            // Convert the bound function reference to a PyObject for storage.
            let func_obj: Py<PyAny> = func.into_any().unbind();
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

fn default_python_path() -> Vec<String> {
    vec![]
}

fn default_module() -> String {
    // If no module specified, use __main__
    "__main__".to_string()
}

pub fn init() -> Result<(), Error> {
    arkflow_core::processor::register_processor_builder(
        "python",
        Arc::new(PythonProcessorBuilder),
    )?;
    register_processor_metadata(ComponentMetadata::with_schema(
        "python",
        "Runs a user-defined Python function (with PyArrow) against each batch.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "script": {"type": "string", "description": "Python source defining the transform function."},
                "function": {"type": "string", "description": "Name of the function to invoke for each batch."},
                "extra_packages": {"type": "array", "items": {"type": "string"}, "description": "Optional list of pip packages to install before running."}
            },
            "required": ["script", "function"]
        }),
    ).with_example(serde_json::json!({
        "script": "def transform(batch):\n    return batch",
        "function": "transform"
    })))
}
