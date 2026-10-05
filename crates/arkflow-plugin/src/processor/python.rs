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
    /// Additional Python paths (earlier entries take precedence)
    #[serde(default = "default_python_path")]
    python_path: Vec<String>,
    /// Upper bound for one UDF call; a longer call fails the batch instead of
    /// blocking the stream forever. The abandoned blocking thread keeps
    /// running until the UDF itself returns.
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
}

/// Default UDF call timeout: 60s.
fn default_timeout_ms() -> u64 {
    60_000
}

/// Process-wide bound on UDF calls occupying blocking threads. A timed-out
/// call cannot be cancelled and keeps its thread (and this permit) until
/// the UDF itself returns; without the bound a hanging UDF leaks one
/// blocking-pool thread per retry until the pool (default 512 threads) is
/// exhausted, stalling every other `spawn_blocking` caller — including
/// the Kafka transactional commit path. All Python processors share one
/// interpreter, so the bound is global.
const INFLIGHT_UDF_LIMIT: usize = 64;
static INFLIGHT_UDFS: std::sync::LazyLock<Arc<tokio::sync::Semaphore>> =
    std::sync::LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(INFLIGHT_UDF_LIMIT)));

struct PythonProcessor {
    func: Py<PyAny>, // Stores the Python function to be called
    timeout_ms: u64,
}

/// Run one UDF call on the blocking thread: convert the batch to PyArrow,
/// invoke the function, convert the returned list back to RecordBatches.
fn call_udf(func: Py<PyAny>, batch: MessageBatchRef) -> Result<Vec<RecordBatch>, Error> {
    Python::attach(|py| -> Result<Vec<RecordBatch>, Error> {
        // Convert MessageBatch to PyArrow
        let py_batch = batch.record_batch().to_pyarrow(py).map_err(|e| {
            Error::Process(format!("Failed to convert MessageBatch to PyArrow: {}", e))
        })?;

        let func_bound = func.bind(py);
        let result = func_bound
            .call1((py_batch,))
            .map_err(|e| Error::Process(format!("Python function call failed: {}", e)))?;

        let py_list = result.cast::<PyList>().map_err(|_| {
            Error::Process("Failed to downcast Python result to PyList".to_string())
        })?;
        py_list
            .into_iter()
            .map(|item| {
                RecordBatch::from_pyarrow_bound(&item).map_err(|e| {
                    Error::Process(format!("Failed to convert PyArrow to RecordBatch: {}", e))
                })
            })
            .collect::<Result<Vec<RecordBatch>, Error>>()
    })
}

#[async_trait]
impl Processor for PythonProcessor {
    async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        let func_to_call = Python::attach(|py| self.func.clone_ref(py));

        let timeout = std::time::Duration::from_millis(self.timeout_ms);
        // The permit lives inside the blocking closure: an async timeout
        // abandons the task but the stuck call keeps holding its permit,
        // so new calls queue here instead of piling more threads.
        let permit =
            INFLIGHT_UDFS.clone().acquire_owned().await.map_err(|e| {
                Error::Process(format!("python UDF in-flight semaphore closed: {e}"))
            })?;
        let handle = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            call_udf(func_to_call, batch)
        });
        let result = tokio::time::timeout(timeout, handle)
            .await
            .map_err(|_| {
                Error::Process(format!(
                    "Python function call timed out after {} ms (the UDF may still occupy its blocking thread and one of {} in-flight slots until it returns)",
                    self.timeout_ms,
                    INFLIGHT_UDF_LIMIT
                ))
            })?
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
            Self::setup_sys_path(py, &config.python_path)?;

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
            Ok(PythonProcessor {
                func: func_obj,
                timeout_ms: config.timeout_ms,
            })
        })
    }

    /// Prepend the configured python paths (in config order, earlier =
    /// higher precedence) followed by the working directory `"."` to
    /// `sys.path`, skipping entries already present. Insert results are
    /// checked — a failed insert is a configuration error, never a panic.
    fn setup_sys_path(py: Python<'_>, python_path: &[String]) -> Result<(), Error> {
        let sys = py
            .import("sys")
            .map_err(|_| Error::Process("Failed to import sys".to_string()))?;
        let binding = sys
            .getattr("path")
            .map_err(|_| Error::Process("Failed to get sys.path".to_string()))?;
        let path = binding
            .cast::<PyList>()
            .map_err(|_| Error::Process("Failed to downcast sys.path".to_string()))?;
        let existing: Vec<String> = path
            .extract()
            .map_err(|e| Error::Process(format!("Failed to read sys.path: {}", e)))?;

        // Desired front segment: [p1, p2, ..., "."] — deduplicated in place
        // (first occurrence wins) and inserted in reverse so the final order
        // matches the configuration (the old code reversed the list and let
        // `"."` fall behind user paths).
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let desired: Vec<&str> = python_path
            .iter()
            .map(|s| s.as_str())
            .chain(std::iter::once("."))
            .filter(|entry| seen.insert(entry))
            .collect();
        for entry in desired.into_iter().rev() {
            if existing.iter().any(|e| e == entry) {
                continue;
            }
            path.insert(0, entry).map_err(|e| {
                Error::Config(format!("Failed to append '{}' to sys.path: {}", entry, e))
            })?;
        }
        Ok(())
    }
}

struct PythonProcessorBuilder;
impl ProcessorBuilder for PythonProcessorBuilder {
    fn build(
        &self,
        _name: Option<&str>,
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
                "script": {"type": "string", "description": "Python source defining the transform function (optional when importing an existing module)."},
                "module": {"type": "string", "default": "__main__", "description": "Python module providing the function."},
                "function": {"type": "string", "description": "Name of the function to invoke for each batch."},
                "python_path": {"type": "array", "items": {"type": "string"}, "default": [], "description": "Extra sys.path entries, earlier entries take precedence."},
                "timeout_ms": {"type": "integer", "minimum": 1, "default": 60000, "description": "Upper bound for one UDF call in milliseconds; longer calls fail the batch."}
            },
            "required": ["function"]
        }),
    ).with_example(serde_json::json!({
        "script": "def transform(batch):\n    return batch",
        "function": "transform"
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_resource() -> Resource {
        Resource {
            temporary: Default::default(),
            input_names: std::cell::RefCell::new(Default::default()),
        }
    }

    fn build(config: serde_json::Value) -> Result<Arc<dyn Processor>, Error> {
        PythonProcessorBuilder.build(None, &Some(config), &test_resource())
    }

    fn sys_path_snapshot(py: Python<'_>) -> Vec<String> {
        py.import("sys")
            .and_then(|sys| sys.getattr("path"))
            .and_then(|p| p.extract::<Vec<String>>())
            .expect("sys.path snapshot")
    }

    #[test]
    fn python_path_order_and_dedupe() {
        // Insert a sentinel prefix list, build a processor, and check the
        // resulting sys.path front segment: config order, then ".".
        let config = serde_json::json!({
            "function": "transform",
            "script": "def transform(batch):\n    return batch",
            "python_path": ["/arkflow/test/a", "/arkflow/test/b", "/arkflow/test/a"],
        });
        build(config).expect("processor must build");
        Python::attach(|py| {
            let path = sys_path_snapshot(py);
            let a = path
                .iter()
                .position(|p| p == "/arkflow/test/a")
                .expect("a present");
            let b = path
                .iter()
                .position(|p| p == "/arkflow/test/b")
                .expect("b present");
            assert!(
                a < b,
                "config order must be preserved: {:?}",
                &path[..4.min(path.len())]
            );
            // dedupe: the second "/arkflow/test/a" must not appear twice
            assert_eq!(
                path.iter().filter(|p| *p == "/arkflow/test/a").count(),
                1,
                "duplicate paths must be inserted once"
            );
            Ok::<(), pyo3::PyErr>(())
        })
        .expect("attach");
    }

    #[test]
    fn second_instance_does_not_readd_paths() {
        let config = serde_json::json!({
            "function": "transform",
            "script": "def transform(batch):\n    return batch",
            "python_path": ["/arkflow/twice"],
        });
        build(config.clone()).expect("first build");
        build(config).expect("second build");
        Python::attach(|py| {
            let path = sys_path_snapshot(py);
            assert_eq!(
                path.iter()
                    .filter(|p| p.as_str() == "/arkflow/twice")
                    .count(),
                1,
                "repeated builds must not pollute sys.path"
            );
            Ok::<(), pyo3::PyErr>(())
        })
        .expect("attach");
    }

    #[tokio::test]
    async fn short_udf_processes_normally() {
        let processor = build(serde_json::json!({
            "function": "transform",
            "script": "def transform(batch):\n    return [batch]",
            "timeout_ms": 5000,
        }))
        .expect("build");
        let batch = MessageBatch::from_string("hello").unwrap();
        let result = processor.process(Arc::new(batch)).await.expect("process");
        match result {
            ProcessResult::Single(b) => assert_eq!(b.len(), 1),
            other => panic!("expected single result, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn long_udf_times_out() {
        // A sleeping UDF (sleep releases the GIL, so other Python work in
        // the process can proceed) that outlives the configured timeout:
        // the stream must fail fast instead of waiting for the UDF.
        let processor = build(serde_json::json!({
            "function": "slow",
            "script": "import time\ndef slow(batch):\n    time.sleep(5)\n    return batch",
            "timeout_ms": 200,
        }))
        .expect("build");
        let batch = MessageBatch::from_string("hello").unwrap();
        let start = std::time::Instant::now();
        let err = processor
            .process(Arc::new(batch))
            .await
            .expect_err("must time out");
        assert!(
            format!("{err}").contains("timed out"),
            "error must mention timeout, got: {err}"
        );
        assert!(
            start.elapsed() < std::time::Duration::from_secs(2),
            "timeout must fire near the configured bound"
        );
    }
}
