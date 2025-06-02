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
use datafusion::arrow::pyarrow::ToPyArrow;
use pyo3::ffi::c_str;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use serde_json::Value;
use std::sync::Arc;
use tracing::info;

struct PythonProcessorConfig {}
struct PythonProcessor {}

#[async_trait]
impl Processor for PythonProcessor {
    async fn process(&self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        Python::with_gil(|py| -> Result<(), Error> {
            let py_batch = batch.to_pyarrow(py).map_err(|_| {
                Error::Process("Failed to convert MessageBatch to PyArrow".to_string())
            })?;
            let locals = PyDict::new(py);
            py.run(
                c_str!(
                    r#"
import base64 
s = 'Hello Rust!'
ret = base64.b64encode(s. encode('utf-8'))
"#
                ),
                None,
                Some(&locals),
            )
            .unwrap();
            let ret = locals.get_item("ret").unwrap().unwrap();
            let b64 = ret.downcast::<PyBytes>().unwrap();
            assert_eq!(b64.as_bytes(), b"SGVsbG8gUnVzdCE=");
            info!("{:?}", String::from_utf8_lossy(b64.as_bytes()));
            Ok(())
        })?;
        Ok(vec![])
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

struct PythonProcessorBuilder;
impl ProcessorBuilder for PythonProcessorBuilder {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<Value>,
        resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        Ok(Arc::new(PythonProcessor {}))
    }
}

pub fn init() -> Result<(), Error> {
    processor::register_processor_builder("python", Arc::new(PythonProcessorBuilder))
}
