//! Python output component
//!
//! Execute Python code with the processed data

use serde::{Deserialize, Serialize};
use std::sync::Arc;

use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Content, Error, MessageBatch};
use async_trait::async_trait;
use pyo3::{prelude::*, types::{PyDict, PyString}};

/// Python output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonOutputConfig {
    /// Python code to execute
    pub code: String,
    /// Optional Python interpreter path
    pub interpreter_path: Option<String>,
    /// Optional environment variables
    pub env_vars: Option<std::collections::HashMap<String, String>>,
}

/// Python output component
pub struct PythonOutput {
    config: PythonOutputConfig,
}

impl PythonOutput {
    /// Create a new Python output component
    pub fn new(config: PythonOutputConfig) -> Result<Self, Error> {
        Ok(Self { config })
    }
}

#[async_trait]
impl Output for PythonOutput {
    async fn connect(&self) -> Result<(), Error> {
        // Initialize Python interpreter and validate code
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            // Compile the Python code to check for syntax errors
            py.run(&self.config.code, None, None)
                .map_err(|e| Error::Config(format!("Invalid Python code: {}", e)))
        })?;
        Ok(())
    }

    async fn write(&self, msg: &MessageBatch) -> Result<(), Error> {
        Python::with_gil(|py| {
            // Create a Python dictionary to hold the input data
            let locals = PyDict::new(py);
            
            // Convert MessageBatch to Python object
            let messages: Vec<&Content> = msg.messages().collect();
            let py_messages: Vec<&PyAny> = messages
                .iter()
                .map(|m| {
                    let data = serde_json::to_string(m)
                        .map_err(|e| Error::Processing(format!("Failed to serialize message: {}", e)))?;
                    Ok(PyString::new(py, &data))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            
            // Add messages to locals dictionary
            locals.set_item("messages", py_messages)
                .map_err(|e| Error::Processing(format!("Failed to set Python variable: {}", e)))?;
            
            // Execute the Python code with the input data
            py.run(&self.config.code, None, Some(locals))
                .map_err(|e| Error::Processing(format!("Failed to execute Python code: {}", e)))
        })?;
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        // Release the GIL and clean up Python resources
        Python::with_gil(|_py| {
            // The Python interpreter will be finalized when the last GIL guard is dropped
        });
        Ok(())
    }
}

pub(crate) struct PythonOutputBuilder;
impl OutputBuilder for PythonOutputBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Python output configuration is missing".to_string(),
            ));
        }
        let config: PythonOutputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(PythonOutput::new(config)?))
    }
}

pub fn init() {
    register_output_builder("python", Arc::new(PythonOutputBuilder));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_python_output_new() {
        let config = PythonOutputConfig {
            code: String::from("print('Hello, World!')"),
            interpreter_path: None,
            env_vars: None,
        };

        let output = PythonOutput::new(config);
        assert!(output.is_ok());
    }
}