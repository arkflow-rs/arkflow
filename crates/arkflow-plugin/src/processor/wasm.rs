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

//! Sandbox in-process WebAssembly transformation of every stream row.
//!
//! The guest module must export a linear `memory`, `alloc(len) -> ptr`,
//! `dealloc(ptr, len)` and a transform function (default name `transform`)
//! with the raw ABI `transform(in_ptr: i32, in_len: i32) -> i64`, where the
//! return value packs the output location as `(ptr << 32) | len`. The host
//! hands each row to the guest as JSON bytes and parses the returned bytes
//! back as a JSON row, so guests written in any `wasm32-unknown-unknown`
//! language work without WASI.

use std::sync::Arc;

use arkflow_core::{
    component::{register_processor_metadata, ComponentMetadata},
    processor::{register_processor_builder, Processor, ProcessorBuilder},
    Error, MessageBatch, MessageBatchRef, ProcessResult, Resource,
};
use async_trait::async_trait;
use datafusion::arrow;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use wasmtime::{Config, Engine, Instance, Module, Store, TypedFunc};

const DEFAULT_FUNCTION: &str = "transform";
const DEFAULT_FUEL: u64 = 10_000_000;
/// Upper bound on guest linear memory growth. Guests allocate their own
/// memory at instantiation; this caps how far `memory.grow` may take it.
const MAX_WASM_MEMORY: u64 = 256 * 1024 * 1024;

pub fn init() -> Result<(), Error> {
    register_processor_builder("wasm", Arc::new(WasmProcessorBuilder))?;
    register_processor_metadata(ComponentMetadata::with_schema(
        "wasm",
        "Runs each row through a sandboxed WebAssembly module (wasmtime) using the raw memory ABI.",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "path": {"type": "string", "description": "Path to the WebAssembly module (.wasm binary or .wat text). The module must export memory, alloc(len)->ptr, dealloc(ptr,len) and the transform function."},
                "function": {"type": "string", "description": "Name of the exported transform function. Defaults to 'transform'."},
                "fuel": {"type": "integer", "description": "Per-row fuel budget for wasmtime's metering. Defaults to 10000000."}
            },
            "required": ["path"]
        }),
    )
    .with_example(serde_json::json!({
        "path": "./transforms/filter.wasm",
        "function": "transform"
    })))?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WasmProcessorConfig {
    path: String,
    #[serde(default = "default_function")]
    function: String,
    #[serde(default = "default_fuel")]
    fuel: u64,
}

fn default_function() -> String {
    DEFAULT_FUNCTION.to_string()
}

fn default_fuel() -> u64 {
    DEFAULT_FUEL
}

/// Bounds guest linear memory growth; allocation beyond the cap fails the
/// guest call instead of exhausting host memory.
struct MemoryLimiter;

impl wasmtime::ResourceLimiter for MemoryLimiter {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> Result<bool, wasmtime::Error> {
        Ok(desired as u64 <= MAX_WASM_MEMORY)
    }

    fn table_growing(
        &mut self,
        _current: u32,
        desired: u32,
        _maximum: Option<u32>,
    ) -> Result<bool, wasmtime::Error> {
        Ok(desired <= 10_000)
    }
}

struct WasmProcessor {
    instance: Instance,
    store: std::sync::Mutex<Store<MemoryLimiter>>,
    function: String,
    fuel: u64,
}

impl WasmProcessor {
    fn build(config: &WasmProcessorConfig) -> Result<Self, Error> {
        let bytes = std::fs::read(&config.path).map_err(|e| {
            Error::Config(format!("wasm processor: failed to read module '{}': {e}", config.path))
        })?;
        let mut wasm_config = Config::new();
        wasm_config.consume_fuel(true);
        let engine = Engine::new(&wasm_config)
            .map_err(|e| Error::Config(format!("wasm processor: engine init failed: {e}")))?;
        let module = Module::new(&engine, &bytes).map_err(|e| {
            Error::Config(format!("wasm processor: module '{}' failed to compile: {e}", config.path))
        })?;

        // The limiter is owned by the store's data: the hook requires a
        // 'static return, so it hands out &mut to the data itself.
        let mut store = Store::new(&engine, MemoryLimiter);
        store.limiter(|data: &mut MemoryLimiter| data);
        // V1 contract: guests are self-contained (they define their own
        // memory and import nothing), so instantiation needs no imports.
        let instance = Instance::new(&mut store, &module, &[]).map_err(|e| {
            Error::Config(format!(
                "wasm processor: module '{}' must be self-contained (no imports): {e}",
                config.path
            ))
        })?;

        // Validate the ABI surface at build time so a misconfigured module
        // fails the component startup instead of the first record.
        let required = [
            ("memory", "memory"),
            ("alloc", "func alloc(len: i32) -> ptr: i32"),
            ("dealloc", "func dealloc(ptr: i32, len: i32)"),
            (&config.function, "func transform(in_ptr: i32, in_len: i32) -> i64"),
        ];
        for (name, desc) in required {
            let export = module
                .get_export(name)
                .ok_or_else(|| {
                    Error::Config(format!(
                        "wasm processor: module '{}' does not export '{name}' ({desc})",
                        config.path
                    ))
                })?;
            match name {
                "memory" => {
                    if !matches!(export, wasmtime::ExternType::Memory(_)) {
                        return Err(Error::Config(format!(
                            "wasm processor: export 'memory' in '{}' is not a memory",
                            config.path
                        )));
                    }
                }
                _ => {
                    if !matches!(export, wasmtime::ExternType::Func(_)) {
                        return Err(Error::Config(format!(
                            "wasm processor: export '{name}' in '{}' is not a function",
                            config.path
                        )));
                    }
                }
            }
        }

        Ok(Self {
            instance,
            store: std::sync::Mutex::new(store),
            function: config.function.clone(),
            fuel: config.fuel,
        })
    }
}

#[async_trait]
impl Processor for WasmProcessor {
    async fn process(&self, msg_batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        if msg_batch.num_rows() == 0 {
            return Ok(ProcessResult::None);
        }
        // Serialize the whole batch once: one JSON object per line.
        let mut buf = Vec::new();
        let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
        writer
            .write(&msg_batch)
            .map_err(|e| Error::Process(format!("wasm processor: row serialization failed: {e}")))?;
        writer
            .finish()
            .map_err(|e| Error::Process(format!("wasm processor: row serialization failed: {e}")))?;
        let input_lines: Vec<&[u8]> = buf
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .collect();

        let mut store = self.store.lock().expect("wasm processor store lock");
        let alloc: TypedFunc<i32, i32> = self
            .instance
            .get_typed_func::<i32, i32>(&mut *store, "alloc")
            .map_err(|e| Error::Process(format!("wasm processor: 'alloc' export mismatch: {e}")))?;
        let dealloc: TypedFunc<(i32, i32), ()> = self
            .instance
            .get_typed_func::<(i32, i32), ()>(&mut *store, "dealloc")
            .map_err(|e| Error::Process(format!("wasm processor: 'dealloc' export mismatch: {e}")))?;
        let transform: TypedFunc<(i32, i32), i64> = self
            .instance
            .get_typed_func::<(i32, i32), i64>(&mut *store, &self.function)
            .map_err(|e| Error::Process(format!("wasm processor: '{}' export mismatch: {e}", self.function)))?;
        let memory = self
            .instance
            .get_memory(&mut *store, "memory")
            .ok_or_else(|| Error::Process("wasm processor: 'memory' export missing".to_string()))?;

        let mut output = Vec::with_capacity(buf.len());
        for line in input_lines {
            store
                .set_fuel(self.fuel)
                .map_err(|e| Error::Process(format!("wasm processor: fuel setup failed: {e}")))?;
            let in_ptr = alloc
                .call(&mut *store, line.len() as i32)
                .map_err(|e| Error::Process(format!("wasm processor: guest alloc failed: {e}")))?;
            memory
                .write(&mut *store, in_ptr as usize, line)
                .map_err(|e| Error::Process(format!("wasm processor: input write failed: {e}")))?;
            let packed = transform.call(&mut *store, (in_ptr, line.len() as i32))
                .map_err(|e| Error::Process(format!("wasm processor: guest trap: {e}")))?;
            dealloc
                .call(&mut *store, (in_ptr, line.len() as i32))
                .map_err(|e| Error::Process(format!("wasm processor: guest dealloc failed: {e}")))?;

            let out_ptr = (packed >> 32) as u32 as usize;
            let out_len = (packed & 0xffff_ffff) as usize;
            let mut out = vec![0u8; out_len];
            memory
                .read(&*store, out_ptr, &mut out)
                .map_err(|e| Error::Process(format!("wasm processor: output read failed: {e}")))?;
            dealloc
                .call(&mut *store, (out_ptr as i32, out_len as i32))
                .map_err(|e| Error::Process(format!("wasm processor: guest dealloc failed: {e}")))?;
            output.extend_from_slice(&out);
            output.push(b'\n');
        }
        drop(store);

        let record_batch = crate::component::json::try_to_arrow(&output, None)?;
        Ok(ProcessResult::Single(Arc::new(MessageBatch::new_arrow(
            record_batch,
        ))))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

struct WasmProcessorBuilder;

impl ProcessorBuilder for WasmProcessorBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "wasm processor: missing configuration (path is required)".to_string(),
            ));
        }
        let value = config.as_ref().unwrap();
        let config: WasmProcessorConfig = serde_json::from_value(value.clone())
            .map_err(|e| Error::Config(format!("wasm processor: invalid configuration: {e}")))?;
        let processor = WasmProcessor::build(&config)?;
        Ok(Arc::new(processor))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use serde_json::json;
    use std::cell::RefCell;
    use std::io::Write as _;

    fn test_resource() -> Resource {
        Resource {
            temporary: Default::default(),
            input_names: RefCell::new(Default::default()),
        }
    }

    /// The minimal guest contract: export memory/alloc/dealloc and echo the
    /// input bytes back. Bulk-memory `memory.copy` moves input to a freshly
    /// allocated region and returns the packed pointer/length.
    const ECHO_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (global $next (mut i32) (i32.const 1024))
  (func (export "alloc") (param $len i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $next))
    (global.set $next (i32.add (local.get $ptr)
      (i32.and (i32.add (local.get $len) (i32.const 7)) (i32.const -8))))
    (local.get $ptr))
  (func (export "dealloc") (param $ptr i32) (param $len i32))
  (func (export "transform") (param $in_ptr i32) (param $in_len i32) (result i64)
    (local $ptr i32)
    (local.set $ptr (call 0 (local.get $in_len)))
    (memory.copy (local.get $ptr) (local.get $in_ptr) (local.get $in_len))
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
      (i64.extend_i32_u (local.get $in_len)))))
"#;

    /// Ignores the input and always emits a constant JSON document, proving
    /// the host parses guest output into the resulting batch.
    const CONSTANT_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (data (i32.const 1024) "{\"wasm\":true}")
  (global $next (mut i32) (i32.const 4096))
  (func (export "alloc") (param $len i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $next))
    (global.set $next (i32.add (local.get $ptr)
      (i32.and (i32.add (local.get $len) (i32.const 7)) (i32.const -8))))
    (local.get $ptr))
  (func (export "dealloc") (param $ptr i32) (param $len i32))
  (func (export "transform") (param $in_ptr i32) (param $in_len i32) (result i64)
    (local $ptr i32)
    (local.set $ptr (call 0 (i32.const 13)))
    (memory.copy (local.get $ptr) (i32.const 1024) (i32.const 13))
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
      (i64.const 13))))
"#;

    /// Loops forever: fuel metering must stop it.
    const SPIN_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "alloc") (param $len i32) (result i32) (i32.const 0))
  (func (export "dealloc") (param $ptr i32) (param $len i32))
  (func (export "transform") (param $in_ptr i32) (param $in_len i32) (result i64)
    (loop $forever (br $forever))
    (i64.const 0)))
"#;

    /// Traps immediately with unreachable.
    const TRAP_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "alloc") (param $len i32) (result i32) (i32.const 0))
  (func (export "dealloc") (param $ptr i32) (param $len i32))
  (func (export "transform") (param $in_ptr i32) (param $in_len i32) (result i64)
    (unreachable)))
"#;

    /// Missing the dealloc export: build-time validation must reject it.
    const INCOMPLETE_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "alloc") (param $len i32) (result i32) (i32.const 0))
  (func (export "transform") (param $in_ptr i32) (param $in_len i32) (result i64)
    (i64.const 0)))
"#;

    fn write_module(name: &str, contents: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "arkflow-wasm-{}-{name}",
            std::process::id()
        ));
        let mut file = std::fs::File::create(&path).expect("create module file");
        file.write_all(contents.as_bytes()).expect("write module");
        path
    }

    fn build_processor(path: &std::path::Path, fuel: Option<u64>) -> Result<Arc<dyn Processor>, Error> {
        let mut config = json!({ "path": path.to_string_lossy() });
        if let Some(fuel) = fuel {
            config["fuel"] = json!(fuel);
        }
        let config = Some(config);
        WasmProcessorBuilder.build(None, &config, &test_resource())
    }

    fn three_row_batch() -> MessageBatchRef {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("batch");
        Arc::new(MessageBatch::new_arrow(batch))
    }

    fn batch_lines(batch: &MessageBatchRef) -> Vec<String> {
        let mut buf = Vec::new();
        let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
        writer.write(&**batch).expect("serialize");
        writer.finish().expect("finish");
        String::from_utf8(buf)
            .expect("utf8")
            .lines()
            .map(|l| l.to_string())
            .collect()
    }

    #[tokio::test]
    async fn echo_module_round_trips_every_row() -> Result<(), Error> {
        let path = write_module("echo.wat", ECHO_WAT);
        let processor = build_processor(&path, None)?;
        let input = three_row_batch();
        let input_lines = batch_lines(&input);

        let result = processor.process(input).await?;
        let ProcessResult::Single(output) = result else {
            panic!("expected a single output batch");
        };
        let output_lines = batch_lines(&output);
        assert_eq!(
            output_lines, input_lines,
            "an echo guest must reproduce every row"
        );
        Ok(())
    }

    #[tokio::test]
    async fn constant_module_output_is_parsed_into_the_batch() -> Result<(), Error> {
        let path = write_module("constant.wat", CONSTANT_WAT);
        let processor = build_processor(&path, None)?;
        let result = processor.process(three_row_batch()).await?;
        let ProcessResult::Single(output) = result else {
            panic!("expected a single output batch");
        };
        let lines = batch_lines(&output);
        assert_eq!(lines.len(), 3, "one output row per input row");
        for line in lines {
            assert!(line.contains("\"wasm\":true"), "unexpected row: {line}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn infinite_loop_is_stopped_by_fuel() -> Result<(), Error> {
        let path = write_module("spin.wat", SPIN_WAT);
        let processor = build_processor(&path, Some(1_000))?;
        let error = processor
            .process(three_row_batch())
            .await
            .expect_err("fuel must stop the guest");
        // Fuel exhaustion surfaces as a guest trap: the key property is that
        // the hostile guest terminates instead of hanging the host.
        assert!(
            error.to_string().contains("guest trap"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn guest_trap_fails_fast() -> Result<(), Error> {
        let path = write_module("trap.wat", TRAP_WAT);
        let processor = build_processor(&path, None)?;
        let error = processor
            .process(three_row_batch())
            .await
            .expect_err("trap must fail the batch");
        assert!(
            error.to_string().contains("guest trap"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn missing_export_is_rejected_at_build_time() {
        let path = write_module("incomplete.wat", INCOMPLETE_WAT);
        let error = match build_processor(&path, None) {
            Err(error) => error,
            Ok(_) => panic!("missing dealloc must fail the build"),
        };
        assert!(
            error.to_string().contains("dealloc"),
            "unexpected error: {error}"
        );
    }
}
