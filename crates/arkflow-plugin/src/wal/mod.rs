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

//! WAL store implementations for arkflow-plugin.
//!
//! Currently ships the `object_store` (S3-compatible) backend, registered
//! under the kind name `"object_store"`. The local `redb` backend lives in
//! `arkflow-core` and is auto-registered.

mod compression;
mod crc;
mod manifest;
mod s3;
mod segment;

pub fn init() -> Result<(), arkflow_core::Error> {
    static INIT: std::sync::OnceLock<Result<(), String>> = std::sync::OnceLock::new();
    match INIT.get_or_init(|| s3::register().map_err(|error| error.to_string())) {
        Ok(()) => Ok(()),
        Err(error) => Err(arkflow_core::Error::Config(error.clone())),
    }
}
