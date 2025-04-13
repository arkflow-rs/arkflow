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

//! Input component module
//!
//! The input component is responsible for receiving data from various sources such as message queues, file systems, HTTP endpoints, and so on.

use std::sync::OnceLock;

pub mod generate;
pub mod http;
pub mod kafka;
pub mod memory;
pub mod mqtt;
pub mod sql;

lazy_static::lazy_static! {
    static ref INITIALIZED: OnceLock<()> = OnceLock::new();
}

pub fn init() {
    INITIALIZED.get_or_init(|| {
        generate::init();
        http::init();
        kafka::init();
        memory::init();
        mqtt::init();
        sql::init();
    });
}
