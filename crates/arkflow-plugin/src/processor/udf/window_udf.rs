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
use datafusion::logical_expr::WindowUDF;
use std::sync::{Arc, RwLock};

lazy_static::lazy_static! {
    pub(crate) static ref UDFS: RwLock<Vec<Arc<WindowUDF>>> = RwLock::new(Vec::new());
}

pub fn register(udf: Arc<WindowUDF>) {
    let mut udfs = UDFS.write().expect("Failed to acquire write lock for UDFS");
    udfs.push(udf);
}
