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

//! Temporary component module
//!
//! This module contains the Temporary trait and its associated builder.

use crate::{Error, MessageBatch};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

lazy_static::lazy_static! {
    static ref TEMPORARY_BUILDERS: RwLock<HashMap<String, Arc<dyn TemporaryBuilder>>> = RwLock::new(HashMap::new());
}

#[async_trait]
pub trait Temporary: Send + Sync {
    async fn init(&self) -> Result<(), Error>;
    async fn refresh(&self) -> Result<MessageBatch, Error>;
    async fn close(&self) -> Result<(), Error>;
}

pub trait TemporaryBuilder: Send + Sync {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Temporary>, Error>;
}
