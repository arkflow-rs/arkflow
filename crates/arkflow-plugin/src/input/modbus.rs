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
use arkflow_core::input::{Ack, Input};
use arkflow_core::{Error, MessageBatch};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_modbus::prelude::tcp;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ModbusInputConfig {
    addr: String,
}
struct ModbusInput {
    config: ModbusInputConfig,
}

impl ModbusInput {
    fn new(config: ModbusInputConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Input for ModbusInput {
    async fn connect(&self) -> Result<(), Error> {
        let socket_addr = self
            .config
            .addr
            .parse()
            .map_err(|_| Error::from("Failed to parse socket address"))?;

        let mut ctx = tcp::connect(socket_addr).await?;
        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        todo!()
    }

    async fn close(&self) -> Result<(), Error> {
        todo!()
    }
}
