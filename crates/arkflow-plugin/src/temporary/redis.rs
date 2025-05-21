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
use arkflow_core::temporary::Temporary;
use arkflow_core::{Error, MessageBatch};
use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::cluster_async::ClusterConnection;
use std::sync::Arc;
use tokio::sync::Mutex;

struct RedisTemporaryConfig {}

struct RedisTemporary {
    cli: Arc<Mutex<Option<Cli>>>,
}

#[derive(Clone)]
enum Cli {
    Single(ConnectionManager),
    Cluster(ClusterConnection),
}

#[async_trait]
impl Temporary for RedisTemporary {
    async fn connect(&self) -> Result<(), Error> {
        todo!()
    }

    async fn refresh(&self) -> Result<MessageBatch, Error> {
        let Some(cli) = self.cli.lock().await.as_ref() else {
            return Err(Error::Disconnection);
        };

        Ok(MessageBatch::new_binary(vec![])?)
    }

    async fn close(&self) -> Result<(), Error> {
        todo!()
    }
}
