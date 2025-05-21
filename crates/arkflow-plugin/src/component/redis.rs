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
use arkflow_core::Error;
use redis::aio::{ConnectionLike, ConnectionManager};
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::{Client, Cmd, Pipeline, RedisFuture, Value};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Mode {
    Cluster { urls: Vec<String> },
    Single { url: String },
}

#[derive(Clone)]
pub(crate) enum Connection {
    Single(ConnectionManager),
    Cluster(ClusterConnection),
}

impl ConnectionLike for Connection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        match self {
            Connection::Single(c) => c.req_packed_command(cmd),
            Connection::Cluster(c) => c.req_packed_command(cmd),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        match self {
            Connection::Single(c) => c.req_packed_commands(cmd, offset, count),
            Connection::Cluster(c) => c.req_packed_commands(cmd, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Connection::Single(c) => c.get_db(),
            Connection::Cluster(c) => c.get_db(),
        }
    }
}

impl Connection {
    pub async fn connect(mode: &Mode) -> Result<Connection, Error> {
        match mode {
            Mode::Single { url } => Self::single_connect(url.clone()).await,
            Mode::Cluster { urls } => Self::cluster_connect(urls.clone()).await,
        }
    }

    async fn single_connect(url: String) -> Result<Connection, Error> {
        let client = Client::open(url)
            .map_err(|e| Error::Connection(format!("Failed to connect to Redis server: {}", e)))?;
        let client = client
            .get_connection_manager()
            .await
            .map_err(|e| Error::Connection(format!("Failed to get connection manager: {}", e)))?;

        Ok(Connection::Single(client))
    }

    async fn cluster_connect(urls: Vec<String>) -> Result<Connection, Error> {
        let client = ClusterClient::new(urls)
            .map_err(|e| Error::Connection(format!("Failed to connect to Redis cluster: {}", e)))?;
        let client = client.get_async_connection().await.map_err(|e| {
            Error::Connection(format!(
                "Failed to get connection from Redis cluster: {}",
                e
            ))
        })?;

        Ok(Connection::Cluster(client))
    }
}
