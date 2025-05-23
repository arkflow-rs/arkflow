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
use crate::component;
use crate::component::redis::Connection;
use arkflow_core::temporary::{Temporary, TemporaryBuilder};
use arkflow_core::{temporary, Error, MessageBatch, Resource};
use async_trait::async_trait;
use datafusion::arrow::array::AsArray;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::ColumnarValue;
use redis::aio::ConnectionLike;
use redis::{cmd, Pipeline};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RedisTemporaryConfig {
    mode: component::redis::Mode,
    redis_type: RedisType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RedisType {
    List,
    String,
}

struct RedisTemporary {
    config: RedisTemporaryConfig,
    cli: Arc<RwLock<Option<Connection>>>,
}

#[async_trait]
impl Temporary for RedisTemporary {
    async fn connect(&self) -> Result<(), Error> {
        let connection = Connection::connect(&self.config.mode).await?;
        let mut cli_lock = self.cli.write().await;
        cli_lock.replace(connection);
        Ok(())
    }

    async fn get(&self, keys: &[ColumnarValue]) -> Result<Option<MessageBatch>, Error> {
        let cli_lock = self.cli.read().await;
        let Some(cli) = cli_lock.as_ref() else {
            return Err(Error::Disconnection);
        };

        let mut connection = cli.clone();

        assert_eq!(keys.len(), 1, "only have a single key");

        let data: Vec<Vec<String>> = match self.config.redis_type {
            RedisType::List => {
                let key = &keys[0];
                let key = Self::get_key(key);
                let mut pipeline = Pipeline::with_capacity(key.len());
                for x in key {
                    pipeline.lrange(x, 0, -1);
                }

                pipeline
                    .query_async::<Vec<Vec<String>>>(&mut connection)
                    .await
                    .map_err(|e| {
                        Error::Process(format!("Error in getting data from Redis: {}", e))
                    })?
            }
            RedisType::String => {
                let key = &keys[0];
                let mut mget = cmd("mget");
                let key = Self::get_key(key).into_iter().collect::<HashSet<_>>();
                for x in key {
                    mget.arg(x);
                }

                let redis::Value::Array(array) = connection
                    .req_packed_command(&mget)
                    .await
                    .map_err(|_| Error::Process("Error in getting data from Redis".to_string()))?
                else {
                    return Err(Error::Process("unexpected type".to_string()));
                };

                let array = array
                    .into_iter()
                    .map(|v| match v {
                        redis::Value::SimpleString(s) => Ok(s),
                        redis::Value::BulkString(v) => String::from_utf8(v)
                            .map_err(|_| Error::Process("unexpected data".to_string())),
                        _ => Err(Error::Process("unexpected type".to_string())),
                    })
                    .collect::<Result<Vec<_>, Error>>()?;

                vec![array]
            }
        };

        let data = data
            .iter()
            .flatten()
            .map(|s| s.as_bytes())
            .collect::<Vec<_>>();
        let json_data: Vec<u8> = data.join(b"\n" as &[u8]);

        component::json::try_to_arrow(&json_data, None).map(|v| Some(v.into()))
    }

    async fn close(&self) -> Result<(), Error> {
        self.cli.write().await.take();
        Ok(())
    }
}

impl RedisTemporary {
    fn new(config: RedisTemporaryConfig) -> Result<Self, Error> {
        Ok(RedisTemporary {
            config,
            cli: Arc::new(RwLock::new(None)),
        })
    }

    fn get_key(key: &ColumnarValue) -> Vec<&str> {
        let mut vec = Vec::with_capacity(1);
        match key {
            ColumnarValue::Array(array) => {
                for s in array.as_string::<i32>() {
                    vec.push(s.unwrap());
                }
            }
            ColumnarValue::Scalar(s) => match &s {
                ScalarValue::Utf8(str) => {
                    vec.push(str.as_ref().unwrap());
                }
                _ => {}
            },
        }
        vec
    }
}

struct RedisTemporaryBuilder;

impl TemporaryBuilder for RedisTemporaryBuilder {
    fn build(
        &self,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Temporary>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Batch processor configuration is missing".to_string(),
            ));
        }
        let config: RedisTemporaryConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(RedisTemporary::new(config)?))
    }
}

pub fn init() -> Result<(), Error> {
    temporary::register_temporary_builder("redis", Arc::new(RedisTemporaryBuilder))
}
