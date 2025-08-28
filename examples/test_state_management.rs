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

//! 测试状态管理功能的示例

use arkflow_core::config::{
    EngineConfig, LoggingConfig, StateBackendType, StateManagementConfig, StreamStateConfig,
};
use arkflow_core::engine_builder::EngineBuilder;
use arkflow_core::input::InputConfig;
use arkflow_core::output::OutputConfig;
use arkflow_core::pipeline::PipelineConfig;
use arkflow_core::stream::StreamConfig;
use arkflow_core::Error;
use tokio_util::sync::CancellationToken;
use tracing::{info, Level};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // 初始化日志
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("开始测试状态管理功能");

    // 创建引擎配置
    let config = EngineConfig {
        streams: vec![StreamConfig {
            input: InputConfig {
                r#type: "file".to_string(),
                path: Some("./examples/data/test.txt".to_string()),
                format: Some("text".to_string()),
                ..Default::default()
            },
            pipeline: PipelineConfig {
                thread_num: 2,
                processors: vec![],
            },
            output: OutputConfig {
                r#type: "stdout".to_string(),
                ..Default::default()
            },
            error_output: None,
            buffer: None,
            temporary: None,
            state: Some(StreamStateConfig {
                operator_id: "test-operator".to_string(),
                enabled: true,
                state_timeout_ms: Some(60000),
                custom_keys: Some(vec!["message_count".to_string()]),
            }),
        }],
        logging: LoggingConfig {
            level: "info".to_string(),
            file_path: None,
            format: arkflow_core::config::LogFormat::PLAIN,
        },
        health_check: Default::default(),
        state_management: StateManagementConfig {
            enabled: true,
            backend_type: StateBackendType::Memory,
            s3_config: None,
            checkpoint_interval_ms: 10000, // 10秒
            retained_checkpoints: 3,
            exactly_once: true,
            state_timeout_ms: 3600000, // 1小时
        },
    };

    // 创建引擎构建器
    let mut engine_builder = EngineBuilder::new(config);

    // 构建流
    info!("构建带有状态管理的流...");
    let mut streams = engine_builder.build_streams().await?;

    // 获取状态管理器
    let state_managers = engine_builder.get_state_managers();
    info!("创建了 {} 个状态管理器", state_managers.len());

    // 创建取消令牌
    let cancellation_token = CancellationToken::new();

    // 启动流
    let mut handles = Vec::new();
    for (i, mut stream) in streams.into_iter().enumerate() {
        let token = cancellation_token.clone();

        let handle = tokio::spawn(async move {
            info!("启动流 {}", i);
            if let Err(e) = stream.run(token).await {
                eprintln!("流 {} 失败: {}", i, e);
            }
            info!("流 {} 已停止", i);
        });

        handles.push(handle);
    }

    // 监控任务
    let monitor_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));

        for _ in 0..10 {
            // 运行50秒
            interval.tick().await;

            // 打印状态统计
            for (operator_id, state_manager) in &state_managers {
                let stats = {
                    let manager = state_manager.read().await;
                    manager.get_state_stats().await
                };

                info!(
                    "操作符 '{}' - 活跃事务: {}, 本地状态: {}, 检查点ID: {}",
                    operator_id,
                    stats.active_transactions,
                    stats.local_states_count,
                    stats.current_checkpoint_id
                );

                // 测试状态存取
                let mut manager = state_manager.write().await;
                let count: Option<u64> = manager
                    .get_state_value(operator_id, &"message_count")
                    .await?;
                info!("消息计数: {:?}", count);

                // 更新计数
                let new_count = count.unwrap_or(0) + 1;
                manager
                    .set_state_value(operator_id, &"message_count", new_count)
                    .await?;
            }
        }
    });

    // 等待一段时间
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

    // 取消所有流
    info!("发送停止信号...");
    cancellation_token.cancel();

    // 等待所有流完成
    for handle in handles {
        handle.await?;
    }

    // 停止监控
    monitor_handle.abort();

    // 关闭状态管理器
    info!("关闭状态管理器...");
    engine_builder.shutdown().await?;

    info!("测试完成");

    Ok(())
}
