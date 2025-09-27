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

use arkflow_core::{
    input::InputConfig, output::OutputConfig, pipeline::PipelineConfig, stream::StreamConfig,
};
use serde_yaml;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 读取YAML配置文件
    let yaml_content = fs::read_to_string("examples/reliable_ack_example.yaml")?;

    // 解析配置
    let config: serde_yaml::Value = serde_yaml::from_str(&yaml_content)?;

    // 创建流配置
    let stream_config: StreamConfig = serde_yaml::from_value(config["streams"][0].clone())?;

    // 构建流 - 自动根据配置选择是否启用可靠确认
    let mut stream = stream_config.build()?;

    println!("✅ 成功创建可靠确认流!");
    println!("📋 配置详情:");
    println!(
        "  - 可靠确认: {}",
        stream_config.reliable_ack.unwrap().enabled
    );
    println!(
        "  - WAL路径: {:?}",
        stream_config.reliable_ack.unwrap().wal_path
    );
    println!(
        "  - 最大重试次数: {:?}",
        stream_config.reliable_ack.unwrap().max_retries
    );
    println!(
        "  - 背压控制: {:?}",
        stream_config.reliable_ack.unwrap().enable_backpressure
    );

    // 运行流处理
    println!("🚀 启动流处理...");

    // 注意：实际使用时需要在异步运行时中运行
    // tokio::runtime::Runtime::new().unwrap().block_on(async {
    //     let cancellation_token = tokio_util::sync::CancellationToken::new();
    //     stream.run(cancellation_token).await.unwrap();
    // });

    Ok(())
}
