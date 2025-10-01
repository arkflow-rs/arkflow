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

use arkflow_core::stream::StreamConfig;
use serde_yaml;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 测试可靠确认配置集成...");

    // 测试配置解析
    let yaml_content = r#"
logging:
  level: debug
streams:
  - input:
      type: memory
      name: test-input
      config:
        data: "test message"
    pipeline:
      thread_num: 2
      processors: []
    output:
      type: stdout
      name: test-output
      config: {}
    reliable_ack:
      enabled: true
      wal_path: "./test_reliable_ack.wal"
      max_pending_acks: 1000
      max_retries: 3
      retry_delay_ms: 500
      enable_backpressure: true
"#;

    // 解析完整配置
    let config: serde_yaml::Value = serde_yaml::from_str(yaml_content)?;

    // 提取流配置
    let stream_config: StreamConfig = serde_yaml::from_value(config["streams"][0].clone())?;

    println!("✅ 配置解析成功!");
    println!("📋 配置详情:");

    if let Some(reliable_ack) = &stream_config.reliable_ack {
        println!("  🔒 可靠确认: 启用");
        println!("  📁 WAL路径: {:?}", reliable_ack.wal_path);
        println!("  🔄 最大重试: {:?}", reliable_ack.max_retries);
        println!("  📊 最大待处理: {:?}", reliable_ack.max_pending_acks);
        println!("  ⏱️ 重试延迟: {:?}ms", reliable_ack.retry_delay_ms);
        println!("  🚦 背压控制: {:?}", reliable_ack.enable_backpressure);
    } else {
        println!("  ⚠️ 可靠确认: 未启用");
    }

    // 测试流构建
    match stream_config.build() {
        Ok(_) => {
            println!("✅ 流构建成功 - 可靠确认机制已集成!");
        }
        Err(e) => {
            println!("❌ 流构建失败: {}", e);
        }
    }

    // 测试未启用可靠确认的情况
    let yaml_content_no_reliable = r#"
logging:
  level: debug
streams:
  - input:
      type: memory
      name: test-input
      config:
        data: "test message"
    pipeline:
      thread_num: 2
      processors: []
    output:
      type: stdout
      name: test-output
      config: {}
"#;

    let config_no_reliable: serde_yaml::Value = serde_yaml::from_str(yaml_content_no_reliable)?;
    let stream_config_no_reliable: StreamConfig =
        serde_yaml::from_value(config_no_reliable["streams"][0].clone())?;

    println!("\n🧪 测试未启用可靠确认的情况...");

    if let Some(reliable_ack) = &stream_config_no_reliable.reliable_ack {
        println!("  ⚠️ 意外: 可靠确认已启用");
    } else {
        println!("  ✅ 正确: 可靠确认未启用");
    }

    match stream_config_no_reliable.build() {
        Ok(_) => {
            println!("✅ 流构建成功 - 普通流模式正常!");
        }
        Err(e) => {
            println!("❌ 流构建失败: {}", e);
        }
    }

    println!("\n🎉 集成测试完成!");
    Ok(())
}
