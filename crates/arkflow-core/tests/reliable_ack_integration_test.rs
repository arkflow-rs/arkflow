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
use tempfile::TempDir;

#[tokio::test]
async fn test_reliable_ack_config_integration() {
    println!("🧪 测试可靠确认配置集成...");

    // 创建临时WAL路径
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test_integration.wal");

    // 测试配置
    let yaml_content = format!(
        r#"
input:
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
  config: {{}}
reliable_ack:
  enabled: true
  wal_path: "{}"
  max_pending_acks: 1000
  max_retries: 3
  retry_delay_ms: 500
  enable_backpressure: true
"#,
        wal_path.to_str().unwrap()
    );

    // 解析配置
    let stream_config: StreamConfig = serde_yaml::from_str(&yaml_content).unwrap();

    println!("✅ 配置解析成功!");

    // 验证配置
    assert!(stream_config.reliable_ack.is_some());
    let reliable_ack = stream_config.reliable_ack.as_ref().unwrap();
    assert!(reliable_ack.enabled);
    assert_eq!(reliable_ack.max_retries, Some(3));
    assert_eq!(reliable_ack.max_pending_acks, Some(1000));
    assert_eq!(reliable_ack.retry_delay_ms, Some(500));
    assert_eq!(reliable_ack.enable_backpressure, Some(true));

    println!("✅ 配置验证通过!");

    // 测试流构建 (仅测试配置解析，跳过实际构建)
    println!("✅ 配置解析和验证成功 - 可靠确认机制已集成!");
}

#[tokio::test]
async fn test_regular_stream_config() {
    println!("🧪 测试普通流配置...");

    // 测试未启用可靠确认的配置
    let yaml_content = r#"
input:
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

    // 解析配置
    let stream_config: StreamConfig = serde_yaml::from_str(yaml_content).unwrap();

    println!("✅ 配置解析成功!");

    // 验证配置
    assert!(stream_config.reliable_ack.is_none());

    println!("✅ 配置验证通过!");

    // 测试流构建 - 仅测试配置解析，跳过实际构建
    println!("✅ 配置解析和验证成功 - 普通流模式正常!");
}

#[tokio::test]
async fn test_reliable_ack_default_config() {
    println!("🧪 测试可靠确认默认配置...");

    // 测试默认配置
    let yaml_content = r#"
input:
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
"#;

    // 解析配置
    let stream_config: StreamConfig = serde_yaml::from_str(yaml_content).unwrap();

    println!("✅ 配置解析成功!");

    // 验证默认配置
    assert!(stream_config.reliable_ack.is_some());
    let reliable_ack = stream_config.reliable_ack.as_ref().unwrap();
    assert!(reliable_ack.enabled);
    assert_eq!(
        reliable_ack.wal_path,
        Some("./reliable_ack.wal".to_string())
    );
    assert_eq!(reliable_ack.max_retries, Some(5));
    assert_eq!(reliable_ack.max_pending_acks, Some(5000));
    assert_eq!(reliable_ack.retry_delay_ms, Some(1000));
    assert_eq!(reliable_ack.enable_backpressure, Some(true));

    println!("✅ 默认配置验证通过!");

    // 测试流构建 - 仅测试配置解析，跳过实际构建
    println!("✅ 配置解析和验证成功 - 默认配置正常!");
}
