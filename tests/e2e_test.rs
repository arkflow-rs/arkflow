// 端到端集成测试
// 用于验证 exactly-once 功能的端到端行为

use std::time::Duration;
use tokio::time::sleep;

#[cfg(test)]
mod e2e_tests {
    use super::*;

    // 注意：这些测试需要 Docker 环境运行
    // 运行命令: cargo test --test e2e_test -- --ignored

    #[tokio::test]
    #[ignore] // 需要手动运行：cargo test --test e2e_test -- --ignored
    async fn test_kafka_to_kafka_transactional() {
        // 测试 Kafka 到 Kafka 的事务性传输
        // 1. 启动 Kafka
        // 2. 创建输入和输出主题
        // 3. 生成测试数据
        // 4. 运行 ArkFlow
        // 5. 验证输出主题的消息数量和内容
        // 6. 验证没有重复消息

        // TODO: 实现 Kafka 集成测试
        // 需要：
        // - Kafka 测试容器
        // - 生成测试数据
        // - 启动 ArkFlow 进程
        // - 验证结果

        println!("Test: Kafka -> Kafka (transactional)");
        println!("Status: SKIPPED (requires Docker environment)");
    }

    #[tokio::test]
    #[ignore]
    async fn test_kafka_to_postgres_upsert() {
        // 测试 Kafka 到 PostgreSQL 的 UPSERT 幂等性
        // 1. 启动 Kafka 和 PostgreSQL
        // 2. 创建测试表
        // 3. 生成测试数据
        // 4. 运行 ArkFlow
        // 5. 验证数据库中的记录
        // 6. 验证没有重复记录（通过 idempotency_key）

        println!("Test: Kafka -> PostgreSQL (UPSERT)");
        println!("Status: SKIPPED (requires Docker environment)");
    }

    #[tokio::test]
    #[ignore]
    async fn test_crash_recovery() {
        // 测试进程崩溃恢复
        // 1. 启动 Kafka 和 ArkFlow
        // 2. 生成测试数据
        // 3. 强制崩溃 ArkFlow 进程
        // 4. 重启 ArkFlow
        // 5. 验证 WAL 恢复
        // 6. 验证幂等性缓存防止重复处理
        // 7. 验证所有消息都被正确处理

        println!("Test: Crash recovery");
        println!("Status: SKIPPED (requires Docker environment)");
    }

    #[tokio::test]
    #[ignore]
    async fn test_duplicate_detection() {
        // 测试重复消息检测
        // 1. 启动 Kafka 和 ArkFlow
        // 2. 生成测试数据并记录幂等性键
        // 3. 再次发送相同幂等性键的消息
        // 4. 验证重复消息被检测并跳过
        // 5. 验证最终一致性

        println!("Test: Duplicate detection");
        println!("Status: SKIPPED (requires Docker environment)");
    }

    #[tokio::test]
    #[ignore]
    async fn test_wal_persistence() {
        // 测试 WAL 持久化
        // 1. 启动 ArkFlow 并处理一些消息
        // 2. 验证 WAL 文件被创建
        // 3. 验证 WAL 内容正确
        // 4. 模拟崩溃
        // 5. 从 WAL 恢复
        // 6. 验证状态完全恢复

        println!("Test: WAL persistence");
        println!("Status: SKIPPED (requires Docker environment)");
    }

    #[tokio::test]
    #[ignore]
    async fn test_idempotency_cache_persistence() {
        // 测试幂等性缓存持久化
        // 1. 处理一些消息并记录到幂等性缓存
        // 2. 验证缓存文件被创建
        // 3. 重启 ArkFlow
        // 4. 验证缓存从磁盘恢复
        // 5. 发送重复消息
        // 6. 验证重复被正确检测

        println!("Test: Idempotency cache persistence");
        println!("Status: SKIPPED (requires Docker environment)");
    }
}

// 辅助函数

/// 等待服务就绪
async fn wait_for_service_ready(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..30 {
        match reqwest::get(url).await {
            Ok(response) if response.status().is_success() => return Ok(()),
            _ => sleep(Duration::from_secs(1)).await,
        }
    }
    Err("Service not ready".into())
}

/// 生成测试消息
fn generate_test_messages(count: usize) -> Vec<String> {
    (1..=count)
        .map(|i| {
            format!(
                r#"{{"id":"order-{}","customer_id":"customer-{}","product_id":"product-{}","quantity":{},"price":{}}}"#,
                i,
                i % 10,
                i % 20,
                i % 5 + 1,
                i * 10 + 99
            )
        })
        .collect()
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_transaction_coordinator_creation() {
        // 测试事务协调器的创建和初始化
        // 这个测试不需要外部依赖

        use arkflow_core::transaction::{TransactionCoordinator, TransactionCoordinatorConfig};
        use std::sync::Arc;

        let config = TransactionCoordinatorConfig {
            timeout: Duration::from_secs(30),
            ..Default::default()
        };

        // 创建临时目录
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_path = temp_dir.path().join("wal");
        let idempotency_path = temp_dir.path().join("idempotency");

        let coordinator = TransactionCoordinator::new(
            config,
            wal_path.to_str().unwrap(),
            idempotency_path.to_str().unwrap(),
        )
        .await;

        assert!(coordinator.is_ok());

        let coordinator = Arc::new(coordinator.unwrap());
        assert_eq!(coordinator.get_active_count().await, 0);

        // 清理
        drop(coordinator);
        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_config_loading() {
        // 测试配置文件加载
        use arkflow_core::config::EngineConfig;

        let config_content = r#"
logging:
  level: debug

exactly_once:
  enabled: true
  transaction_coordinator:
    timeout: 30s
  wal:
    path: "/tmp/test/wal"
    max_size: 10485760
  idempotency:
    capacity: 10000
    ttl: 3600s

streams:
  - name: "test-stream"
    input:
      type: "generate"
    pipeline:
      thread_num: 2
    output:
      type: "drop"
"#;

        let result: Result<EngineConfig, _> = serde_yaml::from_str(config_content);
        assert!(result.is_ok());

        let config = result.unwrap();
        assert!(config.exactly_once.enabled);
        assert_eq!(config.exactly_once.transaction_coordinator.timeout.as_secs(), 30);
    }
}
