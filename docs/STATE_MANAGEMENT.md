# 状态管理功能使用指南

ArkFlow 现在支持通过 YAML 配置文件无缝使用状态管理和事务处理功能。

## 功能特性

- **精确一次处理语义**: 通过检查点和事务保证
- **多种状态后端**: 内存、S3、混合模式
- **自动检查点**: 可配置的检查点间隔
- **故障恢复**: 从最新检查点自动恢复
- **流级别状态管理**: 每个流可以独立配置状态

## 配置示例

### 基本配置

```yaml
# 全局状态管理配置
state_management:
  enabled: true
  backend_type: memory  # memory, s3, hybrid
  checkpoint_interval_ms: 30000  # 30秒
  retained_checkpoints: 5
  exactly_once: true
  state_timeout_ms: 3600000  # 1小时

streams:
  - input:
      type: kafka
      brokers: ["localhost:9092"]
      topics: ["orders"]
    pipeline:
      thread_num: 4
      processors:
        - type: sql
          query: "SELECT user_id, COUNT(*) FROM flow GROUP BY user_id"
    output:
      type: stdout
    # 流级别状态配置
    state:
      operator_id: "order-aggregator"
      enabled: true
      custom_keys:
        - "user_counts"
```

### S3 后端配置

```yaml
state_management:
  enabled: true
  backend_type: s3
  checkpoint_interval_ms: 60000
  exactly_once: true
  
  s3_config:
    bucket: "my-arkflow-state"
    region: "us-east-1"
    prefix: "checkpoints/"
    # 可选：如果不是使用默认 AWS 凭证链
    # access_key_id: "YOUR_ACCESS_KEY"
    # secret_access_key: "YOUR_SECRET_KEY"
    # endpoint_url: "https://s3.amazonaws.com"
```

## 使用方式

### 1. 通过配置文件运行

```bash
# 使用配置文件启动
./target/release/arkflow --config examples/stateful_example.yaml
```

### 2. 编程方式使用

```rust
use arkflow::config::EngineConfig;
use arkflow::engine_builder::EngineBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 加载配置
    let config = EngineConfig::from_file("config.yaml")?;
    
    // 创建引擎构建器
    let mut engine_builder = EngineBuilder::new(config);
    
    // 构建流（会自动集成状态管理）
    let mut streams = engine_builder.build_streams().await?;
    
    // 运行流
    // ... 运行逻辑
    
    Ok(())
}
```

## 状态管理器特性

### 内存后端
- 速度快，适合开发和小规模部署
- 状态存储在内存中，重启后丢失
- 适合无状态或临时状态场景

### S3 后端
- 持久化存储，适合生产环境
- 支持故障恢复
- 自动清理旧检查点

### 混合模式
- 内存缓存 + S3 持久化
- 平衡性能和可靠性
- 定期同步到 S3

## 监控和指标

状态管理器提供以下监控指标：

- 活跃事务数量
- 本地状态数量
- 当前检查点 ID
- 后端类型
- 状态统计信息

## 最佳实践

1. **检查点间隔**: 根据数据量和容错需求调整
   - 高频率：更好的容错，但性能开销大
   - 低频率：性能好，但故障时可能丢失更多数据

2. **状态清理**: 合理设置 `retained_checkpoints`
   - 保留足够的检查点用于恢复
   - 避免无限增长占用存储空间

3. **精确一次 vs 至少一次**
   - 精确一次：需要更多资源，保证数据不重复
   - 至少一次：性能更好，可能产生重复数据

## 故障恢复

当流重启时，状态管理器会：

1. 检查最新的检查点
2. 从 S3（如果配置）加载状态
3. 从检查点位置继续处理

## 示例配置文件

- `examples/stateful_example.yaml` - 基本状态管理示例
- `examples/stateful_s3_example.yaml` - 生产环境 S3 配置示例
- `examples/run_stateful_example.rs` - 编程方式使用示例