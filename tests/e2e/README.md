# ArkFlow Exactly-Once 端到端测试

本目录包含 ArkFlow exactly-once 功能的端到端测试。

## 📋 测试场景

### 1. Kafka → Kafka (事务性支持)
- **目标**: 验证端到端的 2PC 协议和 Kafka 事务
- **验证点**:
  - 消息从输入主题正确传输到输出主题
  - 没有消息丢失
  - 没有重复消息（幂等性）
  - 进程崩溃后能够恢复

### 2. Kafka → HTTP (幂等性支持)
- **目标**: 验证 HTTP 输出的幂等性
- **验证点**:
  - 通过 `Idempotency-Key` header 确保幂等性
  - 重复请求不会导致重复处理

### 3. Kafka → PostgreSQL (UPSERT支持)
- **目标**: 验证 SQL 输出的 UPSERT 幂等性
- **验证点**:
  - 使用 `INSERT ... ON CONFLICT` 实现 UPSERT
  - 通过幂等性键确保记录唯一性
  - 没有重复记录

### 4. 进程崩溃恢复
- **目标**: 验证故障恢复机制
- **验证点**:
  - WAL 能够恢复未完成的事务
  - 幂等性缓存能够防止重复处理
  - 系统重启后能够继续处理

## 🚀 快速开始

### 前置要求

- Docker 和 Docker Compose
- Rust 工具链
- Python 3.8+ 和 pip

### 1. 启动测试环境

```bash
# 启动所有依赖服务 (Kafka, PostgreSQL, HTTP服务器)
docker-compose -f docker-compose.test.yml up -d

# 等待服务就绪
docker-compose -f docker-compose.test.yml logs -f
```

### 2. 构建项目

```bash
cargo build --release
```

### 3. 安装 Python 依赖

```bash
cd tests/e2e
pip install -r requirements.txt
```

### 4. 运行测试

#### 方式1: 使用 Bash 脚本 (推荐)

```bash
./run-e2e-tests.sh
```

#### 方式2: 手动运行

```bash
# 终端1: 启动 ArkFlow
cargo run --release -- --config tests/e2e/configs/kafka-to-kafka.yaml

# 终端2: 生成测试数据
cd tests/e2e
python verify_e2e.py

# 终端3: 验证结果
docker exec postgres psql -U arkflow -d arkflow_test -c "SELECT COUNT(*) FROM orders;"
```

## 📊 测试配置

### Kafka → Kafka
- **配置文件**: `configs/kafka-to-kafka.yaml`
- **输入主题**: `test-input`
- **输出主题**: `test-output`
- **事务类型**: 完整 Kafka 事务

### Kafka → HTTP
- **配置文件**: `configs/kafka-to-http.yaml`
- **输入主题**: `test-input`
- **输出**: HTTP endpoint (localhost:8080)
- **幂等性**: `Idempotency-Key` header

### Kafka → PostgreSQL
- **配置文件**: `configs/kafka-to-postgres.yaml`
- **输入主题**: `test-input`
- **输出表**: `orders`
- **幂等性**: `idempotency_key` 列

## 🔍 验证检查清单

### Kafka → Kafka
- [ ] 输出主题消息数量 = 输入主题消息数量
- [ ] 没有重复的消息 ID
- [ ] 消息内容完整且正确
- [ ] 崩溃后能够恢复处理

### Kafka → HTTP
- [ ] HTTP 服务器收到请求
- [ ] 请求包含 `Idempotency-Key` header
- [ ] 重复请求被正确处理

### Kafka → PostgreSQL
- [ ] 订单表记录数 = 输入消息数
- [ ] 所有记录有唯一的幂等性键
- [ ] 没有重复记录
- [ ] UPSERT 正确工作

## 📈 性能指标

运行测试时会收集以下指标：

- **吞吐量**: 消息/秒
- **延迟**: 端到端处理时间
- **事务成功率**: 成功的事务百分比
- **恢复时间**: 崩溃后恢复所需时间

## 🛠️ 故障排除

### Kafka 连接失败
```bash
# 检查 Kafka 是否运行
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# 查看 Kafka 日志
docker logs kafka
```

### PostgreSQL 连接失败
```bash
# 测试连接
docker exec postgres psql -U arkflow -d arkflow_test -c "SELECT 1;"

# 查看 PostgreSQL 日志
docker logs postgres
```

### 清理环境
```bash
# 停止并删除所有容器
docker-compose -f docker-compose.test.yml down -v

# 清理本地测试数据
rm -rf /tmp/arkflow/e2e/
```

## 📝 测试报告

测试完成后，会在 `/tmp/arkflow/e2e/` 目录下生成：

- `output.log`: ArkFlow 运行日志
- `run1.log`, `run2.log`: 崩溃恢复测试日志
- 其他验证结果

## 🔗 相关文档

- [Exactly-Once 语义文档](../../EXACTLY_ONCE.md)
- [开发计划](../../DEVELOPMENT_PLAN.md)
- [P0 状态报告](../../P0_STATUS.md)
