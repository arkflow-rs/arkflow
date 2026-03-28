# ArkFlow Exactly-Once 端到端测试指南

## 📋 测试框架已完成

端到端测试框架已完全构建完成，包括：

### ✅ 已创建的组件

1. **Docker 环境**
   - `docker-compose.test.yml` - 包含 Kafka、PostgreSQL、HTTP Server、Redis

2. **测试配置**
   - `tests/e2e/configs/kafka-to-kafka.yaml` - Kafka 事务测试
   - `tests/e2e/configs/kafka-to-http.yaml` - HTTP 幂等性测试
   - `tests/e2e/configs/kafka-to-postgres.yaml` - PostgreSQL UPSERT 测试

3. **测试脚本**
   - `tests/e2e/quick-test.sh` - 快速测试（推荐）
   - `tests/e2e/run-e2e-tests.sh` - 完整测试套件
   - `tests/e2e/verify_e2e.py` - Python 验证脚本
   - `tests/e2e/generate_data.py` - 测试数据生成器

4. **数据库初始化**
   - `scripts/init-postgres.sql` - PostgreSQL 表结构和测试数据

5. **集成测试**
   - `tests/e2e_test.rs` - Rust 集成测试

## 🚀 运行测试的步骤

### 前置要求
- Docker Desktop 已安装并运行
- Rust 工具链 (1.88+)
- Python 3.8+ (可选，用于 Python 验证脚本)

### 方式 1: 快速测试（推荐用于开发）

```bash
# 1. 启动 Docker Desktop
# 确保 Docker Desktop 应用程序正在运行

# 2. 启动测试环境
docker-compose -f docker-compose.test.yml up -d

# 3. 等待服务就绪（约 15 秒）
sleep 15

# 4. 构建项目
cargo build --release

# 5. 运行快速测试
./tests/e2e/quick-test.sh

# 6. 清理（可选）
docker-compose -f docker-compose.test.yml down -v
```

### 方式 2: 完整测试套件

```bash
# 1. 启动测试环境
docker-compose -f docker-compose.test.yml up -d

# 2. 等待服务就绪
sleep 20

# 3. 构建项目
cargo build --release

# 4. 安装 Python 依赖（可选）
cd tests/e2e
pip install -r requirements.txt
cd ../..

# 5. 运行完整测试
./tests/e2e/run-e2e-tests.sh

# 6. 清理
docker-compose -f docker-compose.test.yml down -v
```

### 方式 3: 手动测试各个场景

#### 测试 Kafka → Kafka

```bash
# 终端 1: 启动 ArkFlow
cargo run --release -- --config tests/e2e/configs/kafka-to-kafka.yaml

# 终端 2: 生成测试数据
./tests/e2e/generate_data.py --type order --count 100 --topic test-input

# 终端 3: 验证输出
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test-output \
  --from-beginning \
  --timeout-ms 10000 | wc -l
```

#### 测试 Kafka → PostgreSQL

```bash
# 终端 1: 启动 ArkFlow
cargo run --release -- --config tests/e2e/configs/kafka-to-postgres.yaml

# 终端 2: 生成测试数据
./tests/e2e/generate_data.py --type order --count 100 --topic test-input

# 终端 3: 验证数据库
docker exec postgres psql -U arkflow -d arkflow_test -c "
  SELECT COUNT(*) as total_records,
         COUNT(DISTINCT idempotency_key) as unique_keys
  FROM orders;
"
```

## 🔍 验证测试结果

### Kafka → Kafka 验证
```bash
# 检查输出主题消息数
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test-output \
  --from-beginning \
  --timeout-ms 5000 | wc -l

# 预期结果: >= 输入消息数
```

### Kafka → PostgreSQL 验证
```bash
# 检查订单表
docker exec postgres psql -U arkflow -d arkflow_test -c "
  SELECT
    COUNT(*) as total_orders,
    COUNT(DISTINCT idempotency_key) as unique_keys,
    COUNT(*) - COUNT(DISTINCT idempotency_key) as duplicates
  FROM orders
  WHERE id LIKE 'order-%';
"

# 预期结果: total_orders = unique_keys, duplicates = 0
```

### 崩溃恢复验证
```bash
# 查看日志
cat /tmp/arkflow/e2e/*/output.log

# 检查 WAL 文件
ls -lh /tmp/arkflow/e2e/*/wal/

# 检查幂等性缓存
ls -lh /tmp/arkflow/e2e/*/idempotency/
```

## 📊 测试场景说明

### 场景 1: Kafka → Kafka (事务性)
**目的**: 验证端到端的 2PC 协议和 Kafka 事务

**测试内容**:
- 消息从 Kafka 输入主题消费
- 通过 ArkFlow 处理（支持事务协调器）
- 写入 Kafka 输出主题（使用事务）
- 验证没有消息丢失或重复

**预期结果**:
- 输出主题消息数 = 输入主题消息数
- 所有消息具有唯一的 ID
- 没有重复消息

### 场景 2: Kafka → HTTP (幂等性)
**目的**: 验证 HTTP 输出的幂等性

**测试内容**:
- 消息从 Kafka 消费
- 发送到 HTTP endpoint（带 Idempotency-Key header）
- HTTP 服务器记录所有请求
- 验证重复请求被正确处理

**预期结果**:
- HTTP 服务器收到请求
- 请求包含 Idempotency-Key header
- 重复请求被正确识别

### 场景 3: Kafka → PostgreSQL (UPSERT)
**目的**: 验证 SQL UPSERT 的幂等性

**测试内容**:
- 消息从 Kafka 消费
- 使用 INSERT ... ON CONFLICT 写入 PostgreSQL
- 通过 idempotency_key 列确保幂等性
- 验证数据库中没有重复记录

**预期结果**:
- 订单表记录数 = 输入消息数
- 所有 idempotency_key 唯一
- 没有重复记录

### 场景 4: 进程崩溃恢复
**目的**: 验证故障恢复机制

**测试内容**:
- 启动 ArkFlow 并处理部分消息
- 强制崩溃进程
- 重启 ArkFlow
- 验证 WAL 恢复和幂等性缓存

**预期结果**:
- WAL 成功恢复未完成的事务
- 幂等性缓存防止重复处理
- 所有消息最终被正确处理

## 🛠️ 故障排除

### Docker 相关问题

**Docker daemon 未运行**
```bash
# macOS: 启动 Docker Desktop
open -a Docker

# 等待 Docker 就绪
docker ps
```

**端口冲突**
```bash
# 检查端口占用
lsof -i :9092  # Kafka
lsof -i :5432  # PostgreSQL
lsof -i :8080  # HTTP Server

# 如果端口被占用，可以修改 docker-compose.test.yml 中的端口映射
```

### Kafka 相关问题

**Kafka 未就绪**
```bash
# 检查 Kafka 日志
docker logs kafka

# 测试 Kafka 连接
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

**主题未创建**
```bash
# 手动创建主题
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic test-input \
  --partitions 3 \
  --replication-factor 1
```

### PostgreSQL 相关问题

**PostgreSQL 未就绪**
```bash
# 检查 PostgreSQL 日志
docker logs postgres

# 测试连接
docker exec postgres psql -U arkflow -d arkflow_test -c "SELECT 1;"
```

### ArkFlow 相关问题

**配置错误**
```bash
# 验证配置文件
cat tests/e2e/configs/kafka-to-kafka.yaml

# 测试配置解析
./target/release/arkflow --config tests/e2e/configs/kafka-to-kafka.yaml --validate
```

**权限错误**
```bash
# 确保 WAL 目录可写
sudo mkdir -p /tmp/arkflow/e2e
sudo chmod 777 /tmp/arkflow/e2e
```

## 📈 性能基准测试

要测试性能，可以调整测试参数：

```bash
# 生成更多测试数据
./tests/e2e/generate_data.py --count 10000 --topic test-input

# 调整批处理大小
# 修改配置文件中的 batch_size 参数

# 监控吞吐量
docker stats kafka postgres

# 查看指标
curl http://localhost:9091/metrics
```

## 📝 测试报告

测试完成后，结果保存在：

- `/tmp/arkflow/e2e/*/output.log` - ArkFlow 运行日志
- `/tmp/arkflow/e2e/*/wal/` - WAL 文件
- `/tmp/arkflow/e2e/*/idempotency/` - 幂等性缓存

## 🎯 成功标准

测试通过的标准：

1. ✅ 所有服务正常启动
2. ✅ 测试数据成功生成
3. ✅ ArkFlow 成功处理消息
4. ✅ 输出验证通过（无重复、无丢失）
5. ✅ 崩溃恢复成功
6. ✅ WAL 和幂等性缓存正确工作

## 📚 相关文档

- [端到端测试 README](tests/e2e/README.md)
- [测试总结](tests/e2e/TEST_SUMMARY.md)
- [Exactly-Once 语义](EXACTLY_ONCE.md)
- [开发计划](DEVELOPMENT_PLAN.md)

## 🤝 贡献

如果发现测试问题或有改进建议，请：

1. 检查日志文件
2. 记录错误信息
3. 提交 Issue 或 PR

---

**下一步**: 启动 Docker Desktop 并运行 `./tests/e2e/quick-test.sh` 开始测试！
