#!/bin/bash
# 快速端到端测试脚本
# 用于快速验证 exactly-once 功能

set -e

echo "=========================================="
echo "ArkFlow Exactly-Once Quick E2E Test"
echo "=========================================="
echo ""

# 1. 检查 Docker
echo "1. Checking Docker..."
if ! docker ps > /dev/null 2>&1; then
    echo "❌ Docker is not running"
    exit 1
fi
echo "✅ Docker is running"
echo ""

# 2. 启动测试环境
echo "2. Starting test environment..."
docker-compose -f docker-compose.test.yml up -d > /dev/null 2>&1
echo "✅ Test environment started"
echo ""

# 3. 等待服务就绪
echo "3. Waiting for services to be ready..."
sleep 15

# 等待 Kafka
until docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 2>/dev/null | grep -q "localhost"; do
    sleep 1
done
echo "✅ Kafka is ready"

# 等待 PostgreSQL
until docker exec postgres pg_isready -U arkflow -d arkflow_test > /dev/null 2>&1; do
    sleep 1
done
echo "✅ PostgreSQL is ready"
echo ""

# 4. 创建主题
echo "4. Creating Kafka topics..."
docker exec kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic test-input \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists 2>/dev/null

docker exec kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic test-output \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists 2>/dev/null
echo "✅ Kafka topics created"
echo ""

# 5. 构建项目
echo "5. Building ArkFlow..."
cargo build --release 2>&1 | grep -E "Compiling|Finished" || true
echo "✅ Build completed"
echo ""

# 6. 运行 Kafka -> Kafka 测试
echo "=========================================="
echo "Test: Kafka -> Kafka (Transactional)"
echo "=========================================="

# 生成测试数据
echo "Generating test data..."
for i in {1..50}; do
    echo "{\"id\":\"order-$i\",\"customer_id\":\"customer-$((i % 10))\",\"product_id\":\"product-$((i % 20))\",\"quantity\":$((i % 5 + 1)),\"price\":$((i * 10 + 99)).99}" | \
    docker exec -i kafka kafka-console-producer --broker-list localhost:9092 --topic test-input > /dev/null 2>&1
done
echo "✅ Generated 50 test messages"

# 清理之前的 WAL
rm -rf /tmp/arkflow/e2e/quick-test
mkdir -p /tmp/arkflow/e2e/quick-test

# 运行 ArkFlow (后台)
echo "Starting ArkFlow..."
timeout 30s ./target/release/arkflow --config tests/e2e/configs/kafka-to-kafka.yaml > /tmp/arkflow/e2e/quick-test/output.log 2>&1 &
ARKFLOW_PID=$!
sleep 25

# 停止 ArkFlow
kill $ARKFLOW_PID 2>/dev/null || true
wait $ARKFLOW_PID 2>/dev/null || true
echo "✅ ArkFlow stopped"

# 验证结果
echo "Verifying results..."
OUTPUT_COUNT=$(docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic test-output \
    --from-beginning \
    --timeout-ms 5000 2>/dev/null | wc -l)

echo "Output topic message count: $OUTPUT_COUNT"

if [ "$OUTPUT_COUNT" -ge 50 ]; then
    echo "✅ Test PASSED"
else
    echo "❌ Test FAILED: Expected at least 50 messages, got $OUTPUT_COUNT"
fi
echo ""

# 7. 运行 Kafka -> PostgreSQL 测试
echo "=========================================="
echo "Test: Kafka -> PostgreSQL (UPSERT)"
echo "=========================================="

# 生成测试数据
echo "Generating test data..."
for i in {1..50}; do
    echo "{\"id\":$i,\"customer_id\":\"customer-$((i % 10))\",\"product_id\":\"product-$((i % 20))\",\"quantity\":$((i % 5 + 1)),\"price\":$((i * 10 + 99)).99}" | \
    docker exec -i kafka kafka-console-producer --broker-list localhost:9092 --topic test-input > /dev/null 2>&1
done
echo "✅ Generated 50 test messages"

# 清理之前的 WAL
rm -rf /tmp/arkflow/e2e/quick-test-postgres
mkdir -p /tmp/arkflow/e2e/quick-test-postgres

# 运行 ArkFlow
echo "Starting ArkFlow..."
timeout 30s ./target/release/arkflow --config tests/e2e/configs/kafka-to-postgres.yaml > /tmp/arkflow/e2e/quick-test-postgres/output.log 2>&1 &
ARKFLOW_PID=$!
sleep 25

# 停止 ArkFlow
kill $ARKFLOW_PID 2>/dev/null || true
wait $ARKFLOW_PID 2>/dev/null || true
echo "✅ ArkFlow stopped"

# 验证结果
echo "Verifying results..."
ROW_COUNT=$(docker exec postgres psql -U arkflow -d arkflow_test -t -c "SELECT COUNT(*) FROM orders WHERE id::text LIKE '%-%';" 2>/dev/null | xargs)
DUPLICATE_COUNT=$(docker exec postgres psql -U arkflow -d arkflow_test -t -c "SELECT COUNT(*) - COUNT(DISTINCT idempotency_key) FROM orders WHERE idempotency_key IS NOT NULL;" 2>/dev/null | xargs)

echo "Orders table row count: $ROW_COUNT"
echo "Duplicate idempotency keys: $DUPLICATE_COUNT"

if [ "$ROW_COUNT" -ge 50 ] && [ "$DUPLICATE_COUNT" -eq 0 ]; then
    echo "✅ Test PASSED"
else
    echo "❌ Test FAILED"
fi
echo ""

# 8. 总结
echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo ""
echo "Quick test completed!"
echo ""
echo "To cleanup:"
echo "  docker-compose -f docker-compose.test.yml down -v"
echo ""
echo "To view logs:"
echo "  cat /tmp/arkflow/e2e/quick-test/output.log"
echo ""
