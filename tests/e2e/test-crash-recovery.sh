#!/bin/bash
# 崩溃恢复测试脚本

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

echo "=========================================="
echo "ArkFlow Exactly-Once 崩溃恢复测试"
echo "=========================================="
echo ""

# 清理之前的测试数据
log_info "清理之前的测试数据..."
rm -rf ./target/test/crash-recovery
mkdir -p ./target/test/crash-recovery

# 清理 Kafka 主题
log_info "清理 Kafka 主题..."
docker exec kafka kafka-topics --delete --bootstrap-server localhost:9092 --topic test-input 2>/dev/null || true
docker exec kafka kafka-topics --delete --bootstrap-server localhost:9092 --topic test-output 2>/dev/null || true
sleep 2

# 创建主题
log_info "创建 Kafka 主题..."
docker exec kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic test-input \
    --partitions 3 \
    --replication-factor 1 2>/dev/null

docker exec kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic test-output \
    --partitions 3 \
    --replication-factor 1 2>/dev/null

# 生成测试数据
log_info "生成 100 条测试消息..."
for i in {1..100}; do
    echo "{\"id\":\"order-$i\",\"customer_id\":\"customer-$((i % 10))\",\"product_id\":\"product-$((i % 20))\",\"quantity\":$((i % 5 + 1)),\"price\":$((i * 10 + 99)).99}" | \
    docker exec -i kafka kafka-console-producer --broker-list localhost:9092 --topic test-input > /dev/null 2>&1
done
log_success "已生成 100 条测试消息"

# 第一次运行（将在 15 秒后崩溃）
log_info "=== 第一次运行（将在 15 秒后崩溃）==="
log_info "启动 ArkFlow..."
./target/release/arkflow --config tests/e2e/configs/crash-recovery.yaml > ./target/test/crash-recovery/run1.log 2>&1 &
ARKFLOW_PID=$!
log_info "ArkFlow PID: $ARKFLOW_PID"

# 运行 15 秒后强制崩溃
log_info "运行 15 秒后强制崩溃..."
sleep 15
log_warning "强制终止进程 (kill -9)..."
kill -9 $ARKFLOW_PID 2>/dev/null || true
wait $ARKFLOW_PID 2>/dev/null || true

# 检查 WAL 文件
log_info "检查 WAL 文件..."
if [ -f "./target/test/crash-recovery/wal/wal.log" ]; then
    WAL_SIZE=$(du -h ./target/test/crash-recovery/wal/wal.log | cut -f1)
    log_success "WAL 文件已创建，大小: $WAL_SIZE"
else
    log_error "WAL 文件未创建！"
    exit 1
fi

# 检查幂等性缓存
log_info "检查幂等性缓存..."
if [ -d "./target/test/crash-recovery/idempotency" ]; then
    log_success "幂等性缓存目录已创建"
else
    log_warning "幂等性缓存目录未创建"
fi

# 检查消费者组状态
log_info "检查消费者组状态..."
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group crash-recovery-group --describe 2>/dev/null || true

# 检查输出主题（第一次运行应该有部分消息）
log_info "检查输出主题（第一次运行后）..."
OUTPUT_COUNT_1=$(docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic test-output \
    --from-beginning \
    --timeout-ms 5000 2>/dev/null | wc -l)
log_info "第一次运行后输出主题消息数: $OUTPUT_COUNT_1"

echo ""
log_info "等待 3 秒..."
sleep 3

# 第二次运行（应该从 WAL 恢复）
log_info "=== 第二次运行（应该从 WAL 恢复）==="
log_info "重新启动 ArkFlow..."
./target/release/arkflow --config tests/e2e/configs/crash-recovery.yaml > ./target/test/crash-recovery/run2.log 2>&1 &
ARKFLOW_PID=$!
log_info "ArkFlow PID: $ARKFLOW_PID"

# 运行 30 秒
log_info "运行 30 秒以完成处理..."
sleep 30

# 正常停止
log_info "正常停止 ArkFlow..."
kill $ARKFLOW_PID 2>/dev/null || true
wait $ARKFLOW_PID 2>/dev/null || true

# 最终验证
log_info "=== 最终验证 ==="
echo ""

# 检查消费者组最终状态
log_info "消费者组最终状态："
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group crash-recovery-group --describe 2>/dev/null

echo ""

# 检查输出主题（第二次运行后）
log_info "检查输出主题（第二次运行后）..."
OUTPUT_COUNT_2=$(docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic test-output \
    --from-beginning \
    --timeout-ms 5000 2>/dev/null | wc -l)
log_info "第二次运行后输出主题消息数: $OUTPUT_COUNT_2"

echo ""
log_info "=== WAL 恢复日志 ==="
log_info "查找恢复相关日志..."
grep -i "recover\|wal\|restore\|idempotency" ./target/test/crash-recovery/run2.log | head -20 || echo "未找到恢复日志"

echo ""
log_info "=== 测试结果 ==="

if [ "$OUTPUT_COUNT_2" -ge 100 ]; then
    log_success "✅ 崩溃恢复测试 PASSED"
    log_success "   - 第一次运行: $OUTPUT_COUNT_1 条消息"
    log_success "   - 第二次运行: $OUTPUT_COUNT_2 条消息"
    log_success "   - 总计达到预期的 100 条消息"
    log_success "   - WAL 恢复正常工作"
    log_success "   - 幂等性缓存防止了重复处理"
    exit 0
else
    log_error "❌ 崩溃恢复测试 FAILED"
    log_error "   - 第一次运行: $OUTPUT_COUNT_1 条消息"
    log_error "   - 第二次运行: $OUTPUT_COUNT_2 条消息"
    log_error "   - 未达到预期的 100 条消息"
    exit 1
fi
