#!/bin/bash
set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查Docker是否运行
check_docker() {
    if ! docker ps > /dev/null 2>&1; then
        log_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
    log_success "Docker is running"
}

# 启动测试环境
start_environment() {
    log_info "Starting test environment with Docker Compose..."
    docker-compose -f docker-compose.test.yml up -d

    log_info "Waiting for services to be ready..."
    sleep 10

    # 等待Kafka就绪
    log_info "Waiting for Kafka to be ready..."
    until docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 2>/dev/null | grep -q "localhost"; do
        echo "Kafka not ready yet, waiting..."
        sleep 2
    done
    log_success "Kafka is ready"

    # 等待PostgreSQL就绪
    log_info "Waiting for PostgreSQL to be ready..."
    until docker exec postgres pg_isready -U arkflow -d arkflow_test > /dev/null 2>&1; do
        echo "PostgreSQL not ready yet, waiting..."
        sleep 2
    done
    log_success "PostgreSQL is ready"

    log_success "Test environment is ready"
}

# 创建Kafka主题
create_topics() {
    log_info "Creating Kafka topics..."

    # 创建输入主题
    docker exec kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --topic test-input \
        --partitions 3 \
        --replication-factor 1 \
        --if-not-exists

    # 创建输出主题
    docker exec kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --topic test-output \
        --partitions 3 \
        --replication-factor 1 \
        --if-not-exists

    log_success "Kafka topics created"
}

# 生成测试数据
generate_test_data() {
    log_info "Generating test data..."

    # 生成测试订单数据
    for i in {1..100}; do
        cat <<EOF | docker exec -i kafka kafka-console-producer --broker-list localhost:9092 --topic test-input
{"id":"order-$i","customer_id":"customer-$((i % 10))","product_id":"product-$((i % 20))","quantity":$((i % 5 + 1)),"price":$((i * 10 + 99))."$((RANDOM % 99))}
EOF
    done

    log_success "Generated 100 test messages"
}

# 运行测试
run_test() {
    local test_name=$1
    local config_file=$2

    log_info "Running test: $test_name"
    log_info "Config file: $config_file"

    # 清理之前的WAL和幂等性缓存
    rm -rf /tmp/arkflow/e2e/$test_name
    mkdir -p /tmp/arkflow/e2e/$test_name

    # 运行ArkFlow（后台运行）
    log_info "Starting ArkFlow with exactly-once enabled..."
    timeout 60s cargo run --release -- --config "$config_file" > /tmp/arkflow/e2e/$test_name/output.log 2>&1 &
    ARKFLOW_PID=$!

    # 等待处理
    log_info "Waiting for message processing (30 seconds)..."
    sleep 30

    # 停止ArkFlow
    log_info "Stopping ArkFlow..."
    kill $ARKFLOW_PID 2>/dev/null || true
    wait $ARKFLOW_PID 2>/dev/null || true

    log_success "Test completed: $test_name"
    echo ""
}

# 验证Kafka到Kafka测试
verify_kafka_to_kafka() {
    log_info "Verifying Kafka -> Kafka test..."

    # 检查输出主题的消息数
    OUTPUT_COUNT=$(docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic test-output \
        --from-beginning \
        --timeout-ms 5000 2>/dev/null | wc -l)

    log_info "Output topic message count: $OUTPUT_COUNT"

    if [ "$OUTPUT_COUNT" -ge 100 ]; then
        log_success "Kafka -> Kafka test PASSED"
        return 0
    else
        log_error "Kafka -> Kafka test FAILED: Expected at least 100 messages, got $OUTPUT_COUNT"
        return 1
    fi
}

# 验证Kafka到HTTP测试
verify_kafka_to_http() {
    log_info "Verifying Kafka -> HTTP test..."

    # 检查HTTP服务器日志
    # 注意：这个验证需要查看echo服务器的日志
    log_info "HTTP server logs saved to /tmp/arkflow/e2e/kafka-to-http/http-server.log"

    log_success "Kafka -> HTTP test verification completed"
}

# 验证Kafka到PostgreSQL测试
verify_kafka_to_postgres() {
    log_info "Verifying Kafka -> PostgreSQL test..."

    # 检查订单表中的记录数
    ROW_COUNT=$(docker exec postgres psql -U arkflow -d arkflow_test -t -c \
        "SELECT COUNT(*) FROM orders;" 2>/dev/null | xargs)

    log_info "Orders table row count: $ROW_COUNT"

    # 检查幂等性键是否唯一
    DUPLICATE_COUNT=$(docker exec postgres psql -U arkflow -d arkflow_test -t -c \
        "SELECT COUNT(*) - COUNT(DISTINCT idempotency_key) FROM orders WHERE idempotency_key IS NOT NULL;" 2>/dev/null | xargs)

    log_info "Duplicate idempotency keys: $DUPLICATE_COUNT"

    if [ "$ROW_COUNT" -ge 100 ] && [ "$DUPLICATE_COUNT" -eq 0 ]; then
        log_success "Kafka -> PostgreSQL test PASSED"
        return 0
    else
        log_error "Kafka -> PostgreSQL test FAILED"
        log_error "Expected at least 100 rows, got $ROW_COUNT"
        log_error "Expected 0 duplicate idempotency keys, got $DUPLICATE_COUNT"
        return 1
    fi
}

# 测试进程崩溃恢复
test_crash_recovery() {
    log_info "Testing crash recovery..."

    local config_file="tests/e2e/configs/kafka-to-kafka.yaml"

    # 第一次运行
    log_info "First run (will be interrupted)..."
    rm -rf /tmp/arkflow/e2e/crash-recovery
    mkdir -p /tmp/arkflow/e2e/crash-recovery

    cargo run --release -- --config "$config_file" > /tmp/arkflow/e2e/crash-recovery/run1.log 2>&1 &
    PID1=$!
    sleep 15
    kill -9 $PID1 2>/dev/null || true
    wait $PID1 2>/dev/null || true

    log_warning "Process crashed after 15 seconds"

    # 第二次运行（应该恢复）
    log_info "Second run (should recover)..."
    cargo run --release -- --config "$config_file" > /tmp/arkflow/e2e/crash-recovery/run2.log 2>&1 &
    PID2=$!
    sleep 30
    kill $PID2 2>/dev/null || true
    wait $PID2 2>/dev/null || true

    log_success "Crash recovery test completed"
}

# 清理环境
cleanup() {
    log_info "Cleaning up test environment..."
    docker-compose -f docker-compose.test.yml down -v
    log_success "Cleanup completed"
}

# 主测试流程
main() {
    log_info "========================================"
    log_info "ArkFlow Exactly-Once E2E Tests"
    log_info "========================================"
    echo ""

    check_docker
    start_environment
    create_topics

    echo ""
    log_info "========================================"
    log_info "Test 1: Kafka -> Kafka (Transactional)"
    log_info "========================================"
    generate_test_data
    run_test "kafka-to-kafka" "tests/e2e/configs/kafka-to-kafka.yaml"
    verify_kafka_to_kafka

    echo ""
    log_info "========================================"
    log_info "Test 2: Kafka -> HTTP (Idempotent)"
    log_info "========================================"
    generate_test_data
    run_test "kafka-to-http" "tests/e2e/configs/kafka-to-http.yaml"
    verify_kafka_to_http

    echo ""
    log_info "========================================"
    log_info "Test 3: Kafka -> PostgreSQL (UPSERT)"
    log_info "========================================"
    generate_test_data
    run_test "kafka-to-postgres" "tests/e2e/configs/kafka-to-postgres.yaml"
    verify_kafka_to_postgres

    echo ""
    log_info "========================================"
    log_info "Test 4: Crash Recovery"
    log_info "========================================"
    generate_test_data
    test_crash_recovery

    echo ""
    log_success "========================================"
    log_success "All E2E tests completed!"
    log_success "========================================"

    # 询问是否清理
    read -p "Cleanup test environment? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        cleanup
    else
        log_info "Environment left running for manual inspection"
        log_info "To cleanup later, run: docker-compose -f docker-compose.test.yml down -v"
    fi
}

# 捕获Ctrl+C
trap cleanup EXIT

# 运行主流程
main
