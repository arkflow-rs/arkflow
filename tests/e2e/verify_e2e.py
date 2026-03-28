#!/usr/bin/env python3
"""
端到端测试验证脚本
用于验证ArkFlow exactly-once功能的端到端测试
"""

import subprocess
import time
import json
import psycopg2
from kafka import KafkaConsumer, KafkaProducer
import requests
import sys

# 颜色输出
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'

def log_info(msg):
    print(f"{Colors.BLUE}[INFO]{Colors.END} {msg}")

def log_success(msg):
    print(f"{Colors.GREEN}[SUCCESS]{Colors.END} {msg}")

def log_error(msg):
    print(f"{Colors.RED}[ERROR]{Colors.END} {msg}")

def log_warning(msg):
    print(f"{Colors.YELLOW}[WARNING]{Colors.END} {msg}")

class E2ETestVerifier:
    def __init__(self):
        self.kafka_brokers = ['localhost:9092']
        self.postgres_conn = None
        self.http_url = 'http://localhost:8080'

    def connect_postgres(self):
        """连接PostgreSQL数据库"""
        try:
            self.postgres_conn = psycopg2.connect(
                host='localhost',
                port=5432,
                database='arkflow_test',
                user='arkflow',
                password='arkflow123'
            )
            log_success("Connected to PostgreSQL")
            return True
        except Exception as e:
            log_error(f"Failed to connect to PostgreSQL: {e}")
            return False

    def close_postgres(self):
        """关闭PostgreSQL连接"""
        if self.postgres_conn:
            self.postgres_conn.close()

    def verify_kafka_to_kafka(self):
        """验证Kafka到Kafka的事务性"""
        log_info("Verifying Kafka -> Kafka (transactional)...")

        try:
            # 消费输出主题
            consumer = KafkaConsumer(
                'test-output',
                bootstrap_servers=self.kafka_brokers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='verification-consumer',
                consumer_timeout_ms=10000
            )

            messages = []
            for message in consumer:
                try:
                    data = json.loads(message.value.decode('utf-8'))
                    messages.append(data)
                except:
                    pass

            consumer.close()

            log_info(f"Consumed {len(messages)} messages from output topic")

            if len(messages) >= 100:
                # 验证消息完整性
                unique_ids = set()
                for msg in messages:
                    if 'id' in msg:
                        unique_ids.add(msg['id'])

                log_info(f"Unique message IDs: {len(unique_ids)}")

                if len(unique_ids) >= 100:
                    log_success("Kafka -> Kafka test PASSED ✓")
                    log_success(f"  - Total messages: {len(messages)}")
                    log_success(f"  - Unique messages: {len(unique_ids)}")
                    log_success(f"  - No duplicates detected")
                    return True
                else:
                    log_error(f"Expected 100 unique messages, got {len(unique_ids)}")
                    return False
            else:
                log_error(f"Expected at least 100 messages, got {len(messages)}")
                return False

        except Exception as e:
            log_error(f"Kafka -> Kafka verification failed: {e}")
            return False

    def verify_kafka_to_postgres(self):
        """验证Kafka到PostgreSQL的UPSERT幂等性"""
        log_info("Verifying Kafka -> PostgreSQL (UPSERT idempotent)...")

        if not self.connect_postgres():
            return False

        try:
            cursor = self.postgres_conn.cursor()

            # 查询总记录数
            cursor.execute("SELECT COUNT(*) FROM orders WHERE id LIKE 'order-%'")
            total_count = cursor.fetchone()[0]
            log_info(f"Total orders in database: {total_count}")

            # 查询唯一幂等性键数量
            cursor.execute("""
                SELECT COUNT(DISTINCT idempotency_key)
                FROM orders
                WHERE idempotency_key LIKE 'idempotency-order-%'
            """)
            unique_keys = cursor.fetchone()[0]
            log_info(f"Unique idempotency keys: {unique_keys}")

            # 检查重复的幂等性键
            cursor.execute("""
                SELECT idempotency_key, COUNT(*) as cnt
                FROM orders
                WHERE idempotency_key IS NOT NULL
                GROUP BY idempotency_key
                HAVING COUNT(*) > 1
            """)
            duplicates = cursor.fetchall()

            if len(duplicates) > 0:
                log_error(f"Found {len(duplicates)} duplicate idempotency keys!")
                for dup in duplicates[:5]:
                    log_error(f"  - Key {dup[0]}: {dup[1]} occurrences")
                return False
            else:
                log_success("No duplicate idempotency keys found ✓")

            # 验证数据完整性
            cursor.execute("""
                SELECT COUNT(*)
                FROM orders
                WHERE id LIKE 'order-%'
                AND customer_id IS NOT NULL
                AND product_id IS NOT NULL
                AND quantity > 0
                AND price > 0
            """)
            valid_records = cursor.fetchone()[0]

            log_info(f"Valid records: {valid_records}/{total_count}")

            if total_count >= 100 and valid_records == total_count and unique_keys == total_count:
                log_success("Kafka -> PostgreSQL test PASSED ✓")
                log_success(f"  - Total records: {total_count}")
                log_success(f"  - Valid records: {valid_records}")
                log_success(f"  - Unique idempotency keys: {unique_keys}")
                log_success(f"  - Zero duplicates")
                return True
            else:
                log_error("Kafka -> PostgreSQL test FAILED")
                return False

        except Exception as e:
            log_error(f"PostgreSQL verification failed: {e}")
            return False
        finally:
            self.close_postgres()

    def generate_test_data(self, count=100):
        """生成测试数据到Kafka输入主题"""
        log_info(f"Generating {count} test messages...")

        try:
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_brokers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )

            for i in range(1, count + 1):
                data = {
                    'id': f'order-{i}',
                    'customer_id': f'customer-{i % 10}',
                    'product_id': f'product-{i % 20}',
                    'quantity': i % 5 + 1,
                    'price': i * 10 + 99.99
                }
                producer.send('test-input', value=data)

            producer.flush()
            producer.close()

            log_success(f"Generated {count} test messages")
            return True

        except Exception as e:
            log_error(f"Failed to generate test data: {e}")
            return False

    def run_all_tests(self):
        """运行所有验证测试"""
        log_info("=" * 60)
        log_info("ArkFlow Exactly-Once E2E Verification")
        log_info("=" * 60)
        print()

        results = {}

        # 生成测试数据
        self.generate_test_data(100)
        time.sleep(2)

        # 测试1: Kafka -> Kafka
        print()
        log_info("Test 1: Kafka -> Kafka (Transactional)")
        print("-" * 60)
        results['kafka_to_kafka'] = self.verify_kafka_to_kafka()
        print()

        # 测试2: Kafka -> PostgreSQL
        log_info("Test 2: Kafka -> PostgreSQL (UPSERT)")
        print("-" * 60)
        results['kafka_to_postgres'] = self.verify_kafka_to_postgres()
        print()

        # 汇总结果
        log_info("=" * 60)
        log_info("Test Results Summary")
        log_info("=" * 60)

        for test_name, passed in results.items():
            status = f"{Colors.GREEN}PASSED{Colors.END}" if passed else f"{Colors.RED}FAILED{Colors.END}"
            print(f"  {test_name}: {status}")

        print()

        all_passed = all(results.values())
        if all_passed:
            log_success("All tests PASSED! ✓")
            return 0
        else:
            log_error("Some tests FAILED!")
            return 1

if __name__ == '__main__':
    verifier = E2ETestVerifier()
    sys.exit(verifier.run_all_tests())
