#!/usr/bin/env python3
"""
测试数据生成工具
用于生成端到端测试的测试数据
"""

import json
import sys
import time
from kafka import KafkaProducer
import argparse

def generate_order_data(count):
    """生成订单测试数据"""
    orders = []
    for i in range(1, count + 1):
        order = {
            'id': f'order-{i}',
            'customer_id': f'customer-{i % 10}',
            'product_id': f'product-{i % 20}',
            'quantity': i % 5 + 1,
            'price': i * 10 + 99.99,
            'timestamp': int(time.time() * 1000)
        }
        orders.append(order)
    return orders

def generate_event_data(count):
    """生成事件测试数据"""
    events = []
    event_types = ['user_login', 'user_logout', 'page_view', 'click', 'purchase']

    for i in range(1, count + 1):
        event = {
            'id': f'event-{i}',
            'event_type': event_types[i % len(event_types)],
            'user_id': f'user-{i % 50}',
            'data': {
                'page': f'/page-{i % 100}',
                'action': f'action-{i % 20}'
            },
            'timestamp': int(time.time() * 1000)
        }
        events.append(event)
    return events

def send_to_kafka(bootstrap_servers, topic, data, batch_size=10):
    """发送数据到Kafka"""
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )

    total = len(data)
    sent = 0

    print(f"Sending {total} messages to topic '{topic}'...")

    for i, record in enumerate(data):
        producer.send(topic, value=record)
        sent += 1

        if (i + 1) % batch_size == 0:
            producer.flush()
            print(f"  Progress: {sent}/{total} ({sent*100//total}%)")

    producer.flush()
    producer.close()

    print(f"✅ Successfully sent {sent} messages")

def main():
    parser = argparse.ArgumentParser(description='Generate test data for ArkFlow E2E tests')
    parser.add_argument('--type', choices=['order', 'event'], default='order',
                       help='Type of data to generate')
    parser.add_argument('--count', type=int, default=100,
                       help='Number of records to generate')
    parser.add_argument('--brokers', default='localhost:9092',
                       help='Kafka brokers (comma-separated)')
    parser.add_argument('--topic', default='test-input',
                       help='Kafka topic')
    parser.add_argument('--output', help='Output file (instead of sending to Kafka)')
    parser.add_argument('--batch-size', type=int, default=10,
                       help='Batch size for sending')

    args = parser.parse_args()

    # 生成数据
    if args.type == 'order':
        data = generate_order_data(args.count)
    else:
        data = generate_event_data(args.count)

    print(f"Generated {len(data)} {args.type} records")

    # 输出数据
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"✅ Data saved to {args.output}")
    else:
        brokers = args.brokers.split(',')
        send_to_kafka(brokers, args.topic, data, args.batch_size)

if __name__ == '__main__':
    main()
