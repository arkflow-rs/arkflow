-- 创建测试表用于exactly-once验证

-- 订单表 - 测试UPSERT和幂等性
CREATE TABLE IF NOT EXISTS orders (
    id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    idempotency_key VARCHAR(100) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_idempotency_key ON orders(idempotency_key);

-- 创建更新触发器
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON orders
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 事件表 - 测试事务完整性
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    event_data JSONB NOT NULL,
    idempotency_key VARCHAR(100) UNIQUE,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_idempotency_key ON events(idempotency_key);

-- 插入一些测试数据
INSERT INTO orders (id, customer_id, product_id, quantity, price, idempotency_key) VALUES
('order-001', 'customer-1', 'product-1', 2, 99.99, 'test-key-001')
ON CONFLICT (idempotency_key) DO NOTHING;

-- 授权
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO arkflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO arkflow;
