# Tasks: add-control-plane-rate-limiting

## 1. 限速器内核

- [x] 1.1 `crates/arkflow-server/src/api/rate_limit.rs`：令牌桶（惰性补给 + f64 tokens + Instant 注入点供确定性测试）、`Mutex<HashMap<Key, Bucket>>`、`MAX_BUCKETS=1024` 有界 + catch-all 桶 + 请求路径惰性回收空闲桶；key 为凭证的 SHA-256 截断 hex（`sha2` workspace 依赖已就位），不落原文
- [x] 1.2 单测：桶补给边界（burst 内全放行 / 超速拒）、跨凭证隔离、agent 双通道同桶（Bearer header vs legacy query token）、桶表上限触发 catch-all、空闲桶回收、`enabled=false` 零拒绝

## 2. 配置与接线

- [x] 2.1 `ServerConfig` 增 `rate_limit: RateLimitConfig`（serde default 双向兼容；本地 YAML 键为 `health_check.rate_limit`，因 control_api flatten 并入 health_check）；既有配置无该节解析不变的单测
- [x] 2.2 本地面接线：`router()` 版本化前缀内挂限速层（认证内侧），键 = `bearer()` 输出哈希、无凭证 `anonymous`
- [x] 2.3 Hub 面接线：operator 认证层内侧挂限速层，按路径前缀切换键空间（`/agent/` → node 桶，其余 → principal 桶）；豁免清单（健康探针/metrics/OIDC 登录）不经限速器
- [x] 2.4 429 响应：problem+json `rate_limited` + `Retry-After`（向上取整秒）；集成测试断言 429 形状且 handler 未执行、Retry-After 为正整数

## 3. 指标

- [x] 3.1 限速器持 `AtomicU64`（plane × key_kind：principal/node/catch_all），`metrics.rs` 新 family `arkflow_control_plane_rate_limit_rejections_total`，低基数封闭标签
- [x] 3.2 测试：拒绝后 `/metrics` 出口计数增长且标签不含凭证派生值

## 4. 端到端验证

- [x] 4.1 `cargo test -p arkflow-server` 全绿 + `cargo clippy --workspace --all-targets` 无新增告警
- [x] 4.2 默认限值下合法负载无感验证：模拟默认 1s 轮询节奏的 agent 序列（register/heartbeat/report/commands/observations）零拒绝的集成测试

## 5. 文档

- [x] 5.1 `docs/docs/control-plane/http-api-v1.md`：429 语义 + Retry-After + 限速配置节（en）
- [x] 5.2 zh-Hans 对应树同步
- [x] 5.3 `pnpm docs:check` 绿；CHANGELOG 增条目
