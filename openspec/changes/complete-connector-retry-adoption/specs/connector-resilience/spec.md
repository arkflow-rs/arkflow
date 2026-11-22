## MODIFIED Requirements

### Requirement: HTTP 组件的暂态失败重试 SHALL 经单一共享实现且有界退避

对 HTTP 类组件（qdrant/milvus/http/influxdb output、pulsar input、AI 处理器及后续采用者）的暂态失败重试 SHALL 经由 plugin crate 的单一共享重试实现（`RetryUtils`）执行，指数退避 MUST 以 `max_delay_ms` 封顶且幂运算 MUST 使用 checked/saturating 运算——任意用户可配置的重试次数 MUST NOT 产生算术溢出、panic 或回绕；可重试判定 MUST 遵守：HTTP 429、5xx 与传输/连接错误为暂态（重试），其余 4xx 为确定性（单次失败不重试）。非 HTTP 的连接器内重试/退避（mqtt output 的懒重连、kafka input 的 tombstone 结算与 restore seek）SHALL 同经共享实现的调度（整体包裹或保循环形态复用 `advance_backoff`/`sleep_duration`），MUST NOT 手写延迟公式。禁止在组件内手写与共享实现同语义的重试循环。

#### Scenario: 任意大的 retry_count 不溢出
- **WHEN** 组件配置 `retry_count: 64`（或更大）且目标服务持续 5xx
- **THEN** 重试按配置次数执行，每次延迟封顶在 `max_delay_ms`（默认 5s），全程无 panic、无回绕

#### Scenario: 小次数退避间隔与既有场景一致
- **WHEN** 组件配置 `retry_count: 2` 且第二次尝试即成功
- **THEN** 两次尝试之间恰有一次约 100ms 的指数退避等待，与既有「瞬时失败按退避重试」场景兼容

#### Scenario: 4xx 确定性失败不重试
- **WHEN** HTTP 组件收到 400/401/404 类响应
- **THEN** 立即失败返回，不发起重试

#### Scenario: 429 按暂态重试
- **WHEN** HTTP 组件收到 429 响应
- **THEN** 按共享退避策略重试，耗尽后才失败

#### Scenario: pow 写法变体不可绕过钉扎
- **WHEN** 任一 output 或 input 源码出现 `2u64.pow`、`2_u64.pow`、`2u64.saturating_pow`、`checked_pow`、`wrapping_pow` 等任意写法变体的幂退避计算
- **THEN** 结构性卫生测试失败并列出位置（数字字面量下划线归一后按 `.pow(` 族形态匹配）

#### Scenario: 采用方文件必须引用共享实现
- **WHEN** 检查共享重试采用方清单（qdrant/milvus/http/influxdb/mqtt output、kafka/pulsar input、AI 四处理器）的源码
- **THEN** 每个文件的 production 段含 `crate::retry` 引用；清单外文件由 pow 族检测与既有错误变体钉扎覆盖——通用手写循环检测被 D8 明确否决（轮询/ticker 误报不可接受），不在此场景承诺范围内

#### Scenario: 长驻循环经共享调度保取消安全
- **WHEN** mqtt output 懒重连、kafka input tombstone 结算或 restore seek 执行重试等待
- **THEN** 延迟来自共享 `advance_backoff`/`sleep_duration` 调度，既有取消检查与错误文案保持不变
