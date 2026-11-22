# Capability: 连接器弹性

## Purpose

组件级弹性纪律：HTTP/发布类组件的暂态失败重试 SHALL 经 plugin crate 的单一共享重试实现（封顶退避 + 可重试分类），AI 处理器对暂态 HTTP 失败提供有界重试，组件内 delivery 通道容量与内核对齐。禁止组件内手写与共享实现同语义的重试循环（结构性卫生测试钉扎）。(Purpose derived from the `consolidate-connector-skeleton` change.)

## Requirements

### Requirement: HTTP 组件的暂态失败重试 SHALL 经单一共享实现且有界退避

对 HTTP 类组件（qdrant/milvus output、pulsar input、AI 处理器及后续采用者）的暂态失败重试 SHALL 经由 plugin crate 的单一共享重试实现（`RetryUtils`）执行，指数退避 MUST 以 `max_delay_ms` 封顶且幂运算 MUST 使用 checked/saturating 运算——任意用户可配置的重试次数 MUST NOT 产生算术溢出、panic 或回绕；可重试判定 MUST 遵守：HTTP 429、5xx 与传输/连接错误为暂态（重试），其余 4xx 为确定性（单次失败不重试）。禁止在组件内手写与共享实现同语义的重试循环。

#### Scenario: 任意大的 retry_count 不溢出
- **WHEN** 组件配置 `retry_count: 64`（或更大）且目标服务持续 5xx
- **THEN** 重试按配置次数执行，每次延迟封顶在 `max_delay_ms`（默认 5s），全程无 panic、无回绕

#### Scenario: 小次数退避间隔与既有场景一致
- **WHEN** 组件配置 `retry_count: 2` 且第二次尝试即成功
- **THEN** 两次尝试的间隔按指数退避（约 100ms、200ms），与既有「瞬时失败按退避重试」场景兼容

#### Scenario: 4xx 确定性失败不重试
- **WHEN** HTTP 组件收到 400/401/404 类响应
- **THEN** 立即失败返回，不发起重试

#### Scenario: 429 按暂态重试
- **WHEN** HTTP 组件收到 429 响应
- **THEN** 按共享退避策略重试，耗尽后才失败

#### Scenario: 手写重试循环被钉扎禁止
- **WHEN** 任一 output 源码出现裸 `2u64.pow` 式退避计算或与共享实现同语义的手写重试循环
- **THEN** 结构性卫生测试失败并列出位置

### Requirement: AI 处理器 SHALL 对暂态 HTTP 失败提供有界重试

`embedding`、`llm`、`vector_search`、`milvus_search` 四个处理器 SHALL 接受可选 `retry_count` 配置（默认 3，0=关闭）：HTTP 429/5xx/传输错误按共享退避重试；确定性失败（4xx、向量维度不一致、列类型错误）MUST 立即失败且行为与无重试时一致；重试耗尽后的错误透传语义（状态码 + 响应体片段）MUST 与既有场景保持一致。

#### Scenario: embedding 遇 429 重试后成功
- **WHEN** embedding API 对首批请求返回 429、后续请求成功且默认配置（retry_count 未设）
- **THEN** processor 重试并成功产出 embedding 列，最终输出与一次性成功等价

#### Scenario: embedding 确定性失败立即返回
- **WHEN** embedding API 返回 401（认证失败）
- **THEN** processor 立即报错（状态码 + 响应体片段），不发起重试

#### Scenario: retry_count 为 0 关闭重试
- **WHEN** 配置 `retry_count: 0` 且 API 返回 503
- **THEN** 行为与无重试实现一致：单次失败即返回错误

#### Scenario: 重试耗尽后透传语义不变
- **WHEN** API 持续返回 503 且重试次数耗尽
- **THEN** 返回的错误包含状态码与响应体片段，batch 走既有错误语义（如 embedding 的 error_output 透传）

### Requirement: 组件内 delivery 通道容量 SHALL 与内核对齐

input 组件内部的 delivery 通道（`flume::bounded`）SHALL 使用与内核边通道一致的容量常量 1024（内核 `DEFAULT_CHANNEL_CAPACITY`），MUST NOT 使用独立魔数（如 1000）；共享常量 MUST 注释指向内核常量，并以卫生测试禁止回漂。

#### Scenario: 现存 1000 魔数全部收敛
- **WHEN** 扫描 `crates/arkflow-plugin/src/input/` 的全部 `flume::bounded` 调用
- **THEN** 容量均来自共享常量 1024，无 `bounded(1000)` 字面量

#### Scenario: 魔数回漂被拦截
- **WHEN** 新代码在 input 组件内引入 `bounded(1000)` 或其他裸容量魔数
- **THEN** 卫生测试失败并列出文件与行
