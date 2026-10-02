# Capability: NATS 输入输出

## Purpose

NATS input/output 组件契约：普通核心模式与 JetStream 模式的收发语义、鉴权配置、subject 表达式，以及依赖升级（async-nats 0.46 → 0.50）引入的客户端侧校验（subject 合法性、max-payload 预校验）作为错误路径的边界。TLS 栈升级（rustls-webpki 0.102 → 0.103）保持连接能力且不引入新的原生构建依赖。

## Requirements

### Requirement: NATS 输入输出基础契约

NATS 插件 SHALL 支持普通核心模式（订阅/发布 subject）与 JetStream 模式（pull consumer 拉取 / JetStream 发布），连接 SHALL 支持用户名密码、token 鉴权配置（可选）。subject SHALL 支持按消息求值的表达式（输出侧逐行求值发布）。依赖升级（async-nats 0.46 → 0.50）SHALL 保持这两种模式的收发语义与鉴权行为不变。

#### Scenario: 普通模式收发闭环

- **WHEN** 以普通模式配置 NATS 输入订阅一个 subject、NATS 输出发布到同一 subject 并运行流
- **THEN** 发布的消息被输入侧完整接收（payload 与 header 不变），连接使用配置的鉴权（如提供）

#### Scenario: JetStream 模式收发闭环

- **WHEN** 以 JetStream 模式配置输出发布、输入以 pull consumer 绑定对应 stream 与 durable consumer
- **THEN** 消息经 JetStream 持久化后被输入侧按序拉取接收，ack 语义与升级前一致

### Requirement: 客户端侧校验作为错误路径

async-nats 0.47+ 引入的客户端校验 SHALL 作为既有错误路径暴露，不引入 panic 或新错误类型：非法 subject（0.47 起 publish/subscribe 前校验）与超过服务端 max-payload 的发布负载（0.49 起客户端预校验）SHALL 使对应消息处理失败并进入该插件的错误处理路径（错误日志/批错误计数），SHALL NOT 中断进程或使输入输出连接永久失效。

#### Scenario: 非法 subject 提前报错

- **WHEN** 输出侧 subject 表达式求值出非法 subject（如含空格）并发布
- **THEN** 该消息发布失败并进入插件错误路径（记录错误），进程继续运行，后续合法 subject 的发布不受影响

#### Scenario: 超限负载客户端侧失败

- **WHEN** 输出侧发布负载超过服务端公布的 max-payload
- **THEN** 发布在客户端侧失败并进入插件错误路径，错误信息可辨识（不依赖服务端回包）

### Requirement: TLS 栈升级保持连接能力

async-nats 升级引入的 TLS 栈变化（rustls-webpki 0.102 → 0.103、rustls-native-certs 0.7 → 0.8）SHALL 保持既有 TLS 连接能力：证书加载路径与升级前等价，不引入新的构建依赖（TLS provider 保持 `ring`，SHALL NOT 引入 aws-lc-rs / fips 的 C 构建链）。图中的 `rustls-webpki` 0.102 拷贝（NATS 侧，GHSA-82j2-j2ch-gfr8 / GHSA-xgp8-3hg3-c2mh / GHSA-965h-392x-2mh5）SHALL 随升级消失（rumqttc 残留拷贝除外，属另一依赖的上游阻塞项）。

#### Scenario: webpki 收敛到修复线

- **WHEN** 升级落地后检查依赖图
- **THEN** `rustls-webpki` 仅剩 0.103.x（修复线，经 async-nats 与 rustls）与 rumqttc 钉住的 0.102.x 两类拷贝，async-nats 路径不再出现 0.102.x

#### Scenario: 构建不引入新的原生依赖链

- **WHEN** 对比升级前后的锁文件差异
- **THEN** 本次变更不新增任何原生构建依赖包（async-nats 保持默认 `ring` feature；图中预存的 `aws-lc-rs` 来自 rustls/AWS 栈 feature 合一，与本变更无关且数量不变）
