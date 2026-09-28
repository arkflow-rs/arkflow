# hub-ha Delta

## ADDED Requirements

### Requirement: 控制面租约 SHALL 以持久库 CAS 语义选出唯一 leader

持久存储 SHALL 提供单例控制面租约（固定主键行），语义为：`try_acquire` 仅在租约过期或持有者即自己时成功并递增单调 fencing epoch；`renew` 仅在持有者匹配且未过期时延长 TTL；`release` 使持有者的租约立即过期且不改变 epoch。SQLite 与 PostgreSQL 后端 SHALL 以同一契约实现这三个操作，空库启动时幂等建表收敛。

#### Scenario: 过期租约被 standby 接管

- **WHEN** 租约行已过期（expires_at_ms <= now）且 standby 以自己的 holder id 调用 `try_acquire`
- **THEN** 操作成功、epoch 比此前值大 1、持有者变更为该 standby、TTL 从此刻重新起算

#### Scenario: 未过期租约拒绝接管

- **WHEN** 租约由其他 holder 持有且未过期，任一实例调用 `try_acquire`
- **THEN** 操作不改变租约行，返回当前持有者与到期时间，调用方保持 standby

#### Scenario: 持有者续约成功与失效后续约失败

- **WHEN** 持有者在租约未过期时调用 `renew`，随后租约过期后再调用 `renew`
- **THEN** 第一次续约延长 TTL 且 epoch 不变；第二次返回 Lost 且不修改租约行

#### Scenario: 同一 holder 重复 acquire 幂等

- **WHEN** 当前持有者对自己调用 `try_acquire`
- **THEN** 操作按续约处理（TTL 重算，epoch 不再递增），不产生额外的 epoch 跳变

### Requirement: 非 leader Hub SHALL 以 standby 模式运行

启用 HA 的 Hub SHALL 在未持有租约时进入 standby：不执行 stale 扫描、作业调和、rollout 调解与保留清理等周期任务；除健康检查（health/liveness/readiness）与 metrics 导出外的所有 operator 与 agent 路由 SHALL 返回 503 与明确的 standby 错误码；readiness SHALL 报告未就绪并携带角色信息。未启用 HA 的 Hub SHALL 不经过任何租约与领导态检查，行为与现状逐位一致。

#### Scenario: standby 拒绝写请求且不产生副作用

- **WHEN** standby Hub 收到创建 Job 的 POST 请求
- **THEN** 返回 503 与 `hub_standby` 错误码，持久库中不出现新的 Job 记录

#### Scenario: standby 拒绝 agent 注册

- **WHEN** Agent 向 standby Hub 发送注册请求
- **THEN** 返回 503，不创建节点会话与凭据，Agent 按既有重连路径继续重试

#### Scenario: standby 不跑周期调和

- **WHEN** Hub 处于 standby 且经过多个调和周期
- **THEN** 不产生任何作业操作派发、命令下发或保留清理

#### Scenario: 健康与观测端点在 standby 保持可用

- **WHEN** standby Hub 收到 health、liveness、readiness 或 metrics 请求
- **THEN** 请求被正常处理，readiness 报告未就绪且标注角色为 standby

#### Scenario: 未启用 HA 时行为不变

- **WHEN** Hub 以默认配置（ha 未启用）启动并服务
- **THEN** 不读写租约表、无 standby 门控、路由与周期任务行为与启用本能力前一致

### Requirement: 晋升 SHALL 先完成持久态重恢复再服务

standby 取得租约后 SHALL 在开放服务前：清空并重载持久化控制面视图（作业、版本、checkpoint 记录、操作、rollout），恢复失败 SHALL 释放租约并保持 standby（fail-closed），不得以半恢复状态对外服务。节点注册表与放置序 SHALL 在晋升时清空，由 Agent 重新注册重建。

#### Scenario: 晋升重恢复后开放服务

- **WHEN** standby 取得租约且持久态重载成功
- **THEN** leadership 变为 leader、readiness 就绪、恢复出的 Job 出现在列表 API 中

#### Scenario: 晋升时覆盖陈旧内存态

- **WHEN** 一个曾让位的 Hub 再次晋升，其内存中残留上一任期的新于持久库的条目
- **THEN** 晋升后控制面视图与持久库一致，陈旧内存条目不残留

#### Scenario: 恢复失败保持 standby

- **WHEN** 晋升过程中持久态恢复返回错误
- **THEN** 租约被释放、leadership 保持 standby、readiness 保持未就绪

### Requirement: leader SHALL 周期续约并在丢失时让位

leader SHALL 以不超过 TTL/3 的周期续约；续约返回 Lost 或存储不可达时 SHALL 立即让位为 standby（周期任务在下一 tick 停摆、readiness 转未就绪、路由回到 503 门控）。整个故障接管窗口 SHALL 有界于租约 TTL 加一个探测周期。leader 优雅关停时 SHALL 尽力主动释放租约，使 standby 无需等待 TTL 过期即可接管。

#### Scenario: 续约丢失立即让位

- **WHEN** leader 的续约返回 Lost（例如另一实例在 TTL 过期后接管）
- **THEN** 该实例转为 standby，停止周期调和，readiness 报告未就绪

#### Scenario: leader 进程死亡后 standby 有界接管

- **WHEN** leader 进程异常退出且未释放租约
- **THEN** standby 在租约 TTL 过期后的首个尝试周期内取得租约并完成晋升

#### Scenario: 优雅关停主动释放

- **WHEN** leader 收到关停信号正常退出
- **THEN** 租约被立即释放，standby 的下一次 `try_acquire` 即可成功

### Requirement: 启用 HA 的 Hub SHALL 要求持久存储

`ha.enabled=true` 的启动 SHALL 要求配置了持久存储（任意后端）；缺存储时启动 SHALL 在绑定监听前失败。生产多实例部署 SHALL 使用 PostgreSQL 后端；SQLite 后端的租约契约仅用于开发与测试，多实例 SQLite 部署不被支持且启动时 SHALL 记录警告。

#### Scenario: 启用 HA 但缺存储拒绝启动

- **WHEN** `ha.enabled=true` 且未配置持久存储
- **THEN** 启动在监听绑定前失败并说明需要配置存储

### Requirement: 领导态 SHALL 可观测

Hub SHALL 在系统信息端点暴露当前领导角色、fencing epoch 与角色转换计数，并在角色转换时发出事件与日志；readiness 响应体 SHALL 携带 HA 角色。

#### Scenario: 角色转换反映到系统信息与事件流

- **WHEN** Hub 从 standby 晋升为 leader
- **THEN** 系统信息端点报告角色为 leader、epoch 等于取得的租约 epoch、转换计数递增，且事件流出现 `hub.leadership` 转换记录
