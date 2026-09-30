## MODIFIED Requirements

### Requirement: 控制面租约 SHALL 以持久库 CAS 语义选出唯一 leader

持久存储 SHALL 提供单例控制面租约（固定主键行），语义为：`try_acquire` 仅在租约过期或持有者即自己时成功并递增单调 fencing epoch；`renew` 仅在持有者匹配且未过期时延长 TTL；`release` 使持有者的租约立即过期且不改变 epoch。SQLite 与 PostgreSQL 后端 SHALL 以同一契约实现这三个操作，空库启动时幂等建表收敛。

存储 SHALL 暴露当前租约 epoch（`current_lease_epoch`，无租约行返回空）。变更类存储命令 SHALL 携带发送时刻的声明 epoch，并在**执行前**与当前租约 epoch 比对：无租约行（未启用 HA）SHALL 直通；声明值与当前值一致 SHALL 执行；不一致 SHALL 以显式的 stale-leader 错误拒绝且不执行。读命令与租约三操作（acquire/renew/release）SHALL 豁免比对。失去领导（含 standby）的进程声明 epoch 不再与租约一致，其变更写 SHALL 被拒绝。

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

#### Scenario: 旧 leader 的写在接管后被拒绝

- **WHEN** holder A（声明 epoch N）因停顿错过 TTL，standby B 接管使租约 epoch 递增至 N+1，A 在感知失去领导之前发起变更写（携带声明 epoch N）
- **THEN** 存储层拒绝该写并返回显式 stale-leader 错误（含声明值与当前值），对应命令不产生任何持久化副作用；B（声明 N+1）的同类写正常执行

#### Scenario: 无租约行时变更写直通

- **WHEN** 部署未启用 HA（租约表无行），进程以任意声明值发起变更写
- **THEN** 写正常执行，行为与未引入围栏前逐位一致

#### Scenario: standby 与租约操作豁免

- **WHEN** standby（从未取得租约，声明 epoch 为 0）在有租约行存在时发起变更写，或任意进程执行租约 acquire/renew/release 与读命令
- **THEN** standby 的变更写被拒绝（声明 0 ≠ 当前值）；租约操作与读不受围栏影响，select/renew/release 语义保持
